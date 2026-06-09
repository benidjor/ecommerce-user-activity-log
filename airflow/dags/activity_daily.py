# activity_daily: Bronze→Silver→Gold→DuckDB export→정적 빌드를 @daily catchup으로 실행하는 DAG.
#   각 단계는 BashOperator로 터미널/런북과 동일한 spark-submit/venv python 명령을 실행(라인 설명 용이).
#   장애 시연 (a) 자동 복구: 데모 토글 Variable demo_fail_date가 특정 날짜의 silver를
#   첫 시도(try_number==1)에만 강제 실패시켜 자동 retry→성공을 보여준다(핵심 Scala 코드는 무수정).
import os
import sys

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

# callbacks 패키지 import 경로(airflow/ 디렉터리)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from callbacks.discord import notify_retry, notify_failure, notify_success  # noqa: E402

# 프로젝트 루트(절대경로). env ACTIVITY_HOME로 주입, 기본은 이 파일 기준 2단계 상위(repo 루트).
HOME = os.environ.get(
    "ACTIVITY_HOME",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
)
JAR = f"{HOME}/target/scala-2.13/activity-log_2.13-0.1.0.jar"
# spark-submit 공통 add-opens(JDK 17+ 리플렉션 허용). gold는 date collect용 sun.util.calendar까지 필요.
OPENS = ("--add-opens=java.base/java.nio=ALL-UNNAMED "
         "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
OPENS_GOLD = OPENS + " --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

default_args = {
    "retries": 1,                              # 일시적 장애 1회 자동 재시도(시연 (a)의 토대)
    "retry_delay": pendulum.duration(seconds=20),
    "on_retry_callback": notify_retry,
    "on_failure_callback": notify_failure,
    # on_success는 task 단위(데모용) — 단계별 초록 흐름을 보이려는 의도.
    #   backfill 시 task수×일수만큼 성공 알림이 나가므로(예: 6×7=42건) webhook은
    #   장애 데모 구간에만 설정하는 운영을 권장(런북 §5 참고). 미설정 시 조용히 skip.
    "on_success_callback": notify_success,
}

# 데모 토글(a): demo_fail_date와 ds가 같고 첫 시도면 강제 종료 → retry로 복구.
#   Variable 미설정 시 빈 문자열이라 평소엔 발동 안 함. 핵심 변환 코드 밖(이 bash 가드)에만 존재.
DEMO_FAIL_GUARD = (
    'if [ "{{ ds }}" = "{{ var.value.get(\'demo_fail_date\', \'\') }}" ] '
    '&& [ "{{ ti.try_number }}" = "1" ]; then '
    'echo "[DEMO] injected transient fault for {{ ds }} (try 1)"; exit 1; fi'
)

# Silver: incremental 적재(run-date {{ds}}). lookback(전날 폴더)이 있으면 input에 덧붙여 세션 연속성 확보.
SILVER_CMD = f"""
cd {HOME}
{DEMO_FAIL_GUARD}
DAY="{HOME}/data/daily/event_date={{{{ ds }}}}/"
PREV="{HOME}/data/daily/event_date={{{{ prev_ds }}}}/"
IN="$DAY"
[ -d "$PREV" ] && IN="$DAY,$PREV"
spark-submit \\
  --class com.activitylog.Main --master "local[*]" --driver-memory 4g \\
  --conf spark.sql.session.timeZone=UTC \\
  --conf spark.driver.extraJavaOptions="{OPENS}" \\
  {JAR} \\
  --mode incremental --run-date "{{{{ ds }}}}" \\
  --input "$IN" --output "{HOME}/output/activity"
"""

# repair_partition: Silver가 쓴 새 파티션을 activity External Table 메타스토어에 인식시킴(MSCK).
#   activity 테이블은 런북 셋업에서 1회 CREATE EXTERNAL TABLE 등록 전제(스펙 §12.2).
#   MSCK는 전체 파티션을 스캔 — 62일 데모엔 무해하나, 파티션 다수 환경에선
#   날짜별 ALTER TABLE ... ADD PARTITION으로 대체하는 게 더 가볍다(프로덕션 확장 노트).
REPAIR_CMD = f"""
cd {HOME}
spark-sql \\
  --conf spark.sql.session.timeZone=UTC \\
  --conf spark.driver.extraJavaOptions="{OPENS}" \\
  -e "MSCK REPAIR TABLE activity;"
"""

# Gold: activity에서 마트 전체 재계산 후 overwrite(멱등). WAU distinct 고카디널리티라 8g.
GOLD_CMD = f"""
cd {HOME}
spark-submit \\
  --class com.activitylog.GoldMarts --master "local[*]" --driver-memory 8g \\
  --conf spark.sql.session.timeZone=UTC \\
  --conf spark.driver.extraJavaOptions="{OPENS_GOLD}" \\
  {JAR} \\
  --sql-dir sql/gold --output "{HOME}/output/gold"
"""

with DAG(
    dag_id="activity_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2019, 10, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2019, 12, 1, tz="Asia/Seoul"),  # 데이터 창에 가둠(폭주 방지)
    catchup=True,                                              # catchup이 backfill 겸함
    default_args=default_args,
    max_active_runs=1,                                         # 순차(SequentialExecutor 정합)
    tags=["medallion", "demo"],
) as dag:

    silver = BashOperator(task_id="silver", bash_command=SILVER_CMD)

    repair_partition = BashOperator(task_id="repair_partition", bash_command=REPAIR_CMD)

    # gate: 해당 날짜 파티션의 _SUCCESS 마커가 있어야 통과(완료 감지). fs_default는 절대경로 사용.
    #   gate는 silver 직후라 _SUCCESS가 즉시 있는 게 정상 — timeout(120s)은 마커가
    #   안 생기는 이상 징후(silver가 쓰기 실패)를 빨리 드러내려는 짧은 상한.
    gate = FileSensor(
        task_id="gate",
        fs_conn_id="fs_default",
        filepath=f"{HOME}/output/activity/event_date={{{{ ds }}}}/_SUCCESS",
        poke_interval=10,
        timeout=120,
        mode="poke",
    )

    gold = BashOperator(task_id="gold", bash_command=GOLD_CMD)

    export = BashOperator(
        task_id="export",
        bash_command=f"cd {HOME} && dashboard/.venv/bin/python dashboard/export_duckdb.py",
    )

    build = BashOperator(
        task_id="build",
        bash_command=f"cd {HOME} && dashboard/.venv/bin/python dashboard/build.py",
    )

    silver >> repair_partition >> gate >> gold >> export >> build
