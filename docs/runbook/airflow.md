# 런북: Airflow 일별 오케스트레이션 + 장애 2모드 시연

`activity_daily` DAG를 venv `airflow standalone`으로 구동해 Bronze→Silver→Gold→export→build 일별 흐름과
장애 복구 2모드를 시연하는, 실제 실행 절차다. 근거: 설계 스펙 §2·§3.7·§6·§12. 파이프라인은 Scala, 오케스트레이션은 Python(언어 경계).

> 데모는 **SQLite + SequentialExecutor**(공식 개발용). 프로덕션은 LocalExecutor+Postgres → 부하 시 Celery/Kubernetes로 확장.

## 0. 전제
- thin jar 빌드됨: `sbt package` → `target/scala-2.13/activity-log_2.13-0.1.0.jar` (DailySplitter 클래스를 포함하도록 Phase 0 머지 후 **재빌드** 필요 — `unzip -l <jar> | grep DailySplitter`로 확인)
- 대시보드 venv 준비됨(Phase 2 런북 §1): `dashboard/.venv` + `output/gold` 산출 경험.
- Airflow venv 준비됨(Task 2): `airflow/.venv`.
- 기존 코어 backfill로 `output/activity`(62 파티션)·`output/gold`가 이미 있을 수 있다. 데모는 해당 날짜를 **멱등 overwrite**로 재처리하므로 사전 삭제 불필요(설계 결정 6).

## 1. Bronze 일별 분할(DailySplitter, 1회)
```bash
spark-submit \
  --class com.activitylog.DailySplitter --master "local[*]" --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --input "$(pwd)/data/2019-Oct.csv,$(pwd)/data/2019-Nov.csv" \
  --output "$(pwd)/data/daily"
ls -d data/daily/event_date=* | wc -l   # 62 (2019-10-01 ~ 2019-12-01 KST)
```
- 월 CSV 약 13.7GB라 수 분 소요(전체 분할은 1회면 충분 — 이후 데모는 이 산출물 재사용).

## 2. activity External Table 1회 등록(메타스토어)
DAG의 repair_partition(MSCK)은 테이블이 이미 등록돼 있어야 동작한다. repo 루트에서 1회 실행해 Derby 메타스토어에 등록한다(이후 DAG도 repo 루트에서 실행 → 같은 metastore_db 공유). 이미 코어 단계에서 등록했다면(`spark-sql -e "SHOW TABLES;"` 로 `activity` 확인) 생략 가능.
```bash
sed "s|{{OUTPUT_DIR}}|$(pwd)/output/activity|" sql/create_external_table.sql > /tmp/ddl.sql
spark-sql --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -f /tmp/ddl.sql
```

## 3. Airflow 기동(standalone)
```bash
export AIRFLOW_HOME=$(pwd)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export ACTIVITY_HOME=$(pwd)
export PATH="$(pwd)/airflow/.venv/bin:$PATH"        # standalone은 scheduler/webserver/triggerer를 'airflow'로 스폰 → venv가 PATH에 있어야 함
export PYTHONPATH="$(pwd)/airflow/shims:$PYTHONPATH" # macOS setproctitle fork 크래시 회피(no-op 셰임으로 그림자 처리, 아래 설명)
export no_proxy="*"                                 # macOS fork 안전성 표준 가드(이 크래시엔 단독으론 불충분, 함께 권장)
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES      # macOS fork 안전성 표준 가드(Obj-C 초기화 크래시)
airflow standalone   # 웹+스케줄러+admin 자동. 콘솔에 admin 비번 출력(+ .airflow/standalone_admin_password.txt) → http://localhost:8080
```
- `FileNotFoundError: [Errno 2] No such file or directory: 'airflow'`로 scheduler/webserver가 죽으면 → 위 `export PATH` 누락이다(전체 경로로만 실행 시 자식 프로세스가 `airflow`를 못 찾음). PATH 추가 후 재기동.
- 자식(scheduler/webserver의 gunicorn 워커)이 `SIGSEGV`(`crashed on child side of fork`, 스택에 `setproctitle`·`CFBundleGetFunctionPointerForName`)로 죽고 macOS가 "Python 응용 프로그램이 예기치 않게 종료" 대화상자를 반복하면 → 신버전 macOS에서 `setproctitle`이 fork 자식에서 CoreFoundation(LaunchServices)을 호출하다 segfault하는 이슈다. **해결 = `export PYTHONPATH="$(pwd)/airflow/shims:$PYTHONPATH"`**(no-op 셰임 `airflow/shims/setproctitle.py`가 컴파일 패키지를 그림자 처리 — 제목 설정만 무력화, 기능 무영향). `no_proxy`/`OBJC_…`만으론 이 크래시는 안 잡힌다(검증함). backfill 등 단발 CLI에도 같은 env를 export해 두면 안전.
- 데모는 소형 구간만 본다. 전체(62일)는 동일 DAG의 start/end 확장 + 더 긴 구동일 뿐이다.

## 4. 소형 대표 구간 백필(2019-10-01~10-07)
다른 터미널에서(같은 env export 후):
```bash
# end만 좁혀 7일치 run만 생성. (또는 UI에서 backfill 트리거)
airflow/.venv/bin/airflow dags backfill activity_daily -s 2019-10-01 -e 2019-10-07
```
UI Graph/Grid에서 날짜별 run이 silver→…→build로 초록 채워지는 모습을 확인·캡처한다(유동적 일별 흐름).

## 5. 장애 2모드 시연

> **Discord 알림 볼륨 주의**: on_success는 task 단위라 정상 백필 7일이면 6×7=42건이 발송된다. 알림 시연은 **장애 데모 구간에만** 웹훅을 설정하는 것을 권장한다(아래 (a)/(b) 직전 set, 직후 unset). 미설정 시 조용히 skip되어 DAG는 정상 동작한다.

> **@daily 단일일 backfill 주의**: 논리날짜 D의 run은 데이터 구간 `[D, D+1)`을 담당하므로, backfill의 `-e`는 **D 다음 날**이어야 그 run이 포함된다(`-s D -e D`는 폭 0 구간이라 `No run dates`로 아무 run도 안 생긴다). 또 §4에서 이미 돌린 날짜를 다시 쓰려면 `tasks clear` 선행이 필요하니, 데모는 **아직 안 돌린 날짜**(예: 10-08, 10-09)를 쓰는 게 간단하다.

### (a) 자동 복구 — 일시적 오류 → 자동 retry → 성공
```bash
# (선택) 알림을 보려면 이 구간에만 웹훅 설정
airflow/.venv/bin/airflow variables set discord_webhook_url "<DISCORD_WEBHOOK_URL>"

airflow/.venv/bin/airflow variables set demo_fail_date 2019-10-08   # 해당일 silver 첫 시도 강제 실패
airflow/.venv/bin/airflow dags backfill activity_daily -s 2019-10-08 -e 2019-10-09   # -e는 대상일+1
```
- 2019-10-08 silver: try 1 실패(🔄 on_retry Discord) → 20초 뒤 try 2 성공(✅ on_success). UI에서 silver가 up_for_retry→success로 바뀌는 흐름 캡처(노랑은 짧으니 silver `Logs`의 `[DEMO] injected transient fault ... (try 1)` + try 2 성공 로그가 확실한 증거).
- 시연 후 토글 해제: `airflow/.venv/bin/airflow variables set demo_fail_date ""`

### (b) 수동 복구 — 데이터 품질 게이트 실패 → 알림 → 멱등 재처리
검증 게이트(`PartitionWriter.validate`: 행수>0 **그리고** `user_id`·`event_time_utc` not null)를 silver에서 트립하려면,
해당일 입력에 **키가 null인 행**을 둔다. (빈 입력은 트립하지 않는다 — Main은 데이터에 실재하는 날짜만 파티션 루프하므로
빈 입력은 silver가 `partitions=0`으로 정상 종료하고 `_SUCCESS`만 안 생겨 **gate 타임아웃**으로 실패한다. 아래는 silver를 직접 실패시키는 경로.)
```bash
# 데모일 입력을 user_id null 행으로 교체 → silver 검증 게이트(null 키) 발동.
#   event_time이 2019-10-08 16:00 UTC = KST 2019-10-09라 run-date 2019-10-09 필터에 잡힌다.
mv data/daily/event_date=2019-10-09 /tmp/backup_1009
mkdir -p data/daily/event_date=2019-10-09
printf 'event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n2019-10-08 16:00:00 UTC,view,1001,2001,c,b,10.0,,s1\n' \
  > data/daily/event_date=2019-10-09/bad.csv
airflow/.venv/bin/airflow dags backfill activity_daily -s 2019-10-09 -e 2019-10-10   # -e는 대상일+1
```
- silver가 `validation failed for 2019-10-09: null key rows=1`로 retry 1회 소진 후 최종 실패(🚨 on_failure Discord + 로그 링크). UI에서 빨강 확인·캡처.
- 복구: 원본 입력 되돌리고 Clear(멱등 overwrite로 안전 재처리):
```bash
rm -rf data/daily/event_date=2019-10-09 && mv /tmp/backup_1009 data/daily/event_date=2019-10-09
airflow/.venv/bin/airflow tasks clear activity_daily -s 2019-10-09 -e 2019-10-10 -y
```
- 재실행이 silver→…→build 초록(✅ on_success)으로 복구되는 모습 캡처.

## 6. 시연 자료
캡처한 스크린샷(일별 catchup 그래프·(a) retry 복구·(b) 게이트 실패 후 복구·Discord 알림)을 README의 Airflow 절에 첨부한다.

## 트러블슈팅
- `MSCK REPAIR` 에러(table not found): §2 등록을 repo 루트에서 했는지 확인(메타스토어 공유).
- gate 타임아웃: silver가 `_SUCCESS`를 못 썼다는 뜻 → silver 로그 확인(입력 폴더 존재 여부).
- Discord 알림 안 옴: `airflow variables set discord_webhook_url <URL>` 또는 env `DISCORD_WEBHOOK_URL` 설정(미설정 시 조용히 skip).
- backfill이 무겁다(각 일별 run이 Main + GoldMarts 전체 재계산): 데모는 7일로 제한, 전체 62일은 시간 확보 후 동일 명령의 `-e`만 확장.
