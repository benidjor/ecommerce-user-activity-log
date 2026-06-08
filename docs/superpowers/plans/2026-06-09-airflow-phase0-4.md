# Phase 0 + Phase 4 (DailySplitter + Airflow + Discord) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 월 CSV를 KST 일별 Bronze로 분할(DailySplitter)하고, Airflow DAG `activity_daily`가 Bronze→Silver→Gold→DuckDB export→정적 빌드를 `@daily catchup`으로 매일 실행하며 장애 2모드(자동 retry 복구·검증 게이트 실패 후 멱등 복구)와 Discord 알람을 시연한다.

**Architecture:** DailySplitter는 기존 `Schema.Raw`·`TimeUtils`를 재사용하는 얇은 Scala 잡. Airflow는 venv `airflow standalone`(SQLite+SequentialExecutor)로 구동하고, 각 단계는 **BashOperator + spark-submit/venv python**으로 터미널 명령을 그대로 실행한다. Discord 콜백은 stdlib `urllib`로 웹훅에 POST(외부 의존성 0). 모든 수치·시연은 실제 구동 결과로만 보고한다.

**Tech Stack:** Scala 2.13 / Spark 4.1.2 / sbt / ScalaTest(기존), apache-airflow 2.10.5 + pendulum(신규, venv), Python 3.11, pytest.

**PR 전략(스택 금지·각 Phase main에서 분기):**
- 본 브랜치 `docs/airflow-phase0-4-design` = **문서 PR**(스펙 §12 + 본 계획서).
- **Phase 0 PR** = Task 1(DailySplitter). 갱신된 main에서 분기.
- **Phase 4 PR** = Task 2~6. Phase 0 머지 후 갱신된 main에서 분기.

---

## File Structure

| 파일 | 책임 | 생성/수정 |
|---|---|---|
| `src/main/scala/com/activitylog/DailySplitter.scala` | 월 CSV → KST `event_date` partitionBy → `data/daily/` (Phase 0) | 생성 |
| `src/test/scala/com/activitylog/DailySplitterSpec.scala` | KST 자정 경계 분할 단위 테스트 | 생성 |
| `.gitignore` | Airflow 런타임 산출물(`.airflow/`) 무시 | 수정 |
| `airflow/requirements.txt` | apache-airflow 2.10.5 + constraint 안내 | 생성 |
| `airflow/callbacks/discord.py` | on_retry/on_failure/on_success → Discord 웹훅(stdlib urllib) | 생성 |
| `airflow/callbacks/__init__.py` | 패키지 마커 | 생성 |
| `airflow/tests/test_discord.py` | `format_message` 순수 함수 단위 테스트 | 생성 |
| `airflow/dags/activity_daily.py` | DAG(task silver→repair→gate→gold→export→build + 콜백 + 데모 토글) | 생성 |
| `airflow/tests/test_dag.py` | DagBag import·구조·의존 그래프 테스트 | 생성 |
| `docs/runbook/airflow.md` | venv·standalone 셋업 + 백필 구동 + 장애 2모드 재현 절차 | 생성 |
| `README.md` | 사용 도구·언어 경계·복구 장치·장애 지형도·Airflow 한계 갱신 | 수정 |

---

# Phase 0 — DailySplitter (별도 PR)

### Task 1: DailySplitter (월 CSV → KST 일별 Bronze)

**Files:**
- Create: `src/main/scala/com/activitylog/DailySplitter.scala`
- Test: `src/test/scala/com/activitylog/DailySplitterSpec.scala`

스펙 §3.1. 기존 `Schema.Raw`(9개 원본 컬럼)·`TimeUtils.withKstColumns`(KST `event_date` 파생)를 재사용한다(타임존 로직 중복 금지). 출력은 `event_date`로 `partitionBy`한 CSV(헤더 포함) — Silver가 `data/daily/event_date={{ds}}/`를 `Schema.Raw`로 다시 읽는다.

- [ ] **Step 1: 실패 테스트 작성**

`src/test/scala/com/activitylog/DailySplitterSpec.scala`:

```scala
package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// DailySplitterSpec: UTC 자정 경계 행이 올바른 KST event_date 폴더로 분할되는지 검증.
//   TimeUtilsSpec와 같은 경계 케이스(2019-10-31 16:00 UTC = 2019-11-01 KST)를 파일 분할 레벨에서 확인.
class DailySplitterSpec extends AnyFunSuite with SparkTestBase {
  test("rows are partitioned into KST event_date folders at the UTC midnight boundary") {
    val ss = spark
    import ss.implicits._

    // 임시 입력/출력 디렉터리(테스트 종료 후 OS가 정리)
    val tmp    = java.nio.file.Files.createTempDirectory("dsplit").toString
    val inDir  = s"$tmp/in"
    val outDir = s"$tmp/out"

    // Schema.Raw 9개 컬럼 CSV(헤더 포함) 작성.
    // 2019-10-01 00:00:00 UTC → KST 2019-10-01, 2019-10-31 16:00:00 UTC → KST 2019-11-01
    val rows = Seq(
      ("2019-10-01 00:00:00 UTC", "view", 1L, 10L, "c", "b", 1.0, 100L, "s1"),
      ("2019-10-31 16:00:00 UTC", "view", 2L, 20L, "c", "b", 2.0, 200L, "s2")
    ).toDF("event_time", "event_type", "product_id", "category_id",
           "category_code", "brand", "price", "user_id", "user_session")
    rows.write.option("header", "true").mode("overwrite").csv(inDir)

    DailySplitter.split(ss, Seq(inDir), outDir)

    // 출력 파티션 폴더명만 추려 정렬(event_date=... 디렉터리)
    val parts = new java.io.File(outDir).listFiles()
      .filter(_.isDirectory).map(_.getName)
      .filter(_.startsWith("event_date=")).sorted.toList

    assert(parts == List("event_date=2019-10-01", "event_date=2019-11-01"))
  }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `sbt "testOnly com.activitylog.DailySplitterSpec"`
Expected: FAIL — `DailySplitter` 미작성으로 컴파일 에러(`not found: value DailySplitter`).

- [ ] **Step 3: 최소 구현 작성**

`src/main/scala/com/activitylog/DailySplitter.scala`:

```scala
package com.activitylog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// DailySplitter: 월 CSV를 읽어 KST event_date 기준으로 partitionBy 하여
//   data/daily/event_date=YYYY-MM-DD/ 로 분할 저장한다(Bronze 일별 랜딩, 스펙 §3.1).
//   Airflow 일별 run이 자기 날짜 폴더(event_date={{ds}})만 읽도록 사전 분할하는 용도.
object DailySplitter {

  // arg: CLI 인자 배열에서 키 다음 값을 Option으로 꺼낸다(Main.scala와 동일 관용구).
  private def arg(a: Array[String], k: String): Option[String] = {
    val i = a.indexOf(k)
    if (i >= 0 && i + 1 < a.length) Some(a(i + 1)) else None
  }

  // split: raw CSV → KST event_date 파생 → 원본 9개 컬럼 + event_date 선택 → event_date로 partitionBy 기록.
  //   파생 KST 컬럼(event_time_utc/kst)은 저장하지 않는다 — Bronze는 원본에 가깝게 두고,
  //   Silver(Main)가 Schema.Raw로 다시 읽어 자체적으로 KST를 재파생한다.
  def split(spark: SparkSession, inputs: Seq[String], output: String): Unit = {
    // Schema.Raw로 명시 스키마 강제(타입 추론 끔, Main과 동일).
    val raw = spark.read.option("header", "true").schema(Schema.Raw).csv(inputs: _*)
    // 원본 컬럼명 목록(9개) + event_date를 선택. fieldNames는 Schema.Raw 정의 순서.
    val originalCols = Schema.Raw.fieldNames.toSeq.map(col)
    val withDate = TimeUtils.withKstColumns(raw).select((originalCols :+ col("event_date")): _*)
    // partitionBy("event_date"): 경로에 event_date=YYYY-MM-DD/로 인코딩(파일엔 9개 컬럼만 남음).
    //   mode("overwrite"): 같은 출력 경로 재실행 시 깨끗이 덮어씀(데모 준비 멱등).
    withDate.write
      .partitionBy("event_date")
      .option("header", "true")
      .mode("overwrite")
      .csv(output)
  }

  // main: spark-submit 진입점.
  //   사용: --class com.activitylog.DailySplitter --input <월CSV,...> --output data/daily
  def main(args: Array[String]): Unit = {
    val inputs = arg(args, "--input").getOrElse(sys.error("--input required")).split(",").toSeq
    val output = arg(args, "--output").getOrElse(sys.error("--output required"))
    val spark  = SparkSessionFactory.create("daily-splitter")
    try split(spark, inputs, output) finally spark.stop()
  }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `sbt "testOnly com.activitylog.DailySplitterSpec"`
Expected: PASS (1 succeeded).

- [ ] **Step 5: 전체 테스트 회귀 확인**

Run: `sbt test`
Expected: 기존 스펙 전부 + DailySplitterSpec PASS(실패 0).

- [ ] **Step 6: 커밋**

```bash
git add src/main/scala/com/activitylog/DailySplitter.scala src/test/scala/com/activitylog/DailySplitterSpec.scala
git commit -m "feat(bronze): DailySplitter — 월 CSV를 KST event_date 일별 폴더로 분할"
```

---

# Phase 4 — Airflow DAG + Discord (별도 PR, Phase 0 머지 후 main에서 분기)

### Task 2: Airflow 환경 셋업 + requirements + gitignore

**Files:**
- Create: `airflow/requirements.txt`
- Modify: `.gitignore`

Airflow 런타임 산출물은 `AIRFLOW_HOME=$(pwd)/.airflow`에 격리(gitignore)하고, DAG/콜백 코드(`airflow/dags`, `airflow/callbacks`)만 추적한다. `apache-airflow`는 반드시 **공식 constraint 파일**과 함께 설치한다(의존성 충돌 방지).

- [ ] **Step 1: requirements 작성**

`airflow/requirements.txt`:

```text
# Airflow 오케스트레이션 레이어 전용(파이프라인 본체 Scala와 분리 — Spark Application 아님).
# 반드시 공식 constraint와 함께 설치(아래 런북 참고). Discord 알림은 stdlib urllib라 추가 패키지 없음.
apache-airflow==2.10.5
```

- [ ] **Step 2: gitignore에 Airflow 런타임 산출물 추가**

`.gitignore`의 "Spark / Hive 로컬 산출물" 블록 아래에 추가(기존 줄은 건드리지 않음):

```text

# === Airflow 런타임(AIRFLOW_HOME) — 코드(airflow/dags·callbacks)만 추적, 런타임 산출물 무시 ===
.airflow/
airflow/.venv/
```

- [ ] **Step 3: venv 생성 + 설치(공식 constraint)**

Run:

```bash
python3.11 -m venv airflow/.venv
airflow/.venv/bin/pip install --upgrade pip
airflow/.venv/bin/pip install "apache-airflow==2.10.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
airflow/.venv/bin/pip install pytest
```

Expected: 설치 성공. `airflow/.venv/bin/airflow version` → `2.10.5`.

- [ ] **Step 4: standalone 부팅 스모크(수동, 1회)**

Run:

```bash
export AIRFLOW_HOME=$(pwd)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow/.venv/bin/airflow db migrate
airflow/.venv/bin/airflow version
```

Expected: `db migrate` 성공(`.airflow/airflow.db` 생성), `version` → `2.10.5`. (웹서버는 Task 5 시연에서 기동)

- [ ] **Step 5: 커밋**

```bash
git add airflow/requirements.txt .gitignore
git commit -m "chore(airflow): venv requirements(2.10.5+constraint)와 런타임 gitignore 추가"
```

---

### Task 3: Discord 콜백 모듈

**Files:**
- Create: `airflow/callbacks/__init__.py`, `airflow/callbacks/discord.py`
- Test: `airflow/tests/test_discord.py`

콜백의 순수 로직(`format_message`)을 Airflow·네트워크와 분리해 단위 테스트한다. 전송은 stdlib `urllib`로 웹훅 POST(추가 의존성 0). 웹훅 URL은 Airflow Variable `discord_webhook_url`(없으면 env `DISCORD_WEBHOOK_URL`)에서 읽고, 미설정 시 조용히 skip(웹훅 없는 환경에서 DAG가 깨지지 않게).

- [ ] **Step 1: 실패 테스트 작성**

`airflow/tests/test_discord.py`:

```python
# format_message 순수 함수 단위 테스트(네트워크·Airflow 불필요).
import os, sys

# airflow/ 를 import 경로에 추가(callbacks 패키지 로드)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from callbacks.discord import format_message


def test_success_message_has_emoji_status_and_identity():
    msg = format_message(
        emoji="✅", status="SUCCESS",
        dag_id="activity_daily", task_id="silver",
        ds="2019-10-01", try_number=1, log_url="http://x/log",
    )
    assert msg.startswith("✅ [SUCCESS]")
    assert "dag=activity_daily" in msg
    assert "task=silver" in msg
    assert "run=2019-10-01" in msg
    # 성공 메시지엔 로그 링크를 붙이지 않는다.
    assert "http://x/log" not in msg


def test_failed_message_includes_log_url():
    msg = format_message(
        emoji="🚨", status="FAILED",
        dag_id="activity_daily", task_id="silver",
        ds="2019-10-01", try_number=2, log_url="http://x/log",
    )
    assert msg.startswith("🚨 [FAILED]")
    assert "try=2" in msg
    # 실패 메시지엔 디버깅용 로그 링크 포함.
    assert "http://x/log" in msg
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `airflow/.venv/bin/pytest airflow/tests/test_discord.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'callbacks'`(모듈 미작성).

- [ ] **Step 3: 최소 구현 작성**

`airflow/callbacks/__init__.py`:

```python
# callbacks 패키지 마커(빈 파일).
```

`airflow/callbacks/discord.py`:

```python
# discord: Airflow task 콜백 → Discord 웹훅 알림.
#   format_message(순수 함수, 테스트 대상)로 문구를 만들고, _send(stdlib urllib)로 POST.
#   on_retry / on_failure / on_success 세 콜백을 default_args에 연결해 쓴다.
import json
import os
import urllib.request

from airflow.models import Variable


# format_message: 알림 문구 생성(네트워크·Airflow 상태 불필요 → 단위 테스트 가능).
#   status=FAILED일 때만 디버깅용 로그 링크를 덧붙인다.
def format_message(emoji, status, dag_id, task_id, ds, try_number, log_url):
    msg = (f"{emoji} [{status}] dag={dag_id} task={task_id} "
           f"run={ds} try={try_number}")
    if status == "FAILED":
        msg += f"\nlog: {log_url}"
    return msg


# _webhook_url: Airflow Variable 우선, 없으면 환경변수. 둘 다 없으면 None(전송 skip).
#   웹훅 URL은 시크릿이므로 커밋하지 않는다(Variable/env로만 주입).
def _webhook_url():
    url = Variable.get("discord_webhook_url", default_var=None)
    return url or os.getenv("DISCORD_WEBHOOK_URL")


# _send: context에서 식별자를 뽑아 문구를 만들고 웹훅에 POST. 웹훅 미설정 시 조용히 반환.
def _send(emoji, status, context):
    url = _webhook_url()
    if not url:
        return  # 웹훅 미설정 환경(테스트·웹훅 없는 데모)에서 DAG가 깨지지 않도록 skip
    ti = context["task_instance"]
    msg = format_message(
        emoji=emoji, status=status,
        dag_id=ti.dag_id, task_id=ti.task_id,
        ds=context["ds"], try_number=ti.try_number, log_url=ti.log_url,
    )
    data = json.dumps({"content": msg}).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10)  # 실패하면 콜백이 예외 → 로그에 남음


# default_args의 on_*_callback에 그대로 연결할 3개 콜백.
def notify_retry(context):
    _send("🔄", "RETRY", context)


def notify_failure(context):
    _send("🚨", "FAILED", context)


def notify_success(context):
    _send("✅", "SUCCESS", context)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `airflow/.venv/bin/pytest airflow/tests/test_discord.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: 커밋**

```bash
git add airflow/callbacks/__init__.py airflow/callbacks/discord.py airflow/tests/test_discord.py
git commit -m "feat(airflow): Discord 콜백(on_retry/failure/success)과 format_message 단위 테스트"
```

---

### Task 4: DAG `activity_daily`

**Files:**
- Create: `airflow/dags/activity_daily.py`
- Test: `airflow/tests/test_dag.py`

순차 체인 `silver → repair_partition → gate → gold → export → build` + retry + `@daily catchup`(start 2019-10-01, end 2019-12-01) + Discord 콜백 + **격리된 데모 토글(a)**. 경로는 env `ACTIVITY_HOME`(기본=repo 절대경로)로 주입. 데모 토글은 핵심 변환 코드(Scala) 밖, DAG의 silver bash 명령에만 둔다(스펙 P4-5).

- [ ] **Step 1: 실패 테스트 작성**

`airflow/tests/test_dag.py`:

```python
# DAG import 무오류 + task 구성 + 순차 의존 그래프 검증(DagBag, DB·네트워크 불필요).
import os, sys

# DagBag 로드 전 AIRFLOW_HOME/예제 비활성 설정
os.environ.setdefault("AIRFLOW_HOME", os.path.join(os.getcwd(), ".airflow"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from airflow.models import DagBag

DAG_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")


def _dag():
    bag = DagBag(dag_folder=DAG_DIR, include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("activity_daily")
    assert dag is not None
    return dag


def test_tasks_present():
    dag = _dag()
    assert set(dag.task_ids) == {
        "silver", "repair_partition", "gate", "gold", "export", "build"}


def test_linear_dependency_chain():
    dag = _dag()
    assert dag.get_task("repair_partition").upstream_task_ids == {"silver"}
    assert dag.get_task("gate").upstream_task_ids == {"repair_partition"}
    assert dag.get_task("gold").upstream_task_ids == {"gate"}
    assert dag.get_task("export").upstream_task_ids == {"gold"}
    assert dag.get_task("build").upstream_task_ids == {"export"}


def test_catchup_and_bounds():
    dag = _dag()
    assert dag.catchup is True
    assert dag.start_date.strftime("%Y-%m-%d") == "2019-10-01"
    assert dag.end_date.strftime("%Y-%m-%d") == "2019-12-01"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `airflow/.venv/bin/pytest airflow/tests/test_dag.py -v`
Expected: FAIL — `bag.get_dag("activity_daily")`가 None(DAG 미작성)으로 assert 실패.

- [ ] **Step 3: 최소 구현 작성**

`airflow/dags/activity_daily.py`:

```python
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
spark-submit \
  --class com.activitylog.Main --master "local[*]" --driver-memory 4g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="{OPENS}" \
  {JAR} \
  --mode incremental --run-date "{{{{ ds }}}}" \
  --input "$IN" --output "{HOME}/output/activity"
"""

# repair_partition: Silver가 쓴 새 파티션을 activity External Table 메타스토어에 인식시킴(MSCK).
#   activity 테이블은 런북 셋업에서 1회 CREATE EXTERNAL TABLE 등록 전제(스펙 §12.2).
REPAIR_CMD = f"""
cd {HOME}
spark-sql \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="{OPENS}" \
  -e "MSCK REPAIR TABLE activity;"
"""

# Gold: activity에서 마트 전체 재계산 후 overwrite(멱등). WAU distinct 고카디널리티라 8g.
GOLD_CMD = f"""
cd {HOME}
spark-submit \
  --class com.activitylog.GoldMarts --master "local[*]" --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="{OPENS_GOLD}" \
  {JAR} \
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
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `airflow/.venv/bin/pytest airflow/tests/test_dag.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: 전체 airflow 테스트 회귀**

Run: `airflow/.venv/bin/pytest airflow/tests -v`
Expected: discord 2 + dag 3 = 5 passed.

- [ ] **Step 6: 커밋**

```bash
git add airflow/dags/activity_daily.py airflow/tests/test_dag.py
git commit -m "feat(airflow): activity_daily DAG(silver→repair→gate→gold→export→build)+데모 토글"
```

---

### Task 5: 런북 — 셋업 + 백필 + 장애 2모드 시연

**Files:**
- Create: `docs/runbook/airflow.md`

실제로 구동해 본 절차만 적는다(수치·동작은 실제 실행 결과). 시연 자료(스크린샷)는 실제 구동 후 캡처한다.

- [ ] **Step 1: 런북 작성**

`docs/runbook/airflow.md`:

````markdown
# 런북: Airflow 일별 오케스트레이션 + 장애 2모드 시연

`activity_daily` DAG를 venv `airflow standalone`으로 구동해 Bronze→Silver→Gold→export→build 일별 흐름과
장애 복구 2모드를 시연하는, 실제 실행 절차다. 근거: 설계 스펙 §2·§3.7·§6·§12. 파이프라인은 Scala, 오케스트레이션은 Python(언어 경계).

> 데모는 **SQLite + SequentialExecutor**(공식 개발용). 프로덕션은 LocalExecutor+Postgres → 부하 시 Celery/Kubernetes로 확장.

## 0. 전제
- thin jar 빌드됨: `sbt package` → `target/scala-2.13/activity-log_2.13-0.1.0.jar`
- 대시보드 venv 준비됨(Phase 2 런북 §1): `dashboard/.venv` + `output/gold` 산출 경험.
- Airflow venv 준비됨(Task 2): `airflow/.venv`.

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

## 2. activity External Table 1회 등록(메타스토어)
DAG의 repair_partition(MSCK)은 테이블이 이미 등록돼 있어야 동작한다. repo 루트에서 1회 실행해 Derby 메타스토어에 등록한다(이후 DAG도 repo 루트에서 실행 → 같은 metastore_db 공유).
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
airflow/.venv/bin/airflow standalone   # 웹+스케줄러+admin 자동. 콘솔에 admin 비번 출력 → http://localhost:8080
```
- 데모는 소형 구간만 본다. 전체(62일)는 동일 DAG의 start/end 확장 + 더 긴 구동일 뿐이다.

## 4. 소형 대표 구간 백필(2019-10-01~10-07)
다른 터미널에서(같은 env export 후):
```bash
# end만 좁혀 7일치 run만 생성. (또는 UI에서 backfill 트리거)
airflow/.venv/bin/airflow dags backfill activity_daily -s 2019-10-01 -e 2019-10-07
```
UI Graph/Grid에서 날짜별 run이 silver→…→build로 초록 채워지는 모습을 확인·캡처한다(유동적 일별 흐름).

## 5. 장애 2모드 시연

### (a) 자동 복구 — 일시적 오류 → 자동 retry → 성공
```bash
airflow/.venv/bin/airflow variables set demo_fail_date 2019-10-03   # 해당일 silver 첫 시도 강제 실패
airflow/.venv/bin/airflow dags backfill activity_daily -s 2019-10-03 -e 2019-10-03
```
- 2019-10-03 silver: try 1 실패(🔄 on_retry Discord) → 20초 뒤 try 2 성공(✅ on_success). UI에서 silver가 up_for_retry→success로 바뀌는 흐름 캡처.
- 시연 후 토글 해제: `airflow/.venv/bin/airflow variables set demo_fail_date ""`

### (b) 수동 복구 — 데이터 품질 게이트 실패 → 알림 → 멱등 재처리
```bash
# 데모일 입력을 빈 폴더로 만들어 검증 게이트(행수 0) 발동
mkdir -p /tmp/empty_day && printf "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n" > /tmp/empty_day/empty.csv
mv data/daily/event_date=2019-10-04 /tmp/backup_1004
mkdir -p data/daily/event_date=2019-10-04 && cp /tmp/empty_day/empty.csv data/daily/event_date=2019-10-04/
airflow/.venv/bin/airflow dags backfill activity_daily -s 2019-10-04 -e 2019-10-04
```
- silver가 `validation failed`/`row count is 0`로 retry 소진 후 최종 실패(🚨 on_failure Discord + 로그 링크). UI에서 빨강 확인·캡처.
- 복구: 원본 입력 되돌리고 Clear(멱등 overwrite로 안전 재처리):
```bash
rm -rf data/daily/event_date=2019-10-04 && mv /tmp/backup_1004 data/daily/event_date=2019-10-04
airflow/.venv/bin/airflow tasks clear activity_daily -s 2019-10-04 -e 2019-10-04 -y
```
- 재실행이 silver→…→build 초록(✅ on_success)으로 복구되는 모습 캡처.

## 6. 시연 자료
캡처한 스크린샷(일별 catchup 그래프·(a) retry 복구·(b) 게이트 실패 후 복구·Discord 알림)을 README의 Airflow 절에 첨부한다.

## 트러블슈팅
- `MSCK REPAIR` 에러(table not found): §2 등록을 repo 루트에서 했는지 확인(메타스토어 공유).
- gate 타임아웃: silver가 `_SUCCESS`를 못 썼다는 뜻 → silver 로그 확인(입력 폴더 존재 여부).
- Discord 알림 안 옴: `airflow variables set discord_webhook_url <URL>` 또는 env `DISCORD_WEBHOOK_URL` 설정(미설정 시 조용히 skip).
````

- [ ] **Step 2: 런북 절차 실제 구동 검증(verification-before-completion)**

Run: 런북 §1~§5를 실제로 실행. 최소 확인:
- `ls -d data/daily/event_date=* | wc -l` → `62`
- §4 백필 후 UI Grid에서 7일 run 전부 success
- §5(a) 2019-10-03 silver가 up_for_retry→success
- §5(b) 2019-10-04 silver 최종 실패 → 복구 후 success

Expected: 위 동작이 실제로 관찰됨(스크린샷 캡처). 관찰되지 않으면 systematic-debugging으로 원인 수정 후 재구동.

- [ ] **Step 3: 커밋**

```bash
git add docs/runbook/airflow.md
git commit -m "docs(runbook): Airflow 셋업·백필·장애 2모드 시연 절차"
```

---

### Task 6: README 갱신 (사용 도구·언어 경계·복구 장치·Airflow 한계)

**Files:**
- Modify: `README.md`

- [ ] **Step 1: README 보강**

`README.md`에 다음을 반영(기존 구조·문체 유지, 인접 내용 임의 리팩터 금지):
- **사용 도구/언어 경계**: DailySplitter는 Scala(Spark Application). Airflow DAG·Discord 콜백은 Python(오케스트레이션 — Spark Application 아님). 대시보드 Python은 기존대로.
- **복구 장치 ↔ 장애 매핑**: 자동 retry(일시적), 검증 게이트(데이터 품질), 멱등 overwrite+staging→rename(부분 쓰기), FileSensor(지연/누락), DAG 의존(순서). OOM·스큐 등 튜닝성 장애는 장치/설명만 두고 데모는 안 함.
- **장애 2모드 시연**: (a) 자동 복구 (b) 수동 복구 — 캡처 첨부, 런북 `docs/runbook/airflow.md` 링크.
- **Airflow 한계**: SQLite+SequentialExecutor는 개발용. 프로덕션=LocalExecutor+Postgres→Celery/K8s 확장. Airflow UI는 로컬 실행 중에만 보여 README 시연 자료를 항상-확인 매체로 둠.

- [ ] **Step 2: 커밋**

```bash
git add README.md
git commit -m "docs(readme): Airflow·DailySplitter 언어 경계·복구 장치·장애 2모드 시연 반영"
```

---

## Self-Review (작성 후 점검)

**Spec coverage (스펙 §12 대비):**
- §12.0 Phase 3 보류 — 본 계획은 Phase 0+4만 다룸(정합).
- §12.1 P4-1 시연 매체 → Task 5·6(README 시연 자료). P4-2 BashOperator → Task 4. P4-3 standalone → Task 2. P4-4/P4-5 장애 2모드·토글 격리 → Task 4(토글)·Task 5(2모드). P4-6 백필 경계 → Task 4(start/end/catchup) + test_catchup_and_bounds. P4-7 데모 구간 → Task 5 §4.
- §12.2 DAG task1~5 + repair(MSCK)·Derby 공유·env → Task 4 + Task 5 §2~3. ✅
- §12.3 장애 2모드·콜백 의미 → Task 3(콜백)·Task 5(시연). ✅
- §12.4 컴포넌트 → File Structure 전부 대응. §12.5 테스트 → Task 1/3/4 테스트 + Task 5 dry-run 구동. §12.6 언어 경계 → Task 6. ✅

**Placeholder scan:** 모든 코드/명령 블록은 실제 내용. 미정 표현 없음. (constraint URL·jar 경로·클래스명은 실제 확인값.)

**Type/이름 일관성:** `format_message` 인자(emoji/status/dag_id/task_id/ds/try_number/log_url)가 테스트·구현·`_send` 호출에서 일치. DAG task_id(silver/repair_partition/gate/gold/export/build)가 구현·테스트에서 일치. `DailySplitter.split(spark, inputs, output)` 시그니처가 테스트·구현·main에서 일치.
