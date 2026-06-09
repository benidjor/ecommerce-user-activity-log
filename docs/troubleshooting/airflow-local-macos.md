# 트러블슈팅: Airflow 로컬 구동 (standalone, macOS) (Phase 4)

`activity_daily` DAG를 venv `airflow standalone`으로 라이브 구동하며 부딪힌 macOS·Airflow 이슈를 증상·원인·해결 순으로 정리함.
운영 절차는 런북 [docs/runbook/airflow.md](../runbook/airflow.md)에 있고, 이 문서는 그 과정에서 재발 가능성이 큰 함정의 진단을 담음.

> 환경: macOS 26.x (Darwin 25.5) · Python 3.11 · apache-airflow 2.10.5 (SQLite + SequentialExecutor) · Spark 4.1.2.

## 1. jar에 DailySplitter 클래스가 없음

### 증상
`spark-submit --class com.activitylog.DailySplitter ...` → `Failed to load class com.activitylog.DailySplitter`.

### 원인
`target/scala-2.13/activity-log_2.13-0.1.0.jar`가 DailySplitter 추가 (Phase 0) **이전에 빌드된 thin jar**라 클래스 미포함.

### 해결
`sbt package`로 재빌드. 확인: `unzip -l <jar> | grep DailySplitter`.

## 2. `airflow standalone`이 `FileNotFoundError: 'airflow'`로 비정상 종료

### 증상
`airflow/.venv/bin/airflow standalone` 실행 시 scheduler/webserver/triggerer 스레드가 `FileNotFoundError: [Errno 2] No such file or directory: 'airflow'`로 종료.

### 원인
`standalone`은 내부적으로 scheduler·webserver·triggerer를 **`airflow`라는 이름으로 subprocess 스폰**함.
venv를 활성화하지 않고 전체 경로 (`.venv/bin/airflow`)로만 실행하면 자식 프로세스가 PATH에서 `airflow`를 찾지 못함.

### 해결
venv bin을 PATH 앞에 추가한 뒤 실행.
```bash
export PATH="$(pwd)/airflow/.venv/bin:$PATH"
airflow standalone
```

## 3. standalone 자식이 `SIGSEGV`로 반복 크래시 (macOS fork + setproctitle)

### 증상
webserver/scheduler의 gunicorn 워커가 `[ERROR] Worker (pid:…) was sent SIGSEGV!`로 비정상 종료되고, macOS가 "Python 응용 프로그램이 예기치 않게 종료되었습니다" 대화상자를 반복 표시.
UI가 뜨지 않음. PATH (§2)는 해결한 상태.

### 원인
신버전 macOS에서 **`setproctitle` (1.3.4)의 `darwin_set_process_title`이 fork된 자식에서 CoreFoundation (LaunchServices)을 호출하다 segfault**함.
크래시 리포트: `EXC_BAD_ACCESS(SIGSEGV)`, `*** multi-threaded process forked ***` / `crashed on child side of fork pre-exec`, 스택에 `_setproctitle … darwin_set_process_title → CFBundleGetFunctionPointerForName`.
gunicorn 워커가 매 fork마다 제목을 설정하려다 비정상 종료됨.
표준 가드 (`OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`, `no_proxy="*"`)는 **이 크래시엔 불충분** (Obj-C initialize 가드가 아니라 setproctitle의 CF 호출이 문제)

### 해결
`setproctitle`을 제거할 수는 없음 (airflow가 `serve_logs`·`local_executor`·`standard_task_runner`·`dag_processing` 등에서 하드 임포트)
대신 **no-op shim으로 그림자 처리**해 제목 설정만 무력화함 (shim = 원래 패키지를 같은 이름의 빈 모듈로 가려 import는 되되 동작은 안 하게 만드는 가짜 대체, 전부 cosmetic이라 기능 무영향)
- `airflow/shims/setproctitle.py` — `setproctitle`/`getproctitle` 등을 no-op로 정의.
- 구동 시 `export PYTHONPATH="$(pwd)/airflow/shims:$PYTHONPATH"` → 컴파일 패키지 대신 shim이 임포트됨.

backfill 등 task를 fork하는 단발 CLI에도 같은 `PYTHONPATH`를 export해야 task runner의 setproctitle 호출이 비정상 종료되지 않음.

## 4. `@daily` 단일일 backfill이 `No run dates`로 아무것도 안 돎

### 증상
`airflow dags backfill activity_daily -s 2019-10-08 -e 2019-10-08` → `No run dates were found for the given dates and dag interval.` (run 미생성)

### 원인
`@daily`에서 논리날짜 D의 run은 데이터 구간 **`[D, D+1)`**을 담당함.
backfill의 `-e`는 그 구간 끝까지 닿아야 run이 포함되므로 `-s D -e D`는 폭 0 구간이 되어 아무 run도 안 잡힘.

### 해결
`-e`를 대상일 다음 날로. 예: 2019-10-08 run → `-s 2019-10-08 -e 2019-10-09`.

## 5. `tasks clear` 했는데 Grid가 복구 (재실행)되지 않음

### 증상
실패한 backfill run의 입력을 고치고 `airflow tasks clear …` 했지만 task가 다시 실행되지 않고 Grid가 그대로 멈춤.

### 원인
`tasks clear`는 task 상태를 **초기화만** 하고 실행하지 않음.
일반 (스케줄러) run이면 스케줄러가 cleared task를 재실행하지만, **backfill run은 스케줄러가 관리하지 않고** (생성한 backfill job도 이미 종료) 아무도 다시 돌리지 않음.

### 해결
**backfill을 다시 실행**함 (입력은 복구한 상태)
```bash
airflow dags backfill activity_daily -s <D> -e <D+1>   # 필요 시 --rerun-failed-tasks -y
```

## 6. Discord 웹훅 콜백이 `HTTP 403 Forbidden`으로 전송 실패

### 증상
콜백은 실행되는데 (`Executing callback: notify_…`) 알림이 안 오고, 로그에 `urllib.error.HTTPError: HTTP Error 403: Forbidden`.
동일 URL에 `curl`은 정상.

### 원인
Discord (Cloudflare)가 **기본 urllib User-Agent (`Python-urllib/x`)를 403으로 차단**함.
curl은 자체 UA (`curl/…`)라 통과함.

### 해결
요청에 식별 가능한 `User-Agent` 헤더를 추가. (`airflow/callbacks/discord.py`의 `_send`에 `"User-Agent": "activity-daily-airflow/1.0"`.)
검증: 같은 웹훅에 urllib UA → 403, 커스텀 UA → 204.

## 7. 검증 게이트 실패가 silver가 아니라 gate에서 남 / 🚨 Error가 bash 레벨로 보임

### 증상
- (a) 데이터 품질 실패를 재현하려 **빈 입력** (헤더만)을 넣었는데 silver가 성공 (`partitions=0`)하고 gate가 타임아웃으로 실패.
- (b) 🚨 카드 Error가 `Bash command failed … exit code 1`로만 보이고 실제 사유가 안 보임.

### 원인
- (a) `Main`은 데이터에 **실재하는 날짜만** 파티션 루프하므로 (`PartitionWriter.validate`는 그 안에서 호출), 빈 입력은 검증 게이트를 안 타고 `_SUCCESS` 미생성 → gate (FileSensor) 타임아웃이 됨.
- (b) silver는 BashOperator라 콜백이 받는 예외가 bash 레벨 메시지.

### 해결
- (a) silver의 검증 게이트를 직접 트립하려면 **`user_id` null 행**을 넣음 → `validation failed: null key rows=1`로 silver가 실패.
- (b) 실제 사유 (예: 검증 메시지)는 카드의 `🔗 Airflow 로그 열기` 또는 silver `Logs`에서 확인.

## 8. (참고) DagBag 테스트의 start/end 단언

`pendulum.datetime(..., tz="Asia/Seoul")`로 지정한 `start_date`/`end_date`를 Airflow는 **UTC로 저장**함.
`dag.start_date.strftime("%Y-%m-%d")`는 UTC 날짜를 반환하므로, KST 의도를 검증하려면 `.in_timezone("Asia/Seoul")` 변환 후 비교함 (`airflow/tests/test_dag.py`)
