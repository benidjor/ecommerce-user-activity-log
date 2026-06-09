# setproctitle no-op 셰임 (macOS fork 크래시 회피용).
#
# 왜: 신버전 macOS에서 컴파일된 setproctitle(1.3.4)의 darwin_set_process_title이
#   fork된 자식 프로세스에서 CoreFoundation(LaunchServices)을 호출하다 SIGSEGV한다
#   ("crashed on child side of fork"). Airflow webserver/scheduler의 gunicorn 워커가
#   매 fork마다 이를 호출해 반복 크래시 → standalone이 못 뜬다.
#
# 어떻게: PYTHONPATH 앞에 이 디렉터리를 두면 `import setproctitle`이 컴파일 패키지 대신
#   이 모듈로 해석되어 제목 설정이 no-op이 된다. setproctitle 호출은 전부 프로세스 제목
#   표시(cosmetic)용이라 기능에 영향 없다(Airflow는 serve_logs/local_executor/task_runner/
#   dag_processing에서 하드 임포트하므로 '제거'가 아니라 '무력화'가 안전한 길).
#
# 적용: export PYTHONPATH="$(pwd)/airflow/shims:$PYTHONPATH" (런북 §3). 미적용 시 동작 동일.


def setproctitle(title):  # noqa: D401 - 원본 시그니처 유지
    return None


def getproctitle():
    return ""


def setthreadtitle(title):
    return None


def getthreadtitle():
    return ""
