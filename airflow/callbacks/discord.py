# discord: Airflow task 콜백 → Discord 웹훅 알림.
#   format_message(순수 함수, 테스트 대상)로 문구를 만들고, _send(stdlib urllib)로 POST.
#   on_retry / on_failure / on_success 세 콜백을 default_args에 연결해 쓴다.
import json
import os
import urllib.request


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
#   airflow import는 이 함수 안에서 지연 로드 — 모듈 임포트만으로 Airflow를 요구하지 않게 해
#   format_message(순수 함수)를 Airflow 없이 단위 테스트할 수 있게 한다(실행 시점엔 DAG에 항상 존재).
def _webhook_url():
    from airflow.models import Variable
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
