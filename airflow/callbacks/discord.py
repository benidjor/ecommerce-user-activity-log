# discord: Airflow task 콜백 → Discord 웹훅 알림(임베드 카드).
#   build_embed(순수 함수, 테스트 대상)로 색상·필드를 갖춘 임베드를 만들고,
#   _send(stdlib urllib)로 POST. on_retry / on_failure / on_success 세 콜백을 default_args에 연결.
import json
import os
import urllib.request

# 상태별 임베드 좌측 색 바(16진수 정수): 성공=녹색, 실패=빨강, 재시도=주황.
_GREEN = 0x2ECC71
_RED = 0xE74C3C
_ORANGE = 0xF39C12


# build_embed: Discord 임베드(dict) 생성(네트워크·Airflow 상태 불필요 → 단위 테스트 가능).
#   title을 log_url로 하이퍼링크 → 모든 알림에서 제목 클릭 시 Airflow 로그로 바로 이동.
#   status=FAILED일 때만 Error 필드(실제 예외 메시지)를 덧붙여 원인을 즉시 보여준다.
def build_embed(emoji, status, color, dag_id, task_id, ds, try_number,
                max_tries, duration, hostname, log_url, run_id, error=None):
    # duration(초)·host는 None일 수 있어 방어적으로 표시. try는 현재/최대(=retries+1)로.
    dur = f"{duration:.0f}s" if duration is not None else "-"
    fields = [
        {"name": "DAG / Task", "value": f"{dag_id} / {task_id}", "inline": True},
        {"name": "Run (ds)", "value": ds, "inline": True},
        {"name": "Try", "value": f"{try_number}/{max_tries + 1}", "inline": True},
        {"name": "Duration", "value": dur, "inline": True},
        {"name": "Host", "value": hostname or "-", "inline": True},
    ]
    # 실패 시 실제 예외 메시지를 코드블록으로(너무 길면 잘라서) 첨부.
    if status == "FAILED" and error:
        fields.append({"name": "Error", "value": f"```{str(error)[:500]}```", "inline": False})
    embed = {
        "title": f"{emoji} {status} — {task_id}",
        "color": color,
        "fields": fields,
        "footer": {"text": f"run_id={run_id}"},
    }
    # log_url이 있을 때만 제목 하이퍼링크 부여(없는데 url=None이면 Discord가 임베드를 거부).
    if log_url:
        embed["url"] = log_url
    return embed


# _webhook_url: Airflow Variable 우선, 없으면 환경변수. 둘 다 없으면 None(전송 skip).
#   airflow import는 이 함수 안에서 지연 로드 — 모듈 임포트만으로 Airflow를 요구하지 않게 해
#   build_embed(순수 함수)를 Airflow 없이 단위 테스트할 수 있게 한다(실행 시점엔 DAG에 항상 존재).
def _webhook_url():
    from airflow.models import Variable
    url = Variable.get("discord_webhook_url", default_var=None)
    return url or os.getenv("DISCORD_WEBHOOK_URL")


# _send: context에서 식별자·duration·예외를 뽑아 임베드를 만들고 웹훅에 POST. 웹훅 미설정 시 조용히 반환.
def _send(emoji, status, color, context):
    url = _webhook_url()
    if not url:
        return  # 웹훅 미설정 환경(테스트·웹훅 없는 데모)에서 DAG가 깨지지 않도록 skip
    ti = context["task_instance"]
    embed = build_embed(
        emoji=emoji, status=status, color=color,
        dag_id=ti.dag_id, task_id=ti.task_id, ds=context["ds"],
        try_number=ti.try_number, max_tries=ti.max_tries,
        duration=ti.duration, hostname=ti.hostname,
        log_url=ti.log_url, run_id=ti.run_id,
        error=context.get("exception"),  # on_failure에서만 채워짐
    )
    # content는 푸시 미리보기용 짧은 한 줄, embeds가 디테일 카드.
    content = f"{emoji} [{status}] {ti.task_id} · run {context['ds']}"
    data = json.dumps({"content": content, "embeds": [embed]}).encode("utf-8")
    # User-Agent 명시: Discord(Cloudflare)는 기본 urllib UA(Python-urllib/x)를
    #   403 Forbidden으로 막는다 → 식별 가능한 UA를 지정해야 웹훅 POST가 통과한다.
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json",
                 "User-Agent": "activity-daily-airflow/1.0"})
    urllib.request.urlopen(req, timeout=10)  # 실패하면 콜백이 예외 → 로그에 남음


# default_args의 on_*_callback에 그대로 연결할 3개 콜백.
def notify_retry(context):
    _send("🔄", "RETRY", _ORANGE, context)


def notify_failure(context):
    _send("🚨", "FAILED", _RED, context)


def notify_success(context):
    _send("✅", "SUCCESS", _GREEN, context)
