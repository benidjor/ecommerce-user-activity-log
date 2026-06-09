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

# 상태별 상황 설명(한글) — 카드 본문에 무슨 일이 일어났는지 풀어서 요약 설명한다.
#   {dag_id}{task_id}{ds}{tn}{total}{dur} 치환. RETRY/FAILED/SUCCESS 외 상태는 빈 문자열.
_STATUS_NARRATIVE = {
    "RETRY": (
        "⚠️ **{dag_id}** 파이프라인의 **{task_id}** 단계가 `{ds}` 실행에서 "
        "이번 시도(try {tn}/{total})에 실패했습니다.\n"
        "일시적 오류로 보고 **자동 재시도**합니다 — 잠시 후 같은 작업을 다시 실행하며, "
        "성공하면 이후 단계로 정상 이어집니다. 별도 조치는 필요 없습니다."
    ),
    "FAILED": (
        "❌ **{dag_id}** 파이프라인의 **{task_id}** 단계가 `{ds}` 실행에서 "
        "자동 재시도(try {tn}/{total})까지 모두 실패해 **최종 실패**했습니다.\n"
        "이 단계가 막혀 이후 단계(repair_partition→…→build)는 실행되지 않았습니다. "
        "아래 **Error**와 로그에서 원인을 확인하고, 입력·상태를 바로잡은 뒤 "
        "해당 날짜를 **Clear(재처리)**하면 멱등하게 복구됩니다."
    ),
    "SUCCESS": (
        "✅ **{dag_id}** 파이프라인의 **{task_id}** 단계가 `{ds}` 실행에서 "
        "정상 완료되었습니다 (try {tn}/{total}, {dur})."
    ),
}

# task별 단계 설명(한글) — 이 task가 파이프라인에서 무슨 일을 하는지.
_STAGE_DESC = {
    "silver": "Bronze 일별 데이터를 dedup·KST 변환·5분 갭 세션화하여 Silver(activity)로 적재하는 단계입니다.",
    "repair_partition": "새로 쓴 파티션을 Hive 메타스토어에 인식시키는(MSCK REPAIR) 단계입니다.",
    "gate": "`_SUCCESS` 마커로 Silver 적재 완료를 확인하는 검증 게이트입니다.",
    "gold": "activity 테이블에서 WAU 등 Gold 마트를 재집계(멱등)하는 단계입니다.",
    "export": "Gold 마트를 대시보드용 DuckDB 파일로 export하는 단계입니다.",
    "build": "정적 대시보드(HTML)를 빌드하는 단계입니다.",
}


# build_embed: Discord 임베드(dict) 생성(네트워크·Airflow 상태 불필요 → 단위 테스트 가능).
#   title을 log_url로 하이퍼링크 → 모든 알림에서 제목 클릭 시 Airflow 로그로 바로 이동.
#   description에 상태 상황 설명 + 단계 설명을 한글로 풀어서 보여준다.
#   status=FAILED일 때만 Error 필드(실제 예외 메시지)를 덧붙여 원인을 즉시 보여준다.
def build_embed(emoji, status, color, dag_id, task_id, ds, try_number,
                max_tries, duration, hostname, log_url, run_id, error=None):
    # duration(초)·host는 None일 수 있어 방어적으로 표시. try는 현재/최대(=retries+1)로.
    dur = f"{duration:.0f}s" if duration is not None else "-"
    # 본문: 상황 설명(상태) + 이 단계가 하는 일(task) + 클릭 가능한 로그 링크.
    narrative = _STATUS_NARRATIVE.get(status, "").format(
        dag_id=dag_id, task_id=task_id, ds=ds,
        tn=try_number, total=max_tries + 1, dur=dur)
    stage = _STAGE_DESC.get(task_id, "파이프라인 단계입니다.")
    parts = [narrative, f"ℹ️ **이 단계가 하는 일**: {stage}"]
    if log_url:
        # 본문에도 클릭 가능한 로그 링크를 노출(제목 하이퍼링크와 별개로 눈에 띄게).
        parts.append(f"🔗 [Airflow 로그 열기]({log_url})")
    description = "\n\n".join(p for p in parts if p)
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
        "description": description,
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
