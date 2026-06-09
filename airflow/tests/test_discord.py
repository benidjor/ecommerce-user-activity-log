# build_embed 순수 함수 단위 테스트(네트워크·Airflow 불필요).
import os, sys

# airflow/ 를 import 경로에 추가(callbacks 패키지 로드)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from callbacks.discord import build_embed


def _fields(embed):
    # 필드 리스트를 name→value 딕셔너리로 변환(검증 편의).
    return {f["name"]: f["value"] for f in embed["fields"]}


def test_success_embed_fields_and_clickable_log():
    e = build_embed(
        emoji="✅", status="SUCCESS", color=0x2ECC71,
        dag_id="activity_daily", task_id="silver", ds="2019-10-09",
        try_number=2, max_tries=1, duration=14.0, hostname="host-1",
        log_url="http://x/log", run_id="run1",
    )
    assert e["title"] == "✅ SUCCESS — silver"
    assert e["color"] == 0x2ECC71
    # 제목 클릭 → 로그(모든 알림 공통). url이 log_url로 설정돼야 한다.
    assert e["url"] == "http://x/log"
    f = _fields(e)
    assert f["DAG / Task"] == "activity_daily / silver"
    assert f["Run (ds)"] == "2019-10-09"
    assert f["Try"] == "2/2"               # try_number/(max_tries+1)
    assert f["Duration"] == "14s"
    assert f["Host"] == "host-1"
    # 성공 메시지엔 Error 필드가 없다.
    assert "Error" not in f


def test_failed_embed_includes_error_and_log():
    e = build_embed(
        emoji="🚨", status="FAILED", color=0xE74C3C,
        dag_id="activity_daily", task_id="silver", ds="2019-10-09",
        try_number=2, max_tries=1, duration=3.0, hostname="host-1",
        log_url="http://x/log", run_id="run1",
        error="validation failed for 2019-10-09: null key rows=1",
    )
    assert e["title"].startswith("🚨 FAILED")
    assert e["color"] == 0xE74C3C
    assert e["url"] == "http://x/log"
    f = _fields(e)
    # 실패 메시지엔 실제 예외 메시지가 Error 필드로 포함된다.
    assert "Error" in f
    assert "null key rows=1" in f["Error"]


def test_retry_embed_no_error_but_has_log():
    e = build_embed(
        emoji="🔄", status="RETRY", color=0xF39C12,
        dag_id="activity_daily", task_id="silver", ds="2019-10-09",
        try_number=1, max_tries=1, duration=2.0, hostname="host-1",
        log_url="http://x/log", run_id="run1",
    )
    assert e["title"].startswith("🔄 RETRY")
    assert e["color"] == 0xF39C12
    # RETRY는 FAILED가 아니므로 Error 필드 없음, 단 로그 링크는 있어야 한다.
    f = _fields(e)
    assert "Error" not in f
    assert e["url"] == "http://x/log"


def test_embed_handles_missing_duration_and_host():
    # duration·host가 None이어도 임베드가 안전하게 생성돼야 한다(방어적 표시).
    e = build_embed(
        emoji="✅", status="SUCCESS", color=0x2ECC71,
        dag_id="activity_daily", task_id="gold", ds="2019-10-09",
        try_number=1, max_tries=1, duration=None, hostname=None,
        log_url="http://x/log", run_id="run1",
    )
    f = _fields(e)
    assert f["Duration"] == "-"
    assert f["Host"] == "-"
