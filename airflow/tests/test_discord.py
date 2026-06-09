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


def test_retry_message_excludes_log_url():
    # RETRY는 FAILED가 아니므로 로그 링크를 붙이지 않는다(format_message 분기 문서화).
    msg = format_message(
        emoji="🔄", status="RETRY",
        dag_id="activity_daily", task_id="silver",
        ds="2019-10-01", try_number=1, log_url="http://x/log",
    )
    assert msg.startswith("🔄 [RETRY]")
    assert "http://x/log" not in msg
