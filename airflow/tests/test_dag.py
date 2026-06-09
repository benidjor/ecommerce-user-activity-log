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
    # Airflow는 start_date/end_date를 UTC로 저장한다.
    # KST(Asia/Seoul)로 변환해 원래 지정값(2019-10-01, 2019-12-01)을 확인한다.
    assert dag.start_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d") == "2019-10-01"
    assert dag.end_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d") == "2019-12-01"


def test_sequential_and_retry_contract():
    # 순차 실행 보장(max_active_runs=1)과 자동 retry 1회 — 장애 복구 설계의 핵심 계약을 고정.
    dag = _dag()
    assert dag.max_active_runs == 1
    # retries는 default_args로 모든 task에 적용 — silver로 대표 확인.
    assert dag.get_task("silver").retries == 1
