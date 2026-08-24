"""TLC Taxi Zone Bronze/Silver1 적재 DAG.

원래 schedule=None(수동 트리거) "1회성" DAG였는데, ingest_taxi_zone_shapefile이
ETag 기반 변경 감지를 갖추면서(src/taxi_zone/bronze.py 참고) 매달 다시
돌려도 안전해졌다 — 원본이 그대로면 다운로드도, Silver1 재생성도,
Asset emit도 전부 스킵한다. Wayback Machine으로 실측한 실제 변경 주기가
1~2년에 한 번이라(2024-03~2024-10 무변경, 다음 변경 2026-02), 매달 확인하면
탐지 지연 최대 1개월로 충분히 빠르면서 불필요한 재계산은 없다.

build_taxi_zone_silver1은 Asset("taxi_zone_silver1_updated")를 outlet으로
내보낸다 — zone_segment_pipeline이 lion_pipeline의 lion_dim_segment_ready와
함께 이 Asset을 구독해서, 둘 중 하나만 갱신돼도 자동으로 재실행된다
(TriggerDagRunOperator로 DAG 이름을 직접 지정하던 방식에서 전환 — 두 소스가
같은 날 겹치면 중복 실행되던 문제가 있었음). build_taxi_zone_silver1이
변경 없음으로 스킵되면 이 Asset도 emit되지 않아 zone_segment_pipeline이
매달 헛돌지 않는다.

TaskFlow(@task)를 쓴다 - 예전에는 PythonOperator + op_kwargs에 Jinja
문자열("{{ ti.xcom_pull(...) }}")로 XCom을 넘겼는데, 이러면 Airflow가
기본적으로 렌더링 결과를 문자열로 반환한다(DAG에 render_template_as_native_obj=True를
안 준 이상) - ingest_taxi_zone_shapefile이 반환한 dict가
"{'changed': True, ...}" 같은 그냥 문자열이 되어 build()의
shapefile_result.get("changed", True) 호출이 AttributeError로 죽었다
(실제로 겪음). TaskFlow는 XCom을 파이썬 객체 그대로 넘겨줘서 이 문제
자체가 없다.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.taxi_zone.bronze import ingest_taxi_zone_shapefile
from src.taxi_zone.silver1 import build as build_taxi_zone_silver1

default_args = {
    "retries": 2,
}

TAXI_ZONE_SILVER1_UPDATED = Asset("taxi_zone_silver1_updated")


@dag(
    dag_id="taxi_zone_pipeline",
    description="TLC Taxi Zone 정적 참조 데이터 S3 Bronze/Silver1 적재 (월 1회 변경 확인)",
    schedule="0 4 1 * *",          # 매월 1일 새벽 4시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["taxi_zone", "monthly"],
)
def taxi_zone_pipeline():

    @task(task_id="ingest_taxi_zone_shapefile")
    def ingest_shapefile() -> dict:
        return ingest_taxi_zone_shapefile()

    @task(task_id="build_taxi_zone_silver1", outlets=[TAXI_ZONE_SILVER1_UPDATED])
    def build_silver1(shapefile_result: dict) -> str:
        return build_taxi_zone_silver1(shapefile_result)

    build_silver1(ingest_shapefile())


taxi_zone_pipeline()
