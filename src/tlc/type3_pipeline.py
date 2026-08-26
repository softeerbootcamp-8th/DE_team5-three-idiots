"""TLC type=3 Zone Gold2 기록과 Segment RDS 서빙값 적재 태스크."""

from __future__ import annotations

import json
import re
import shutil
from datetime import date
from pathlib import Path
from uuid import uuid4

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset

from src.common.config import (
    EMR_MAX_EXECUTORS_TLC_INGEST,
    EMR_MAX_EXECUTORS_TLC_TYPE3_SERVING,
    GOLD2_DIR,
    SERVING_TABLE_TYPE3,
    SILVER1_DIR,
    SILVER2_DIR,
    TAXI_TYPES,
    TLC_TYPE3_ROLLING_WEEKS,
)
from src.common.logger import get_logger
from src.common.spark import to_spark_path
from src.silver2.zone_segment import MAP_ZONE_SEGMENT_VERSION_PATH, current_mapping_version
from src.tlc.emr import run_tlc_emr_operation
from src.tlc.gold2 import select_latest_date_partitions


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_type3")

TLC_SILVER1_ROOT = SILVER1_DIR / "tlc"
TYPE3_DAILY_ROOT = GOLD2_DIR / "tlc" / "type3_zone_daily"
MAP_ZONE_SEGMENT_PATH = SILVER2_DIR / "map_zone_segment.parquet"
TYPE3_STAGING_ROOT = TYPE3_DAILY_ROOT / "_staging"
TYPE3_MONTH_MARKER_ROOT = TYPE3_DAILY_ROOT / "_month_success"
TYPE3_PUBLISH_STATE_PATH = TYPE3_DAILY_ROOT / "_rds_publish_state.json"
TLC_TYPE3_GOLD2_READY = Asset("tlc_type3_gold2_ready")
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def _complete_silver_paths_for_month(
    month: str,
    silver_root=TLC_SILVER1_ROOT,
) -> tuple[list[str], list[str]]:
    """월별 네 taxi_type의 완료된 Silver1 경로와 누락 타입을 반환한다."""

    completed_paths = []
    missing_types = []
    for taxi_type in TAXI_TYPES:
        path = silver_root / f"{taxi_type}_tripdata_{month}"
        if (path / "_SUCCESS").exists():
            completed_paths.append(to_spark_path(path))
        else:
            missing_types.append(taxi_type)
    return completed_paths, missing_types


def _staging_run_path(run_id: str, staging_root=TYPE3_STAGING_ROOT):
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"잘못된 Type 3 staging run_id입니다: {run_id}")
    return staging_root / f"run_id={run_id}"


_SERVICE_MONTH_PATTERN = re.compile(r"_tripdata_(\d{4}-\d{2})\.parquet$")


def _months_from_silver_results(silver_results: list[dict]) -> list[str]:
    """오늘 새로 Silver1까지 끝난 파일들에서 서비스 월(YYYY-MM)만 뽑는다
    (중복 제거, 정렬).

    이전엔 실행마다 최근 4개월 전체를 다시 스캔해서 예전에 실패로 밀린
    달까지 자동으로 다시 찾아 재시도했지만, 그 복구는 사람이 Slack 실패
    알림을 보고 수동으로 재실행하는 쪽으로 단순화했다 - 이 함수는 오직
    "오늘 새로 받은 파일이 속한 달"만 후보로 낸다. 실제로 그 달의 4개
    taxi_type이 다 갖춰졌는지는 build_type3_staged_records가 다시 확인한다.

    silver_results는 flat list[dict]여야 한다 - build_silver.expand()로
    taxi_type 청크별 매핑 실행된 결과를 모으면 청크 하나당 리스트 하나,
    즉 list[list[dict]]가 되므로 호출하는 쪽(build_type3_staged_records)에서
    먼저 한 겹 풀어서(flatten) 넘겨야 한다."""

    months = set()
    for item in silver_results:
        match = _SERVICE_MONTH_PATTERN.search(item["filename"])
        if match:
            months.add(match.group(1))
    return sorted(months)


def _type3_metadata_is_current(
    item: dict,
    window_start: date,
    window_end: date,
    mapping_version: str | None,
) -> bool:
    """S3 배포 상태가 현재 롤링 윈도우와 zone-segment 매핑을
    모두 가리키는지 확인한다.

    날짜 범위만 보면, LION/Taxi Zone이 갱신돼 zone-segment 매핑이
    바뀌었는데도 TLC 날짜 범위가 그대로라는 이유로 RDS Type 3 값이
    조용히 갱신되지 않는 문제가 있었다 - 매핑 버전도 함께 비교한다."""

    return (
        item.get("status") == "COMPLETED"
        and item.get("window_start") == window_start.isoformat()
        and item.get("window_end") == window_end.isoformat()
        and item.get("mapping_version") == mapping_version
    )


def _read_type3_publish_state(state_path=TYPE3_PUBLISH_STATE_PATH) -> dict:
    """S3(로컬 개발에서는 파일)의 Type 3 RDS 배포 상태를 읽는다."""

    if not state_path.exists():
        return {}
    try:
        state = json.loads(state_path.read_text())
    except (OSError, TypeError, json.JSONDecodeError):
        logger.exception("Type 3 RDS 배포 상태를 읽지 못해 다시 적재합니다: %s", state_path)
        return {}
    return state if isinstance(state, dict) else {}


def _write_type3_publish_state(
    metadata: dict,
    state_path=TYPE3_PUBLISH_STATE_PATH,
) -> None:
    """Type 3 RDS 적재가 끝난 뒤 다음 실행이 확인할 상태를 S3에 기록한다."""

    if isinstance(state_path, Path):
        state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(metadata, ensure_ascii=False, sort_keys=True))


def _type3_reference_exists(map_zone_segment_path=MAP_ZONE_SEGMENT_PATH) -> bool:
    """운영 Type 3에 필요한 Zone-Segment 매핑이 있는지 확인한다(순수 함수)."""

    return map_zone_segment_path.exists()


@task(pool="tlc_ingest_pool", pool_slots=17)
def build_type3_staged_records(silver_results: list[dict]) -> dict:
    """오늘 새로 Silver1까지 끝난 파일들의 서비스 월에 대해서만, EMR에서
    Zone Gold2 결과를 임시 경로에 저장한다.

    신규 파일만 처리한다 - 이전 실행에서 실패해 밀린 달이 있어도 여기서
    자동으로 다시 찾아 재시도하지 않는다. 그 경우엔 실패한 태스크의
    retries가 소진되며 Slack 알림이 오고, 사람이 원인을 고친 뒤 그
    실행을 수동으로 재시도한다.

    silver_results는 build_silver.expand()가 taxi_type 청크별로 매핑
    실행된 결과라서 청크 하나당 리스트 하나, 즉 list[list[dict]]로 들어온다
    - _months_from_silver_results에 넘기기 전에 한 겹 풀어(flatten) 평평한
    파일 dict 목록으로 만든다."""

    flat_silver_results = [
        item for chunk in silver_results for item in chunk
    ]
    months = _months_from_silver_results(flat_silver_results)
    if not months:
        raise AirflowSkipException("처리할 Type 3 월이 없어 갱신을 건너뜁니다")

    run_id = uuid4().hex
    run_path = _staging_run_path(run_id)
    ready_months = []
    for month in months:
        silver_paths, missing_types = _complete_silver_paths_for_month(month)
        if missing_types:
            logger.info(
                "Type 3 월 계산 보류: month=%s missing_taxi_types=%s",
                month,
                missing_types,
            )
            continue
        ready_months.append({"month": month, "silver_paths": silver_paths})

    if not ready_months:
        raise AirflowSkipException(
            "네 taxi_type이 모두 준비된 신규 월이 없어 Type 3 갱신을 건너뜁니다"
        )

    return run_tlc_emr_operation(
        "build_type3_stage",
        {
            "run_id": run_id,
            "run_path": str(run_path),
            "months": ready_months,
        },
        max_executors=EMR_MAX_EXECUTORS_TLC_INGEST,
    )


@task(pool="tlc_ingest_pool", pool_slots=17)
def validate_type3_staged_records(stage_result: dict) -> dict:
    """EMR에서 월별 임시 결과를 검증하고 승격 계획을 반환한다."""

    _staging_run_path(stage_result["run_id"])
    return run_tlc_emr_operation(
        "validate_type3_stage",
        {"stage_result": stage_result},
        max_executors=EMR_MAX_EXECUTORS_TLC_INGEST,
    )


@task(pool="tlc_ingest_pool", pool_slots=17)
def publish_type3_daily_records(validated_stage: dict) -> dict:
    """EMR에서 검증된 날짜 파티션을 운영 경로에 반영하고, 성공한 경우에만
    serving DAG가 구독하는 tlc_type3_gold2_ready Asset을 발행한다."""

    _staging_run_path(validated_stage["run_id"])
    return run_tlc_emr_operation(
        "publish_type3_daily",
        {
            "validated_stage": validated_stage,
            "daily_root": str(TYPE3_DAILY_ROOT),
            "marker_root": str(TYPE3_MONTH_MARKER_ROOT),
        },
        max_executors=EMR_MAX_EXECUTORS_TLC_INGEST,
    )


@task
def cleanup_type3_staging(published_result: dict) -> None:
    """운영 경로 승격에 성공한 실행의 임시 결과를 삭제한다."""

    run_path = _staging_run_path(published_result["run_id"])
    if not run_path.exists():
        return
    if isinstance(run_path, Path):
        shutil.rmtree(run_path)
    else:
        run_path.rmtree()
    logger.info("Type 3 staging 정리 완료: %s", run_path)


@task(trigger_rule="none_failed")
def check_type3_publish_needed(_published_result=None) -> dict:
    """S3 최신 N주와 마지막 RDS 배포 상태를 비교해 적재 여부를 판단한다."""

    _, window_start, window_end = select_latest_date_partitions(
        TYPE3_DAILY_ROOT.glob("date=*"),
        TLC_TYPE3_ROLLING_WEEKS * 7,
    )
    mapping_version = current_mapping_version()
    metadata = _read_type3_publish_state()
    if _type3_metadata_is_current(metadata, window_start, window_end, mapping_version):
        raise AirflowSkipException(
            f"RDS Type 3가 이미 최신입니다: {window_start}~{window_end} "
            f"(mapping_version={mapping_version})"
        )

    logger.info(
        "RDS Type 3 갱신 필요: current_window_end=%s current_mapping_version=%s "
        "target=%s~%s target_mapping_version=%s",
        metadata.get("window_end"),
        metadata.get("mapping_version"),
        window_start,
        window_end,
        mapping_version,
    )
    return {
        "daily_path": str(TYPE3_DAILY_ROOT),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "mapping_version": mapping_version,
    }


@task.short_circuit(ignore_downstream_trigger_rules=False)
def check_type3_reference_ready(_publish_plan=None) -> bool:
    """운영 Type 3에 필요한 Zone-Segment 매핑이 있는지 확인한다.

    zone_segment_pipeline이 아직 한 번도 안 돌았거나(최초 배포) 재부트스트랩
    중이면 이 매핑이 없을 수 있다. 예전엔 이 경우 FileNotFoundError를 던져서
    DAG run 전체가 실패로 확정됐는데(재시도 3회 소진 후), segment_time_
    pipeline의 check_dim_segment_exists와 같은 이유로 - 의존 파일이 아직
    없다고 DAG 전체를 실패시키는 대신, 이번 실행의 RDS 갱신만 조용히
    건너뛴다.

    ignore_downstream_trigger_rules=False로 명시한다(주의: 복수형 rules -
    ignore_downstream_trigger_rule로 쓰면 _ShortCircuitDecoratedOperator가
    이 kwarg를 못 받아 DAG 파싱 자체가 TypeError로 깨진다, Airflow 3.3.0
    실측). 기본값(True)이면 이 태스크가 short-circuit될 때 도달 가능한
    모든 하위 태스크를 trigger_rule과 무관하게 강제로 skip시켜서,
    check_type3_rds_freshness에 일부러 걸어둔 trigger_rule="none_failed"
    (발행이 skip되어도 이번 Asset 이벤트의 최신성 검사는 실행한다는 의도)까지
    무시하고 같이 skip시켜 버린다. False로
    두면 직접 하위인 publish_type3_rolling_values만 skip되고,
    check_type3_rds_freshness는 자기 trigger_rule을 그대로 따른다."""

    exists = _type3_reference_exists()
    if not exists:
        logger.warning(
            "%s 없음 - zone_segment_pipeline이 아직 매핑을 만들지 않았거나 "
            "수동 부트스트랩이 필요함. 이번 실행은 Type 3 RDS 갱신을 건너뛴다.",
            MAP_ZONE_SEGMENT_PATH,
        )
    return exists


# check_type3_publish_needed가 gap을 감지하면 같은 실행에서 곧바로 발행까지
# 끝나는 게 정상이라, S3 Gold2의 최신 롤링 window_end와 RDS에 실제 반영된
# window_end 사이 gap은 건강한 상태에서 거의 항상 0이어야 한다. 이 값을
# 넘게 벌어져 있으면 "TLC가 아직 새 월을 안 줌"(정상, 몇 달씩 불규칙) 때문이
# 아니라 - 그 경우엔 Gold2도 같이 오래돼 있어 gap이 안 생긴다 - 발행 단계
# 자체가 멈췄다는 신호로 본다(예: check_type3_reference_ready가 매핑 파일
# 누락으로 며칠째 조용히 skip 중인 경우).
TYPE3_FRESHNESS_MAX_LAG_DAYS = 3


def _type3_rds_lag_days(gold_window_end: date, publish_state: dict) -> int:
    """S3 Gold2 최신 롤링 window_end와 RDS에 마지막으로 반영된 window_end
    사이 며칠 차이나는지 계산한다(순수 함수 - 테스트용으로 분리)."""

    rds_window_end_raw = publish_state.get("window_end")
    if rds_window_end_raw is None:
        raise ValueError(
            "RDS Type 3 배포 상태가 없습니다(아직 한 번도 publish된 적이 "
            "없거나 상태 파일이 손상됨) - 최초 배포 여부를 확인하세요"
        )
    rds_window_end = date.fromisoformat(rds_window_end_raw)
    return (gold_window_end - rds_window_end).days


@task(trigger_rule="none_failed")
def check_type3_rds_freshness(_published_values=None) -> None:
    """RDS Type 3의 window_end가 S3 Gold2 최신 롤링 window_end보다
    TYPE3_FRESHNESS_MAX_LAG_DAYS일 이상 뒤처져 있으면 실패시킨다.

    기준을 오늘 날짜가 아니라 S3 Gold2에 이미 쌓여 있는 최신 데이터로 잡는
    이유: TLC 공개가 몇 달씩 불규칙하게 늦어져도 Gold2와 RDS는 같은 실행
    안에서 함께 갱신되므로, 둘 사이 gap은 TLC의 공개 지연과 무관하게 거의
    항상 0이다 - "아직 새 데이터가 없음"과 "파이프라인이 멈췄음"을 이 gap이
    자동으로 구분해준다.

    Asset 발행 때만 도는 배치 태스크라 API 요청마다 도는 서빙 경로와 달리
    실패 알림이 폭주하지 않는다. 실패하면 기존 on_failure_callback(Slack)이
    그대로 재사용된다. 이벤트 자체가 장기간 발생하지 않는 정체는 이 태스크가
    실행되지 않으므로 별도 외부 모니터링으로 감시해야 한다."""

    _, _, gold_window_end = select_latest_date_partitions(
        TYPE3_DAILY_ROOT.glob("date=*"),
        TLC_TYPE3_ROLLING_WEEKS * 7,
    )

    lag_days = _type3_rds_lag_days(gold_window_end, _read_type3_publish_state())
    if lag_days > TYPE3_FRESHNESS_MAX_LAG_DAYS:
        raise ValueError(
            f"RDS Type 3가 S3 Gold2보다 {lag_days}일 뒤처져 있습니다 "
            f"(gold_window_end={gold_window_end}) - check_type3_reference_ready 등 "
            "발행 단계가 계속 조용히 skip되고 있는지 확인 필요"
        )

    logger.info(
        "RDS Type 3 최신성 정상: gold_window_end=%s lag_days=%s",
        gold_window_end,
        lag_days,
    )


@task(pool="tlc_type3_serving_pool", pool_slots=30)
def publish_type3_rolling_values(publish_plan: dict) -> dict:
    """EMR에서 최근 N주 평균을 계산하고 RDS에 적재한다."""

    if not SERVING_TABLE_TYPE3:
        raise ValueError("SERVING_TABLE_TYPE3 환경변수가 필요합니다")

    daily_path = publish_plan["daily_path"]
    if daily_path != str(TYPE3_DAILY_ROOT):
        raise ValueError(f"예상하지 못한 Type 3 날짜별 경로입니다: {daily_path}")

    result = run_tlc_emr_operation(
        "publish_type3_rolling",
        {
            "publish_plan": publish_plan,
            "daily_root": str(TYPE3_DAILY_ROOT),
            "mapping_path": str(MAP_ZONE_SEGMENT_PATH),
            # EMR Serverless 컨테이너는 Airflow와 별개 환경이라 S3_BUCKET_DATA
            # 같은 .env 값이 안 넘어간다 - EMR 쪽에서 current_mapping_version()을
            # 인자 없이 부르면 SILVER2_DIR가 버킷명 없이(None) 잘못 계산돼서
            # "s3://None/..." 경로로 타임아웃난다(실제로 겪은 장애). Airflow
            # 쪽에서 이미 올바르게 계산된 경로를 문자열로 그대로 넘긴다.
            "mapping_version_path": str(MAP_ZONE_SEGMENT_VERSION_PATH),
            "table_name": SERVING_TABLE_TYPE3,
            "rolling_weeks": TLC_TYPE3_ROLLING_WEEKS,
            "mapping_version": publish_plan.get("mapping_version"),
        },
        max_executors=EMR_MAX_EXECUTORS_TLC_TYPE3_SERVING,
    )
    _write_type3_publish_state(
        {
            "status": "COMPLETED",
            "window_start": result["window_start"],
            "window_end": result["window_end"],
            "rolling_weeks": result["rolling_weeks"],
            "mapping_version": result.get("mapping_version"),
        }
    )
    return result
