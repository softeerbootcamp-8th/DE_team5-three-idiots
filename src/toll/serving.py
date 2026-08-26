"""통행료(type4) 서빙 조회 — 배치 파이프라인(gold.py)과 분리된 경량 모듈.

gold.py는 pandas/yaml/geopandas 같은 무거운 배치 처리 의존성을 쓰는데,
Lambda 서빙 이미지(requirements-lambda.txt)엔 그게 없다. get_toll_value()는
RDS 조회 하나뿐이라 그 의존성이 전혀 필요 없는데, nav_api.py가 gold.py에서
그대로 import하면 모듈 전체가 로드되면서 무거운 import까지 실행돼 Lambda
콜드 스타트 자체가 ModuleNotFoundError로 죽는다(실제로 배포 후
/api/navigation/values 전체가 500으로 죽는 것으로 확인됨). 그래서 서빙에
필요한 최소 코드만 여기로 분리한다. gold.py는 이 모듈에서 다시 import해서
기존 호출부(테스트 포함) 하위 호환을 유지한다.
"""

from __future__ import annotations

from src.common import db, gold_snapshot
from src.common.config import SERVING_TABLE_TYPE4
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_serving")

# RDS 자체가 응답 불가능할 때 대신 쓰는 S3 스냅샷(gold.py의 write_gold_items가
# RDS 쓰기 성공 시마다 갱신). Lambda 웜 인스턴스에서 최초 1회만 로드해서
# 재사용한다 - 통행료 대상 segment만이라 전체를 통째로 담아도 작다.
_snapshot_loaded = False
_snapshot: dict[str, float] = {}


def _load_snapshot_once() -> None:
    global _snapshot_loaded, _snapshot
    if _snapshot_loaded:
        return
    _snapshot = gold_snapshot.read_snapshot("type4")
    _snapshot_loaded = True


def get_toll_value(segment_id: str) -> float:
    """서빙 조회 함수(단건). 시설/zone에 해당 안 하는 segment는 0을 반환한다
    (무결점 응답 원칙 — null/에러 없음). gold.py 등 기존 호출부 하위
    호환용으로 남겨둔다 — 여러 세그먼트를 한 번에 조회할 때는
    get_toll_values()를 쓴다(RDS 호출 횟수를 줄임)."""

    return get_toll_values([segment_id])[0]


def get_toll_values(segment_ids: list[str]) -> list[float]:
    """서빙 조회 함수(배치). segment_ids 순서/중복을 그대로 유지해서
    반환한다 - 중복 제거 후 한 번에 조회하고 원래 순서로 다시 매핑한다.

    RDS 호출 자체가 실패하면(커넥션/네트워크 등) S3 스냅샷으로 넘어간다.
    RDS가 정상 응답했는데 특정 segment가 없는 건 "진짜로 통행료 대상이
    아닌 도로"로 간주해서 스냅샷을 거치지 않고 바로 0으로 응답한다 -
    RDS가 멀쩡한데 이미 없다고 확인된 값에 예전 스냅샷을 섞으면 두 실패
    모드(값이 없음 vs RDS가 죽음)가 헷갈린다."""

    unique_ids = list(dict.fromkeys(segment_ids))
    keys = [{"segment_id": segment_id} for segment_id in unique_ids]
    tier: dict[str, str] = {}

    try:
        found = db.batch_get_items(SERVING_TABLE_TYPE4, keys)
        values = {
            segment_id: float(found[(segment_id,)].get("value", 0))
            for segment_id in unique_ids
            if (segment_id,) in found
        }
        # RDS 호출 자체는 성공했으므로, 이 segment에 통행료가 없는 것도
        # "진짜 통행료 대상이 아님"이지 폴백이 아니다(위 docstring 참고) -
        # 전부 rds 계층으로 센다.
        for segment_id in unique_ids:
            tier[segment_id] = "rds"
    except Exception:
        logger.exception("[toll_serving] RDS 조회 실패 - S3 스냅샷으로 폴백합니다")
        _load_snapshot_once()
        values = {
            segment_id: _snapshot[segment_id]
            for segment_id in unique_ids
            if segment_id in _snapshot
        }
        for segment_id in unique_ids:
            tier[segment_id] = "snapshot" if segment_id in _snapshot else "hardcoded"

    # 요청당 한 번만 남긴다 - Grafana의 "Type4 fallback 계층 비율" 패널이
    # 이 로그를 집계한다.
    tier_counts = {"rds": 0, "snapshot": 0, "hardcoded": 0}
    for segment_id in segment_ids:
        tier_counts[tier[segment_id]] += 1
    logger.info(
        f"[type4_fallback_tier_summary] rds={tier_counts['rds']} "
        f"snapshot={tier_counts['snapshot']} hardcoded={tier_counts['hardcoded']} "
        f"total={len(segment_ids)}"
    )

    return [values.get(segment_id, 0.0) for segment_id in segment_ids]
