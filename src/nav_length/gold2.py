"""
Gold2 — type2(길이) 최종 산출물을 RDS 포맷으로 변환하고 upsert한다.

RDS는 세그먼트당 항목 1개(length_ft)만 저장한다 — 길이는 시간에
따라 변하지 않으므로 버킷을 반복 저장하지 않는다(설계 문서 6절).
"""

from __future__ import annotations

import statistics
from datetime import date

import pandas as pd

from src.common import gold_snapshot
from src.common.config import GLOBAL_PARTITION_KEY, SERVING_TABLE_TYPE2_KEY_COLUMNS
from src.common.db import replace_table_snapshot
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_length_gold2")


def to_serving_items(df: pd.DataFrame, *, today: date | None = None) -> list[dict]:
    """(segment_id, length_ft) pandas DataFrame을 RDS 항목 리스트로 변환한다.

    결과가 작아(세그먼트당 1개, 최대 몇십만 건) 파이썬 리스트로 다뤄도 안전하다.

    length_ft는 LION 원본을 그대로 반영한 정적 참조값이라 "수집일"이라는
    개념이 따로 없다 - updated_date(이 Gold2 실행일)만 채운다. 예전엔
    collected_date도 항상 같은 값으로 같이 채웠는데, 실행일 하나를 두
    컬럼에 중복 저장하는 것뿐이라 컬럼 자체를 없앴다(2026-08-25 스키마
    정리 - src/common/config.py의 SERVING_TABLE_TYPE2_COLUMNS 참고).
    today를 인자로 받는 건 테스트에서 고정된 날짜로 검증하기 위함이고,
    안 넘기면 실제 실행일(오늘)을 쓴다.

    GLOBAL_PARTITION_KEY 행도 여기서 같이 만든다 - segment_id를 모르거나
    RDS에 아직 없는 경우의 기본값이다(src/serving/nav_lookup.py의
    _lookup_global_default 참고). 예전엔 scripts/seed_rds_defaults.py로
    한 번 수동 시딩한 임의값(300)을 그대로 썼는데, 이 파이프라인이 매번
    실측 length_ft의 중앙값으로 자동 갱신하면 배포 시점 감이 아니라
    실제 데이터 기반 값을 유지할 수 있다.
    """
    today = (today or date.today()).isoformat()
    rows = df[["segment_id", "length_ft"]].itertuples(index=False)

    items = [
        {
            "segment_id": row.segment_id,
            "value": round(row.length_ft),
            "updated_date": today,
        }
        for row in rows
    ]

    if items:
        median_length = round(statistics.median(item["value"] for item in items))
        items.append({
            "segment_id": GLOBAL_PARTITION_KEY,
            "value": median_length,
            "updated_date": today,
        })

    return items


def write_to_rds(items: list[dict], table_name: str) -> int:
    """RDS(PostgreSQL)의 테이블 전체를 items로 교체하고, 성공하면 S3 Gold
    스냅샷도 최신 상태로 다시 내보낸다(src/serving/nav_lookup.py의 RDS
    장애 폴백이 읽는 것). 대상 segment 수가 작아서(세그먼트당 값 1개)
    통째로 담아도 작다 - GLOBAL_PARTITION_KEY 기본값도 items 안에 이미
    포함돼 있어 스냅샷에 자연히 같이 실린다.

    upsert(batch_write_items)가 아니라 replace_table_snapshot을 쓴다 -
    items는 매번 LION 전체를 다시 훑어 만든 완전한 정답 집합이라, LION
    갱신으로 세그먼트가 사라지거나 routable하지 않게 되면 이번 items에서
    빠진다. upsert만 하면 그런 폐기 세그먼트의 옛 길이값이 RDS에 영구히
    남으므로, 전체를 통째로 교체해서 이번 items에 없는 행이 스왑과 함께
    자연히 사라지게 한다. 스냅샷 갱신 자체가 실패해도 RDS 쓰기는
    이미 끝난 뒤라 파이프라인을 실패시키지 않는다."""
    replace_table_snapshot(table_name, items, key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS)
    logger.info(f"[nav_length_gold2] RDS 스냅샷 교체 완료: table={table_name} count={len(items)}")

    try:
        snapshot = {item["segment_id"]: item["value"] for item in items}
        gold_snapshot.write_snapshot("type2", snapshot)
    except Exception:
        logger.exception("[nav_length_gold2] S3 Gold 스냅샷 갱신 실패(RDS 쓰기 자체는 성공)")

    return len(items)
