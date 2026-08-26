from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.serving import nav_lookup

# 순수 로직 테스트(배치 조회를 monkeypatch로 대체)는 RDS가 없어도 돈다 -
# 실제 테이블에 값을 심어두고 조회하는 테스트에만 개별로
# @requires_postgres를 붙인다.
requires_postgres = pytest.mark.usefixtures("require_postgres")

# 뉴욕 날짜 경계 테스트가 실행 시각에 의존하지 않도록
# _new_york_today()를 아래 autouse fixture에서 고정한다.
TODAY_DATE = date(2026, 8, 26)
TODAY = datetime.combine(TODAY_DATE, datetime.min.time())
YESTERDAY = TODAY - timedelta(days=1)


@pytest.fixture(autouse=True)
def _clear_caches():
    """메모리 캐시/S3 스냅샷 로드 상태가 테스트 간에 새지 않도록 초기화한다."""
    with patch.object(nav_lookup, "_new_york_today", return_value=TODAY_DATE):
        nav_lookup._memory_cache.clear()
        nav_lookup._s3_snapshot_loaded = False
        nav_lookup._s3_snapshot = {}
        nav_lookup._length_snapshot_loaded = False
        nav_lookup._length_snapshot = {}
        yield
        nav_lookup._memory_cache.clear()
        nav_lookup._s3_snapshot_loaded = False
        nav_lookup._s3_snapshot = {}
        nav_lookup._length_snapshot_loaded = False
        nav_lookup._length_snapshot = {}


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.SERVING_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.SERVING_TABLE_TYPE2


def test_add_seconds_advances_within_same_hour():
    assert nav_lookup._add_seconds("12:00", 600) == "12:10"


def test_add_seconds_wraps_past_midnight():
    assert nav_lookup._add_seconds("23:50", 900) == "00:05"


# ---------------------------------------------------------------------------
# _is_fresh / _resolve_from_row — 한 행 안에서 Fresh Exact -> Historical AVG
# -> SPEC Estimate 순서를 고르는 순수 로직.
# ---------------------------------------------------------------------------

def test_is_fresh_true_only_for_todays_date():
    assert nav_lookup._is_fresh(TODAY) is True
    assert nav_lookup._is_fresh(YESTERDAY) is False


def test_is_fresh_false_for_missing_or_malformed_value():
    assert nav_lookup._is_fresh(None) is False
    assert nav_lookup._is_fresh("not-a-date") is False  # 파싱 자체가 안 되는 문자열


def test_is_fresh_parses_isoformat_string_from_s3_snapshot_round_trip():
    """gold_snapshot.write_snapshot()이 datetime을 JSON에 실을 때
    last_sample_at.isoformat()으로 문자열화하고, read_snapshot()은 이를
    다시 datetime으로 복원하지 않는다 - S3 폴백 경로에서 _is_fresh가 받는
    값은 실제로는 항상 이 문자열이다. 문자열이어도 날짜가 오늘이면 fresh로
    판정해야 한다(회귀 테스트: 문자열이라는 이유만으로 무조건 stale
    처리되던 버그)."""
    assert nav_lookup._is_fresh(TODAY.isoformat()) is True
    assert nav_lookup._is_fresh(YESTERDAY.isoformat()) is False


def test_is_fresh_converts_timezone_aware_value_to_new_york_date():
    # 2026-08-27 03:30 UTC는 뉴욕에서는 아직 8월 26일 23:30이다.
    still_today_in_new_york = datetime(2026, 8, 27, 3, 30, tzinfo=timezone.utc)
    # 04:00 UTC는 뉴욕 8월 27일 00:00이므로 더 이상 "오늘"이 아니다.
    tomorrow_in_new_york = datetime(2026, 8, 27, 4, 0, tzinfo=timezone.utc)

    assert nav_lookup._is_fresh(still_today_in_new_york.isoformat()) is True
    assert nav_lookup._is_fresh(tomorrow_in_new_york.isoformat()) is False


def test_resolve_from_row_prefers_fresh_exact():
    row = {"value": 30, "avg": 40, "last_sample_at": TODAY}
    assert nav_lookup._resolve_from_row(row) == (30, "fresh")


def test_resolve_from_row_falls_back_to_avg_when_value_is_stale():
    row = {"value": 30, "avg": 40, "last_sample_at": YESTERDAY}
    assert nav_lookup._resolve_from_row(row) == (40, "avg")



def test_resolve_from_row_returns_none_when_row_is_none_or_empty():
    assert nav_lookup._resolve_from_row(None) == (None, None)
    assert nav_lookup._resolve_from_row({"value": None, "avg": None}) == (None, None)


# ---------------------------------------------------------------------------
# Type1 — RDS 정상 응답
# ---------------------------------------------------------------------------

def test_resolve_uses_fresh_exact_when_collected_today():
    rows = {"1": {"1200": {"value": 30, "avg": 999, "last_sample_at": TODAY}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]


def test_resolve_falls_back_to_avg_when_value_is_stale():
    rows = {"1": {"1200": {"value": 30, "avg": 40, "last_sample_at": YESTERDAY}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_falls_back_to_hardcoded_constant_when_slot_missing_but_rds_up():
    # RDS는 정상 응답했지만 이 세그먼트/슬롯 자체가 없는 경우 - 메모리/S3
    # 폴백으로 내려가지 않고 곧장 코드 상수로 간다.
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value={}):
        result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_type2_has_no_avg_tier_goes_straight_to_default():
    # RDS 연결 자체는 되고(fast-fail 커넥션 획득 성공) 조회 결과가 비어있는
    # 경우 - _get_fast_rds_connection도 같이 mock해야 batch_get_items가
    # 실제로 호출되는 지점까지 도달한다.
    with patch.object(nav_lookup, "_get_fast_rds_connection", return_value=None), \
         patch.object(nav_lookup, "batch_get_items", return_value={}) as mock_batch:
        result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[2]]
    mock_batch.assert_called()


# ---------------------------------------------------------------------------
# Type1 — RDS 자체가 응답 불가능한 경우: 메모리 캐시 -> S3 스냅샷 -> 코드 상수
# ---------------------------------------------------------------------------

def test_resolve_falls_back_to_s3_snapshot_when_rds_unreachable():
    # 스냅샷의 last_sample_at은 문자열(JSON 왕복)이지만 날짜가 오늘이면
    # fresh로 채택돼야 한다(회귀 테스트 - 문자열이라는 이유만으로 무조건
    # avg/코드 상수로 떨어지던 버그).
    snapshot = {"1": {"1200": {"value": 77, "last_sample_at": TODAY.isoformat()}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value=snapshot):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [77]


def test_resolve_falls_back_to_hardcoded_when_s3_snapshot_value_is_stale():
    # last_sample_at이 어제 날짜인 문자열이면 fresh가 아니므로, avg가
    # 없는 이 케이스는 코드 상수로 떨어지는 게 맞다.
    snapshot = {"1": {"1200": {"value": 77, "last_sample_at": YESTERDAY.isoformat()}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value=snapshot):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_uses_s3_snapshot_avg_when_rds_unreachable():
    snapshot = {"1": {"1200": {"avg": 55}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value=snapshot):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [55]


def test_resolve_uses_memory_cache_before_reloading_s3_snapshot():
    # 1번째 요청(RDS 정상)에서 성공적으로 읽은 값이 메모리 캐시에 남는다.
    rows = {"1": {"1200": {"value": 30, "avg": None, "last_sample_at": TODAY}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    # 2번째 요청은 RDS가 죽었다고 가정 - S3를 한 번도 안 불러도 메모리
    # 캐시로 응답할 수 있어야 한다.
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot") as mock_read_snapshot:
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]
    mock_read_snapshot.assert_not_called()


def test_resolve_falls_back_to_hardcoded_constant_when_everything_fails():
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={}):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 2


# ---------------------------------------------------------------------------
# 누적 경로 시각 계산 (segment_ids를 경로 순서로 취급)
# ---------------------------------------------------------------------------

def test_resolve_time_values_uses_cumulative_elapsed_time_per_segment():
    rows = {
        "1": {"1200": {"value": 1800, "avg": None, "last_sample_at": TODAY}},
        "2": {
            "1200": {"value": 111, "avg": None, "last_sample_at": TODAY},
            "1230": {"value": 999, "avg": None, "last_sample_at": TODAY},
        },
    }
    # 세그먼트 1: 12:00 슬롯에 1800초(30분) 소요 -> 세그먼트 2는 12:30에 도착.
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [1800, 999]


def test_resolve_time_values_logs_fallback_tier_summary(caplog):
    # Grafana의 fallback 히트율 대시보드(CloudWatch Logs Insights)가 이
    # 요약 로그 한 줄을 집계한다 - 세그먼트마다 로그를 안 남기고 요청당
    # 한 번만 남기는지, tier별 개수가 맞는지 확인한다.
    rows = {
        "fresh_seg": {"1200": {"value": 10, "avg": None, "last_sample_at": TODAY}},
        "avg_seg": {"1200": {"value": None, "avg": 20, "last_sample_at": None}},
    }
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows), \
         caplog.at_level("INFO", logger="src.serving.nav_lookup"):
        nav_lookup.resolve_segment_values(["fresh_seg", "avg_seg", "missing_seg"], 1, "12:00")

    summary_logs = [r.message for r in caplog.records if "[fallback_tier_summary]" in r.message]
    assert len(summary_logs) == 1
    assert "fresh=1" in summary_logs[0]
    assert "avg=1" in summary_logs[0]
    assert "hardcoded=1" in summary_logs[0]
    assert "total=3" in summary_logs[0]


def test_resolve_time_values_same_segment_twice_uses_different_buckets():
    rows = {
        "loop": {
            "1200": {"value": 1800, "avg": None, "last_sample_at": TODAY},
            "1230": {"value": 77, "avg": None, "last_sample_at": TODAY},
        },
    }
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["loop", "loop"], 1, "12:00")

    assert result == [1800, 77]


def test_resolve_never_raises_on_invalid_type():
    result = nav_lookup.resolve_segment_values(["1", "2"], 3, "12:00")

    assert len(result) == 2
    assert all(isinstance(v, int) for v in result)


def test_resolve_never_raises_on_malformed_time():
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value={}):
        result = nav_lookup.resolve_segment_values(["1"], 1, "not-a-time")

    assert len(result) == 1
    assert isinstance(result[0], int)


# ---------------------------------------------------------------------------
# Type2 (길이) — 시간 무관, GLOBAL 기본값 폴백. 실제 RDS 왕복은 통합 테스트로.
# ---------------------------------------------------------------------------

@requires_postgres
def test_resolve_type2_reads_written_value():
    from src.common.config import SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS
    from src.common.db import put_item
    from tests.conftest import reset_table

    table = nav_lookup.SERVING_TABLE_TYPE2
    reset_table(table, SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS)
    put_item(table, {"segment_id": "1", "value": 500}, key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS)

    result = nav_lookup.resolve_segment_values(["1", "1"], 2, "09:00")

    assert result == [500, 500]


@requires_postgres
def test_resolve_type2_falls_back_to_global_default_when_missing():
    from src.common.config import (
        GLOBAL_PARTITION_KEY,
        SERVING_TABLE_TYPE2_COLUMNS,
        SERVING_TABLE_TYPE2_KEY_COLUMNS,
    )
    from src.common.db import put_item
    from tests.conftest import reset_table

    table = nav_lookup.SERVING_TABLE_TYPE2
    reset_table(table, SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS)
    put_item(
        table,
        {"segment_id": GLOBAL_PARTITION_KEY, "value": 300},
        key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS,
    )

    result = nav_lookup.resolve_segment_values(["missing-segment"], 2, "09:00")

    assert result == [300]


def test_resolve_type2_falls_back_to_hardcoded_constant_when_rds_and_snapshot_both_fail():
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={}):
        result = nav_lookup.resolve_segment_values(["1", "2"], 2, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[2]] * 2


def test_resolve_type2_falls_back_to_s3_snapshot_when_rds_unreachable():
    # RDS 자체가 죽었을 때는 GLOBAL 재조회(RDS) 대신 스냅샷을 바로 쓴다 -
    # 스냅샷에 있는 segment는 그 값, 없는 segment는 스냅샷의 GLOBAL 값으로.
    snapshot = {"1": 150, "GLOBAL": 200}
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value=snapshot):
        result = nav_lookup.resolve_segment_values(["1", "missing"], 2, "12:00")

    assert result == [150, 200]


def test_resolve_type2_loads_snapshot_only_once_per_process():
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={"1": 150}) as mock_read:
        nav_lookup.resolve_segment_values(["1"], 2, "12:00")
        nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    mock_read.assert_called_once()


def test_resolve_type2_logs_fallback_tier_summary_when_rds_up(caplog):
    # 메인 조회에선 rds-seg만 찾아지고, global-seg는 없어서 GLOBAL
    # 재조회(RDS)로 채워진다 - Grafana의 "Type2 fallback 계층 비율" 패널이
    # 이 로그를 집계한다.
    def fake_batch_get_items(table_name, keys, conn=None):
        if len(keys) == 1 and keys[0]["segment_id"] == nav_lookup.GLOBAL_PARTITION_KEY:
            return {(nav_lookup.GLOBAL_PARTITION_KEY,): {"value": 999}}
        return {("rds-seg",): {"value": 100}}

    with patch.object(nav_lookup, "_get_fast_rds_connection", return_value=None), \
         patch.object(nav_lookup, "batch_get_items", side_effect=fake_batch_get_items), \
         caplog.at_level("INFO", logger="src.serving.nav_lookup"):
        nav_lookup.resolve_segment_values(["rds-seg", "global-seg"], 2, "12:00")

    summary_logs = [r.message for r in caplog.records if "[type2_fallback_tier_summary]" in r.message]
    assert len(summary_logs) == 1
    assert "rds=1" in summary_logs[0]
    assert "global=1" in summary_logs[0]
    assert "snapshot=0" in summary_logs[0]
    assert "hardcoded=0" in summary_logs[0]
    assert "total=2" in summary_logs[0]


def test_resolve_type2_logs_fallback_tier_summary_when_rds_down(caplog):
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={"1": 150}), \
         caplog.at_level("INFO", logger="src.serving.nav_lookup"):
        nav_lookup.resolve_segment_values(["1", "missing"], 2, "12:00")

    summary_logs = [r.message for r in caplog.records if "[type2_fallback_tier_summary]" in r.message]
    assert len(summary_logs) == 1
    assert "rds=0" in summary_logs[0]
    assert "global=0" in summary_logs[0]
    assert "snapshot=1" in summary_logs[0]
    assert "hardcoded=1" in summary_logs[0]
    assert "total=2" in summary_logs[0]
