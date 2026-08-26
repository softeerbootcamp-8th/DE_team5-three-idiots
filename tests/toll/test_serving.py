from unittest.mock import patch

import pytest

from src.toll import serving
from src.toll.serving import get_toll_value, get_toll_values


@pytest.fixture(autouse=True)
def _clear_snapshot_cache():
    """모듈 전역 스냅샷 캐시가 테스트 간에 새지 않도록 초기화한다."""
    serving._snapshot_loaded = False
    serving._snapshot = {}
    yield
    serving._snapshot_loaded = False
    serving._snapshot = {}


def test_get_toll_values_returns_values_in_order():
    with patch(
        "src.toll.serving.db.batch_get_items",
        return_value={
            ("1",): {"segment_id": "1", "value": 2.75},
            ("2",): {"segment_id": "2", "value": 17.0},
        },
    ) as mock_batch:
        result = get_toll_values(["1", "2"])

    assert result == [2.75, 17.0]
    mock_batch.assert_called_once()


def test_get_toll_values_defaults_missing_segments_to_zero():
    with patch("src.toll.serving.db.batch_get_items", return_value={}):
        result = get_toll_values(["nonexistent"])

    assert result == [0.0]


def test_get_toll_values_dedupes_before_querying_then_restores_duplicates():
    with patch(
        "src.toll.serving.db.batch_get_items",
        return_value={("1",): {"segment_id": "1", "value": 5.0}},
    ) as mock_batch:
        result = get_toll_values(["1", "1", "1"])

    assert result == [5.0, 5.0, 5.0]
    keys_arg = mock_batch.call_args.args[1]
    assert len(keys_arg) == 1


def test_get_toll_values_falls_back_to_zero_when_rds_and_snapshot_both_fail():
    with patch("src.toll.serving.db.batch_get_items", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={}):
        result = get_toll_values(["1", "2", "3"])

    assert result == [0.0, 0.0, 0.0]


def test_get_toll_values_falls_back_to_s3_snapshot_when_rds_unreachable():
    # RDS 자체가 죽었을 때는 스냅샷에 있는 값을 그대로 쓴다 - 스냅샷에
    # 없는 segment만 0으로 떨어진다.
    with patch("src.toll.serving.db.batch_get_items", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={"1": 2.75}):
        result = get_toll_values(["1", "2"])

    assert result == [2.75, 0.0]


def test_get_toll_values_loads_snapshot_only_once_per_process():
    with patch("src.toll.serving.db.batch_get_items", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={"1": 2.75}) as mock_read:
        get_toll_values(["1"])
        get_toll_values(["1"])

    mock_read.assert_called_once()


def test_get_toll_values_logs_fallback_tier_summary_when_rds_up(caplog):
    # RDS가 살아있으면 조회됐든 안 됐든(통행료 대상 아님) 전부 rds 계층 -
    # Grafana의 "Type4 fallback 계층 비율" 패널이 이 로그를 집계한다.
    with patch(
        "src.toll.serving.db.batch_get_items",
        return_value={("1",): {"segment_id": "1", "value": 5.0}},
    ), caplog.at_level("INFO", logger="src.toll.serving"):
        get_toll_values(["1", "not-a-toll-road"])

    summary_logs = [r.message for r in caplog.records if "[type4_fallback_tier_summary]" in r.message]
    assert len(summary_logs) == 1
    assert "rds=2" in summary_logs[0]
    assert "snapshot=0" in summary_logs[0]
    assert "hardcoded=0" in summary_logs[0]
    assert "total=2" in summary_logs[0]


def test_get_toll_values_logs_fallback_tier_summary_when_rds_down(caplog):
    with patch("src.toll.serving.db.batch_get_items", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={"1": 2.75}), \
         caplog.at_level("INFO", logger="src.toll.serving"):
        get_toll_values(["1", "2"])

    summary_logs = [r.message for r in caplog.records if "[type4_fallback_tier_summary]" in r.message]
    assert len(summary_logs) == 1
    assert "rds=0" in summary_logs[0]
    assert "snapshot=1" in summary_logs[0]
    assert "hardcoded=1" in summary_logs[0]
    assert "total=2" in summary_logs[0]


def test_get_toll_value_delegates_to_get_toll_values():
    with patch(
        "src.toll.serving.db.batch_get_items",
        return_value={("1",): {"segment_id": "1", "value": 5.0}},
    ):
        assert get_toll_value("1") == 5.0
