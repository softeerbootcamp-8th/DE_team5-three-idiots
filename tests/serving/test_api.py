from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.serving import api


class FakeCursor:
    def __init__(self, parent):
        self._parent = parent
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params=None):
        if self._parent.fail:
            raise RuntimeError("temporary RDS failure")
        if params is None:
            # 헬스체크 쿼리("SELECT 1") — 정상 연결로 취급한다.
            self._result = []
            return
        # flat 스키마(segment_id, dow, time) 조회라 dow/time도 함께 넘어온다
        # (src/serving/api.py의 _fetch_batch 참고) — fake는 실제 dow/time
        # 매칭 없이 항상 같은 값을 돌려줘서, 테스트가 어떤 requested_at을
        # 쓰든 값을 찾을 수 있게 한다.
        segment_ids, _dow, _bucket = params
        self._parent.calls.append(list(segment_ids))
        self._result = [
            (segment_id, self._parent.values[segment_id])
            for segment_id in reversed(segment_ids)
            if segment_id in self._parent.values
        ]

    def fetchall(self):
        return self._result


class FakeConnection:
    """RDS 커넥션을 흉내내는 fake — get_type3_values()에 conn=으로 직접
    주입해서 실제 DB 없이 조회 로직만 검증한다."""

    closed = False

    def __init__(self, values=None, fail=False):
        self.values = values or {}
        self.fail = fail
        self.calls = []

    def cursor(self):
        return FakeCursor(self)


@pytest.fixture(autouse=True)
def reset_type3_state():
    api._db_connection = None
    api._type3_snapshot_loaded = False
    api._type3_zone_snapshot = {}
    api._type3_mapping_snapshot = {}
    yield
    api._db_connection = None
    api._type3_snapshot_loaded = False
    api._type3_zone_snapshot = {}
    api._type3_mapping_snapshot = {}


def test_db_connection_uses_short_operational_timeouts(monkeypatch):
    calls = []

    class FakeConn:
        closed = False
        autocommit = False

        def cursor(self):
            return FakeCursor(FakeConnection())

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return FakeConn()

    monkeypatch.setattr(api, "SERVING_TABLE_TYPE3", "navigation-values")
    monkeypatch.setattr(api, "RDS_HOST", "localhost")
    monkeypatch.setattr(api, "RDS_DB", "navdb")
    monkeypatch.setattr(api.psycopg2, "connect", fake_connect)

    assert api.get_db_connection() is api.get_db_connection()
    assert len(calls) == 1

    kwargs = calls[0]
    assert kwargs["connect_timeout"] == 1
    assert "statement_timeout=1000" in kwargs["options"]


def test_navigation_request_is_validated_and_normalized_by_pydantic():
    request = api.NavigationValuesRequest.model_validate(
        [[" 0077356 "], 3, "2026-08-21T12:00:00"]
    )

    assert request.root == (["0077356"], 3, datetime(2026, 8, 21, 12, 0))


def test_get_type3_values_batches_and_preserves_input_order():
    segment_ids = [f"{value:07d}" for value in range(101)]
    requested = [segment_ids[100], segment_ids[0], segment_ids[100], *segment_ids[1:100]]
    conn = FakeConnection({segment_id: index for index, segment_id in enumerate(segment_ids)})

    result = api.get_type3_values(
        requested,
        datetime(2026, 8, 21, 12, 0),
        conn=conn,
        table_name="navigation-values",
    )

    assert result == [100.0, 0.0, 100.0, *[float(value) for value in range(1, 100)]]
    assert len(conn.calls) == 2
    assert max(len(call) for call in conn.calls) == 100


def test_get_type3_values_logs_rds_query_duration(caplog):
    # db.py의 batch_get_items()와 같은 형식의 로그 - Grafana의 "타입별 RDS
    # 쿼리 응답시간" 패널이 table 필드로 두 경로(db.py/api.py)를 같이
    # 집계한다.
    conn = FakeConnection({"0077356": 18.5})

    with caplog.at_level("INFO", logger="src.serving.api"):
        api.get_type3_values(
            ["0077356"],
            datetime(2026, 8, 21, 12, 0),
            conn=conn,
            table_name="navigation-values",
        )

    duration_logs = [r.message for r in caplog.records if "[rds_query_duration]" in r.message]
    assert len(duration_logs) == 1
    assert "table=navigation-values" in duration_logs[0]


def test_get_type3_values_falls_back_to_hardcoded_when_rds_fails_and_no_snapshot():
    # RDS 장애 + 스냅샷에도 없는 세그먼트는 기본값(0.0)으로 응답해야 한다.
    # Type3는 "최근 RDS 성공값" 메모리 캐시를 두지 않으므로, 직전 요청에서
    # RDS로 조회됐던 세그먼트라도 이 시점엔 그 값을 재사용하지 않는다.
    requested_at = datetime(2026, 8, 21, 12, 0)
    with patch("src.common.gold_snapshot.read_snapshot", return_value={}):
        result = api.get_type3_values(
            ["0077356", "0088421"],
            requested_at,
            conn=FakeConnection(fail=True),
            table_name="navigation-values",
        )

    assert result == [0.0, 0.0]


def test_get_type3_values_falls_back_to_zone_snapshot_when_rds_unreachable():
    # RDS로 조회 못 한 segment는 zone 스냅샷 + segment→zone 매핑으로 재구성한다.
    requested_at = datetime(2026, 8, 21, 12, 0)  # FRI, 1200
    zone_snapshot = {"42#FRI#1200": 33.0}
    mapping_snapshot = {"0088421": 42}

    def fake_read_snapshot(type_name):
        return zone_snapshot if type_name == "type3_zone" else mapping_snapshot

    with patch("src.common.gold_snapshot.read_snapshot", side_effect=fake_read_snapshot):
        result = api.get_type3_values(
            ["0088421", "unmapped-segment"],
            requested_at,
            conn=FakeConnection(fail=True),
            table_name="navigation-values",
        )

    assert result == [33.0, 0.0]


def test_get_type3_values_logs_fallback_tier_summary(caplog):
    # rds/snapshot/hardcoded 세 계층을 한 요청 안에서 다 겪게 만들어서
    # 요청당 한 번 남는 요약 로그의 계층별 개수가 맞는지 확인한다 - Grafana의
    # "Type3 fallback 계층 비율" 패널이 이 로그를 집계한다.
    requested_at = datetime(2026, 8, 21, 12, 0)  # FRI, 1200
    zone_snapshot = {"42#FRI#1200": 33.0}
    mapping_snapshot = {"snapshot-seg": 42}

    def fake_read_snapshot(type_name):
        return zone_snapshot if type_name == "type3_zone" else mapping_snapshot

    with patch("src.common.gold_snapshot.read_snapshot", side_effect=fake_read_snapshot), \
         caplog.at_level("INFO", logger="src.serving.api"):
        api.get_type3_values(
            ["rds-seg", "snapshot-seg", "hardcoded-seg"],
            requested_at,
            conn=FakeConnection({"rds-seg": 20.0}),
            table_name="navigation-values",
        )

    summary_logs = [
        r.message for r in caplog.records
        if "[type3_fallback_tier_summary]" in r.message and "total=3" in r.message
    ]
    assert len(summary_logs) == 1
    assert "rds=1" in summary_logs[0]
    assert "cache=" not in summary_logs[0]
    assert "snapshot=1" in summary_logs[0]
    assert "hardcoded=1" in summary_logs[0]


def test_get_type3_values_loads_snapshot_only_once_per_process():
    requested_at = datetime(2026, 8, 21, 12, 0)

    with patch(
        "src.common.gold_snapshot.read_snapshot", return_value={}
    ) as mock_read:
        api.get_type3_values(
            ["0088421"], requested_at, conn=FakeConnection(fail=True), table_name="navigation-values",
        )
        api.get_type3_values(
            ["0088421"], requested_at, conn=FakeConnection(fail=True), table_name="navigation-values",
        )

    # zone + 매핑 2개 파일 -> 최초 1회씩(총 2번), 그 이후 재호출에서는 안 늘어난다.
    assert mock_read.call_count == 2


def test_navigation_values_accepts_agreed_array_request(monkeypatch):
    monkeypatch.setattr(
        api,
        "get_type3_values",
        lambda segment_ids, requested_at: [18.5, 7.0],
    )
    client = TestClient(api.app)

    response = client.post(
        "/api/navigation/values",
        json=[["0077356", "0088421"], 3, "2026-08-21T12:00:00"],
    )

    assert response.status_code == 200
    assert response.json() == [18.5, 7.0]


def test_navigation_values_rejects_unsupported_type():
    client = TestClient(api.app)

    response = client.post(
        "/api/navigation/values",
        json=[["0077356"], 4, "2026-08-21T12:00:00"],
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "literal_error"
