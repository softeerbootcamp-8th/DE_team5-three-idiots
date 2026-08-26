"""
세그먼트 지표 조회 + fallback 체인

"무조건 응답"(설계 문서 7절)을 구현하는 핵심 모듈. 키가 없는 경우와 RDS
호출 자체가 실패(예외)하는 경우를 구분하지 않고 똑같이 다음 fallback
단계로 넘어간다.

Type1(시간)은 segment_metrics_type1(segment_id+time 복합키, src/common/
config.py 참고)로 서빙한다. 한 행 안에 오늘 실측값(value)과 그 시간대의
과거 평균(avg)을 같이 들고 있어서, "뉴욕 기준 오늘 값이 있으면 그걸,
없으면 평균을"
판단이 조회 한 번으로 끝난다. DynamoDB에서 RDS로 옮기며 잃은 멀티 AZ
자동 failover(가용성)를 보완하려고, RDS 자체가 응답 불가능한 경우를 위한
별도 폴백 계층(메모리 캐시 -> S3 Gold 스냅샷)을 추가로 둔다:

  [RDS 정상 응답]
  1. Fresh Exact — (segment_id, time) 행의 value. last_sample_at의 날짜가
     뉴욕 기준 오늘인 경우만 채택한다.
  2. Historical AVG — 같은 행의 avg(이 시간대의 과거 평균 - src/nav_time/
     gold2.py가 실측이 들어올 때마다 증분 갱신한다).
  3. 코드 상수

  [RDS 자체가 응답 불가능 — 연결 실패/타임아웃]
  1. 메모리 캐시(이 프로세스가 이전에 RDS에서 성공적으로 읽은 (segment_id,
     time) 값)
  2. S3 Gold 스냅샷(src/common/gold_snapshot.py) — RDS가 정상일 때 Gold
     파이프라인이 미리 내보내둔 세그먼트×시간대별 스냅샷을 처음 미스가 날
     때 한 번만 통째로 로드해 메모리에 얹는다. 이 안에서도 위와 같은
     exact(오늘 것만) -> avg 순서를 그대로 재현한다.
  3. 코드 상수

  RDS는 정상 응답했는데 그 (segment_id, time)에 해당하는 행 자체가 없는
  경우("값이 없음")는 메모리/S3 폴백으로 내려가지 않고 곧장 코드 상수로
  간다 - 메모리/S3는 "RDS 자체가 죽었을 때 예전에 알던 값을 대신 쓰는"
  용도라, RDS가 멀쩡한데 원래 없는 데이터에 그걸 섞으면 두 실패 모드가
  헷갈린다.

Type1(소요시간)의 segment_ids는 경로를 순서대로 나열한 것으로 간주한다.
요청 시각은 첫 세그먼트에만 그대로 쓰고, 이후 세그먼트는 앞 세그먼트들의
누적 소요시간만큼 시각을 이동해서 조회한다. 다만 어떤 시간 슬롯이 필요할지는
그 누적시각을 계산해봐야 알 수 있어서, RDS 조회 자체는 요청당 딱 한 번만
한다 - segment_id별로 존재 가능한 행이 PK(segment_id, time) 특성상 최대
48개(30분 슬롯)로 정해져 있어 필요할지 모르는 슬롯까지 전부 미리 배치로
가져와도 데이터량이 작다. 그 결과로 순차 누적시각 계산 자체는 이 로컬
dict만 보고 끝나서(RDS를 더 안 건드림), "세그먼트 수만큼 RDS 왕복이 쌓여
응답이 느려지거나 타임아웃 나는" 문제 자체가 없어진다(_resolve_time_values
참고) - 예전에 있던 circuit breaker/시간 예산 방어 로직은 그래서 필요 없다.

Type2(길이)는 segment_metrics_type2(segment_id 단일키)로 서빙한다 - 시간과
무관한 정적값이라 정확한 segment_id 값 -> GLOBAL 기본값 -> 코드 상수
순서를 그대로 유지한다(멀티 AZ 손실을 보완하는 메모리/S3 폴백 계층은
아직 type1에만 있다 - type2 확장은 이후 과제, TODO 팀 검토 필요).
"""

from __future__ import annotations

import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql

from src.common import gold_snapshot
from src.common.config import GLOBAL_PARTITION_KEY, SERVING_TABLE_TYPE1, SERVING_TABLE_TYPE2
from src.common.db import batch_get_items, new_connection
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_lookup")

_NY_TZ = ZoneInfo("America/New_York")

# RDS 자체 장애(연결 불가/응답 없음)를 얼마나 기다렸다가 fallback으로
# 넘어갈지에 대한 값 - 같은 리전 안에서 정상 조회는 수십 ms 안에 끝나므로
# 1초/1000ms는 그 대비 넉넉한 여유값이다. 정확한 실측(p50/p99)은 아직
# 없어서 CloudWatch 데이터가 쌓이면 재조정이 필요하다(TODO 팀 검토 필요).
# db.py의 공유 커넥션(배치 쓰기와 공용)과는 별도로 이 값만 쓰는 커넥션을
# 둔다 - 배치 쓰기는 대량 upsert라 1초 넘게 걸리는 게 정상이라, 공유
# 커넥션에 이 타임아웃을 걸면 정상 동작까지 실패로 처리된다.
_RDS_CONNECT_TIMEOUT_SECONDS = 1
_RDS_STATEMENT_TIMEOUT_MS = 1000

# RDS/GLOBAL#DEFAULT/메모리/S3까지 전부 실패했을 때 쓰는 최후의 상수. 외부
# 호출이 전혀 없어 어떤 저장소도 완전히 응답 불가능한 상황에서도 동작한다.
# TODO(팀 검토 필요): scripts/seed_rds_defaults.py의 기본값과 동일한
# 정성적 초안.
_HARDCODED_DEFAULTS = {1: 45, 2: 300}

# RDS 장애 시 쓰는 메모리 캐시. (segment_id, time) -> 그 슬롯의 마지막 성공
# 조회 행. 상한을 안 걸면 Lambda 인스턴스가 오래 켜져 있을 때 계속 커져서
# OOM으로 함수 자체가 죽을 수 있어(관측값이 없는 것보다 훨씬 나쁜 실패)
# 개수 상한 + 가장 오래된 것부터 제거한다.
_MEMORY_CACHE_MAX_SIZE = 50_000
_memory_cache: dict[tuple[str, str], dict] = {}

# S3 Gold 스냅샷은 이 프로세스(Lambda 웜 인스턴스)에서 처음 필요할 때 딱
# 한 번만 통째로 읽는다 - 세그먼트마다 매번 S3를 부르면 RDS가 죽어있는
# 동안 세그먼트 수만큼 S3 호출이 쌓이는 문제가 재발한다.
_s3_snapshot_loaded = False
_s3_snapshot: dict[str, dict[str, dict]] = {}

# Type2(길이) 전용 S3 스냅샷 - segment_id당 값 하나뿐이라 위 type1
# 스냅샷과 구조가 달라 별도로 둔다. GLOBAL_PARTITION_KEY 기본값도 이
# 스냅샷 안에 자연히 포함돼 있다(src/nav_length/gold2.py 참고).
_length_snapshot_loaded = False
_length_snapshot: dict[str, float] = {}

# 서빙 전용 fast-fail RDS 커넥션. db.py의 공유 커넥션과 별개로 지연 생성해서
# Lambda 웜스타트 사이에 재사용한다.
_fast_rds_connection = None


def _get_fast_rds_connection():
    """짧은 connect_timeout/statement_timeout이 걸린 커넥션을 재사용한다.
    끊어졌으면(RDS 재부팅, 유휴 타임아웃 등) db.py._get_connection()과 동일한
    방식으로 재연결한다."""
    global _fast_rds_connection
    if _fast_rds_connection is not None and not _fast_rds_connection.closed:
        try:
            with _fast_rds_connection.cursor() as cur:
                cur.execute("SELECT 1")
            return _fast_rds_connection
        except psycopg2.Error:
            logger.warning("fast-fail RDS 커넥션이 끊어져 재연결합니다")
    _fast_rds_connection = new_connection(
        connect_timeout=_RDS_CONNECT_TIMEOUT_SECONDS,
        statement_timeout_ms=_RDS_STATEMENT_TIMEOUT_MS,
    )
    return _fast_rds_connection


def time_to_bucket(time_str: str) -> str:
    """'HH:MM' -> 'HHMM' (30분 단위로 내림)."""
    hour_str, minute_str = time_str.split(":")
    bucket_minute = (int(minute_str) // 30) * 30
    return f"{int(hour_str):02d}{bucket_minute:02d}"


def _add_seconds(time_str: str, seconds: int) -> str:
    """'HH:MM'에 초를 더해 다시 'HH:MM'으로 반환한다. 하루(86400초)를 넘기면
    24시간으로 wrap한다 - 버킷 조회에는 시:분만 필요해서 날짜는 추적하지
    않는다."""
    hour_str, minute_str = time_str.split(":")
    total_seconds = (int(hour_str) * 3600 + int(minute_str) * 60 + seconds) % 86400
    new_hour, remainder_seconds = divmod(total_seconds, 3600)
    new_minute = remainder_seconds // 60
    return f"{new_hour:02d}:{new_minute:02d}"


def table_for_type(type_: int) -> str:
    if type_ == 1:
        return SERVING_TABLE_TYPE1
    if type_ == 2:
        return SERVING_TABLE_TYPE2
    raise ValueError(f"알 수 없는 type: {type_}")


def _batch_fetch_type1_rows(segment_ids: list[str], table_name: str) -> dict[str, dict[str, dict]]:
    """요청에 포함된 segment_id 전체가 가질 수 있는 모든 (time) 행을 한 번의
    쿼리로 가져온다. segment_id별 존재 가능한 행은 최대 48개(30분 슬롯)뿐이라
    "이번에 어떤 슬롯이 필요할지" 미리 몰라도 전부 가져오는 비용이 작다
    (_resolve_time_values 참고). batch_get_items는 정확한 키(segment_id+time
    둘 다)로만 조회할 수 있어 이 "segment_id만으로 그 세그먼트의 모든 슬롯"
    조회에는 못 쓴다 - 그래서 커넥션을 직접 빌려 커스텀 쿼리를 짠다.

    반환값은 segment_id -> {time: {"value","avg","last_sample_at"}}.
    RDS 연결/쿼리 실패는 삼키지 않고 그대로 던진다 - 호출부가 이걸로 "RDS
    자체가 죽었다"를 판단해서 메모리/S3 폴백으로 넘어간다. 커넥션은
    _get_fast_rds_connection()의 짧은 connect/statement 타임아웃이 걸린
    걸 써서, RDS가 느리기만 해도(완전히 다운되지 않아도) 이 판단이
    제때 내려지게 한다."""
    if not segment_ids:
        return {}

    unique_ids = list(dict.fromkeys(segment_ids))
    started = time.monotonic()
    try:
        conn = _get_fast_rds_connection()
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT segment_id, time, value, avg, last_sample_at "
                    "FROM {table} WHERE segment_id = ANY(%s)"
                ).format(table=sql.Identifier(table_name)),
                (unique_ids,),
            )
            rows = cur.fetchall()
    finally:
        logger.info(
            f"[nav_lookup] RDS type1 배치 조회 소요 시간: "
            f"{(time.monotonic() - started) * 1000:.0f}ms"
        )

    result: dict[str, dict[str, dict]] = {}
    for segment_id, time_slot, value, avg, last_sample_at in rows:
        result.setdefault(segment_id, {})[time_slot] = {
            "value": float(value) if value is not None else None,
            "avg": float(avg) if avg is not None else None,
            "last_sample_at": last_sample_at,
        }
    return result


def _new_york_today() -> date:
    """서버/Lambda 시스템 타임존과 무관하게 뉴욕의 오늘 날짜를 반환한다."""
    return datetime.now(_NY_TZ).date()


def _is_fresh(last_sample_at) -> bool:
    """last_sample_at의 날짜가 뉴욕 기준 오늘인지 확인한다.

    시:분 단위가 아니라 "오늘 수집된 값인지"만 본다(TODO 팀 검토 필요 -
    하루 단위 신선도로 충분하다는 판단). 값이 없거나 형식이 이상하면
    (레거시 데이터 등) 안전한 쪽으로
    "신선하지 않음"으로 처리해 다음 단계로 내려가게 한다.

    RDS에서 직접 읽은 행은 psycopg2가 datetime 객체를 주지만, S3 Gold
    스냅샷(gold_snapshot.py)을 거쳐 온 값은 JSON 직렬화 때문에 문자열
    (last_sample_at.isoformat() 결과)이다 - 그대로 두면 RDS 장애 폴백 중엔
    방금 저장된 값도 항상 "형식이 이상함"으로 오판돼 fresh 판정을 절대
    못 받는다. 그래서 문자열이면 먼저 datetime으로 되돌려 본다.

    Socrata data_as_of에서 만든 timezone-naive datetime은 뉴욕 현지
    시각으로 해석한다. timezone-aware 값은 뉴욕 시간으로 변환한 뒤
    날짜를 비교해 UTC 자정 경계에서 오판하지 않게 한다."""
    if isinstance(last_sample_at, str):
        try:
            last_sample_at = datetime.fromisoformat(last_sample_at)
        except ValueError:
            return False
    if not isinstance(last_sample_at, datetime):
        return False

    if last_sample_at.utcoffset() is not None:
        sample_date = last_sample_at.astimezone(_NY_TZ).date()
    else:
        # RDS TIMESTAMP(without time zone)에 저장된 data_as_of는 뉴욕
        # 현지 시각이므로 timezone-naive 값의 날짜를 그대로 쓴다.
        sample_date = last_sample_at.date()
    return sample_date == _new_york_today()


def _resolve_from_row(row: dict | None) -> tuple[int | None, str | None]:
    """한 (segment_id, time) 행(또는 메모리 캐시/S3 스냅샷의 같은 모양
    dict)에서 Fresh Exact -> Historical AVG 순서로 값을 뽑는다. 둘 다
    없으면 (None, None)을 돌려줘서 호출부가 다음 단계로 내려가게 한다.

    두 번째 반환값(tier)은 어떤 단계에서 값을 뽑았는지("fresh"/"avg")다 -
    Grafana 대시보드용 fallback 히트율 집계에 쓴다(_resolve_time_values
    참고)."""
    if row is None:
        return None, None
    if row.get("value") is not None and _is_fresh(row.get("last_sample_at")):
        return round(row["value"]), "fresh"
    if row.get("avg") is not None:
        return round(row["avg"]), "avg"
    return None, None


def _remember_in_memory(segment_id: str, time_slot: str, row: dict) -> None:
    key = (segment_id, time_slot)
    _memory_cache[key] = row
    if len(_memory_cache) > _MEMORY_CACHE_MAX_SIZE:
        oldest_key = next(iter(_memory_cache))
        del _memory_cache[oldest_key]


def _load_s3_snapshot_once() -> None:
    global _s3_snapshot_loaded, _s3_snapshot
    if _s3_snapshot_loaded:
        return
    _s3_snapshot = gold_snapshot.read_snapshot("type1")
    _s3_snapshot_loaded = True


def _resolve_from_fallback(segment_id: str, time_slot: str) -> tuple[int, str]:
    """RDS 자체가 응답 불가능할 때: 메모리 캐시 -> S3 스냅샷(최초 미스 때
    한 번만 로드) -> 코드 상수 순서로 내려간다."""
    row = _memory_cache.get((segment_id, time_slot))

    if row is None:
        _load_s3_snapshot_once()
        row = _s3_snapshot.get(segment_id, {}).get(time_slot)
        if row is not None:
            _remember_in_memory(segment_id, time_slot, row)

    value, tier = _resolve_from_row(row)
    if value is not None:
        return value, tier

    logger.warning(
        f"메모리 캐시/S3 스냅샷에도 값 없음 -> 코드 상수 사용: "
        f"segment_id={segment_id} time={time_slot}"
    )
    return _HARDCODED_DEFAULTS[1], "hardcoded"


def resolve_segment_values(segment_ids: list[str], type_: int, time_str: str) -> list[int]:
    """요청받은 segment_ids 순서(중복 포함)대로 값을 반환한다. 항상 길이가
    같은 리스트를 반환한다 — 절대 예외를 던지지 않는다(이 함수가 최후의
    방어선이다 — 상위에 입력 검증 레이어가 없어도 안전해야 한다)."""
    try:
        return _resolve_segment_values_inner(segment_ids, type_, time_str)
    except Exception:
        logger.exception(
            f"resolve_segment_values 예상치 못한 오류 - 코드 상수로 응답: "
            f"type={type_} time={time_str}"
        )
        fallback_value = _HARDCODED_DEFAULTS.get(type_, _HARDCODED_DEFAULTS[1])
        return [fallback_value] * len(segment_ids)


def _resolve_segment_values_inner(segment_ids: list[str], type_: int, time_str: str) -> list[int]:
    table_name = table_for_type(type_)

    if type_ == 1:
        return _resolve_time_values(segment_ids, table_name, time_str)
    return _resolve_length_values(segment_ids, table_name)


def _lookup_global_default(table_name: str) -> int:
    started = time.monotonic()
    try:
        key = {"segment_id": GLOBAL_PARTITION_KEY}
        items = batch_get_items(table_name, [key], conn=_get_fast_rds_connection())
        item = items.get(tuple(key.values()))
        if item is not None:
            return round(item["value"])
    except Exception:
        logger.exception(f"RDS GLOBAL 기본값 조회 실패: table={table_name}")
    finally:
        logger.info(
            f"[nav_lookup] RDS GLOBAL 기본값 조회 소요 시간: "
            f"{(time.monotonic() - started) * 1000:.0f}ms"
        )

    logger.warning("GLOBAL 기본값까지 실패 -> 코드 상수 사용: type=2")
    return _HARDCODED_DEFAULTS[2]


def _load_length_snapshot_once() -> None:
    global _length_snapshot_loaded, _length_snapshot
    if _length_snapshot_loaded:
        return
    _length_snapshot = gold_snapshot.read_snapshot("type2")
    _length_snapshot_loaded = True


def _resolve_length_values(segment_ids: list[str], table_name: str) -> list[int]:
    """Type2(길이)는 시간과 무관해 세그먼트당 값이 하나뿐이다 - 중복
    segment_id는 한 번만 조회해서 재사용해도 안전하다.

    RDS는 살아있는데 특정 segment만 없으면 GLOBAL 기본값(RDS의 실측
    중앙값, src/nav_length/gold2.py 참고)으로 채운다. RDS 자체가
    죽었으면 그 RDS 재조회(_lookup_global_default) 대신 S3 스냅샷으로
    바로 넘어간다 - 이미 죽은 RDS에 재시도성 호출을 또 보낼 이유가
    없다. 스냅샷에도 없으면 스냅샷 자체에 실려있는 GLOBAL 값을 쓰고,
    스냅샷마저 실패하면 최후의 코드 상수로 떨어진다."""
    unique_ids = list(dict.fromkeys(segment_ids))
    resolved: dict[str, int] = {}
    tier: dict[str, str] = {}

    started = time.monotonic()
    try:
        keys = [{"segment_id": sid} for sid in unique_ids]
        items = batch_get_items(table_name, keys, conn=_get_fast_rds_connection())
        for sid in unique_ids:
            item = items.get((sid,))
            if item is None:
                continue
            try:
                resolved[sid] = round(item["value"])
                tier[sid] = "rds"
            except (KeyError, ValueError, TypeError):
                logger.exception(
                    f"RDS 항목 형식 오류(다음 fallback 단계로 넘어감): "
                    f"table={table_name} segment_id={sid}"
                )

        remaining = [sid for sid in unique_ids if sid not in resolved]
        if remaining:
            default_value = _lookup_global_default(table_name)
            for sid in remaining:
                resolved[sid] = default_value
                tier[sid] = "global"
    except Exception:
        logger.exception(f"RDS batch_get_items 실패 -> S3 스냅샷으로 폴백: table={table_name}")
        _load_length_snapshot_once()
        fallback_default = _length_snapshot.get(GLOBAL_PARTITION_KEY, _HARDCODED_DEFAULTS[2])
        for sid in unique_ids:
            if sid not in resolved:
                resolved[sid] = round(_length_snapshot.get(sid, fallback_default))
                tier[sid] = "snapshot" if sid in _length_snapshot else "hardcoded"
    finally:
        logger.info(
            f"[nav_lookup] RDS type2 배치 조회 소요 시간: "
            f"{(time.monotonic() - started) * 1000:.0f}ms"
        )

    # 요청당 한 번만 남긴다 - Grafana의 "Type2 fallback 계층 비율" 패널이
    # 이 로그를 집계한다. rds=세그먼트 자체 row를 RDS에서 직접 찾음,
    # global=RDS는 살아있지만 이 세그먼트 값이 없어 GLOBAL 기본값 사용,
    # snapshot=RDS 자체가 죽어 S3 스냅샷의 세그먼트별 값 사용,
    # hardcoded=스냅샷에도 없어 코드 상수 사용.
    tier_counts = {"rds": 0, "global": 0, "snapshot": 0, "hardcoded": 0}
    for sid in segment_ids:
        tier_counts[tier.get(sid, "hardcoded")] += 1
    logger.info(
        f"[type2_fallback_tier_summary] rds={tier_counts['rds']} "
        f"global={tier_counts['global']} snapshot={tier_counts['snapshot']} "
        f"hardcoded={tier_counts['hardcoded']} total={len(segment_ids)}"
    )

    return [resolved[sid] for sid in segment_ids]


def _resolve_time_values(segment_ids: list[str], table_name: str, time_str: str) -> list[int]:
    """Type1(소요시간)은 segment_ids를 경로 순서로 간주한다. 세그먼트 k의
    조회 시각은 "요청 시각 + 세그먼트 1..k-1의 소요시간 합"이다 - 그
    세그먼트에 실제로 도착하는 시점의 슬롯을 봐야 하기 때문이다. 이 누적
    시각은 앞 세그먼트의 조회 *결과*에 의존하므로 순서대로(순차) 처리해야
    하고, 같은 segment_id가 경로에 두 번 나와도(루프) 등장 위치의 누적
    시각이 다르면 값도 다를 수 있어 중복 제거를 하지 않는다.

    RDS 조회는 요청당 딱 한 번(_batch_fetch_type1_rows)만 하고, 이후
    누적시각 계산은 그 결과 dict만 보고 순수 로컬 연산으로 끝낸다 - 그
    한 번의 호출이 실패하면(RDS 자체 장애) 모든 세그먼트를 메모리/S3
    폴백으로 채운다. RDS 호출이 요청당 하나뿐이라 "연속 실패"나 "순차
    호출이 쌓여 느려짐" 같은 문제 자체가 없어져 별도 circuit
    breaker/시간 예산이 필요 없다."""
    try:
        rows_by_segment = _batch_fetch_type1_rows(segment_ids, table_name)
    except Exception:
        logger.exception(
            f"RDS 배치 조회 실패 -> 전체 세그먼트 메모리/S3 폴백으로 응답: table={table_name}"
        )
        rows_by_segment = None

    values: list[int] = []
    elapsed_seconds = 0
    # Grafana 대시보드의 fallback 히트율(fresh/avg/hardcoded 비율) 집계용 -
    # 세그먼트마다 로그를 남기면 요청당 수백 줄까지 늘어날 수 있어서, 요청
    # 하나당 요약 한 줄만 마지막에 남긴다.
    tier_counts = {"fresh": 0, "avg": 0, "hardcoded": 0}

    for sid in segment_ids:
        lookup_time = _add_seconds(time_str, elapsed_seconds)
        bucket = time_to_bucket(lookup_time)

        if rows_by_segment is None:
            # RDS 자체가 응답 불가능했던 경우 - 메모리/S3 폴백으로 내려간다.
            value, tier = _resolve_from_fallback(sid, bucket)
        else:
            row = rows_by_segment.get(sid, {}).get(bucket)
            if row is not None:
                _remember_in_memory(sid, bucket, row)
            value, tier = _resolve_from_row(row)
            if value is None:
                # RDS는 정상 응답했지만 이 슬롯 자체가 없는 경우 - "RDS가
                # 죽었을 때의 예전 값"과 성격이 다르므로 메모리/S3로 내려가지
                # 않고 곧장 코드 상수로 간다.
                value, tier = _HARDCODED_DEFAULTS[1], "hardcoded"

        tier_counts[tier] += 1
        values.append(value)
        elapsed_seconds += value

    logger.info(
        f"[fallback_tier_summary] type=1 fresh={tier_counts['fresh']} "
        f"avg={tier_counts['avg']} hardcoded={tier_counts['hardcoded']} "
        f"total={len(segment_ids)}"
    )
    return values
