"""TLC Type 3의 Spark 롤링 계산과 RDS(PostgreSQL) Gold 적재 로직."""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    avg,
    broadcast,
    col,
    count,
    dayofweek,
    element_at,
    floor,
    format_string,
    hour,
    lit,
    max as spark_max,
    minute,
    to_date,
)

from src.common import db
from src.common.config import (
    SERVING_TABLE_TYPE3_KEY_COLUMNS,
    TLC_TYPE3_DOW_NAMES,
    TLC_TYPE3_ID,
)
from src.common.spark import to_spark_path


TYPE_ID = TLC_TYPE3_ID
TIME_SLOT_MINUTES = 30
TIME_SLOTS = tuple(
    f"{hour_value:02d}{minute_value:02d}"
    for hour_value in range(24)
    for minute_value in range(0, 60, TIME_SLOT_MINUTES)
)
DATE_PARTITION_PATTERN = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")

# expand_zone_values_to_segments의 zone→segment fan-out join 결과를 몇
# 파티션으로 나눌지. broadcast join은 결과 파티션 수를 왼쪽(segments)
# 파티션 수 그대로 물려받는데, mapping parquet 파일 자체의 파티션 수(보통
# 1~2개)에 맡겨두면 세그먼트 수가 많을 때(7,300만 건까지) 태스크 한두
# 개에 다 몰려서 OOM으로 executor가 죽는 사고가 있었다.
SEGMENT_EXPANSION_PARTITIONS = 200

# RDS 쓰기 병렬도. executor마다 자기 파티션을 독립 커넥션으로 쓰게 해서,
# driver가 toLocalIterator()로 한 줄씩 순차 처리할 때보다 wall-clock을
# 파티션 수만큼 나눈다(Airflow heartbeat timeout 예방 — segment 수가 많으면
# 순차 처리가 5분을 넘겨 태스크가 강제 종료되는 사고가 있었다).
#
# DynamoDB 시절엔 순간 처리량 한도(RequestLimitExceeded) 때문에 32에서
# 10으로 낮췄었다 - 그때는 세그먼트당 항목 1개(JSONB 중첩)라 총 21.8만
# 행이었다. 2026-08-24 스키마 개편으로 세그먼트당 336행(요일×시간 슬롯)으로
# flat하게 펼치면서 총 행 수가 위 SEGMENT_EXPANSION_PARTITIONS와 똑같은
# 규모(7,300만 건)까지 늘었는데, 이 값을 안 맞춰서 EMR 잡이
# ExecutorDeadException/FetchFailedException으로 실패하는 사고가 실제로
# 있었다.
#
# SEGMENT_EXPANSION_PARTITIONS(위)가 이미 겪었던 것과 똑같은 사고다 —
# expand_zone_values_to_segments가 7,300만 건을 200개 파티션으로 잘 나눠서
# 넘겨주는데, 여기서 그걸 10개로 다시 뭉쳐버리면서(repartition) 그 이전
# 수정이 무효화됐다. 같은 값으로 맞춘다 - RDS는 "동시 커넥션 수"가
# 한도라(소형 인스턴스 기준 max_connections 보통 100 안팎) 파티션 수
# 자체보다 "동시에 실행되는 파티션 수 × 아래 스레드 수"가 실제 동시
# 커넥션 수를 결정한다(동시 실행 파티션 수는 executor 코어 수만큼으로
# 제한됨) - 파티션 수만 늘리는 건 이 한도에 영향을 안 준다.
TYPE3_RDS_WRITE_PARTITIONS = SEGMENT_EXPANSION_PARTITIONS

# 파티션 하나(executor 하나) 안에서 쓰기를 몇 개의 스레드로 나눠 돌릴지.
# RDS 쓰기도 CPU가 아니라 네트워크 왕복 대기가 지배적인 I/O bound 작업이라,
# 파티션 내부도 스레드로 쪼개면 wall-clock을 줄일 수 있다(세그먼트 수가
# 늘어나 파티션당 수백만 건을 순차로 쓰면 시간이 선형으로 늘어나는 걸
# 완화하기 위함). 스레드마다 db.new_connection()으로 자기만의 커넥션을 열어
# 쓴다(psycopg2 커넥션은 스레드 간 공유가 안전하지 않음).
# 동시 커넥션 수는 대략 TYPE3_RDS_WRITE_PARTITIONS × 이 값이므로(기본
# 200 × 2 = 400이지만, 동시 실행 파티션 수는 executor 코어 수로 제한되니
# 실제 동시 커넥션은 그보다 훨씬 작다), RDS의 max_connections를 넘지
# 않게 여유를 두고 안전이 확인되면 조심스럽게 올릴 것. PgBouncer/RDS
# Proxy 앞단이 있다면 그쪽 풀 크기도 같이 고려해야 한다.
TYPE3_RDS_WRITE_THREADS_PER_PARTITION = 2

TAXI_ZONE_IDS = tuple(range(1, 264))
DOW_NAMES = TLC_TYPE3_DOW_NAMES
SPARK_DOW_NAMES = (DOW_NAMES[-1], *DOW_NAMES[:-1])


def _month_bounds(service_month: str) -> tuple[date, date]:
    """YYYY-MM의 시작일과 다음 달 시작일을 반환한다."""

    year, month = (int(value) for value in service_month.split("-"))
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return start, end


def build_daily_zone_frame(
    spark: SparkSession,
    silver_paths: list[str],
    service_month: str | None = None,
    zone_ids: tuple[int, ...] = TAXI_ZONE_IDS,
) -> DataFrame:
    """TLC 운행을 Zone×날짜×30분 단위의 Gold2 승차 건수로 만든다."""

    if not silver_paths:
        raise ValueError("Type 3 날짜별 기록을 만들 Silver1 경로가 없습니다")
    if not zone_ids:
        raise ValueError("Type 3 집계 대상 Taxi Zone이 없습니다")

    source = spark.read.parquet(*silver_paths).select(
        "pickup_datetime",
        "pickup_location_id",
    )
    valid = source.filter(
        col("pickup_datetime").isNotNull()
        & col("pickup_location_id").between(1, 263)
    )
    if service_month is not None:
        month_start, next_month_start = _month_bounds(service_month)
        valid = valid.filter(
            (to_date(col("pickup_datetime")) >= lit(month_start))
            & (to_date(col("pickup_datetime")) < lit(next_month_start))
        )
    if not valid.limit(1).count():
        raise ValueError(f"Type 3 유효 승차 데이터가 없습니다: {service_month or '전체'}")

    prepared = (
        valid
        .withColumn("date", to_date(col("pickup_datetime")))
        .withColumn(
            "slot_minute",
            (floor(minute(col("pickup_datetime")) / 30) * 30).cast("int"),
        )
        .withColumn(
            "time",
            format_string(
                "%02d%02d",
                hour(col("pickup_datetime")),
                col("slot_minute"),
            ),
        )
    )
    zone_counts = (
        prepared
        .groupBy(
            col("pickup_location_id").cast("int").alias("zone_id"),
            "date",
            "time",
        )
        .agg(count("*").cast("double").alias("value"))
    )
    dates = prepared.select("date").distinct()
    slots = spark.createDataFrame([(slot,) for slot in TIME_SLOTS], ["time"])
    zones = spark.createDataFrame(
        [(int(zone_id),) for zone_id in zone_ids],
        ["zone_id"],
    )
    zone_grid = zones.crossJoin(dates).crossJoin(slots)
    return (
        zone_grid
        .join(zone_counts, on=["zone_id", "date", "time"], how="left")
        .fillna({"value": 0.0})
        .select(
            "zone_id",
            lit(TYPE_ID).cast("int").alias("type"),
            "date",
            "time",
            col("value").cast("double").alias("value"),
        )
    )


def validate_daily_zone_month(
    spark: SparkSession,
    stage_path,
    service_month: str,
    expected_zone_ids: tuple[int, ...] = TAXI_ZONE_IDS,
) -> dict:
    """운영 경로로 승격하기 전 월별 Zone Gold2 결과를 검증한다."""

    staged = (
        spark.read
        .option("basePath", to_spark_path(stage_path))
        .parquet(to_spark_path(stage_path))
        .withColumn("date", to_date(col("date")))
    )
    required_columns = {"zone_id", "type", "date", "time", "value"}
    missing_columns = required_columns - set(staged.columns)
    if missing_columns:
        raise ValueError(f"Type 3 staging 필수 컬럼 없음: {missing_columns}")

    month_start, next_month_start = _month_bounds(service_month)
    invalid = staged.filter(
        col("zone_id").isNull()
        | col("type").isNull()
        | col("date").isNull()
        | col("time").isNull()
        | col("value").isNull()
        | (col("type") != TYPE_ID)
        | (col("value") < 0)
        | ~col("time").isin(list(TIME_SLOTS))
        | (col("date") < lit(month_start))
        | (col("date") >= lit(next_month_start))
    ).limit(1).count()
    if invalid:
        raise ValueError(f"Type 3 staging 값 검증 실패: {service_month}")

    duplicate = (
        staged.groupBy("zone_id", "type", "date", "time")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError(f"Type 3 staging 복합 키 중복: {service_month}")

    expected_dates = {
        month_start + timedelta(days=offset)
        for offset in range((next_month_start - month_start).days)
    }
    actual_dates = {row["date"] for row in staged.select("date").distinct().collect()}
    if actual_dates != expected_dates:
        missing_dates = sorted(expected_dates - actual_dates)
        extra_dates = sorted(actual_dates - expected_dates)
        raise ValueError(
            f"Type 3 staging 날짜 불일치: month={service_month} "
            f"missing={missing_dates[:5]} extra={extra_dates[:5]}"
        )

    expected_zones = set(expected_zone_ids)
    actual_zones = {
        row["zone_id"]
        for row in staged.select("zone_id").distinct().collect()
    }
    if actual_zones != expected_zones:
        raise ValueError(
            f"Type 3 staging Zone coverage 불일치: "
            f"{len(actual_zones)}/{len(expected_zones)}"
        )

    actual_rows = staged.count()
    expected_rows = len(expected_zones) * len(expected_dates) * len(TIME_SLOTS)
    if actual_rows != expected_rows:
        raise ValueError(
            f"Type 3 staging 행 수 불일치: {actual_rows}/{expected_rows} "
            f"(month={service_month})"
        )

    return {
        "month": service_month,
        "rows": actual_rows,
        "zones": len(actual_zones),
        "dates": len(actual_dates),
    }


def select_latest_date_partitions(
    partition_paths,
    rolling_days: int = 28,
) -> tuple[list, date, date]:
    """date=YYYY-MM-DD 경로 중 최신 N개의 연속 파티션을 선택한다."""

    if rolling_days <= 0:
        raise ValueError("rolling_days는 1 이상이어야 합니다")

    dated_paths = {}
    for path in partition_paths:
        match = DATE_PARTITION_PATTERN.match(path.name)
        if not match:
            continue
        partition_date = date.fromisoformat(match.group(1))
        if partition_date in dated_paths:
            raise ValueError(f"Type 3 날짜 파티션 중복 발견: {partition_date}")
        dated_paths[partition_date] = path

    if len(dated_paths) < rolling_days:
        raise ValueError(
            f"최근 {rolling_days}일 계산에 필요한 날짜 파티션이 부족합니다: "
            f"{len(dated_paths)}개"
        )

    selected_dates = sorted(dated_paths)[-rolling_days:]
    window_start = selected_dates[0]
    window_end = selected_dates[-1]
    expected_dates = [
        window_start + timedelta(days=offset)
        for offset in range(rolling_days)
    ]
    if selected_dates != expected_dates:
        missing_dates = sorted(set(expected_dates) - set(selected_dates))
        raise ValueError(
            f"최근 {rolling_days}개 날짜 파티션이 연속적이지 않습니다. "
            f"누락 날짜: {missing_dates[:5]}"
        )

    return [dated_paths[value] for value in selected_dates], window_start, window_end


def build_weekday_rolling_frame(
    daily: DataFrame,
    rolling_weeks: int,
) -> tuple[DataFrame, date, date]:
    """최신 N주에서 같은 요일·시간대의 평균을 만든다."""

    if rolling_weeks <= 0:
        raise ValueError("rolling_weeks는 1 이상이어야 합니다")

    latest_date = daily.agg(spark_max("date").alias("latest_date")).first()["latest_date"]
    if latest_date is None:
        raise ValueError("Type 3 날짜별 S3 기록이 비어 있습니다")
    window_days = rolling_weeks * len(DOW_NAMES)
    window_start = latest_date - timedelta(days=window_days - 1)

    window = daily.filter(col("date").between(lit(window_start), lit(latest_date)))
    if not window.limit(1).count():
        raise ValueError("Type 3 롤링 윈도우에 데이터가 없습니다")

    actual_dates = {row["date"] for row in window.select("date").distinct().collect()}
    expected_dates = {
        window_start + timedelta(days=offset)
        for offset in range(window_days)
    }
    if actual_dates != expected_dates:
        missing_dates = sorted(expected_dates - actual_dates)
        raise ValueError(
            f"최근 {rolling_weeks}주 연속 데이터가 필요합니다. "
            f"누락 날짜: {missing_dates[:5]}"
        )

    rolling = (
        window
        .withColumn(
            "dow",
            element_at(
                array(*(lit(name) for name in SPARK_DOW_NAMES)),
                dayofweek(col("date")),
            ),
        )
        .groupBy("zone_id", "type", "dow", "time")
        .agg(
            avg("value").cast("double").alias("value"),
            count("*").alias("sample_count"),
        )
    )
    return rolling, window_start, latest_date


def expand_zone_values_to_segments(
    rolling: DataFrame,
    mapping: DataFrame,
) -> DataFrame:
    """작은 Zone 평균 결과를 마지막에 Segment 서빙 단위로 확장한다.

    Zone당 평균 하나가 그 zone에 속한 segment 개수만큼(요일×시간까지 곱하면
    세그먼트 21만 개 기준 7,300만 건까지) 복제되는 fan-out join이다.
    broadcast join의 결과 파티션 수는 왼쪽(segments)의 파티션 수를 그대로
    물려받는데, mapping이 원본 parquet 파일 그대로라 파티션이 몇 개
    안 되면(실측 1~2개) 이 7,300만 건이 태스크 한두 개에 몰려서 OOM으로
    executor가 죽는 사고가 실제로 있었다(shuffle map stage에서 exit code
    137로 반복 종료). join 전에 segments를 넉넉히 repartition해서 fan-out
    결과가 여러 태스크에 고르게 퍼지게 한다."""

    missing_rolling = {"zone_id", "type", "dow", "time", "value"} - set(
        rolling.columns
    )
    if missing_rolling:
        raise ValueError(f"Type 3 Zone 평균 필수 컬럼 없음: {missing_rolling}")
    missing_mapping = {"segment_id", "zone_id"} - set(mapping.columns)
    if missing_mapping:
        raise ValueError(f"Zone-Segment 매핑 필수 컬럼 없음: {missing_mapping}")

    segments = mapping.select(
        col("segment_id").cast("string").alias("segment_id"),
        col("zone_id").cast("int").alias("zone_id"),
    ).repartition(SEGMENT_EXPANSION_PARTITIONS, "zone_id")
    return (
        segments
        .join(broadcast(rolling), on="zone_id", how="inner")
        .select("segment_id", "type", "dow", "time", "value")
    )


def validate_segment_values(
    segment_values: DataFrame,
    mapping: DataFrame,
) -> dict:
    """각 Segment에 요일 7개×시간 48개의 Type 3 값이 있는지 검증한다."""

    required = {"segment_id", "type", "dow", "time", "value"}
    missing = required - set(segment_values.columns)
    if missing:
        raise ValueError(f"Type 3 Segment Gold2 필수 컬럼 없음: {missing}")

    segments = mapping.select(
        col("segment_id").cast("string").alias("segment_id"),
        col("zone_id").cast("int").alias("zone_id"),
    )
    if segments.filter(col("segment_id").isNull() | col("zone_id").isNull()).limit(1).count():
        raise ValueError("Zone-Segment 매핑에 NULL이 있습니다")
    if segments.groupBy("segment_id").count().filter(col("count") > 1).limit(1).count():
        raise ValueError("Zone-Segment 매핑의 segment_id가 중복됩니다")

    invalid = segment_values.filter(
        col("segment_id").isNull()
        | col("type").isNull()
        | col("dow").isNull()
        | col("time").isNull()
        | col("value").isNull()
        | (col("type") != TYPE_ID)
        | (col("value") < 0)
        | ~col("dow").isin(list(DOW_NAMES))
        | ~col("time").isin(list(TIME_SLOTS))
    ).limit(1).count()
    if invalid:
        raise ValueError("RDS 적재 전 Type 3 Segment 값 검증 실패")

    duplicate = (
        segment_values
        .groupBy("segment_id", "type", "dow", "time")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError("Type 3 Segment Gold2 복합 키 중복 발견")

    expected_segments = segments.count()
    actual_segments = segment_values.select("segment_id").distinct().count()
    if actual_segments != expected_segments:
        raise ValueError(
            f"Type 3 Segment coverage 불일치: {actual_segments}/{expected_segments}"
        )

    actual_rows = segment_values.count()
    expected_rows = expected_segments * len(DOW_NAMES) * len(TIME_SLOTS)
    if actual_rows != expected_rows:
        raise ValueError(
            f"Type 3 Segment 행 수 불일치: {actual_rows}/{expected_rows}"
        )

    return {"segments": actual_segments, "rows": actual_rows}


def _write_rows_chunk(table_name: str, rows_chunk: list, collected_date: date) -> None:
    """행 묶음 하나를 자기만의 RDS 커넥션으로 upsert한다.

    psycopg2 커넥션은 스레드 간에 안전하게 공유할 수 없으므로, 청크마다
    (=스레드마다) db.new_connection()으로 새로 연다.

    row 하나가 (segment_id, dow, time) 슬롯 하나(값 하나)다 - 예전엔
    세그먼트당 336개 값을 JSONB 하나에 중첩해서 아이템 1개로 묶어 썼지만
    (DynamoDB 최소 과금 단위를 피하려는 목적, git 이력 참고), 실제 조회가
    항상 "세그먼트 여러 개 x 시각 하나"라서 dow/time을 진짜 컬럼으로 꺼내는
    flat 스키마로 바꿨다(src/common/config.py의 SERVING_TABLE_TYPE3_COLUMNS
    참고) - 세그먼트당 336행이 되지만 조회 쪽(src/serving/api.py)이 훨씬
    단순해진다."""

    if not rows_chunk:
        return

    items = [
        {
            "segment_id": str(row["segment_id"]),
            "dow": row["dow"],
            "time": str(row["time"]).zfill(4),
            "value": float(row["value"]),
            "collected_date": collected_date.isoformat(),
            "updated_date": date.today().isoformat(),
        }
        for row in rows_chunk
    ]

    conn = db.new_connection()
    try:
        db.batch_write_items(
            table_name,
            items,
            key_columns=SERVING_TABLE_TYPE3_KEY_COLUMNS,
            conn=conn,
        )
    finally:
        conn.close()


def _write_type3_partition(table_name: str, collected_date: date):
    """executor 파티션 하나를 다시 스레드 여러 개로 쪼개 RDS에 쓴다.

    RDS 쓰기는 네트워크 왕복 대기가 대부분인 I/O bound 작업이라, 파티션
    하나를 한 스레드가 순차로(for row in rows) 처리하면 세그먼트 수가 많을 때
    wall-clock이 그대로 늘어난다. 파티션 안에서도 TYPE3_RDS_WRITE_THREADS_PER_PARTITION개
    스레드로 나눠 동시에(스레드마다 자기 커넥션으로) 써서 이걸 완화한다.

    청크 중 하나에서 예외가 나면 as_completed 루프에서 future.result()가 그
    예외를 다시 던져서 write_type3_rolling_to_rds까지 전파된다. 그러면
    Airflow가 S3 완료 상태 파일을 쓰지 않으므로 다음 DAG 실행에서 다시
    처리한다."""

    def _write(rows) -> None:
        rows = list(rows)
        if not rows:
            return

        thread_count = min(TYPE3_RDS_WRITE_THREADS_PER_PARTITION, len(rows))
        chunks = [rows[i::thread_count] for i in range(thread_count)]

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = [
                executor.submit(_write_rows_chunk, table_name, chunk, collected_date)
                for chunk in chunks
            ]
            for future in as_completed(futures):
                future.result()

    return _write


def write_type3_rolling_to_rds(
    table_name: str,
    rolling: DataFrame,
    window_end: date,
) -> int:
    """검증된 Spark 롤링 결과를 executor 병렬로 Type 3 테이블에 저장한다.

    이전엔 driver가 toLocalIterator()로 한 줄씩 순차로 batch_writer를
    호출했다 — segment 수가 많으면(zone 값이 segment마다 복제되므로 수만~
    수십만 건) 이 태스크 하나가 Airflow heartbeat timeout(기본 300초)을
    넘겨 강제 종료되는 사고가 실제로 있었다. foreachPartition으로
    executor마다 자기 파티션을 병렬로 쓰게 바꿔서 wall-clock을 파티션
    수만큼 나눈다.

    window_end는 이 롤링 평균이 반영하는 TLC 데이터의 마지막 날짜다 -
    각 행의 collected_date로 그대로 찍는다(언제 계산됐는지가 아니라 어떤
    데이터 스냅샷을 반영하는지를 남기기 위함). updated_date는 이 함수가
    실제로 실행되는 날짜(오늘)로 따로 채운다.
    """

    to_write = rolling.select("segment_id", "dow", "time", "value").repartition(
        TYPE3_RDS_WRITE_PARTITIONS
    )
    written = to_write.count()
    to_write.foreachPartition(_write_type3_partition(table_name, window_end))
    return written
