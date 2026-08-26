"""
Gold2 — type1(시간) 최종 산출물 계산 + RDS 포맷/upsert

30분 버킷 하나엔 그 30분 동안 들어온 5분 단위 판독값이 최대 6개 있다.
시간순으로 1,2,...,n번째 판독값에 1:2:...:n 비율로 증가하는 가중치(최근
값이 가장 큰 비중)를 준 가중평균 속도를 구하고, LION 길이(length_ft)로
나눠 세그먼트별 통행시간(초)을 구한다.

과거 평균(avg)은 세그먼트 전체가 아니라 "이 (segment_id, time) 슬롯"
단위다 - 한 행 안에 오늘 실측값(value)과 그 슬롯의 과거 평균(avg)이 같이
있어서, 서빙 쪽(src/serving/nav_lookup.py)이 "오늘 값이 있으면 그걸,
없으면 평균을" 판단을 조회 한 번으로 끝낼 수 있다. 이번 실행에서 바뀐
슬롯 하나만큼만 증분 갱신한다(48개 슬롯을 매번 다 다시 읽지 않음).

RDS에는 슬롯 값과 그 슬롯의 avg를 함께 upsert하고, 성공하면 S3 Gold
스냅샷도 갱신한다(src/common/gold_snapshot.py) - DynamoDB에서 RDS로
옮기며 잃은 멀티 AZ 자동 failover를 보완하기 위해, RDS 자체가 응답
불가능할 때 서빙 쪽이 이 스냅샷으로 대체한다(src/serving/nav_lookup.py 참고).

단위: SPEED는 mph, length_ft는 feet. 시간(초) = (길이_ft / 5280) / 속도_mph * 3600.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from psycopg2 import sql
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat,
    count as spark_count,
    floor,
    hour,
    lpad,
    max as spark_max,
    minute,
    row_number,
    sum as spark_sum,
)

from src.common import gold_snapshot
from src.common.config import BUCKET_MINUTES, SERVING_TABLE_TYPE1_KEY_COLUMNS
from src.common.db import batch_get_items, batch_write_items, get_shared_connection
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_time_gold2")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0

# 슬롯별 avg 갱신 시 새 값의 최대 반영 비중을 1/AVG_SMOOTHING_WINDOW로
# 묶는다. 저장되는 count 자체는 계속 늘어나지만(그 슬롯이 총 몇 번
# 갱신됐는지에 대한 정직한 기록), 평균 공식에서 나누는 수는 이 값에서
# 멈춘다 - 안 그러면 배치가 수백 개 쌓였을 때 새 값 하나의 영향력이
# 1/수백로 사라져서, 도로 공사 등으로 실제 통행시간이 바뀌어도 avg가
# 거의 반응하지 않게 된다.
AVG_SMOOTHING_WINDOW = 10


def _bucket_column():
    bucket_minute = floor(minute("observed_at") / BUCKET_MINUTES) * BUCKET_MINUTES
    return concat(
        lpad(hour("observed_at").cast("string"), 2, "0"),
        lpad(bucket_minute.cast("int").cast("string"), 2, "0"),
    )


def compute_time_seconds(silver2_df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame:
    """(segment_id, speed, observed_at)를 30분 버킷별 가중평균 통행시간(초)으로 집계한다.

    한 버킷 안에서 시간순으로 매긴 순위(rank)를 가중치로 쓴다 — n개 판독값이면
    1:2:...:n 비율(최근 값일수록 크게), 삼각수 n*(n+1)/2로 정규화한다.

    last_observed_at은 그 버킷을 구성한 판독값들의 observed_at 중 가장 최근
    값이다 - RDS에 저장된 버킷 값이 어느 원본 데이터로 계산됐는지 나타내는
    식별자이자(to_serving_items의 재시도 판별용), 서빙 쪽이 "오늘 값인지"
    (freshness) 판단할 때 날짜 부분만 뽑아 쓰는 값이기도 하다.
    """

    spark = silver2_df.sparkSession
    length_df = spark.createDataFrame(dim_segment_length_df[["segment_id", "length_ft"]])

    bucketed = silver2_df.withColumn("bucket", _bucket_column())

    window_spec = Window.partitionBy("segment_id", "bucket").orderBy("observed_at")
    ranked = bucketed.withColumn("rank", row_number().over(window_spec))

    counts = ranked.groupBy("segment_id", "bucket").agg(spark_count("*").alias("n"))
    ranked = ranked.join(counts, on=["segment_id", "bucket"])

    weighted = ranked.withColumn(
        "weighted_speed",
        col("speed") * col("rank") / (col("n") * (col("n") + 1) / 2),
    )

    bucket_avg_speed = (
        weighted.groupBy("segment_id", "bucket")
        .agg(
            spark_sum("weighted_speed").alias("avg_speed"),
            spark_max("observed_at").alias("last_observed_at"),
        )
        .filter(col("avg_speed") > 0)
    )

    joined = bucket_avg_speed.join(length_df, on="segment_id", how="inner")

    return joined.select(
        "segment_id",
        "bucket",
        "last_observed_at",
        (
            (col("length_ft") / _FEET_PER_MILE) / col("avg_speed") * _SECONDS_PER_HOUR
        ).alias("time_seconds"),
    )


def validate_bucket_time_seconds(bucket_df: DataFrame) -> DataFrame:
    """RDS에 쓰기 전 계산된 통행시간이 0보다 큰지 확인한다.

    Gold1(filter_valid_speed)은 속도 판독값만 보고 걸러서, length_ft<=0인
    세그먼트(LION dim_segment에 그런 행이 섞여 있을 수 있음 - nav_length의
    Gold1은 is_routable/length_ft>0으로 거르지만 이 도메인은 그 필터를
    타지 않는다)는 여기까지 그대로 들어와 time_seconds<=0으로 계산될 수
    있다. 이런 값이 서빙 테이블에 그대로 upsert되면 nav_lookup.py가 잘못된
    통행시간을 응답하게 되므로, 쓰기 직전에 걸러서 즉시 실패시킨다 -
    다음 정상 실행(30분 뒤)까지 기다리는 대신 원인을 바로 알 수 있게
    하기 위함이다."""

    invalid = bucket_df.filter(col("time_seconds") <= 0).limit(1).count()
    if invalid:
        raise ValueError(
            "Type1(시간) 계산 결과에 0 이하의 통행시간이 있습니다 "
            "(length_ft<=0인 세그먼트가 섞였을 가능성 - dim_segment 확인 필요)"
        )
    return bucket_df


def to_serving_items(bucket_df: DataFrame, table_name: str, *, today: date | None = None) -> list[dict]:
    """버킷별 값을 RDS 항목 리스트로 변환한다 - 슬롯별 과거 평균(avg)도
    증분 갱신해서 같은 행에 같이 싣는다.

    bucket_df는 compute_time_seconds의 반환값이어야 한다 - segment_id/bucket/
    time_seconds뿐 아니라 last_observed_at(TimestampType) 컬럼도 필수다.

    avg는 이 (segment_id, bucket) 슬롯 자체의 과거 평균이다(세그먼트 전체
    평균이 아니다) - 증분 갱신 공식:
      - 이 슬롯에 기존 행이 아예 없으면(최초 적재): new_avg = new_value, new_count = 1
      - 기존 행의 last_sample_at이 이번 값과 같으면(Airflow 재시도 등으로
        같은 배치가 다시 들어온 경우): avg/count를 그대로 승계한다 - 이미
        반영한 배치를 또 반영하면 그 배치가 평균에 중복으로 잡힌다.
      - 그 외(진짜 새 배치): new_count = old_count + 1,
        new_avg = old_avg + (new_value - old_avg) / min(new_count, AVG_SMOOTHING_WINDOW)
        (new_count 자체는 상한 없이 계속 증가 - 나누는 수만 묶는다)
      - count 없는 레거시 행(예전 스키마가 남긴 값)이면 몇 개로 만들어진
        평균인지 알 수 없어 리셋한다(new_avg = new_value, new_count = 1).

    last_sample_at을 배치 식별자로 쓰는 이유: 날짜 단위(옛 collected_date)로는
    같은 날 다시 들어오는 배치가 재시도인지, 지연 도착 데이터가 반영된
    진짜 새 배치인지 구분할 수 없다.

    today는 updated_date에 쓸 오늘 날짜다 - 테스트에서 고정된 날짜로
    검증하기 위해 인자로 받고, 안 넘기면 실제 오늘을 쓴다.
    """
    today = (today or date.today()).isoformat()

    rows = bucket_df.collect()

    new_items = [
        {
            "segment_id": row["segment_id"],
            "time": row["bucket"],
            "value": round(row["time_seconds"]),
            "last_sample_at": row["last_observed_at"].isoformat(),
        }
        for row in rows
    ]

    if not new_items:
        return []

    lookup_keys = [
        {"segment_id": item["segment_id"], "time": item["time"]} for item in new_items
    ]
    existing = batch_get_items(table_name, lookup_keys)

    items = []
    for item in new_items:
        key = (item["segment_id"], item["time"])
        old_row = existing.get(key)
        new_value = item["value"]

        if old_row is None:
            new_count = 1
            new_avg = new_value
        elif old_row.get("last_sample_at") == item["last_sample_at"]:
            # 이미 반영한 배치의 재시도 - avg/count를 그대로 승계하고
            # 또 한 번 반영하지 않는다(중복 카운트 방지).
            new_avg = float(old_row.get("avg", new_value))
            new_count = int(old_row["count"]) if old_row.get("count") is not None else 1
        else:
            old_count = int(old_row["count"]) if old_row.get("count") is not None else 0
            if old_count == 0:
                # count 없는 레거시 행(값은 있는데 몇 개로 만들어진 평균인지
                # 모름) - 델타를 섞으면 평균이 발산하므로 리셋한다.
                new_count = 1
                new_avg = new_value
            else:
                old_avg = float(old_row.get("avg", 0))
                new_count = old_count + 1
                new_avg = old_avg + (new_value - old_avg) / min(new_count, AVG_SMOOTHING_WINDOW)

        items.append({
            **item,
            "avg": round(new_avg),
            "count": new_count,
            "updated_date": today,
        })

    return items


def _export_snapshot(table_name: str) -> dict[str, dict[str, dict]]:
    """S3 Gold 스냅샷(gold_snapshot.py)에 실어보낼 원본을 RDS 전체에서 뽑는다.

    부분 병합이 아니라 매번 테이블 전체를 다시 내보낸다 - 이 파이프라인
    하나만 이 스냅샷 파일을 쓰므로 경합 문제는 없지만, "RDS 현재 상태를
    그대로 다시 내보내기"가 어차피 제일 단순하고 안전하다. 반환값은
    segment_id -> {time: {"value","avg","last_sample_at"}}."""
    conn = get_shared_connection()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT segment_id, time, value, avg, last_sample_at FROM {table}").format(
                table=sql.Identifier(table_name)
            )
        )
        rows = cur.fetchall()

    snapshot: dict[str, dict[str, dict]] = {}
    for segment_id, time_slot, value, avg, last_sample_at in rows:
        entry = {}
        if value is not None and last_sample_at is not None:
            entry["value"] = float(value)
            entry["last_sample_at"] = last_sample_at.isoformat()
        if avg is not None:
            entry["avg"] = float(avg)
        if entry:
            snapshot.setdefault(segment_id, {})[time_slot] = entry
    return snapshot


def write_to_rds(items: list[dict], table_name: str) -> int:
    """RDS(PostgreSQL)에 upsert하고, 성공하면 S3 Gold 스냅샷도 최신
    상태로 다시 내보낸다(src/serving/nav_lookup.py의 RDS 장애 폴백이
    읽는 것). 스냅샷 갱신 자체가 실패해도 RDS 쓰기는 이미 끝난 뒤라
    파이프라인을 실패시키지 않는다 - 다음 정상 실행 때 다시 시도되면
    충분하다."""
    batch_write_items(table_name, items, key_columns=SERVING_TABLE_TYPE1_KEY_COLUMNS)
    logger.info(f"[nav_time_gold2] RDS upsert 완료: table={table_name} count={len(items)}")

    try:
        snapshot = _export_snapshot(table_name)
        gold_snapshot.write_snapshot("type1", snapshot)
    except Exception:
        logger.exception("[nav_time_gold2] S3 Gold 스냅샷 갱신 실패(RDS 쓰기 자체는 성공)")

    return len(items)
