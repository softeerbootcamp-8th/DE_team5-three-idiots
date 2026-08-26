"""내비게이션 데이터 파이프라인 공통 설정."""

import os
from pathlib import Path

from cloudpathlib import S3Path
from dotenv import load_dotenv


# find_dotenv()의 스택 프레임 추적 방식은 PySpark executor의 worker
# 프로세스처럼 콜스택이 얕은 곳에서 이 모듈이 import되면 AssertionError로
# 죽는다(EMR Serverless에서 foreachPartition 안에서 src.common.db를
# import할 때 실제로 발생). .env 경로를 직접 넘겨 find_dotenv() 호출
# 자체를 피한다. 파일이 없으면 조용히 넘어간다(예: EMR 컨테이너에는 .env가
# 없음).
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# TLC 원본 데이터
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# NYC DOT 실시간 도로 속도 데이터(Socrata API)
DATASETS = {
    "speed": "https://data.cityofnewyork.us/resource/i4gi-tjb9.json",
}

# 매일 다음 공개 후보 1개월과 최근 완료 3개월을 확인한다.
TLC_PUBLISH_LAG_MONTHS = 2
RECENT_MONTHS_WINDOW = 4
TLC_TIMEZONE = "America/New_York"
TLC_TYPE3_ID = 3
TLC_TYPE3_DOW_NAMES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
TLC_TYPE3_ROLLING_WEEKS = int(os.getenv("TLC_TYPE3_ROLLING_WEEKS", "8"))

# 로컬 경로
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
TMP_DIR = PROJECT_ROOT / "data" / "tmp"

# "local"은 로컬 디스크와 Spark local[*]를 사용한다. 운영 기본값은 "aws"다.
APP_ENV = os.getenv("APP_ENV", "aws")

# AWS에서는 정적 키 대신 EC2 IAM Role로 인증한다.
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_DATA = os.getenv("S3_BUCKET_DATA")

if APP_ENV == "local":
    BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
    SILVER1_DIR = PROJECT_ROOT / "data" / "silver1"
    SILVER2_DIR = PROJECT_ROOT / "data" / "silver2"
    GOLD2_DIR = PROJECT_ROOT / "data" / "gold2"
    GOLD_CACHE_DIR = PROJECT_ROOT / "data" / "gold_cache"
else:
    BRONZE_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/bronze")
    SILVER1_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/silver1")
    SILVER2_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/silver2")
    GOLD2_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/gold2")
    GOLD_CACHE_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/gold_cache")

# GOLD_CACHE_DIR: RDS가 완전히 응답 불가능할 때 쓰는 "마지막으로 성공한
# 값" 스냅샷 저장 위치(src/serving/nav_lookup.py의 S3 폴백 참고). Gold/S3는
# 이미 멀티 AZ로 복제되는 관리형 스토리지라 RDS(Multi-AZ 안 쓰면 단일
# 인스턴스)보다 훨씬 죽기 어렵다 - RDS 장애 시 이 스냅샷으로 대체한다.

# ==========================
# RDS (Gold 서빙 테이블) 설정
# ==========================
#
# nav 골드 데이터셋(segment_id x type 조회)은 원래 DynamoDB로 서빙했다 —
# 접근 패턴이 key-value 조회(BatchGetItem)뿐이라는 이유였다. 비용(요청
# 기반 과금이 30분 주기 대량 upsert 워크로드에 불리)과 계정 정책(DynamoDB
# 사용 금지)으로 RDS(PostgreSQL)로 전환했다 — src/common/db.py 참고.
# 자세한 배경은 docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md.

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

# ==========================
# EMR Serverless (Spark 잡 실행) 설정
# ==========================
#
# TLC Spark 잡(build_silver 등)을 Airflow worker 안에서 SparkSession으로
# 직접 여는 대신 EMR Serverless에 제출한다 — spark-master/worker 컨테이너를
# EC2에 상주시키지 않고, 무거운 컴퓨트를 온디맨드로 분리하기 위함
# (src/common/emr_serverless.py 참고). APP_ENV=local 로컬 개발 모드는 아직
# 이 경로를 지원하지 않는다 — EMR Serverless는 실제 AWS 계정이 있어야
# 제출 가능해서 로컬 대체 수단이 없다.

EMR_APPLICATION_ID = os.getenv("EMR_APPLICATION_ID")
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN")

EMR_JOBS_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/emr-jobs")

# segment_time/tlc_ingest_daily/tlc_type3_serving_daily 세 DAG가 EMR
# Serverless 계정 전체 vCPU 쿼터(64, Service Quotas 콘솔 값)를 동시에 나눠
# 쓰다가 서로 충돌하는 사고(2026-08)가 있었다. DAG별로 고정 예산을 나눠
# (합계 64: tlc_ingest 17 + tlc_type3_serving 30 + segment_time 17,
# Airflow pool_slots도 각 DAG 파일에서 같은 숫자를 씀) 그 안에서만
# executor를 쓰게 강제한다 - 실측 피크가 예산을 넘는 job(예:
# tlc-build-type3-stage 실측 최대 64 vCPU)도 있어 캡을 걸면 그만큼
# 느려지지만, 세 DAG가 항상 동시에 돌 수 있는 걸 우선한 트레이드오프다.
# driver가 고정 4 vCPU를 쓰므로(CloudWatch WorkerCpuAllocated 실측),
# maxExecutors = (예산 - 4) // 4.
EMR_MAX_EXECUTORS_TLC_INGEST = 3  # 예산 17
EMR_MAX_EXECUTORS_TLC_TYPE3_SERVING = 6  # 예산 30
EMR_MAX_EXECUTORS_SEGMENT_TIME = 3  # 예산 17

# ==========================
# HTTP 설정
# ==========================

# HEAD / GET Timeout
HTTP_TIMEOUT = 60
CHUNK_SIZE = 8192
USER_AGENT = {"User-Agent": "Navigation-Data-Project/1.0"}

# NYC Open Data(Socrata) 페이지당 최대 조회 건수
SOCRATA_PAGE_SIZE = 50_000

# Airflow 장애 알림
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ==========================
# 2016 하차 위경도 Hotspot 설정
# ==========================

# BigQuery로 받은 2016년 하차 위경도 grid(bq-results.csv)의 좌표계.
# TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 위경도 기준으로
# zone 내부 분포를 볼 수 있는 마지막 해 데이터다.
BQ_HOTSPOT_CRS = "EPSG:4326"

# zone 내부 세그먼트별 spatial_weight 계산 시, grid point가 0건 매칭된
# 세그먼트도 완전히 0이 되지 않게 하는 라플라스 스무딩 상수. 정성적 초안이다
# (TODO, 팀 검토 필요) — docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
LAPLACE_SMOOTHING_ALPHA = 1.0

# grid point 하나(8~11m 셀)가 세그먼트에 매칭될 때, 이 반경(feet) 이내 세그먼트
# 전부를 후보로 삼아 거리 역가중으로 나눠 배분한다. venue-도로 매핑에 쓴
# TICKETMASTER_LION_BUFFER_FT(200ft)보다 좁게 잡은 이유는 grid 셀 자체가 훨씬
# 작기 때문이다. 정성적 초안이다(TODO, 팀 검토 필요).
HOTSPOT_SEGMENT_BUFFER_FT = 100

# 반경 안 세그먼트에 거리 역가중(1/(distance+epsilon))을 매길 때, point가 세그먼트
# 위에 정확히 있어 distance=0이 되는 경우의 0-division만 막는 최소 상수. 정성적
# 초안이다(TODO, 팀 검토 필요).
HOTSPOT_INVERSE_DISTANCE_EPSILON_FT = 1.0

# ==========================
# 세그먼트 지표 API — RDS 서빙 저장소 설정
# ==========================
#
# 타입별로 완전히 분리된 테이블을 쓴다(팀원이 타입별로 독립 개발하기 때문 —
# 접두사 컨벤션이 아니라 물리적으로 다른 테이블). 자세한 설계 근거는
# docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md 6절 참고
# (DynamoDB 기준으로 쓰였지만 "타입별 별도 테이블" 논리는 RDS에도 동일하게
# 적용된다). PostgreSQL 테이블명은 소문자+언더스코어 컨벤션을 따른다.
#
# 테이블마다 스키마가 다르다 — DynamoDB 시절의 범용 sk/value 컬럼을
# 유지하지 않고 PostgreSQL에서 의미가 바로 드러나는 이름을 쓴다.
# *_COLUMNS는 기본키를 제외한 컬럼, *_KEY_COLUMNS는 그 테이블의 복합키를
# 정의한다. updated_date는 더 이상 db.py가 자동으로 채워주는 시스템
# 컬럼이 아니라 여기 COLUMNS에 명시된 일반 컬럼이다 - 호출부가 직접 값을
# 넣어야 한다(2026-08-24 스키마 개편, db.py의 ensure_table/batch_write_items
# 참고).

SERVING_TABLE_TYPE1 = os.getenv("SERVING_TABLE_TYPE1", "segment_metrics_type1")
# segment_id+time(HHMM 30분 슬롯)이 복합키다. 한 행 안에 오늘 실측값(value)과
# 그 슬롯의 과거 평균(avg)을 같이 들고 있어서, 폴백 판단(오늘 값이 없으면
# avg로)이 조회 한 번으로 끝난다(src/serving/nav_lookup.py 참고) - 팀원이
# 만든 type1 폴백 체인(Fresh Exact -> Historical AVG -> 코드 상수,
# PR #138/#139)과 같은 구조를 이 스키마 위에 얹은 것이다. 팀 버전에는 도로
# 스펙(길이/제한속도) 기반 추정치(SPEC Estimate) 단계가 하나 더 있었는데,
# 이 Gold 파이프라인은 그 값을 쓰지 않기로 해서 컬럼 자체를 안 둔다.
SERVING_TABLE_TYPE1_COLUMNS = {
    "value": "NUMERIC NOT NULL",
    "avg": "NUMERIC",
    "count": "INTEGER",
    # 이 슬롯 avg에 마지막으로 반영한 원본 판독값의 시각(observed_at 중
    # 최댓값). 같은 배치가 Airflow 재시도로 다시 들어와도 avg/count를 또
    # 증가시키지 않기 위한 식별자다 - collected_date(날짜 단위)는 같은 날
    # 다시 들어오는 배치(재시도든, 지연 도착 데이터로 인한 진짜 새
    # 배치든)를 구분 못 해서 이 용도로 못 쓴다(src/nav_time/gold2.py
    # to_serving_items 참고). 이 값에서 날짜만 뽑으면 collected_date와
    # 같은 정보라 별도 컬럼을 안 둔다.
    "last_sample_at": "TIMESTAMP",
    "updated_date": "DATE",
}
SERVING_TABLE_TYPE1_KEY_COLUMNS = ("segment_id", "time")

SERVING_TABLE_TYPE2 = os.getenv("SERVING_TABLE_TYPE2", "segment_metrics_type2")
# 길이는 시간과 무관해 세그먼트당 행 하나뿐이다. GLOBAL 행도 같은 value
# 컬럼에 기본값을 가진다. length_ft는 정적 참조값이라 "수집일"이라는
# 개념이 따로 없다 - collected_date는 항상 updated_date와 같은 값(Gold2
# 실행일)으로 채워지던 중복 컬럼이라 없앴다(2026-08-25 스키마 정리).
SERVING_TABLE_TYPE2_COLUMNS = {
    "value": "NUMERIC NOT NULL",
    "updated_date": "DATE",
}
SERVING_TABLE_TYPE2_KEY_COLUMNS = ("segment_id",)

SERVING_TABLE_TYPE3 = os.getenv("SERVING_TABLE_TYPE3", "segment_metrics_type3")
# 세그먼트당 요일×시간 슬롯마다 독립된 행이다 - segment_id+dow+time이
# 복합키다(예전엔 세그먼트당 1행에 336개 값을 JSONB로 중첩했었다 - 실제
# 조회가 항상 "세그먼트 여러 개 x 시각 하나"라서, dow/time을 진짜 컬럼으로
# 꺼내는 게 조회에 더 맞고 코드도 단순해진다). "요일" 대신 "dow"를 쓴다 -
# db.py의 _validate_identifier가 컬럼명을 ASCII만 허용해서(psycopg2.sql.
# Identifier로 안전하게 조립하기 전 방어용 정규식) 한글 컬럼명은 여기서
# 막힌다.
SERVING_TABLE_TYPE3_COLUMNS = {
    "value": "NUMERIC NOT NULL",
    "collected_date": "DATE",
    "updated_date": "DATE",
}
SERVING_TABLE_TYPE3_KEY_COLUMNS = ("segment_id", "dow", "time")

SERVING_TABLE_TYPE4 = os.getenv("SERVING_TABLE_TYPE4", "segment_metrics_type4")
# 통행료도 시간/요일 무관 - 세그먼트당 행 하나뿐이다. Type2와 같은 이유로
# collected_date를 없앴다 - 정적 요금표라 updated_date와 항상 같은 값이었다.
SERVING_TABLE_TYPE4_COLUMNS = {
    "value": "NUMERIC NOT NULL",
    "updated_date": "DATE",
}
SERVING_TABLE_TYPE4_KEY_COLUMNS = ("segment_id",)

# GLOBAL_PARTITION_KEY: 실제 segment_id가 아닌 예약된 PK — 배포 시점에 수동으로
# 심어두는 type2 전역 기본값 전용 파티션(scripts/seed_rds_defaults.py 참고).
GLOBAL_PARTITION_KEY = "GLOBAL"

# 하루를 30분 단위로 나눈 버킷 수(00:00~23:30 -> 48개). 버킷 키는 "HHMM" 문자열.
BUCKET_MINUTES = 30

# ==========================
# EMR Serverless (Spark job 실행) 설정
# ==========================
#
# Airflow worker 프로세스 안에서 SparkSession을 직접 여는 대신, 변환 로직을
# 담은 스크립트(spark_jobs/*.py)를 EMR Serverless에 제출하고 완료를 기다린다
# (src/common/emr_serverless.py 참고).

EMR_APPLICATION_ID = os.getenv("EMR_APPLICATION_ID")
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN")

if APP_ENV == "local":
    EMR_JOBS_DIR = PROJECT_ROOT / "data" / "emr-jobs"
else:
    EMR_JOBS_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/emr-jobs")

# EMR Serverless job이 spark.archives로 실어가는 패키징된 파이썬 venv
# (pandas/geopandas/shapely/pyproj 등 서드파티 의존성). requirements.txt가
# 바뀌면 scripts/package_emr_dependencies.sh로 다시 만들어 올려야 한다.
EMR_PYTHON_ENV_S3_PATH = EMR_JOBS_DIR / "python-env" / "pyspark_deps.tar.gz"

# ==========================
# 속도(speed) - LION 매핑 설정
# ==========================
#
# ticketmaster/gold1.py의 venue-LION 매핑과 동일한 buffer+nearest 패턴을
# 쓴다 — 대상이 Point(venue)가 아니라 LineString(속도 링크)이라는 점만 다르다.

SPEED_CRS = "EPSG:4326"

# NYC LION 좌표계(feet 단위) — 거리 계산은 이 좌표계로 변환해서 한다.
LION_CRS = "EPSG:2263"

# 속도 링크 주변 도로 매핑 반경(feet). 도로 링크는 보통 LION 세그먼트 여러
# 개로 쪼개져 있어(하나의 corridor가 여러 블록으로 나뉨), venue보다 좁게
# 잡아도 충분히 겹친다 — 정성적 초안(TODO, 팀 검토 필요).
SPEED_LION_BUFFER_FT = 50

# fallback nearest 매핑 품질 기준.
SPEED_LION_WARN_DISTANCE_FT = 200
SPEED_LION_MAX_DISTANCE_FT = 1000

# 이 미만인 속도 판독값은 계산에서 제외한다(0 또는 비정상적으로 낮은 값 —
# 정차/정지 상태로 잘못 기록된 값과 실제 정체를 구분하기 위한 정성적
# 초안, TODO 팀 검토 필요).
MIN_VALID_SPEED_MPH = 1.0
