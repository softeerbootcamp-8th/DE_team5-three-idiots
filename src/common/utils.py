"""공통 유틸."""

import requests
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session():
    """네트워크 오류 시 자동 재시도하는 세션."""
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry),
    )

    return session


def save_parquet(df, out_dir, filename="data.parquet"):
    """DataFrame을 parquet으로 저장한다.

    out_dir는 S3Path다 — S3는 업로드가 완료된 객체만 노출하므로(부분 쓰기가
    안 보임) 로컬 파일시스템에서 하던 tmp-then-rename 흉내가 필요 없다.
    pandas가 S3Path를 로컬 캐시 경로로 오해하지 않도록 str()로 넘긴다.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    final = out_dir / filename
    df.to_parquet(str(final), index=False)

    return final

def clean_street(value):
    """
    도로명 공백/대소문자를 정리한다.
 
    원본 데이터는 "WEST   19 STREET", "  12 STREET" 처럼
    앞뒤·중간 공백이 불규칙해 그대로 두면 JOIN이 실패한다.
    """
 
    if not isinstance(value, str):
        return None
 
    cleaned = re.sub(r"\s+", " ", value).strip().upper()
 
    return cleaned or None
 