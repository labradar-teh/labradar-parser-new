"""
Общая логика сессии, retry и запросов.
"""
import sys
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def build_session(extra_headers: Optional[dict] = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    headers = {**DEFAULT_HEADERS}
    if extra_headers:
        headers.update(extra_headers)
    session.headers.update(headers)
    return session


def fetch(session: requests.Session, url: str, timeout: int = 40) -> str:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


def safe_fetch(
    session: requests.Session,
    url: str,
    timeout: int = 40,
    label: str = "",
) -> Optional[str]:
    try:
        return fetch(session, url, timeout=timeout)
    except Exception as exc:
        tag = f"[{label}]" if label else ""
        print(f"{tag}[fetch-error] {url}: {exc}", file=sys.stderr)
        return None


def polite_fetch(
    session: requests.Session,
    url: str,
    delay: float = 0.15,
    timeout: int = 40,
    label: str = "",
) -> Optional[str]:
    """Fetch + sleep после запроса."""
    result = safe_fetch(session, url, timeout=timeout, label=label)
    time.sleep(delay)
    return result
