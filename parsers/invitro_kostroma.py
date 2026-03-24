#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict, deque
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "invitro"
CITY = "Кострома"
BASE_URL = "https://www.invitro.ru"
START_URL = f"{BASE_URL}/analizes/for-doctors/kostroma/"
DEFAULT_DELAY = 0.15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def fetch(session: requests.Session, url: str, timeout: int = 40) -> str:
    response = session.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    return response.text


def safe_fetch(session: requests.Session, url: str, timeout: int = 40) -> str | None:
    try:
        return fetch(session, url, timeout=timeout)
    except Exception as exc:
        print(f"[invitro][fetch-error] {url}: {exc}", file=sys.stderr)
        return None


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def is_invitro_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in {"www.invitro.ru", "invitro.ru"}


def is_kostroma_analysis_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not is_invitro_url(url):
        return False
    return path.startswith("/analizes/for-doctors/kostroma/")


def is_detail_url(url: str) -> bool:
    if not is_kostroma_analysis_url(url):
        return False

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if "/docs/" in path:
        return False
    if "clear_cache" in url:
        return False

    # реальные карточки обычно имеют .../<group>/<test_id>/
    return bool(re.search(r"/analizes/for-doctors/kostroma/\d+/\d+/?$", path + "/"))


def extract_links(html: str) -> list[str]:
    soup = soup_from_html(html)
    result: list[str] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_kostroma_analysis_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        result.append(href)

    for raw in re.findall(r'https?://www\.invitro\.ru/analizes/for-doctors/kostroma/[^"\']+', html):
        href = normalize_url(raw)
        if href not in seen and is_kostroma_analysis_url(href):
            seen.add(href)
            result.append(href)

    for raw in re.findall(r'/analizes/for-doctors/kostroma/[^"\']+', html):
        href = normalize_url(raw)
        if href not in seen and is_kostroma_analysis_url(href):
            seen.add(href)
            result.append(href)

    return result


def collect_detail_urls(session: requests.Session, delay: float) -> list[str]:
    queue = deque([START_URL])
    visited_pages: set[str] = set()
    detail_urls: OrderedDict[str, None] = OrderedDict()

    while queue:
        url = queue.popleft()
        if url in visited_pages:
            continue
        visited_pages.add(url)

        html = safe_fetch(session, url)
        if not html:
            continue

        links = extract_links(html)

        for link in links:
            if is_detail_url(link):
                detail_urls.setdefault(link, None)
            elif link not in visited_pages:
                queue.append(link)

        print(
            f"[invitro][crawl] pages={len(visited_pages)} "
            f"queue={len(queue)} detail_urls={len(detail_urls)} current={url}"
        )
        time.sleep(delay)

        if len(visited_pages) > 2500:
            print("[invitro][warn] crawl page limit reached")
            break

    return list(detail_urls.keys())


def extract_analysis_name(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    if h1:
        text = clean_text(h1.get_text(" ", strip=True))
        if text:
            return text

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title = clean_text(title.split("|")[0].split(" - ")[0].split(" — ")[0])
    return title if title else None


def extract_category(soup: BeautifulSoup, url: str) -> str:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, .breadcrumbs a, [class*="breadcrumb"] a'):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {
        "Главная",
        "Анализы",
        "Анализы и цены",
        "Иваново",
        "Для врачей",
    }
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]

    parts = [p for p in urlparse(url).path.split("/") if p]
    if len(parts) >= 2:
        return "Анализы"
    return "Без категории"


def extract_price(html: str) -> int | str:
    text = clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))

    patterns = [
        r"Цена\s*:\s*([\d\s]+)\s*руб",
        r"Цена\s*:\s*([\d\s]+)\s*₽",
        r"Цена исследования[^.]*?-\s*([\d\s]+)\s*руб",
        r"Цена исследования[^.]*?-\s*([\d\s]+)\s*₽",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            value = re.sub(r"\D", "", m.group(1))
            if value:
                return int(value)

    # fallback: ищем первый price-блок, но не "Итого"
    soup = soup_from_html(html)
    for node in soup.find_all(string=re.compile(r"Цена", re.I)):
        parent_text = clean_text(node.parent.get_text(" ", strip=True) if node.parent else "")
        if "Итого" in parent_text:
            continue
        m = re.search(r"([\d\s]+)\s*(?:руб|₽)", parent_text, flags=re.I)
        if m:
            value = re.sub(r"\D", "", m.group(1))
            if value:
                return int(value)

    return ""


def parse_detail_page(session: requests.Session, url: str) -> dict | None:
    html = safe_fetch(session, url)
    if not html:
        return None

    soup = soup_from_html(html)

    analysis_name = extract_analysis_name(soup)
    if not analysis_name:
        return None

    if "Правила подготовки" in analysis_name:
        return None

    price = extract_price(html)
    category = extract_category(soup, url)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price,
        "url": url,
    }


def export_rows(rows: list[dict], out_csv: Path, out_xlsx: Path) -> None:
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Invitro Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    detail_urls = collect_detail_urls(session, args.delay)
    print(f"[invitro] collected detail urls: {len(detail_urls)}")

    rows: list[dict] = []
    for i, url in enumerate(detail_urls, 1):
        row = parse_detail_page(session, url)
        if row:
            rows.append(row)

        if i % 100 == 0:
            print(f"[invitro] parsed {i}/{len(detail_urls)} | rows={len(rows)}")

        time.sleep(args.delay)

    dedup = OrderedDict()
    for row in rows:
        key = (str(row["analysis_name"]).strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    csv_path = outdir / "invitro_kostroma.csv"
    xlsx_path = outdir / "invitro_kostroma.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[invitro] saved: {csv_path}")
    print(f"[invitro] saved: {xlsx_path}")
    print(f"[invitro] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
