#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "gemotest"
CITY = "Кострома"
BASE_URL = "https://gemotest.ru"
START_URL = f"{BASE_URL}/kostroma/catalog/"
DEFAULT_DELAY = 0.15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru,en;q=0.9",
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


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def fetch(session: requests.Session, url: str, timeout: int = 40) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def is_catalog_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != urlparse(BASE_URL).netloc:
        return False
    path = parsed.path.rstrip("/") + "/"
    return path.startswith("/ivanovo/catalog/")


def is_probable_analysis_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not path.startswith("/ivanovo/catalog/"):
        return False
    parts = [p for p in path.split("/") if p]
    # /ivanovo/catalog/<category>/<subcategory>/.../<slug>
    return len(parts) >= 4


def extract_links_from_catalog_page(html: str) -> List[str]:
    soup = soup_from_html(html)
    found: List[str] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_catalog_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        found.append(href)

    return found


def parse_next_data_links(html: str) -> List[str]:
    links: List[str] = []
    seen: Set[str] = set()

    next_data_matches = re.findall(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    for block in next_data_matches:
        try:
            data = json.loads(block)
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                for _, v in obj.items():
                    yield from walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    yield from walk(item)
            elif isinstance(obj, str):
                yield obj

        for value in walk(data):
            if "/ivanovo/catalog/" in value:
                href = normalize_url(value)
                if is_catalog_url(href) and href not in seen:
                    seen.add(href)
                    links.append(href)

    return links


def collect_all_catalog_and_analysis_links(session: requests.Session, delay: float) -> List[str]:
    queue: List[str] = [START_URL]
    seen_pages: Set[str] = set()
    analysis_urls: OrderedDict[str, None] = OrderedDict()

    while queue:
        url = queue.pop(0)
        if url in seen_pages:
            continue
        seen_pages.add(url)

        try:
            html = fetch(session, url)
        except Exception as exc:
            print(f"[gemotest][error] page {url}: {exc}", file=sys.stderr)
            continue

        links = extract_links_from_catalog_page(html)
        links += parse_next_data_links(html)

        for link in links:
            if is_probable_analysis_url(link):
                analysis_urls.setdefault(link, None)
            elif link not in seen_pages and link not in queue:
                queue.append(link)

        print(
            f"[gemotest] crawled pages={len(seen_pages)} "
            f"queued={len(queue)} analyses={len(analysis_urls)} current={url}"
        )
        time.sleep(delay)

    return list(analysis_urls.keys())


def parse_price_from_text(text: str) -> Optional[int]:
    text = normalize_spaces(text)

    patterns = [
        r"(\d[\d\s]{0,20})\s*₽",
        r"(\d[\d\s]{0,20})\s*руб\.?",
    ]
    candidates: List[int] = []

    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            raw = re.sub(r"\D", "", m.group(1))
            if not raw:
                continue
            value = int(raw)
            if 50 <= value <= 500000:
                candidates.append(value)

    if not candidates:
        return None

    return candidates[0]


def extract_category(soup: BeautifulSoup) -> str:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, [class*="breadcrumb"] a'):
        txt = normalize_spaces(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {
        "Главная",
        "Иваново",
        "Каталог анализов и услуг",
        "Каталог",
        "Анализы",
    }
    crumbs = [c for c in crumbs if c not in blacklist]

    if len(crumbs) >= 2:
        return crumbs[-2]
    if crumbs:
        return crumbs[-1]
    return "Без категории"


def parse_analysis_page(html: str, url: str) -> Optional[Dict]:
    soup = soup_from_html(html)

    h1 = soup.find("h1")
    if not h1:
        return None

    analysis_name = normalize_spaces(h1.get_text(" ", strip=True))
    if not analysis_name:
        return None

    price = None

    # 1. сначала пробуем взять цену из "коротких" UI-блоков рядом с заголовком/кнопкой
    priority_selectors = [
        '[class*="price"]',
        '[class*="Price"]',
        '[data-testid*="price"]',
        '[itemprop="price"]',
    ]
    for sel in priority_selectors:
        for node in soup.select(sel):
            txt = normalize_spaces(node.get_text(" ", strip=True))
            val = parse_price_from_text(txt)
            if val is not None:
                price = val
                break
        if price is not None:
            break

    # 2. fallback по первым кускам текста страницы
    if price is None:
        top_chunks = []
        for node in soup.find_all(["h1", "h2", "div", "section", "span", "p"], limit=250):
            txt = normalize_spaces(node.get_text(" ", strip=True))
            if txt:
                top_chunks.append(txt)
        compact_text = " | ".join(top_chunks[:120])
        price = parse_price_from_text(compact_text)

    # 3. общий fallback по всей странице
    if price is None:
        full_text = normalize_spaces(soup.get_text(" ", strip=True))
        price = parse_price_from_text(full_text)

    if price is None:
        return None

    category = extract_category(soup)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price,
        "url": url,
    }


def export_rows(rows: List[Dict], out_csv: Path, out_xlsx: Path) -> None:
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Gemotest Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    analysis_urls = collect_all_catalog_and_analysis_links(session, args.delay)
    print(f"[gemotest] collected analysis urls: {len(analysis_urls)}")

    rows: List[Dict] = []
    for idx, url in enumerate(analysis_urls, start=1):
        try:
            html = fetch(session, url)
            parsed = parse_analysis_page(html, url)
            if parsed is None:
                print(f"[gemotest][warn] skip no price/name: {url}", file=sys.stderr)
                continue

            rows.append(parsed)

            if idx % 100 == 0:
                print(f"[gemotest] parsed {idx}/{len(analysis_urls)}")
            time.sleep(args.delay)

        except Exception as exc:
            print(f"[gemotest][error] item {url}: {exc}", file=sys.stderr)

    deduped: OrderedDict[tuple, Dict] = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].lower(), row["url"])
        deduped[key] = row

    final_rows = list(deduped.values())

    csv_path = outdir / "gemotest_kostroma.csv"
    xlsx_path = outdir / "gemotest_kostroma.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[gemotest] saved: {csv_path}")
    print(f"[gemotest] saved: {xlsx_path}")
    print(f"[gemotest] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
