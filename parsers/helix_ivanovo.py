#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "helix"
CITY = "Иваново"
BASE_URL = "https://helix.ru"
ROOT_URL = f"{BASE_URL}/ivanovo/catalog/190-vse-analizy"
DEFAULT_DELAY = 0.08

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


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    full = urljoin(BASE_URL, url.split("#")[0])
    parsed = urlparse(full)
    query = parse_qs(parsed.query, keep_blank_values=True)

    kept = {}
    if "page" in query:
        kept["page"] = query["page"][0]

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/") or "/",
            "",
            urlencode(kept),
            "",
        )
    )


def add_page_param(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["page"] = [str(page)]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/") or "/",
            "",
            urlencode({k: v[0] for k, v in query.items()}),
            "",
        )
    )


def fetch(session: requests.Session, url: str, timeout: int = 40) -> str:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


def safe_fetch(session: requests.Session, url: str, timeout: int = 40) -> str | None:
    try:
        return fetch(session, url, timeout=timeout)
    except Exception as exc:
        print(f"[helix][fetch-error] {url}: {exc}", file=sys.stderr)
        return None


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def parse_last_page(html: str) -> int:
    nums = set()

    for m in re.finditer(r"[?&]page=(\d+)", html):
        nums.add(int(m.group(1)))

    soup = soup_from_html(html)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            nums.add(int(m.group(1)))

    return max(nums) if nums else 1


def is_item_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    return "/catalog/item/" in path


def item_url_to_ivanovo(url: str) -> str:
    full = normalize_url(url)
    parsed = urlparse(full)
    path = parsed.path.rstrip("/")

    if path.startswith("/ivanovo/catalog/item/"):
        return full

    if path.startswith("/catalog/item/"):
        tail = path[len("/catalog/item/"):]
        return f"{BASE_URL}/ivanovo/catalog/item/{tail}"

    return full


def extract_item_links(html: str) -> list[str]:
    soup = soup_from_html(html)
    result = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_item_url(href):
            continue
        href = item_url_to_ivanovo(href)
        if href not in seen:
            seen.add(href)
            result.append(href)

    for raw in re.findall(r'https://helix\.ru(?:/ivanovo)?/catalog/item/\d{2}-\d{3}', html):
        href = item_url_to_ivanovo(raw)
        if href not in seen:
            seen.add(href)
            result.append(href)

    for raw in re.findall(r'/(?:ivanovo/)?catalog/item/\d{2}-\d{3}', html):
        href = item_url_to_ivanovo(raw)
        if href not in seen:
            seen.add(href)
            result.append(href)

    return result


def extract_category_from_breadcrumbs(soup: BeautifulSoup) -> str:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, .breadcrumbs a, [class*="breadcrumb"] a'):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {"Главная", "Сдать анализы", CITY}
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]
    return "Анализы"


def extract_price(html: str) -> int | str:
    text = clean_text(soup_from_html(html).get_text(" ", strip=True))

    patterns = [
        r"Стоимость\s*:?\s*([\d\s]+)\s*₽",
        r"Цена\s*:?\s*([\d\s]+)\s*₽",
        r"Стоимость\s*:?\s*([\d\s]+)\s*руб",
        r"Цена\s*:?\s*([\d\s]+)\s*руб",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            raw = re.sub(r"\D", "", m.group(1))
            if raw:
                return int(raw)

    return ""


def parse_item_page(html: str, url: str) -> dict | None:
    soup = soup_from_html(html)

    h1 = soup.find("h1")
    if not h1:
        return None

    analysis_name = clean_text(h1.get_text(" ", strip=True))
    if not analysis_name:
        return None

    analysis_name = re.sub(rf"\s+в\s+{re.escape(CITY)}\s*$", "", analysis_name, flags=re.I).strip()
    price = extract_price(html)
    category = extract_category_from_breadcrumbs(soup)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price,
        "url": url,
    }


def collect_all_item_urls(session: requests.Session, delay: float) -> list[str]:
    root_html = safe_fetch(session, ROOT_URL)
    if not root_html:
        return []

    last_page = parse_last_page(root_html)
    print(f"[helix] total catalog pages: {last_page}")

    item_urls = OrderedDict()

    for page in range(1, last_page + 1):
        page_url = ROOT_URL if page == 1 else add_page_param(ROOT_URL, page)
        html = root_html if page == 1 else safe_fetch(session, page_url)

        if not html:
            continue

        links = extract_item_links(html)
        for link in links:
            item_urls.setdefault(link, None)

        print(f"[helix] page {page}/{last_page} -> found {len(links)} | total={len(item_urls)}")
        time.sleep(delay)

    return list(item_urls.keys())


def export_rows(rows: list[dict], out_csv: Path, out_xlsx: Path) -> None:
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Helix Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()
    item_urls = collect_all_item_urls(session, args.delay)
    print(f"[helix] collected item urls: {len(item_urls)}")

    rows = []
    for i, url in enumerate(item_urls, 1):
        html = safe_fetch(session, url)
        if not html:
            continue

        row = parse_item_page(html, url)
        if row:
            rows.append(row)

        if i % 100 == 0:
            print(f"[helix] parsed {i}/{len(item_urls)} | rows={len(rows)}")

        time.sleep(args.delay)

    dedup = OrderedDict()
    for row in rows:
        key = (str(row["analysis_name"]).strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    csv_path = outdir / "helix_ivanovo.csv"
    xlsx_path = outdir / "helix_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[helix] saved: {csv_path}")
    print(f"[helix] saved: {xlsx_path}")
    print(f"[helix] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
