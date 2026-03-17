#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "helix"
CITY = "ivanovo"
CITY_LABEL = "Иваново"
BASE_URL = "https://helix.ru"
ROOT_CATALOG_URL = f"{BASE_URL}/{CITY}/catalog/190-vse-analizy"
ROOT_NAV_URL = f"{BASE_URL}/{CITY}/catalog/190-vse-analizy"
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
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    parsed = urlparse(urljoin(BASE_URL, url))
    cleaned = parsed._replace(fragment="")
    if cleaned.query:
        params = parse_qs(cleaned.query, keep_blank_values=True)
        if "page" in params:
            q = f"page={params['page'][0]}"
        else:
            q = ""
        cleaned = cleaned._replace(query=q)
    return urlunparse(cleaned)


def fetch(session: requests.Session, url: str, timeout: int = 40) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def parse_last_page_from_html(html: str) -> int:
    pages = [int(m.group(1)) for m in re.finditer(r"[?&]page=(\d+)", html)]
    text_pages = [int(m.group(1)) for m in re.finditer(r">\s*(\d{1,3})\s*<", html)]
    nums = pages + text_pages
    return max(nums) if nums else 1


def extract_category_links(nav_html: str) -> List[Tuple[str, str]]:
    soup = soup_from_html(nav_html)
    items: List[Tuple[str, str]] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        text = normalize_spaces(a.get_text(" ", strip=True))
        if not href.startswith(f"{BASE_URL}/{CITY}/catalog/"):
            continue
        if "/catalog/item/" in href:
            continue
        if text in {"Главная", "В каталог", "Helixbook", "Скидки и акции", "Адреса"}:
            continue
        if not re.search(r"/catalog/\d+-", href):
            continue
        if href.endswith("/190-vse-analizy"):
            continue
        if href in seen:
            continue
        seen.add(href)
        items.append((text or "Без категории", href))

    items.insert(0, ("Все анализы", ROOT_CATALOG_URL))
    return items


def make_listing_page_urls(first_page_url: str, last_page: int) -> List[str]:
    if last_page <= 1:
        return [first_page_url]
    result = [first_page_url]
    for page in range(2, last_page + 1):
        delimiter = "&" if "?" in first_page_url else "?"
        result.append(f"{first_page_url}{delimiter}page={page}")
    return result


def extract_item_urls_from_listing(html: str) -> List[str]:
    soup = soup_from_html(html)
    urls: List[str] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if f"/{CITY}/catalog/item/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)

    if urls:
        return urls

    for raw in re.findall(rf'https://helix\.ru/{CITY}/catalog/item/[0-9]{{2}}-[0-9]{{3}}', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            urls.append(href)

    for raw in re.findall(rf'/{CITY}/catalog/item/[0-9]{{2}}-[0-9]{{3}}', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            urls.append(href)

    return urls


def clean_name(text: str) -> str:
    text = normalize_spaces(text)
    text = re.sub(rf"\s+в\s+{CITY_LABEL}\s*$", "", text, flags=re.IGNORECASE)
    return text.strip(" /")


def parse_price(text: str) -> Optional[int]:
    m = re.search(r"Стоимость\s*:?\s*([\d\s]+)\s*₽", text, flags=re.IGNORECASE)
    if not m:
        amounts = re.findall(r"([\d\s]+)\s*₽", text)
        if not amounts:
            return None
        return int(re.sub(r"\D", "", amounts[0]))
    return int(re.sub(r"\D", "", m.group(1)))


def parse_helix_item_page(html: str, url: str) -> Dict[str, Optional[str]]:
    soup = soup_from_html(html)

    h1 = soup.find("h1")
    if h1:
        name = clean_name(h1.get_text(" ", strip=True))
    else:
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        name = clean_name(title.split("–")[0])

    full_text = normalize_spaces(soup.get_text(" ", strip=True))
    price = parse_price(full_text)

    return {
        "analysis_name": name,
        "price": price,
        "url": url,
    }


def collect_listing_map(session: requests.Session, delay: float) -> OrderedDict:
    nav_html = fetch(session, ROOT_NAV_URL)
    categories = extract_category_links(nav_html)
    listing_map: OrderedDict[str, str] = OrderedDict()

    for category_name, category_url in categories:
        print(f"[helix] category: {category_name} -> {category_url}")
        html = fetch(session, category_url)
        last_page = parse_last_page_from_html(html)
        page_urls = make_listing_page_urls(category_url, last_page)

        for idx, page_url in enumerate(page_urls, start=1):
            if idx > 1:
                html = fetch(session, page_url)
            item_urls = extract_item_urls_from_listing(html)
            print(f"  page {idx}/{last_page}: {len(item_urls)} items")
            for item_url in item_urls:
                existing = listing_map.get(item_url)
                if existing in (None, "", "Все анализы") and category_name != "Все анализы":
                    listing_map[item_url] = category_name
                elif existing is None:
                    listing_map[item_url] = category_name
            time.sleep(delay)

    root_html = fetch(session, ROOT_CATALOG_URL)
    root_last_page = parse_last_page_from_html(root_html)
    for page_url in make_listing_page_urls(ROOT_CATALOG_URL, root_last_page):
        html = root_html if page_url == ROOT_CATALOG_URL else fetch(session, page_url)
        for item_url in extract_item_urls_from_listing(html):
            listing_map.setdefault(item_url, "Все анализы")
        time.sleep(delay)

    return listing_map


def export_rows(rows: List[Dict], out_csv: Path, out_xlsx: Path) -> None:
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
    listing_map = collect_listing_map(session, args.delay)

    print(f"[helix] unique item urls from listings: {len(listing_map)}")

    rows: List[Dict] = []
    for idx, (item_url, category_name) in enumerate(listing_map.items(), start=1):
        try:
            html = fetch(session, item_url)
            parsed = parse_helix_item_page(html, item_url)
            if not parsed["analysis_name"] or parsed["price"] is None:
                print(f"[helix][warn] incomplete item: {item_url}", file=sys.stderr)
                continue

            rows.append(
                {
                    "lab": LAB,
                    "city": CITY_LABEL,
                    "category": category_name,
                    "analysis_name": parsed["analysis_name"],
                    "price": int(parsed["price"]),
                    "url": parsed["url"],
                }
            )
            if idx % 100 == 0:
                print(f"[helix] parsed {idx}/{len(listing_map)}")
            time.sleep(args.delay)
        except Exception as exc:
            print(f"[helix][error] {item_url}: {exc}", file=sys.stderr)

    deduped: OrderedDict[Tuple[str, int, str], Dict] = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].lower(), row["price"], row["url"])
        deduped[key] = row

    final_rows = list(deduped.values())
    csv_path = outdir / "helix_ivanovo.csv"
    xlsx_path = outdir / "helix_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[helix] saved: {csv_path}")
    print(f"[helix] saved: {xlsx_path}")
    print(f"[helix] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
