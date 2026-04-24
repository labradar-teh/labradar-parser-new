#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helix parser — Иваново, Кострома, Ярославль.
"""

import argparse
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Optional

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers.session import build_session, polite_fetch, safe_fetch
from helpers.text import clean_text, extract_price_from_text, title_from_soup, category_from_breadcrumbs
from helpers.export import export_rows
from helpers.filters import is_trash_name, is_trash_url
from helpers.stats import RunStats

LAB = "helix"
BASE_URL = "https://helix.ru"

CITY_CONFIG = {
    "ivanovo": ("Иваново", "https://helix.ru/ivanovo/catalog/190-vse-analizy", "ivanovo"),
    "kostroma": ("Кострома", "https://helix.ru/kostroma/catalog/", "kostroma"),
    "yaroslavl": ("Ярославль", "https://helix.ru/yaroslavl/catalog/", "yaroslavl"),
}

CRUMB_BLACKLIST = {"Главная", "Сдать анализы", "Анализы", "Все анализы", "Иваново", "Кострома", "Ярославль"}

_CODE_RE = re.compile(r"/catalog/item/(\d{2}-\d{3})")


def is_helix_item_url(url: str, region: str) -> bool:
    return f"/{region}/catalog/item/" in url or "/catalog/item/" in url


def is_helix_catalog_url(url: str, region: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc not in {"helix.ru", "www.helix.ru"}:
        return False
    path = parsed.path
    return (
        f"/{region}/catalog/" in path or
        "/catalog/" in path
    ) and "/catalog/item/" not in path


def check_region_exists(session, region: str, start_url: str) -> bool:
    html = safe_fetch(session, start_url, label=LAB)
    if not html:
        return False
    soup = BeautifulSoup(html, "html.parser")
    title_text = clean_text(soup.title.get_text() if soup.title else "")
    return "404" not in title_text and "не найден" not in title_text.lower()


def extract_links(html: str, region: str) -> tuple:
    soup = BeautifulSoup(html, "html.parser")
    items: OrderedDict = OrderedDict()
    cats: OrderedDict = OrderedDict()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = urljoin(BASE_URL, href)
        if is_helix_item_url(full, region):
            items[full] = None
        elif is_helix_catalog_url(full, region):
            cats[full] = None
        if "?page=" in full:
            cats[full] = None

    return list(items.keys()), list(cats.keys())


def extract_price(soup: BeautifulSoup) -> Optional[int]:
    for sel in (
        '[class*="price"]', '[class*="Price"]',
        '[class*="cost"]', '[itemprop="price"]',
    ):
        for node in soup.select(sel):
            val = extract_price_from_text(clean_text(node.get_text(" ", strip=True)))
            if val:
                return val

    text = clean_text(soup.get_text(" "))
    for pattern in (
        r"[Сс]тоимость[:\s]+([\d\s]+)\s*(?:₽|руб)",
        r"[Цц]ена[:\s]+([\d\s]+)\s*(?:₽|руб)",
        r"([\d\s]+)\s*₽",
    ):
        m = re.search(pattern, text, re.I)
        if m:
            val = extract_price_from_text(m.group(1) + " ₽")
            if val:
                return val
    return None


def extract_category(soup: BeautifulSoup, region: str) -> str:
    cat = category_from_breadcrumbs(soup, CRUMB_BLACKLIST)
    if cat:
        return cat
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = clean_text(a.get_text(" ", strip=True))
        if f"/{region}/catalog/" in href and "/catalog/item/" not in href and txt:
            if txt.lower() not in {t.lower() for t in CRUMB_BLACKLIST}:
                return txt
    return "Анализы"


def parse_item_page(session, url: str, city: str, region: str, delay: float, stats: RunStats) -> Optional[dict]:
    html = polite_fetch(session, url, delay=delay, label=LAB)
    if not html:
        stats.page_err(url)
        return None

    stats.card_found()
    soup = BeautifulSoup(html, "html.parser")

    code_m = _CODE_RE.search(url)
    if not code_m:
        stats.row_filtered("нет кода анализа в URL")
        return None
    code = code_m.group(1)

    if code.startswith("40-"):
        stats.row_filtered("код 40-xxx (забор)")
        return None

    name = title_from_soup(soup)
    if not name:
        stats.row_filtered("нет названия")
        return None

    name = re.sub(r"\s+в\s+\S+\s*$", "", name, flags=re.I).strip()

    reason = is_trash_name(name) or is_trash_url(url)
    if reason:
        stats.row_filtered(reason)
        return None

    price = extract_price(soup)
    if not price:
        stats.row_filtered("нет цены")
        return None

    category = extract_category(soup, region)

    return {
        "_code": code,
        "lab": LAB,
        "city": city,
        "category": category,
        "analysis_name": name,
        "price": price,
        "url": url,
        "analysis_code": code,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_region_url": CITY_CONFIG[region][1],
    }


def run(region: str, outdir: Path, delay: float) -> int:
    if region not in CITY_CONFIG:
        print(f"[{LAB}] Unknown region: {region}", file=sys.stderr)
        return 1

    city_name, start_url, _ = CITY_CONFIG[region]
    stats = RunStats(f"{LAB}/{region}")

    print(f"[{LAB}] Starting {city_name}, start_url={start_url}", file=sys.stderr)
    session = build_session()

    if not check_region_exists(session, region, start_url):
        print(
            f"[{LAB}][warn] Региональный каталог не найден для {city_name} ({start_url}). "
            f"Пропускаем без ошибки.",
            file=sys.stderr,
        )
        export_rows([], outdir / f"{LAB}_{region}.csv", outdir / f"{LAB}_{region}.xlsx")
        return 0

    visited_pages: set = set()
    queue = [start_url]
    data: OrderedDict = OrderedDict()

    while queue:
        page_url = queue.pop(0)
        if page_url in visited_pages:
            continue
        visited_pages.add(page_url)
        stats.page_ok()

        item_links, next_links = extract_links(
            polite_fetch(session, page_url, delay=delay, label=LAB) or "", region
        )

        for item_url in item_links:
            row = parse_item_page(session, item_url, city_name, region, delay, stats)
            if row:
                code = row.pop("_code")
                if code not in data:
                    data[code] = row
                    stats.row_saved()

        for nxt in next_links:
            if nxt not in visited_pages:
                queue.append(nxt)

        print(
            f"[{LAB}][crawl] pages={len(visited_pages)} "
            f"queue={len(queue)} rows={len(data)} current={page_url}",
            file=sys.stderr,
        )

        if len(visited_pages) > 3000:
            print(f"[{LAB}][warn] crawl page limit reached", file=sys.stderr)
            break

    final_rows = sorted(data.values(), key=lambda x: (x["category"], x["analysis_name"]))

    csv_path = outdir / f"{LAB}_{region}.csv"
    xlsx_path = outdir / f"{LAB}_{region}.xlsx"
    n = export_rows(final_rows, csv_path, xlsx_path)

    stats.print_summary()
    print(f"[{LAB}] saved {n} rows → {csv_path}", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Helix parser")
    parser.add_argument("--region", required=True, choices=list(CITY_CONFIG.keys()))
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--delay", type=float, default=0.1)
    args = parser.parse_args()
    return run(args.region, Path(args.outdir), args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
