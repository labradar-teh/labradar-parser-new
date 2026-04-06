#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gemotest parser — Иваново и Кострома.

Улучшения vs оригинала:
- параметризован по региону
- parse_next_data_links: извлекает ссылки из __NEXT_DATA__ (Next.js)
- extract_price: 3-уровневый fallback
- RunStats
- фильтрация через helpers/filters.py
- для Костромы: /kostroma/catalog/ (если нет — фиксируем в логах и не падаем)
"""

import argparse
import json
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

LAB = "gemotest"
BASE_URL = "https://gemotest.ru"

CITY_CONFIG = {
    "ivanovo": ("Иваново", "/ivanovo/catalog/"),
    "kostroma": ("Кострома", "/kostroma/catalog/"),
}

CRUMB_BLACKLIST = {
    "Главная", "Иваново", "Кострома",
    "Каталог анализов и услуг", "Каталог", "Анализы",
}


def catalog_prefix(region: str) -> str:
    return CITY_CONFIG[region][1]


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def is_catalog_url(url: str, region: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc not in {"gemotest.ru", "www.gemotest.ru"}:
        return False
    return parsed.path.startswith(catalog_prefix(region))


def is_detail_url(url: str, region: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not path.startswith(catalog_prefix(region)):
        return False
    parts = [p for p in path.split("/") if p]
    if len(parts) < 4:
        return False
    if re.search(r"[?&]page=\d+", url) or re.search(r"/page/\d+/?$", path):
        return False
    return True


def extract_links(html: str, region: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    result: list[str] = []

    def add(href: str):
        h = normalize_url(href)
        if h not in seen and is_catalog_url(h, region):
            seen.add(h)
            result.append(h)

    for a in soup.find_all("a", href=True):
        add(a["href"])

    # ссылки из __NEXT_DATA__
    for block in re.findall(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, flags=re.DOTALL | re.IGNORECASE
    ):
        try:
            data = json.loads(block)
        except Exception:
            continue
        for value in _walk_strings(data):
            if catalog_prefix(region) in value:
                add(value)

    # raw regex fallback
    pfx = catalog_prefix(region)
    for raw in re.findall(re.escape(pfx) + r'[^"\'\s<>]+', html):
        add(raw)

    return result


def _walk_strings(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_strings(item)
    elif isinstance(obj, str):
        yield obj


def check_region_exists(session, region: str) -> bool:
    """Проверяет, что у региона есть каталог. Для Костромы может не быть."""
    url = BASE_URL + catalog_prefix(region)
    html = safe_fetch(session, url, label=LAB)
    if not html:
        return False
    # Если нас редиректнули на главную или выдали 404-подобную страницу
    soup = BeautifulSoup(html, "html.parser")
    title_text = clean_text(soup.title.get_text() if soup.title else "")
    if "404" in title_text or "не найден" in title_text.lower():
        return False
    return True


def extract_price(soup: BeautifulSoup) -> Optional[int]:
    for sel in (
        '[class*="price"]', '[class*="Price"]',
        '[data-testid*="price"]', '[itemprop="price"]',
    ):
        for node in soup.select(sel):
            val = extract_price_from_text(clean_text(node.get_text(" ", strip=True)))
            if val:
                return val

    # fallback по верхней части страницы
    chunks = []
    for node in soup.find_all(["h1","h2","div","section","span","p"], limit=250):
        txt = clean_text(node.get_text(" ", strip=True))
        if txt:
            chunks.append(txt)
    val = extract_price_from_text(" | ".join(chunks[:120]))
    if val:
        return val

    # последний fallback — вся страница
    return extract_price_from_text(clean_text(soup.get_text(" ")))


def extract_category(soup: BeautifulSoup) -> str:
    cat = category_from_breadcrumbs(soup, CRUMB_BLACKLIST)
    return cat if cat else "Без категории"


def collect_detail_urls(
    session, start_url: str, region: str, delay: float, stats: RunStats
) -> list[str]:
    queue = [start_url]
    visited: set[str] = set()
    detail_urls: OrderedDict[str, None] = OrderedDict()

    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        html = polite_fetch(session, url, delay=delay, label=LAB)
        if not html:
            stats.page_err(url)
            continue

        stats.page_ok()
        links = extract_links(html, region)

        for link in links:
            if is_detail_url(link, region):
                detail_urls.setdefault(link, None)
            elif link not in visited and link not in queue:
                queue.append(link)

        print(
            f"[{LAB}][crawl] pages={len(visited)} "
            f"queued={len(queue)} analyses={len(detail_urls)} current={url}",
            file=sys.stderr,
        )

        if len(visited) > 3000:
            print(f"[{LAB}][warn] crawl page limit reached", file=sys.stderr)
            break

    return list(detail_urls.keys())


def parse_analysis_page(
    session, url: str, city: str, delay: float, stats: RunStats
) -> Optional[dict]:
    html = polite_fetch(session, url, delay=delay, label=LAB)
    if not html:
        stats.page_err(url)
        return None

    stats.card_found()
    soup = BeautifulSoup(html, "html.parser")
    name = title_from_soup(soup)
    if not name:
        stats.row_filtered("нет названия")
        return None

    reason = is_trash_name(name) or is_trash_url(url)
    if reason:
        stats.row_filtered(reason)
        return None

    price = extract_price(soup)
    if price is None:
        stats.row_filtered("нет цены")
        return None

    category = extract_category(soup)

    return {
        "lab": LAB,
        "city": city,
        "category": category,
        "analysis_name": name,
        "price": price,
        "url": url,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_region_url": BASE_URL + catalog_prefix(region_from_url(url)),
    }


def region_from_url(url: str) -> str:
    for r in CITY_CONFIG:
        if catalog_prefix(r) in url:
            return r
    return "unknown"


def run(region: str, outdir: Path, delay: float) -> int:
    if region not in CITY_CONFIG:
        print(f"[{LAB}] Unknown region: {region}", file=sys.stderr)
        return 1

    city_name, catalog_path = CITY_CONFIG[region]
    start_url = BASE_URL + catalog_path
    stats = RunStats(f"{LAB}/{region}")

    print(f"[{LAB}] Starting {city_name}, start_url={start_url}", file=sys.stderr)
    session = build_session()

    # Проверяем наличие регионального каталога
    if not check_region_exists(session, region):
        print(
            f"[{LAB}][warn] Региональный каталог не найден для {city_name} ({start_url}). "
            f"Пропускаем без ошибки.",
            file=sys.stderr,
        )
        # Создаём пустые файлы
        slug = region
        export_rows([], outdir / f"{LAB}_{slug}.csv", outdir / f"{LAB}_{slug}.xlsx")
        return 0

    detail_urls = collect_detail_urls(session, start_url, region, delay, stats)
    print(f"[{LAB}] collected detail urls: {len(detail_urls)}", file=sys.stderr)

    rows: list[dict] = []
    for i, url in enumerate(detail_urls, 1):
        row = parse_analysis_page(session, url, city_name, delay, stats)
        if row:
            rows.append(row)
            stats.row_saved()
        if i % 100 == 0:
            print(f"[{LAB}] parsed {i}/{len(detail_urls)} | rows={len(rows)}", file=sys.stderr)

    dedup: OrderedDict = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].lower(), row["url"])
        dedup[key] = row
    final_rows = list(dedup.values())

    slug = region
    csv_path = outdir / f"{LAB}_{slug}.csv"
    xlsx_path = outdir / f"{LAB}_{slug}.xlsx"
    n = export_rows(final_rows, csv_path, xlsx_path)

    stats.print_summary()
    print(f"[{LAB}] saved {n} rows → {csv_path}", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Gemotest parser")
    parser.add_argument("--region", required=True, choices=list(CITY_CONFIG.keys()))
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--delay", type=float, default=0.15)
    args = parser.parse_args()
    return run(args.region, Path(args.outdir), args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
