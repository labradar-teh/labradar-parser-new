#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Citilab parser — Иваново и Кострома.

Улучшения vs оригинала:
- параметризован через CITY_CONFIG, одна кодовая база
- расширен extract_links: fallback через regex по raw HTML
- улучшена пагинация: ищет ?page=N и /page/N/
- extract_price: 4 уровня fallback + CSS-селекторы
- RunStats: полная статистика запуска
- фильтрация через helpers/filters.py
"""

import argparse
import re
import sys
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers.session import build_session, polite_fetch
from helpers.text import clean_text, extract_price_from_text, title_from_soup, category_from_breadcrumbs
from helpers.export import export_rows
from helpers.filters import is_trash_name, is_trash_url
from helpers.stats import RunStats

LAB = "citilab"
BASE_URL = "https://citilab.ru"

# Конфиг по городам: (city_name, catalog_path, region_slug)
CITY_CONFIG = {
    "ivanovo": ("Иваново", "/ivanovo/catalog/", "ivanovo"),
    "kostroma": ("Кострома", "/kostroma/catalog/", "kostroma"),
}

CRUMB_BLACKLIST = {"Главная", "Иваново", "Кострома", "Каталог", "Каталог анализов"}


def _catalog_prefix(region: str) -> str:
    return f"/{region}/catalog/"


def is_citilab_url(url: str, region: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc not in {"citilab.ru", "www.citilab.ru"}:
        return False
    return parsed.path.startswith(_catalog_prefix(region))


def is_detail_url(url: str, region: str) -> bool:
    if not is_citilab_url(url, region):
        return False
    path = urlparse(url).path.rstrip("/")
    parts = [p for p in path.split("/") if p]
    # /<region>/catalog/<cat>/<subcat>/... — минимум 4 части
    if len(parts) < 4:
        return False
    last = parts[-1].lower()
    if last in {"catalog", "analizy", "analiz", "uslugi", "service", "services"}:
        return False
    # страницы пагинации — не карточки
    if re.search(r"[?&]page=\d+", url) or re.search(r"/page/\d+/?$", path):
        return False
    return True


def is_category_url(url: str, region: str) -> bool:
    return is_citilab_url(url, region) and not is_detail_url(url, region)


def normalize_url(url: str) -> str:
    full = urljoin(BASE_URL, url.split("#")[0])
    parsed = urlparse(full)
    path = parsed.path.rstrip("/") + "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def extract_links(html: str, region: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    result: list[str] = []

    def add(href: str):
        h = normalize_url(href)
        if h not in seen and is_citilab_url(h, region):
            seen.add(h)
            result.append(h)

    for a in soup.find_all("a", href=True):
        add(a["href"])

    prefix = _catalog_prefix(region)
    for raw in re.findall(
        r'https?://(?:www\.)?citilab\.ru' + re.escape(prefix) + r'[^"\'\s<>]+', html
    ):
        add(raw)
    for raw in re.findall(re.escape(prefix) + r'[^"\'\s<>]+', html):
        add(raw)

    # пагинация
    for m in re.finditer(r'[?&]page=(\d+)', html):
        page = int(m.group(1))
        for n in range(1, page + 3):
            for base_url in list(seen):
                paged = re.sub(r'[?&]page=\d+', '', base_url).rstrip("/") + f"?page={n}"
                add(paged)

    return result


def extract_price(soup: BeautifulSoup, html: str) -> str | int:
    # 1. Блоки с CSS-классами price
    for sel in (
        '[class*="price"]', '[class*="Price"]',
        '[class*="cost"]', '[class*="Cost"]',
        '[itemprop="price"]', '[data-price]',
    ):
        for node in soup.select(sel):
            val = extract_price_from_text(clean_text(node.get_text(" ", strip=True)))
            if val:
                return val
            # data-атрибуты
            for attr in ("data-price", "content"):
                raw = node.get(attr, "")
                if raw:
                    val = extract_price_from_text(str(raw))
                    if val:
                        return val

    # 2. Паттерны по полному тексту
    full_text = clean_text(BeautifulSoup(html, "lxml").get_text(" "))
    for pattern in (
        r"[Цц]ена\s*:?\s*([\d\s]+)\s*(?:₽|руб)",
        r"[Сс]тоимость\s*:?\s*([\d\s]+)\s*(?:₽|руб)",
    ):
        m = re.search(pattern, full_text, re.I)
        if m:
            val = extract_price_from_text(m.group(1) + " ₽")
            if val:
                return val

    # 3. Fallback по первому числу с ₽/руб на странице
    val = extract_price_from_text(full_text)
    return val if val else ""


def extract_category(soup: BeautifulSoup, url: str) -> str:
    cat = category_from_breadcrumbs(soup, CRUMB_BLACKLIST)
    if cat:
        return cat
    parts = [p for p in urlparse(url).path.split("/") if p]
    if len(parts) >= 4:
        return parts[-2].replace("-", " ").strip()
    return "Анализы"


def collect_detail_urls(
    session, start_url: str, region: str, delay: float, stats: RunStats
) -> list[str]:
    queue = deque([start_url])
    visited: set[str] = set()
    detail_urls: OrderedDict[str, None] = OrderedDict()

    while queue:
        url = queue.popleft()
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
            elif is_category_url(link, region) and link not in visited:
                queue.append(link)

        print(
            f"[{LAB}][crawl] pages={len(visited)} "
            f"queue={len(queue)} detail_urls={len(detail_urls)} current={url}",
            file=sys.stderr,
        )

        if len(visited) > 3000:
            print(f"[{LAB}][warn] crawl page limit reached", file=sys.stderr)
            break

    return list(detail_urls.keys())


def parse_detail_page(
    session, url: str, city: str, delay: float, stats: RunStats
) -> dict | None:
    html = polite_fetch(session, url, delay=delay, label=LAB)
    if not html:
        stats.page_err(url)
        return None

    stats.card_found()
    soup = BeautifulSoup(html, "lxml")
    name = title_from_soup(soup)
    if not name:
        stats.row_filtered("нет названия")
        return None

    # фильтрация мусора
    reason = is_trash_name(name) or is_trash_url(url)
    if reason:
        stats.row_filtered(reason)
        return None

    price = extract_price(soup, html)
    category = extract_category(soup, url)

    return {
        "lab": LAB,
        "city": city,
        "category": category,
        "analysis_name": name,
        "price": price,
        "url": url,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_region_url": url,
    }


def run(region: str, outdir: Path, delay: float) -> int:
    if region not in CITY_CONFIG:
        print(f"[{LAB}] Unknown region: {region}", file=sys.stderr)
        return 1

    city_name, catalog_path, _ = CITY_CONFIG[region]
    start_url = BASE_URL + catalog_path
    stats = RunStats(f"{LAB}/{region}")

    print(f"[{LAB}] Starting {city_name}, start_url={start_url}", file=sys.stderr)
    session = build_session()

    detail_urls = collect_detail_urls(session, start_url, region, delay, stats)
    print(f"[{LAB}] collected detail urls: {len(detail_urls)}", file=sys.stderr)

    rows: list[dict] = []
    for i, url in enumerate(detail_urls, 1):
        row = parse_detail_page(session, url, city_name, delay, stats)
        if row:
            rows.append(row)
            stats.row_saved()
        if i % 100 == 0:
            print(f"[{LAB}] parsed {i}/{len(detail_urls)} | rows={len(rows)}", file=sys.stderr)

    # дедупликация
    dedup: OrderedDict = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].strip().lower(), row["url"])
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
    parser = argparse.ArgumentParser(description="Citilab parser")
    parser.add_argument("--region", required=True, choices=list(CITY_CONFIG.keys()))
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--delay", type=float, default=0.12)
    args = parser.parse_args()
    return run(args.region, Path(args.outdir), args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
