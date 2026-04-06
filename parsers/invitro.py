#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Invitro parser — Иваново и Кострома.

Улучшения vs оригинала:
- параметризован по региону
- расширен is_detail_url: поддерживает /analizes/for-doctors/<city>/<g>/<id>/
- extract_price: добавлен поиск по JSON-LD и data-атрибутам
- RunStats
"""

import argparse
import json
import re
import sys
from collections import OrderedDict, deque
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

LAB = "invitro"
BASE_URL = "https://www.invitro.ru"

CITY_CONFIG = {
    "ivanovo": ("Иваново", "/analizes/for-doctors/ivanovo/"),
    "kostroma": ("Кострома", "/analizes/for-doctors/kostroma/"),
}

CRUMB_BLACKLIST = {
    "Главная", "Анализы", "Анализы и цены",
    "Иваново", "Кострома", "Для врачей",
}

_DETAIL_RE = {
    "ivanovo": re.compile(r"/analizes/for-doctors/ivanovo/\d+/\d+/?$"),
    "kostroma": re.compile(r"/analizes/for-doctors/kostroma/\d+/\d+/?$"),
}


def catalog_prefix(region: str) -> str:
    return CITY_CONFIG[region][1]


def is_invitro_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in {"www.invitro.ru", "invitro.ru"}


def is_analysis_url(url: str, region: str) -> bool:
    if not is_invitro_url(url):
        return False
    return urlparse(url).path.startswith(catalog_prefix(region))


def is_detail_url(url: str, region: str) -> bool:
    if not is_analysis_url(url, region):
        return False
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if "/docs/" in path or "clear_cache" in url:
        return False
    return bool(_DETAIL_RE[region].search(path + "/"))


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def extract_links(html: str, region: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    result: list[str] = []

    def add(href: str):
        h = normalize_url(href)
        if h not in seen and is_analysis_url(h, region):
            seen.add(h)
            result.append(h)

    for a in soup.find_all("a", href=True):
        add(a["href"])

    pfx = catalog_prefix(region)
    for raw in re.findall(r'https?://(?:www\.)?invitro\.ru' + re.escape(pfx) + r'[^"\'\s<>]+', html):
        add(raw)
    for raw in re.findall(re.escape(pfx) + r'[^"\'\s<>]+', html):
        add(raw)

    return result


def check_region_exists(session, region: str) -> bool:
    url = BASE_URL + catalog_prefix(region)
    html = safe_fetch(session, url, label=LAB)
    if not html:
        return False
    soup = BeautifulSoup(html, "lxml")
    title_text = clean_text(soup.title.get_text() if soup.title else "")
    return "404" not in title_text and "не найден" not in title_text.lower()


def extract_price(soup: BeautifulSoup, html: str) -> int | str:
    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                price_val = data.get("price") or (
                    data.get("offers", {}).get("price") if isinstance(data.get("offers"), dict) else None
                )
                if price_val:
                    from helpers.text import clean_price
                    val = clean_price(str(price_val))
                    if val:
                        return val
        except Exception:
            pass

    # 2. CSS price-блоки
    for sel in ('[class*="price"]', '[class*="Price"]', '[itemprop="price"]'):
        for node in soup.select(sel):
            val = extract_price_from_text(clean_text(node.get_text(" ", strip=True)))
            if val:
                return val
            attr = node.get("content") or node.get("data-price")
            if attr:
                from helpers.text import clean_price
                v = clean_price(str(attr))
                if v:
                    return v

    # 3. Паттерны по тексту
    full_text = clean_text(BeautifulSoup(html, "lxml").get_text(" "))
    for pattern in (
        r"[Цц]ена\s*:\s*([\d\s]+)\s*(?:руб|₽)",
        r"[Цц]ена\s+исследования[^.]*?\s+([\d\s]+)\s*(?:руб|₽)",
    ):
        m = re.search(pattern, full_text, re.I)
        if m:
            val = extract_price_from_text(m.group(1) + " ₽")
            if val:
                return val

    # 4. Первое вхождение «Цена» рядом с числом
    for node in soup.find_all(string=re.compile(r"[Цц]ена", re.I)):
        parent_text = clean_text(node.parent.get_text(" ") if node.parent else "")
        if "Итого" in parent_text:
            continue
        val = extract_price_from_text(parent_text)
        if val:
            return val

    return ""


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
            elif link not in visited:
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
    session, url: str, city: str, region: str, delay: float, stats: RunStats
) -> Optional[dict]:
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
    if "Правила подготовки" in name:
        stats.row_filtered("правила подготовки")
        return None

    reason = is_trash_name(name) or is_trash_url(url)
    if reason:
        stats.row_filtered(reason)
        return None

    price = extract_price(soup, html)
    cat = category_from_breadcrumbs(soup, CRUMB_BLACKLIST) or "Анализы"

    return {
        "lab": LAB,
        "city": city,
        "category": cat,
        "analysis_name": name,
        "price": price,
        "url": url,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_region_url": BASE_URL + catalog_prefix(region),
    }


def run(region: str, outdir: Path, delay: float) -> int:
    if region not in CITY_CONFIG:
        print(f"[{LAB}] Unknown region: {region}", file=sys.stderr)
        return 1

    city_name, catalog_path = CITY_CONFIG[region]
    start_url = BASE_URL + catalog_path
    stats = RunStats(f"{LAB}/{region}")

    print(f"[{LAB}] Starting {city_name}, start_url={start_url}", file=sys.stderr)
    session = build_session()

    if not check_region_exists(session, region):
        print(
            f"[{LAB}][warn] Региональный каталог не найден для {city_name}. Пропускаем.",
            file=sys.stderr,
        )
        export_rows([], outdir / f"{LAB}_{region}.csv", outdir / f"{LAB}_{region}.xlsx")
        return 0

    detail_urls = collect_detail_urls(session, start_url, region, delay, stats)
    print(f"[{LAB}] collected detail urls: {len(detail_urls)}", file=sys.stderr)

    rows: list[dict] = []
    for i, url in enumerate(detail_urls, 1):
        row = parse_detail_page(session, url, city_name, region, delay, stats)
        if row:
            rows.append(row)
            stats.row_saved()
        if i % 100 == 0:
            print(f"[{LAB}] parsed {i}/{len(detail_urls)} | rows={len(rows)}", file=sys.stderr)

    dedup: OrderedDict = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].strip().lower(), row["url"])
        dedup[key] = row
    final_rows = list(dedup.values())

    csv_path = outdir / f"{LAB}_{region}.csv"
    xlsx_path = outdir / f"{LAB}_{region}.xlsx"
    n = export_rows(final_rows, csv_path, xlsx_path)

    stats.print_summary()
    print(f"[{LAB}] saved {n} rows → {csv_path}", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Invitro parser")
    parser.add_argument("--region", required=True, choices=list(CITY_CONFIG.keys()))
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--delay", type=float, default=0.15)
    args = parser.parse_args()
    return run(args.region, Path(args.outdir), args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
