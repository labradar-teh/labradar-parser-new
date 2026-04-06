#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mediks parser — Иваново и Кострома.

Улучшения vs оригинала:
- параметризован по региону
- extract_price: без изменений (уже многоуровневый), добавлен JSON-LD
- категории: обход рекурсивный через deque
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
from typing import Optional, List

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers.session import build_session, polite_fetch, safe_fetch
from helpers.text import clean_text, extract_price_from_text, title_from_soup, category_from_breadcrumbs
from helpers.export import export_rows
from helpers.filters import is_trash_name, is_trash_url
from helpers.stats import RunStats

LAB = "mediks"
BASE_URL = "https://medikslab.ru"

CITY_CONFIG = {
    "ivanovo": ("Иваново", "/ivanovo/"),
    "kostroma": ("Кострома", "/kostroma/"),
}

CRUMB_BLACKLIST = {
    "Главная", "Иваново", "Кострома", "Анализы",
    "Сдать анализы", "Основной каталог",
}


def city_prefix(region: str) -> str:
    return CITY_CONFIG[region][1]


def is_mediks_url(url: str, region: str) -> bool:
    parsed = urlparse(url)
    return (
        parsed.netloc in {"medikslab.ru", "www.medikslab.ru"}
        and parsed.path.startswith(city_prefix(region))
    )


def is_category_url(url: str, region: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    pfx = city_prefix(region).rstrip("/")
    return path.startswith(f"{pfx}/analizy")


def is_detail_url(url: str, region: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    pfx = city_prefix(region).rstrip("/")
    return path.startswith(f"{pfx}/analiz/")


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def check_region_exists(session, region: str) -> bool:
    start_url = BASE_URL + city_prefix(region) + "analizy"
    html = safe_fetch(session, start_url, label=LAB)
    if not html:
        return False
    soup = BeautifulSoup(html, "lxml")
    title_text = clean_text(soup.title.get_text() if soup.title else "")
    return "404" not in title_text and "не найден" not in title_text.lower()


def extract_links(html: str, region: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    result: List[str] = []

    def add(href: str):
        h = normalize_url(href)
        if h not in seen and is_mediks_url(h, region):
            seen.add(h)
            result.append(h)

    for a in soup.find_all("a", href=True):
        add(a["href"])

    pfx = city_prefix(region)
    for raw in re.findall(r'https?://(?:www\.)?medikslab\.ru' + re.escape(pfx) + r'[^"\'\s<>]+', html):
        add(raw)
    for raw in re.findall(re.escape(pfx) + r'[^"\'\s<>]+', html):
        add(raw)

    return result


def extract_price(soup: BeautifulSoup) -> Optional[int]:
    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                price_val = data.get("price") or (
                    data.get("offers", {}).get("price")
                    if isinstance(data.get("offers"), dict) else None
                )
                if price_val:
                    from helpers.text import clean_price
                    val = clean_price(str(price_val))
                    if val:
                        return val
        except Exception:
            pass

    # 2. Блоки «Стоимость»
    for node in soup.find_all(string=re.compile(r"Стоимость", re.I)):
        parent = node.parent
        if parent:
            parent_text = clean_text(parent.get_text(" "))
            val = extract_price_from_text(parent_text)
            if val:
                return val
            if parent.parent:
                around = [
                    clean_text(c.get_text(" "))
                    for c in parent.parent.find_all(recursive=False)
                ]
                val = extract_price_from_text(" | ".join(around))
                if val:
                    return val

    # 3. CSS-классы
    for sel in (
        '[class*="price"]', '[class*="Price"]',
        '[class*="cost"]', '[class*="Cost"]',
        '[itemprop="price"]',
    ):
        for node in soup.select(sel):
            val = extract_price_from_text(clean_text(node.get_text(" ")))
            if val:
                return val

    # 4. Кнопки / CTA
    for node in soup.find_all(["button", "a", "div", "span"]):
        txt = clean_text(node.get_text(" "))
        if not txt:
            continue
        if any(w in txt.lower() for w in ["запис", "заказать", "в корзину", "стоимость", "цена"]):
            val = extract_price_from_text(txt)
            if val:
                return val
        for attr_name in ("data-price", "data-cost"):
            attr_val = node.get(attr_name, "")
            if attr_val:
                from helpers.text import clean_price
                v = clean_price(str(attr_val))
                if v:
                    return v

    # 5. Скрипты
    for script in soup.find_all("script"):
        txt = script.string or ""
        for pattern in (
            r'"price"\s*:\s*"?(?P<price>\d{2,6})"?',
            r'"cost"\s*:\s*"?(?P<price>\d{2,6})"?',
        ):
            m = re.search(pattern, txt)
            if m:
                from helpers.text import clean_price
                v = clean_price(m.group("price"))
                if v:
                    return v

    # 6. Верхние блоки страницы
    chunks = [
        clean_text(node.get_text(" "))
        for node in soup.find_all(["h1","h2","div","span","p"], limit=200)
    ]
    val = extract_price_from_text(" | ".join(c for c in chunks if c)[:4000])
    if val:
        return val

    # 7. Последний fallback
    return extract_price_from_text(clean_text(soup.get_text(" ")))


def collect_detail_urls(
    session, start_url: str, region: str, delay: float, stats: RunStats
) -> List[str]:
    queue = deque([start_url])
    visited: set[str] = set()
    details: OrderedDict[str, None] = OrderedDict()

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
                details.setdefault(link, None)
            elif is_category_url(link, region) and link not in visited:
                queue.append(link)

        print(
            f"[{LAB}][crawl] pages={len(visited)} "
            f"queue={len(queue)} detail_urls={len(details)} current={url}",
            file=sys.stderr,
        )

        if len(visited) > 1500:
            print(f"[{LAB}][warn] crawl page limit reached", file=sys.stderr)
            break

    return list(details.keys())


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

    reason = is_trash_name(name) or is_trash_url(url)
    if reason:
        stats.row_filtered(reason)
        return None

    price = extract_price(soup)
    cat = category_from_breadcrumbs(soup, CRUMB_BLACKLIST) or "Анализы"

    return {
        "lab": LAB,
        "city": city,
        "category": cat,
        "analysis_name": name,
        "price": price if price is not None else "",
        "url": url,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_region_url": BASE_URL + city_prefix(region) + "analizy",
    }


def run(region: str, outdir: Path, delay: float) -> int:
    if region not in CITY_CONFIG:
        print(f"[{LAB}] Unknown region: {region}", file=sys.stderr)
        return 1

    city_name, _ = CITY_CONFIG[region]
    start_url = BASE_URL + city_prefix(region) + "analizy"
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
    parser = argparse.ArgumentParser(description="Mediks parser")
    parser.add_argument("--region", required=True, choices=list(CITY_CONFIG.keys()))
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--delay", type=float, default=0.2)
    args = parser.parse_args()
    return run(args.region, Path(args.outdir), args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
