#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kislorod-doctor parser.

Кислород — локальная лаборатория Иваново, отдельного раздела Кострома нет.
Это корректно фиксируется в логах без падения.

Улучшения vs оригинала:
- убран хардкод MAX_PAGES=223 → автодетект последней страницы
- добавлен обход подкатегорий (собираем ссылки на категории тоже)
- retry-сессия через helpers
- RunStats
"""

import argparse
import re
import sys
from collections import OrderedDict, deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Optional

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers.session import build_session, polite_fetch, safe_fetch
from helpers.text import clean_text, extract_price_from_text, title_from_soup, category_from_breadcrumbs
from helpers.export import export_rows
from helpers.filters import is_trash_name, is_trash_url
from helpers.stats import RunStats

LAB = "kislorod"
BASE_URL = "https://kislorod-doctor.ru"
START_URL = f"{BASE_URL}/analyzes/"

CITY_CONFIG = {
    "ivanovo": ("Иваново", START_URL),
    # Кострома: нет регионального каталога
    "kostroma": ("Кострома", None),
}

CRUMB_BLACKLIST = {"Главная", "Анализы", "Иваново", "Кострома"}


def is_kislorod_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in {"kislorod-doctor.ru", "www.kislorod-doctor.ru"}


def is_analyzes_url(url: str) -> bool:
    if not is_kislorod_url(url):
        return False
    return "/analyzes" in urlparse(url).path


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def detect_last_page(html: str) -> int:
    """Ищет последний номер страницы в пагинаторе."""
    soup = BeautifulSoup(html, "html.parser")
    max_page = 1
    for a in soup.find_all("a", href=True):
        m = re.search(r"PAGEN_1=(\d+)", a["href"])
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def parse_listing_page(html: str, page_url: str) -> list[dict]:
    """
    Парсит страницу-листинг Кислорода.
    Анализы представлены как ссылки формата:
    «Название анализа  NNN руб.»
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # Собираем ссылки-анализы: ссылки ведут на /analyzes/<slug>/ и содержат цену
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = clean_text(a.get_text(" ", strip=True))

        # только ссылки на отдельные анализы (не категории)
        full_url = normalize_url(href)
        if not is_analyzes_url(full_url):
            continue

        # в тексте ссылки должна быть цена
        if "руб" not in text.lower() and "₽" not in text:
            continue

        # разбираем: «Название  NNN руб.»
        m = re.match(r"^(.*?)\s+([\d][\d\s]*)(?:\s*руб\.?|\s*₽)\s*$", text, re.I)
        if not m:
            continue

        name = clean_text(m.group(1))
        price_str = m.group(2)
        price = extract_price_from_text(price_str + " ₽")

        if not name or not price:
            continue

        reason = is_trash_name(name)
        if reason:
            continue

        rows.append({
            "lab": LAB,
            "city": "Иваново",
            "category": "Анализы",
            "analysis_name": name,
            "price": price,
            "url": full_url if full_url.startswith(BASE_URL) else page_url,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "source_region_url": START_URL,
        })

    return rows


def collect_categories(session, stats: RunStats, delay: float) -> list[str]:
    """Собирает URL всех подкатегорий анализов."""
    html = polite_fetch(session, START_URL, delay=delay, label=LAB)
    if not html:
        return [START_URL]

    soup = BeautifulSoup(html, "html.parser")
    cats = OrderedDict()
    cats[START_URL] = None

    for a in soup.select("a[href]"):
        href = normalize_url(a.get("href", ""))
        txt = clean_text(a.get_text(" ", strip=True))
        if not is_analyzes_url(href):
            continue
        # ссылки на категории — без PAGEN_ и в пути /analyzes/
        if "PAGEN_1" not in href and href.count("/") >= 4:
            cats[href] = None

    return list(cats.keys())


def run(region: str, outdir: Path, delay: float) -> int:
    if region not in CITY_CONFIG:
        print(f"[{LAB}] Unknown region: {region}", file=sys.stderr)
        return 1

    city_name, cat_url = CITY_CONFIG[region]
    stats = RunStats(f"{LAB}/{region}")

    if cat_url is None:
        print(
            f"[{LAB}][info] У лаборатории Кислород нет регионального каталога "
            f"для {city_name}. Пропускаем (только Иваново).",
            file=sys.stderr,
        )
        export_rows([], outdir / f"{LAB}_{region}.csv", outdir / f"{LAB}_{region}.xlsx")
        return 0

    print(f"[{LAB}] Starting {city_name}, start_url={cat_url}", file=sys.stderr)
    session = build_session()

    # собираем список категорий
    category_urls = collect_categories(session, stats, delay)
    print(f"[{LAB}] categories found: {len(category_urls)}", file=sys.stderr)

    data: dict[str, dict] = {}

    for cat_url in category_urls:
        # узнаём последнюю страницу
        html = polite_fetch(session, cat_url, delay=delay, label=LAB)
        if not html:
            stats.page_err(cat_url)
            continue

        stats.page_ok()
        last_page = detect_last_page(html)

        # обходим все страницы категории
        for page_num in range(1, last_page + 1):
            if page_num == 1:
                page_url = cat_url
                page_html = html
            else:
                sep = "&" if "?" in cat_url else "?"
                page_url = f"{cat_url.rstrip('/')}/{sep}PAGEN_1={page_num}"
                page_html = polite_fetch(session, page_url, delay=delay, label=LAB)
                if not page_html:
                    stats.page_err(page_url)
                    continue
                stats.page_ok()

            rows = parse_listing_page(page_html, page_url)
            for row in rows:
                stats.card_found()
                key = row["analysis_name"].lower()
                if key not in data:
                    data[key] = row
                    stats.row_saved()

        print(
            f"[{LAB}] cat={cat_url} last_page={last_page} collected={len(data)}",
            file=sys.stderr,
        )

    final_rows = sorted(data.values(), key=lambda x: x["analysis_name"].lower())

    csv_path = outdir / f"{LAB}_{region}.csv"
    xlsx_path = outdir / f"{LAB}_{region}.xlsx"
    n = export_rows(final_rows, csv_path, xlsx_path)

    stats.print_summary()
    print(f"[{LAB}] saved {n} rows → {csv_path}", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Kislorod parser")
    parser.add_argument("--region", required=True, choices=list(CITY_CONFIG.keys()))
    parser.add_argument("--outdir", default="output")
    parser.add_argument("--delay", type=float, default=0.1)
    args = parser.parse_args()
    return run(args.region, Path(args.outdir), args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
