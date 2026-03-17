#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict, deque
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


LAB = "kislorod"
CITY = "Иваново"
BASE_URL = "https://kislorod-doctor.ru"
START_URL = f"{BASE_URL}/analyzes/"
DEFAULT_DELAY = 0.15


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0]).rstrip("/") + "/"


def is_kislorod_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in {"kislorod-doctor.ru", "www.kislorod-doctor.ru"}


def is_catalog_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/") + "/"
    return path.startswith("/analyzes/")


def is_detail_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/") + "/"
    parts = [p for p in path.split("/") if p]
    # /analyzes/<category>/<item>/
    return len(parts) >= 3 and parts[0] == "analyzes"


def extract_price_candidates(text: str) -> List[int]:
    text = clean_text(text)
    result = []

    for m in re.finditer(r"(\d[\d\s]{0,20})\s*(?:₽|руб\.?)", text, flags=re.I):
        raw = re.sub(r"\D", "", m.group(1))
        if not raw:
            continue
        value = int(raw)
        if 50 <= value <= 500000:
            result.append(value)

    return result


def extract_price_from_html(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "lxml")

    # 1. Явные price-блоки
    for node in soup.select(
        '[class*="price"], [class*="Price"], [class*="cost"], [class*="Cost"], [itemprop="price"]'
    ):
        txt = clean_text(node.get_text(" ", strip=True))
        prices = extract_price_candidates(txt)
        if prices:
            return prices[0]

    # 2. Блоки со словом "Стоимость" / "Цена"
    for node in soup.find_all(string=re.compile(r"(Стоимость|Цена)", re.I)):
        parent = node.parent
        if parent:
            txt = clean_text(parent.get_text(" ", strip=True))
            prices = extract_price_candidates(txt)
            if prices:
                return prices[0]

            if parent.parent:
                around = []
                for child in parent.parent.find_all(recursive=False):
                    chunk = clean_text(child.get_text(" ", strip=True))
                    if chunk:
                        around.append(chunk)
                prices = extract_price_candidates(" | ".join(around))
                if prices:
                    return prices[0]

    # 3. Script / JSON
    for script in soup.find_all("script"):
        txt = script.string or script.get_text(" ", strip=True) or ""
        if not txt:
            continue

        patterns = [
            r'"price"\s*:\s*"?(?P<price>\d{2,6})"?',
            r'"Price"\s*:\s*"?(?P<price>\d{2,6})"?',
            r'"cost"\s*:\s*"?(?P<price>\d{2,6})"?',
            r'"amount"\s*:\s*"?(?P<price>\d{2,6})"?',
        ]
        for pattern in patterns:
            m = re.search(pattern, txt)
            if m:
                value = int(m.group("price"))
                if 50 <= value <= 500000:
                    return value

        prices = extract_price_candidates(txt)
        if prices:
            return prices[0]

    # 4. Верх страницы
    chunks = []
    for node in soup.find_all(["h1", "h2", "div", "span", "p", "section"], limit=250):
        txt = clean_text(node.get_text(" ", strip=True))
        if txt:
            chunks.append(txt)
    prices = extract_price_candidates(" | ".join(chunks[:120]))
    if prices:
        return prices[0]

    # 5. Вся страница
    full_text = clean_text(soup.get_text(" ", strip=True))
    prices = extract_price_candidates(full_text)
    if prices:
        return prices[0]

    return None


def extract_name_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    if h1:
        name = clean_text(h1.get_text(" ", strip=True))
        if len(name) >= 2:
            return name

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title = clean_text(title.split("|")[0].split("—")[0])
    return title if len(title) >= 2 else None


def extract_category_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2:
        return parts[1].replace("-", " ").strip()
    return "Без категории"


def extract_links_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    result = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_kislorod_url(href):
            continue
        if not is_catalog_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        result.append(href)

    for raw in re.findall(r'https?://(?:www\.)?kislorod-doctor\.ru/analyzes/[^"\']+', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            result.append(href)

    for raw in re.findall(r'/analyzes/[^"\']+', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            result.append(href)

    return result


def collect_catalog_and_detail_urls(page, delay: float) -> List[str]:
    queue = deque([START_URL])
    visited_catalog: Set[str] = set()
    detail_urls: OrderedDict[str, None] = OrderedDict()

    while queue:
        url = queue.popleft()
        if url in visited_catalog:
            continue
        visited_catalog.add(url)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(800)
            html = page.content()
        except Exception as e:
            print(f"[kislorod][crawl-error] {url}: {e}", file=sys.stderr)
            continue

        links = extract_links_from_html(html)

        for link in links:
            if is_detail_url(link):
                detail_urls.setdefault(link, None)
            else:
                if link not in visited_catalog:
                    queue.append(link)

        print(
            f"[kislorod][crawl] pages={len(visited_catalog)} "
            f"queue={len(queue)} detail_urls={len(detail_urls)} current={url}"
        )
        time.sleep(delay)

        if len(visited_catalog) > 3000:
            print("[kislorod][warn] crawl page limit reached")
            break

    return list(detail_urls.keys())


def parse_detail_page(page, url: str) -> Optional[Dict]:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(600)
        html = page.content()
    except Exception as e:
        print(f"[kislorod][detail-error] {url}: {e}", file=sys.stderr)
        return None

    analysis_name = extract_name_from_html(html)
    if not analysis_name:
        return None

    price = extract_price_from_html(html)
    category = extract_category_from_url(url)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price if price is not None else "",
        "url": url,
    }


def export_rows(rows: List[Dict], out_csv: Path, out_xlsx: Path) -> None:
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Kislorod Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="ru-RU",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        detail_urls = collect_catalog_and_detail_urls(page, args.delay)
        print(f"[kislorod] collected detail urls: {len(detail_urls)}")

        rows = []
        for i, url in enumerate(detail_urls, 1):
            data = parse_detail_page(page, url)
            if data:
                rows.append(data)

            if i % 100 == 0:
                print(f"[kislorod] parsed {i}/{len(detail_urls)} | rows={len(rows)}")

            time.sleep(args.delay)

        browser.close()

    dedup = OrderedDict()
    for row in rows:
        key = (str(row["analysis_name"]).strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    csv_path = outdir / "kislorod_ivanovo.csv"
    xlsx_path = outdir / "kislorod_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[kislorod] saved: {csv_path}")
    print(f"[kislorod] saved: {xlsx_path}")
    print(f"[kislorod] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
