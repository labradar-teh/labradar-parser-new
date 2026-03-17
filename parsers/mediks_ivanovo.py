#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
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


LAB = "mediks"
CITY = "Иваново"
BASE_URL = "https://medikslab.ru"
START_URL = f"{BASE_URL}/ivanovo/analizy"
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


def is_mediks_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != urlparse(BASE_URL).netloc:
        return False
    return parsed.path.startswith("/ivanovo/")


def is_category_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    return path.startswith("/ivanovo/analizy")


def is_detail_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    return path.startswith("/ivanovo/analiz/")


def extract_category_links(start_html: str) -> List[str]:
    soup = soup_from_html(start_html)
    found: List[str] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_mediks_url(href):
            continue
        if not is_category_url(href):
            continue
        if is_detail_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        found.append(href)

    # если главная страница категорий сама содержит карточки, оставим ее тоже
    if START_URL not in seen:
        found.insert(0, START_URL)

    return found


def extract_detail_links(html: str) -> List[str]:
    soup = soup_from_html(html)
    found: List[str] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_mediks_url(href):
            continue
        if not is_detail_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        found.append(href)

    if found:
        return found

    # fallback regex по сырому HTML
    for raw in re.findall(r'/ivanovo/analiz/[^"\']+', html):
        href = normalize_url(raw)
        if is_detail_url(href) and href not in seen:
            seen.add(href)
            found.append(href)

    return found


def extract_category_from_detail_url(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    # /ivanovo/analiz/<slug>/<category>
    if len(parts) >= 4:
        return parts[-1].replace("-", " ").strip().capitalize()
    return "Без категории"


def parse_price_value(text: str) -> Optional[int]:
    text = normalize_spaces(text)
    m = re.search(r"(\d[\d\s]{0,20})\s*₽", text)
    if not m:
        m = re.search(r"(\d[\d\s]{0,20})\s*руб\.?", text, flags=re.IGNORECASE)
    if not m:
        return None

    value = re.sub(r"\D", "", m.group(1))
    if not value:
        return None

    num = int(value)
    if 50 <= num <= 500000:
        return num
    return None


def parse_main_price_from_text(full_text: str) -> Optional[int]:
    text = normalize_spaces(full_text)

    # 1. приоритетный блок "Стоимость"
    m = re.search(
        r"Стоимость\s*([\d\s]+)\s*₽",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        return int(re.sub(r"\D", "", m.group(1)))

    # 2. fallback между "Стоимость" и "Взятие биоматериала"
    m = re.search(
        r"Стоимость(.*?)Взятие биоматериала",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        val = parse_price_value(m.group(1))
        if val is not None:
            return val

    # 3. если есть несколько цен, пытаемся не брать биоматериал
    prices = []
    for match in re.finditer(r"(\d[\d\s]{0,20})\s*₽", text):
        num = int(re.sub(r"\D", "", match.group(1)))
        if 50 <= num <= 500000:
            prices.append(num)

    if not prices:
        return None

    # обычно первая цена после блока "Стоимость" — цена анализа
    return prices[0]


def extract_category_from_breadcrumbs(soup: BeautifulSoup) -> Optional[str]:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, [class*="breadcrumb"] a'):
        txt = normalize_spaces(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {
        "Главная",
        "Иваново",
        "Сдать анализы",
        "Анализы",
        "Основной каталог",
    }
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]
    return None


def parse_detail_page(html: str, url: str) -> Optional[Dict]:
    soup = soup_from_html(html)

    h1 = soup.find("h1")
    if not h1:
        return None

    analysis_name = normalize_spaces(h1.get_text(" ", strip=True))
    if not analysis_name:
        return None

    # сначала ищем "Стоимость" точечно
    price = None

    for node in soup.find_all(text=re.compile(r"Стоимость", re.IGNORECASE)):
        parent_text = normalize_spaces(node.parent.get_text(" ", strip=True) if node.parent else "")
        price = parse_main_price_from_text(parent_text)
        if price is not None:
            break

    # fallback по всему тексту страницы
    if price is None:
        full_text = normalize_spaces(soup.get_text(" ", strip=True))
        price = parse_main_price_from_text(full_text)

    if price is None:
        return None

    category = extract_category_from_breadcrumbs(soup) or extract_category_from_detail_url(url)

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
    parser = argparse.ArgumentParser(description="Mediks Lab Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    start_html = fetch(session, START_URL)
    category_urls = extract_category_links(start_html)
    print(f"[mediks] category pages: {len(category_urls)}")

    detail_urls: OrderedDict[str, None] = OrderedDict()
    for idx, category_url in enumerate(category_urls, start=1):
        try:
            html = fetch(session, category_url)
            links = extract_detail_links(html)
            for link in links:
                detail_urls.setdefault(link, None)

            print(
                f"[mediks] categories {idx}/{len(category_urls)} "
                f"-> found {len(links)} detail links | total={len(detail_urls)}"
            )
            time.sleep(args.delay)
        except Exception as exc:
            print(f"[mediks][error] category {category_url}: {exc}", file=sys.stderr)

    print(f"[mediks] unique detail urls: {len(detail_urls)}")

    rows: List[Dict] = []
    for idx, url in enumerate(detail_urls.keys(), start=1):
        try:
            html = fetch(session, url)
            parsed = parse_detail_page(html, url)
            if parsed is None:
                print(f"[mediks][warn] skip no price/name: {url}", file=sys.stderr)
                continue

            rows.append(parsed)

            if idx % 100 == 0:
                print(f"[mediks] parsed {idx}/{len(detail_urls)}")
            time.sleep(args.delay)
        except Exception as exc:
            print(f"[mediks][error] item {url}: {exc}", file=sys.stderr)

    deduped: OrderedDict[tuple, Dict] = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].lower(), row["url"])
        deduped[key] = row

    final_rows = list(deduped.values())

    csv_path = outdir / "mediks_ivanovo.csv"
    xlsx_path = outdir / "mediks_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[mediks] saved: {csv_path}")
    print(f"[mediks] saved: {xlsx_path}")
    print(f"[mediks] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
