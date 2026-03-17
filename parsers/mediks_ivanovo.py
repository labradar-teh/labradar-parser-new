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
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "mediks"
CITY = "Иваново"
BASE_URL = "https://medikslab.ru"
START_URL = f"{BASE_URL}/ivanovo/analizy"
DEFAULT_DELAY = 0.2

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


def fetch(session: requests.Session, url: str, timeout: int = 40) -> str:
    response = session.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    return response.text


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    return urljoin(BASE_URL, url.split("#")[0])


def is_mediks_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc in {"medikslab.ru", "www.medikslab.ru"} and parsed.path.startswith("/ivanovo/")


def is_category_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    return path.startswith("/ivanovo/analizy")


def is_detail_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    return path.startswith("/ivanovo/analiz/")


def parse_price_candidates(text: str) -> List[int]:
    text = clean_text(text)
    candidates = []

    for m in re.finditer(r"(\d[\d\s]{0,20})\s*(?:₽|руб\.?)", text, flags=re.I):
        raw = re.sub(r"\D", "", m.group(1))
        if not raw:
            continue
        value = int(raw)
        if 50 <= value <= 500000:
            candidates.append(value)

    return candidates


def extract_price_from_text(text: str) -> Optional[int]:
    candidates = parse_price_candidates(text)
    return candidates[0] if candidates else None


def extract_price(soup: BeautifulSoup) -> Optional[int]:
    # 1. Блоки со словом "Стоимость"
    for node in soup.find_all(string=re.compile(r"Стоимость", re.I)):
        parent = node.parent
        if parent:
            parent_text = clean_text(parent.get_text(" ", strip=True))
            price = extract_price_from_text(parent_text)
            if price is not None:
                return price

            if parent.parent:
                around = []
                for child in parent.parent.find_all(recursive=False):
                    txt = clean_text(child.get_text(" ", strip=True))
                    if txt:
                        around.append(txt)
                price = extract_price_from_text(" | ".join(around))
                if price is not None:
                    return price

    # 2. Блоки с price/cost
    for node in soup.select(
        '[class*="price"], [class*="Price"], [class*="cost"], [class*="Cost"], [itemprop="price"]'
    ):
        txt = clean_text(node.get_text(" ", strip=True))
        price = extract_price_from_text(txt)
        if price is not None:
            return price

    # 3. Кнопки / CTA / data-атрибуты
    for node in soup.find_all(["button", "a", "div", "span"]):
        txt = clean_text(node.get_text(" ", strip=True))
        if not txt:
            continue
        if any(word in txt.lower() for word in ["запис", "заказать", "в корзину", "оформить", "стоимость", "цена"]):
            price = extract_price_from_text(txt)
            if price is not None:
                return price

        for attr_name, attr_val in node.attrs.items():
            if isinstance(attr_val, list):
                attr_val = " ".join(map(str, attr_val))
            attr_text = clean_text(str(attr_val))
            price = extract_price_from_text(attr_text)
            if price is not None:
                return price

    # 4. Script / JSON
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

        price = extract_price_from_text(txt)
        if price is not None:
            return price

    # 5. Верхняя часть страницы
    chunks = []
    for node in soup.find_all(["h1", "h2", "div", "span", "p", "section"], limit=250):
        txt = clean_text(node.get_text(" ", strip=True))
        if txt:
            chunks.append(txt)
    top_text = " | ".join(chunks[:120])
    price = extract_price_from_text(top_text)
    if price is not None:
        return price

    # 6. Последний fallback по всей странице
    full_text = clean_text(soup.get_text(" ", strip=True))
    return extract_price_from_text(full_text)


def extract_category_name_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1]
    slug = slug.replace("-", " ").strip()
    return slug if slug else "Без категории"


def extract_category(soup: BeautifulSoup, fallback_url: str) -> str:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, .breadcrumbs a, [class*="breadcrumb"] a'):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {
        "Главная",
        "Иваново",
        "Анализы",
        "Сдать анализы",
        "Основной каталог",
    }
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]

    return extract_category_name_from_url(fallback_url)


def extract_analysis_name(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1:
        name = clean_text(h1.get_text(" ", strip=True))
        if len(name) >= 2:
            return name

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title = clean_text(title.split("|")[0].split("—")[0])
    return title if len(title) >= 2 else None


def extract_links(html: str) -> List[str]:
    soup = soup_from_html(html)
    result = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_mediks_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        result.append(href)

    for raw in re.findall(r'https?://(?:www\.)?medikslab\.ru/ivanovo/[^"\']+', html):
        href = normalize_url(raw)
        if is_mediks_url(href) and href not in seen:
            seen.add(href)
            result.append(href)

    for raw in re.findall(r'/ivanovo/[^"\']+', html):
        href = normalize_url(raw)
        if is_mediks_url(href) and href not in seen:
            seen.add(href)
            result.append(href)

    return result


def collect_category_urls(session: requests.Session, delay: float) -> List[str]:
    html = fetch(session, START_URL)
    links = extract_links(html)

    categories = OrderedDict()
    categories[START_URL] = None

    for link in links:
        if is_category_url(link) and not is_detail_url(link):
            categories.setdefault(link, None)

    return list(categories.keys())


def collect_detail_urls(session: requests.Session, delay: float) -> List[str]:
    queue = deque(collect_category_urls(session, delay))
    visited = set()
    details = OrderedDict()

    while queue:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            html = fetch(session, url)
        except Exception as e:
            print(f"[mediks][crawl-error] {url}: {e}", file=sys.stderr)
            continue

        links = extract_links(html)

        for link in links:
            if is_detail_url(link):
                details.setdefault(link, None)
            elif is_category_url(link) and link not in visited:
                queue.append(link)

        print(f"[mediks][crawl] pages={len(visited)} queue={len(queue)} detail_urls={len(details)} current={url}")
        time.sleep(delay)

        if len(visited) > 1000:
            print("[mediks][warn] crawl page limit reached")
            break

    return list(details.keys())


def parse_detail_page(session: requests.Session, url: str) -> Optional[Dict]:
    html = fetch(session, url)
    soup = soup_from_html(html)

    analysis_name = extract_analysis_name(soup)
    if not analysis_name:
        return None

    price = extract_price(soup)
    category = extract_category(soup, url)

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
    parser = argparse.ArgumentParser(description="Mediks Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    detail_urls = collect_detail_urls(session, args.delay)
    print(f"[mediks] collected detail urls: {len(detail_urls)}")

    rows = []
    for i, url in enumerate(detail_urls, 1):
        try:
            data = parse_detail_page(session, url)
            if data:
                rows.append(data)

            if i % 100 == 0:
                print(f"[mediks] parsed {i}/{len(detail_urls)} | rows={len(rows)}")

            time.sleep(args.delay)
        except Exception as e:
            print(f"[mediks][parse-error] {url}: {e}", file=sys.stderr)

    if not rows:
        print("[mediks] WARNING: 0 rows collected")

    dedup = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    csv_path = outdir / "mediks_ivanovo.csv"
    xlsx_path = outdir / "mediks_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[mediks] saved: {csv_path}")
    print(f"[mediks] saved: {xlsx_path}")
    print(f"[mediks] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
