#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict, deque
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "citilab"
CITY = "Кострома"
BASE_URL = "https://citilab.ru"
START_URL = f"{BASE_URL}/kostroma/catalog/"
DEFAULT_DELAY = 0.12

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

BAD_URL_PARTS = {
    "/promo-",
    "/akts",
    "/akcii",
    "/news",
    "/novost",
    "/doctors",
    "/adres",
    "/contacts",
    "/about",
    "/corporate",
    "/vacanc",
    "/franch",
    "/medicinskie-centry",
    "/dopolnitel",
}

BAD_TITLE_PARTS = {
    "акция",
    "акции",
    "скидк",
    "выезд медсестры",
    "взятие биоматериала",
    "дальний пригород",
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


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url: str) -> str:
    full = urljoin(BASE_URL, url.split("#")[0])
    parsed = urlparse(full)
    path = parsed.path.rstrip("/") + "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def fetch(session: requests.Session, url: str, timeout: int = 35) -> str:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


def safe_fetch(session: requests.Session, url: str, timeout: int = 35) -> str | None:
    try:
        return fetch(session, url, timeout=timeout)
    except Exception as exc:
        print(f"[citilab][fetch-error] {url}: {exc}", file=sys.stderr)
        return None


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def is_citilab_catalog_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc not in {"citilab.ru", "www.citilab.ru"}:
        return False
    return parsed.path.startswith("/kostroma/catalog/")


def is_bad_catalog_url(url: str) -> bool:
    low = url.lower()
    return any(part in low for part in BAD_URL_PARTS)


def is_detail_url(url: str) -> bool:
    if not is_citilab_catalog_url(url):
        return False
    if is_bad_catalog_url(url):
        return False

    path = urlparse(url).path.rstrip("/")
    parts = [p for p in path.split("/") if p]

    # /ivanovo/catalog/<...>/<...>/... — достаточно глубокий путь
    if len(parts) < 4:
        return False

    last = parts[-1].lower()

    # отсекаем явные разделы/служебные страницы
    if last in {
        "catalog",
        "analizy",
        "analiz",
        "uslugi",
        "service",
        "services",
    }:
        return False

    return True


def is_category_url(url: str) -> bool:
    if not is_citilab_catalog_url(url):
        return False
    if is_bad_catalog_url(url):
        return False
    return not is_detail_url(url)


def extract_links(html: str) -> list[str]:
    soup = soup_from_html(html)
    result: list[str] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_citilab_catalog_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        result.append(href)

    for raw in re.findall(r'https?://(?:www\.)?citilab\.ru/kostroma/catalog/[^"\']+', html):
        href = normalize_url(raw)
        if href not in seen and is_citilab_catalog_url(href):
            seen.add(href)
            result.append(href)

    for raw in re.findall(r'/kostroma/catalog/[^"\']+', html):
        href = normalize_url(raw)
        if href not in seen and is_citilab_catalog_url(href):
            seen.add(href)
            result.append(href)

    return result


def extract_price(html: str) -> int | str:
    text = clean_text(soup_from_html(html).get_text(" ", strip=True))

    patterns = [
        r"Цена\s*:?\s*([\d\s]+)\s*(?:₽|руб)",
        r"Стоимость\s*:?\s*([\d\s]+)\s*(?:₽|руб)",
        r"Стоимость исследования\s*:?\s*([\d\s]+)\s*(?:₽|руб)",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            raw = re.sub(r"\D", "", m.group(1))
            if raw:
                value = int(raw)
                if 50 <= value <= 500000:
                    return value

    # fallback: ищем первый адекватный price-блок
    soup = soup_from_html(html)
    for node in soup.select(
        '[class*="price"], [class*="Price"], [class*="cost"], [class*="Cost"], [itemprop="price"]'
    ):
        txt = clean_text(node.get_text(" ", strip=True))
        m = re.search(r"([\d\s]+)\s*(?:₽|руб)", txt, flags=re.I)
        if m:
            raw = re.sub(r"\D", "", m.group(1))
            if raw:
                value = int(raw)
                if 50 <= value <= 500000:
                    return value

    return ""


def extract_name(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    if h1:
        txt = clean_text(h1.get_text(" ", strip=True))
        if txt:
            return txt

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title = clean_text(title.split("|")[0].split("—")[0].split(" - ")[0])
    return title if title else None


def extract_category(soup: BeautifulSoup, url: str) -> str:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, .breadcrumbs a, [class*="breadcrumb"] a'):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {"Главная", "Иваново", "Каталог"}
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]

    parts = [p for p in urlparse(url).path.split("/") if p]
    if len(parts) >= 4:
        return parts[-2].replace("-", " ").strip()
    return "Анализы"


def looks_like_bad_row(name: str, url: str, price: int | str) -> bool:
    low_name = name.lower()
    low_url = url.lower()

    if any(part in low_name for part in BAD_TITLE_PARTS):
        return True
    if is_bad_catalog_url(low_url):
        return True
    if price == 1:
        return True
    return False


def collect_detail_urls(session: requests.Session, delay: float) -> list[str]:
    queue = deque([START_URL])
    visited_pages: set[str] = set()
    detail_urls: OrderedDict[str, None] = OrderedDict()

    while queue:
        url = queue.popleft()
        if url in visited_pages:
            continue
        visited_pages.add(url)

        html = safe_fetch(session, url)
        if not html:
            continue

        links = extract_links(html)

        for link in links:
            if is_detail_url(link):
                detail_urls.setdefault(link, None)
            elif is_category_url(link) and link not in visited_pages:
                queue.append(link)

        print(
            f"[citilab][crawl] pages={len(visited_pages)} "
            f"queue={len(queue)} detail_urls={len(detail_urls)} current={url}"
        )
        time.sleep(delay)

        if len(visited_pages) > 2500:
            print("[citilab][warn] crawl page limit reached")
            break

    return list(detail_urls.keys())


def parse_detail_page(session: requests.Session, url: str) -> dict | None:
    html = safe_fetch(session, url)
    if not html:
        return None

    soup = soup_from_html(html)
    name = extract_name(soup)
    if not name or len(name) < 3:
        return None

    price = extract_price(html)
    category = extract_category(soup, url)

    if looks_like_bad_row(name, url, price):
        return None

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": name,
        "price": price,
        "url": url,
    }


def export_rows(rows: list[dict], out_csv: Path, out_xlsx: Path) -> None:
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Citilab Ivanovo parser")
    parser.add_argument("--outdir", default="out", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()
    detail_urls = collect_detail_urls(session, args.delay)
    print(f"[citilab] collected detail urls: {len(detail_urls)}")

    rows: list[dict] = []
    for i, url in enumerate(detail_urls, 1):
        row = parse_detail_page(session, url)
        if row:
            rows.append(row)

        if i % 100 == 0:
            print(f"[citilab] parsed {i}/{len(detail_urls)} | rows={len(rows)}")

        time.sleep(args.delay)

    dedup = OrderedDict()
    for row in rows:
        key = (str(row["analysis_name"]).strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    csv_path = outdir / "citilab_kostroma.csv"
    xlsx_path = outdir / "citilab_kostroma.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[citilab] saved: {csv_path}")
    print(f"[citilab] saved: {xlsx_path}")
    print(f"[citilab] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
