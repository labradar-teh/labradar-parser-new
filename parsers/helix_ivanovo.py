#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LAB = "helix"
CITY = "Иваново"
CITY_SLUG = "ivanovo"
BASE_URL = "https://helix.ru"
ROOT_URL = f"{BASE_URL}/{CITY_SLUG}/catalog/190-vse-analizy"
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
    response = session.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    return response.text


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def parse_last_page(html: str) -> int:
    numbers = set()

    for m in re.finditer(r"[?&]page=(\d+)", html):
        numbers.add(int(m.group(1)))

    soup = soup_from_html(html)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        mm = re.search(r"[?&]page=(\d+)", href)
        if mm:
            numbers.add(int(mm.group(1)))

    return max(numbers) if numbers else 1


def make_page_urls(base_url: str, last_page: int) -> List[str]:
    if last_page <= 1:
        return [base_url]
    urls = [base_url]
    for page in range(2, last_page + 1):
        sep = "&" if "?" in base_url else "?"
        urls.append(f"{base_url}{sep}page={page}")
    return urls


def is_item_url(url: str) -> bool:
    path = urlparse(url).path
    return path.startswith(f"/{CITY_SLUG}/catalog/item/")


def is_category_url(url: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    if path.startswith(f"/{CITY_SLUG}/catalog/item"):
        return False
    if path.startswith("/catalog/item"):
        return False
    return "/catalog/" in path


def extract_item_links(html: str) -> List[str]:
    soup = soup_from_html(html)
    found: List[str] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if not is_item_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        found.append(href)

    if found:
        return found

    # fallback regex
    for raw in re.findall(rf'https://helix\.ru/{CITY_SLUG}/catalog/item/\d{{2}}-\d{{3}}', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            found.append(href)

    for raw in re.findall(rf'/{CITY_SLUG}/catalog/item/\d{{2}}-\d{{3}}', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            found.append(href)

    return found


def extract_root_category_links(html: str) -> List[Tuple[str, str]]:
    """
    На ивановской странице категории часто даны как /catalog/... (без /ivanovo/).
    Мы их забираем все, а потом переводим в city-specific URL.
    """
    soup = soup_from_html(html)
    found: List[Tuple[str, str]] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        text = normalize_spaces(a.get_text(" ", strip=True))

        if not text:
            continue
        if "catalog" not in href:
            continue
        if "/catalog/item/" in href:
            continue

        if text in {
            "В каталог",
            "Главная",
            "Адреса",
            "Скидки и акции",
            "Helixbook",
            "Заказать",
            "Далее",
        }:
            continue

        if text == "Все анализы":
            continue

        # интересуют именно разделы анализов
        if href in seen:
            continue
        seen.add(href)
        found.append((text, href))

    return found


def to_city_category_url(generic_or_city_url: str) -> str:
    """
    Если уже city-specific — оставляем.
    Если общий /catalog/... — пробуем перевести в /ivanovo/catalog/...
    """
    parsed = urlparse(generic_or_city_url)
    path = parsed.path.rstrip("/")

    if path.startswith(f"/{CITY_SLUG}/catalog/"):
        return generic_or_city_url

    if path.startswith("/catalog/"):
        tail = path[len("/catalog/"):]
        return f"{BASE_URL}/{CITY_SLUG}/catalog/{tail}"

    return generic_or_city_url


def resolve_city_category_url(session: requests.Session, href: str) -> Optional[str]:
    """
    1) пробуем прямой city-specific URL
    2) если не работает, открываем общий URL и ищем canonical/og:url для Иваново
    """
    candidate = to_city_category_url(href)

    try:
        r = session.get(candidate, timeout=30, allow_redirects=True)
        if r.ok and f"/{CITY_SLUG}/catalog/" in r.url:
            return r.url.split("#")[0]
        if r.ok and f"/{CITY_SLUG}/catalog/" in candidate:
            return candidate
    except Exception:
        pass

    try:
        html = fetch(session, href)
        soup = soup_from_html(html)

        for sel in ['link[rel="canonical"]', 'meta[property="og:url"]']:
            for node in soup.select(sel):
                val = node.get("href") or node.get("content")
                if not val:
                    continue
                val = normalize_url(val)
                if f"/{CITY_SLUG}/catalog/" in val:
                    return val
    except Exception:
        return None

    return None


def extract_category_from_item_breadcrumbs(soup: BeautifulSoup) -> str:
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, [class*="breadcrumb"] a'):
        txt = normalize_spaces(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {
        "Главная",
        "Сдать анализы",
        CITY,
    }
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]
    return "Без категории"


def parse_price_from_item_page(text: str) -> Optional[int]:
    text = normalize_spaces(text)

    m = re.search(r"Стоимость\s*:\s*([\d\s]+)\s*₽", text, flags=re.IGNORECASE)
    if m:
        return int(re.sub(r"\D", "", m.group(1)))

    m = re.search(r"Стоимость\s*([\d\s]+)\s*₽", text, flags=re.IGNORECASE)
    if m:
        return int(re.sub(r"\D", "", m.group(1)))

    return None


def clean_item_name(text: str) -> str:
    text = normalize_spaces(text)
    text = re.sub(rf"\s+в\s+{re.escape(CITY)}\s*$", "", text, flags=re.IGNORECASE)
    return text.strip(" /")


def parse_item_page(html: str, url: str) -> Optional[Dict]:
    soup = soup_from_html(html)

    h1 = soup.find("h1")
    if not h1:
        return None

    analysis_name = clean_item_name(h1.get_text(" ", strip=True))
    if not analysis_name:
        return None

    full_text = normalize_spaces(soup.get_text(" ", strip=True))
    price = parse_price_from_item_page(full_text)
    if price is None:
        return None

    category = extract_category_from_item_breadcrumbs(soup)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price,
        "url": url,
    }


def collect_root_items(session: requests.Session, delay: float) -> OrderedDict:
    html = fetch(session, ROOT_URL)
    last_page = parse_last_page(html)

    items: OrderedDict[str, str] = OrderedDict()

    for idx, page_url in enumerate(make_page_urls(ROOT_URL, last_page), start=1):
        page_html = html if idx == 1 else fetch(session, page_url)
        page_items = extract_item_links(page_html)
        for item_url in page_items:
            items.setdefault(item_url, "Все анализы")
        print(f"[helix] root pages {idx}/{last_page}: {len(page_items)} items | total={len(items)}")
        time.sleep(delay)

    return items


def collect_category_items(session: requests.Session, delay: float) -> OrderedDict:
    root_html = fetch(session, ROOT_URL)
    raw_categories = extract_root_category_links(root_html)

    categories: OrderedDict[str, str] = OrderedDict()

    for category_name, href in raw_categories:
        city_url = resolve_city_category_url(session, href)
        if not city_url:
            continue
        categories.setdefault(city_url, category_name)

    print(f"[helix] resolved category pages: {len(categories)}")

    items: OrderedDict[str, str] = OrderedDict()

    for idx, (category_url, category_name) in enumerate(categories.items(), start=1):
        try:
            html = fetch(session, category_url)
            last_page = parse_last_page(html)

            for page_idx, page_url in enumerate(make_page_urls(category_url, last_page), start=1):
                page_html = html if page_idx == 1 else fetch(session, page_url)
                page_items = extract_item_links(page_html)
                for item_url in page_items:
                    items.setdefault(item_url, category_name)

                print(
                    f"[helix] category {idx}/{len(categories)} | "
                    f"{category_name} | page {page_idx}/{last_page} | "
                    f"items={len(page_items)} | total={len(items)}"
                )
                time.sleep(delay)

        except Exception as exc:
            print(f"[helix][error] category {category_url}: {exc}", file=sys.stderr)

    return items


def export_rows(rows: List[Dict], out_csv: Path, out_xlsx: Path) -> None:
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Helix Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Куда сохранить CSV/XLSX")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Пауза между запросами")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    root_items = collect_root_items(session, args.delay)
    category_items = collect_category_items(session, args.delay)

    merged_items: OrderedDict[str, str] = OrderedDict()

    for url, category in category_items.items():
        merged_items[url] = category

    for url, category in root_items.items():
        merged_items.setdefault(url, category)

    print(f"[helix] unique item urls collected: {len(merged_items)}")

    rows: List[Dict] = []
    for idx, (item_url, fallback_category) in enumerate(merged_items.items(), start=1):
        try:
            html = fetch(session, item_url)
            parsed = parse_item_page(html, item_url)
            if parsed is None:
                print(f"[helix][warn] skip no name/price: {item_url}", file=sys.stderr)
                continue

            if parsed["category"] == "Без категории":
                parsed["category"] = fallback_category

            rows.append(parsed)

            if idx % 100 == 0:
                print(f"[helix] parsed {idx}/{len(merged_items)}")
            time.sleep(args.delay)

        except Exception as exc:
            print(f"[helix][error] item {item_url}: {exc}", file=sys.stderr)

    deduped: OrderedDict[Tuple[str, str], Dict] = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].lower(), row["url"])
        deduped[key] = row

    final_rows = list(deduped.values())

    csv_path = outdir / "helix_ivanovo.csv"
    xlsx_path = outdir / "helix_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[helix] saved: {csv_path}")
    print(f"[helix] saved: {xlsx_path}")
    print(f"[helix] total rows: {len(final_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
