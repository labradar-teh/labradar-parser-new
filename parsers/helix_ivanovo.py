#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

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
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


def build_session():
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


def fetch(session, url, timeout=40):
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


def soup_from_html(html):
    return BeautifulSoup(html, "lxml")


def clean_text(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url):
    url = urljoin(BASE_URL, url.split("#")[0])
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)

    # оставляем только page
    new_query = {}
    if "page" in query:
        new_query["page"] = query["page"][0]

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/") or "/",
            "",
            urlencode(new_query),
            "",
        )
    )


def add_page_param(url, page):
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["page"] = [str(page)]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            "",
            urlencode({k: v[0] for k, v in query.items()}),
            "",
        )
    )


def parse_last_page(html):
    nums = set()

    for m in re.finditer(r"[?&]page=(\d+)", html):
        nums.add(int(m.group(1)))

    soup = soup_from_html(html)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            nums.add(int(m.group(1)))

    return max(nums) if nums else 1


def is_item_url(url):
    path = urlparse(url).path.rstrip("/")
    return path.startswith(f"/{CITY_SLUG}/catalog/item/")


def is_category_url(url):
    path = urlparse(url).path.rstrip("/")
    if "/catalog/item/" in path:
        return False
    return path.startswith(f"/{CITY_SLUG}/catalog/") or path.startswith("/catalog/")


def convert_to_city_category(url):
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if path.startswith(f"/{CITY_SLUG}/catalog/"):
        return normalize_url(url)

    if path.startswith("/catalog/"):
        tail = path[len("/catalog/"):]
        return normalize_url(f"{BASE_URL}/{CITY_SLUG}/catalog/{tail}")

    return normalize_url(url)


def extract_item_links(html):
    soup = soup_from_html(html)
    result = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        if is_item_url(href) and href not in seen:
            seen.add(href)
            result.append(href)

    if result:
        return result

    # fallback regex
    for raw in re.findall(rf'https://helix\.ru/{CITY_SLUG}/catalog/item/\d{{2}}-\d{{3}}', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            result.append(href)

    for raw in re.findall(rf'/{CITY_SLUG}/catalog/item/\d{{2}}-\d{{3}}', html):
        href = normalize_url(raw)
        if href not in seen:
            seen.add(href)
            result.append(href)

    return result


def extract_category_links(html):
    soup = soup_from_html(html)
    result = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = normalize_url(a["href"])
        text = clean_text(a.get_text(" ", strip=True))

        if not text:
            continue
        if not is_category_url(href):
            continue
        if is_item_url(href):
            continue

        if text.lower() in {
            "главная",
            "в каталог",
            "заказать",
            "далее",
            "адреса",
            "helixbook",
            "скидки и акции",
            "все анализы",
        }:
            continue

        city_href = convert_to_city_category(href)
        if city_href not in seen:
            seen.add(city_href)
            result.append((text, city_href))

    return result


def extract_category_from_breadcrumbs(soup):
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, .breadcrumbs a, [class*="breadcrumb"] a'):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {"Главная", CITY, "Сдать анализы"}
    crumbs = [c for c in crumbs if c not in blacklist]

    if crumbs:
        return crumbs[-1]
    return "Без категории"


def extract_price(text):
    text = clean_text(text)

    m = re.search(r"Стоимость\s*:?\s*([\d\s]+)\s*₽", text, flags=re.I)
    if m:
        raw = re.sub(r"\D", "", m.group(1))
        if raw:
            return int(raw)

    return None


def parse_item_page(html, url):
    soup = soup_from_html(html)

    h1 = soup.find("h1")
    if not h1:
        return None

    analysis_name = clean_text(h1.get_text(" ", strip=True))
    analysis_name = re.sub(rf"\s+в\s+{re.escape(CITY)}\s*$", "", analysis_name, flags=re.I).strip()

    if not analysis_name:
        return None

    full_text = clean_text(soup.get_text(" ", strip=True))
    price = extract_price(full_text)
    if price is None:
        return None

    category = extract_category_from_breadcrumbs(soup)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price,
        "url": url,
    }


def collect_root_items(session, delay):
    html = fetch(session, ROOT_URL)
    last_page = parse_last_page(html)

    items = OrderedDict()

    for page in range(1, last_page + 1):
        page_url = ROOT_URL if page == 1 else add_page_param(ROOT_URL, page)
        page_html = html if page == 1 else fetch(session, page_url)

        links = extract_item_links(page_html)
        for link in links:
            items.setdefault(link, "Все анализы")

        print(f"[helix][root] page {page}/{last_page} | items={len(links)} | total={len(items)}")
        time.sleep(delay)

    return items, html


def collect_category_items(session, root_html, delay):
    categories = extract_category_links(root_html)
    items = OrderedDict()

    print(f"[helix] category pages found: {len(categories)}")

    for idx, (category_name, category_url) in enumerate(categories, 1):
        try:
            html = fetch(session, category_url)
            last_page = parse_last_page(html)

            for page in range(1, last_page + 1):
                page_url = category_url if page == 1 else add_page_param(category_url, page)
                page_html = html if page == 1 else fetch(session, page_url)

                links = extract_item_links(page_html)
                for link in links:
                    items.setdefault(link, category_name)

                print(
                    f"[helix][cat] {idx}/{len(categories)} {category_name} "
                    f"| page {page}/{last_page} | items={len(links)} | total={len(items)}"
                )
                time.sleep(delay)

        except Exception as e:
            print(f"[helix][category-error] {category_url}: {e}", file=sys.stderr)

    return items


def export_rows(rows, out_csv, out_xlsx):
    df = pd.DataFrame(rows, columns=["lab", "city", "category", "analysis_name", "price", "url"])
    df = df.drop_duplicates(subset=["url"]).sort_values(["category", "analysis_name"], na_position="last")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False)


def main():
    parser = argparse.ArgumentParser(description="Helix Ivanovo parser")
    parser.add_argument("--outdir", default="output", help="Output directory")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    root_items, root_html = collect_root_items(session, args.delay)
    category_items = collect_category_items(session, root_html, args.delay)

    merged = OrderedDict()

    for url, category in category_items.items():
        merged[url] = category
    for url, category in root_items.items():
        merged.setdefault(url, category)

    print(f"[helix] unique item urls: {len(merged)}")

    rows = []
    for i, (url, fallback_category) in enumerate(merged.items(), 1):
        try:
            html = fetch(session, url)
            row = parse_item_page(html, url)
            if not row:
                continue

            if row["category"] == "Без категории":
                row["category"] = fallback_category

            rows.append(row)

            if i % 100 == 0:
                print(f"[helix] parsed {i}/{len(merged)} | rows={len(rows)}")

            time.sleep(args.delay)

        except Exception as e:
            print(f"[helix][item-error] {url}: {e}", file=sys.stderr)

    if not rows:
        raise RuntimeError("Helix parser returned 0 rows")

    dedup = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    csv_path = outdir / "helix_ivanovo.csv"
    xlsx_path = outdir / "helix_ivanovo.xlsx"
    export_rows(final_rows, csv_path, xlsx_path)

    print(f"[helix] saved: {csv_path}")
    print(f"[helix] saved: {xlsx_path}")
    print(f"[helix] total rows: {len(final_rows)}")


if __name__ == "__main__":
    main()
