# parser_kislorod.py

import csv
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://kislorod-doctor.ru"
START_URLS = [
    "https://kislorod-doctor.ru/catalog/analizy/",
    "https://kislorod-doctor.ru/analizy/",
]
CITY_NAME = "Иваново"
LAB_NAME = "Кислород"

OUTPUT_DIR = "output"
OUTPUT_CSV = f"{OUTPUT_DIR}/kislorod_ivanovo.csv"
OUTPUT_XLSX = f"{OUTPUT_DIR}/kislorod_ivanovo.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

BAD_NAME_PATTERNS = [
    r"\bакц",
    r"\bскид",
    r"\bвзятие\b",
    r"\bзабор\b",
    r"\bбиоматериал",
    r"\bуслуг",
    r"\bуслуга\b",
    r"\bприем\b",
    r"\bприём\b",
    r"\bконсультац",
    r"\bвыезд\b",
    r"\bузи\b",
    r"\bэкг\b",
    r"\bээг\b",
    r"\bмассаж\b",
    r"\bкапельниц",
    r"\bпроцедур",
    r"\bманипуляц",
    r"\bчекап\b",
    r"\bcheck[- ]?up\b",
    r"\bкомплекс\b",
    r"\bпрофиль\b",
]

GOOD_URL_PARTS = [
    "/anal",
    "/catalog/",
    "/price",
    "/prices",
]

TIMEOUT = 30
SLEEP = 0.1
MAX_PAGES = 3000


session = requests.Session()
session.headers.update(HEADERS)


def ensure_output_dir():
    import os
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_space(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def clean_url(url):
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))


def same_domain(url):
    return urlparse(url).netloc.endswith(urlparse(BASE_URL).netloc)


def fetch(url):
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    time.sleep(SLEEP)
    return resp.text


def clean_price(text):
    if not text:
        return None
    m = re.findall(r"\d[\d\s]*", text.replace("\xa0", " "))
    if not m:
        return None
    digits = re.sub(r"[^\d]", "", max(m, key=len))
    if not digits:
        return None
    val = int(digits)
    if val <= 1:
        return None
    return val


def is_bad_name(name):
    low = (name or "").lower()
    return any(re.search(p, low) for p in BAD_NAME_PATTERNS)


def looks_like_analysis(name):
    if not name:
        return False
    if len(name) < 3:
        return False
    if is_bad_name(name):
        return False
    return True


def looks_like_catalog_url(url):
    low = url.lower()
    if not same_domain(url):
        return False
    if any(x in low for x in ["/wp-", "/tag/", "/author/", "/news/", "/blog/", "/kontakt", "/about", "/vacancy"]):
        return False
    return any(part in low for part in GOOD_URL_PARTS)


def extract_links(soup, current_url):
    found = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        url = clean_url(urljoin(current_url, href))
        if looks_like_catalog_url(url):
            found.add(url)
    return found


def detect_category(soup):
    crumbs = []
    for el in soup.select('nav a, .breadcrumb a, .breadcrumbs a, [itemprop="breadcrumb"] a'):
        t = normalize_space(el.get_text(" ", strip=True))
        if t and t.lower() not in {"главная", "каталог"}:
            crumbs.append(t)
    if crumbs:
        return crumbs[-1]

    h1 = soup.find("h1")
    if h1:
        t = normalize_space(h1.get_text(" ", strip=True))
        if t and "анализ" not in t.lower():
            return t

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    title = normalize_space(title.split("|")[0].split("—")[0])
    return title or "Анализы"


def candidate_name_price_pairs_from_page(soup):
    rows = []

    selectors = [
        ".product-card, .catalog-item, .price-item, .analysis-item, .item, .product",
        "tr",
        "li",
        ".elementor-widget-container > div",
    ]

    seen = set()

    for selector in selectors:
        for node in soup.select(selector):
            text = normalize_space(node.get_text(" ", strip=True))
            if len(text) < 5:
                continue

            price = clean_price(text)
            if not price:
                continue

            name = None

            for sel in [
                ".product-card__title",
                ".catalog-item__title",
                ".analysis-item__title",
                ".price-item__title",
                ".item-title",
                ".title",
                "h2",
                "h3",
                "h4",
                "strong",
                "b",
                "td:first-child",
            ]:
                el = node.select_one(sel)
                if el:
                    cand = normalize_space(el.get_text(" ", strip=True))
                    if cand and cand != text:
                        name = cand
                        break

            if not name:
                parts = re.split(r"\s{2,}| \d[\d\s]* ?(?:₽|руб|р\.)", text, maxsplit=1)
                if parts:
                    name = normalize_space(parts[0])

            if not looks_like_analysis(name):
                continue

            key = (name.lower(), price)
            if key in seen:
                continue
            seen.add(key)
            rows.append((name, price))

        if rows:
            return rows

    return rows


def extract_detail_name(soup):
    for sel in ["h1", ".product_title", ".entry-title", ".analysis-title", ".page-title"]:
        el = soup.select_one(sel)
        if el:
            name = normalize_space(el.get_text(" ", strip=True))
            if looks_like_analysis(name):
                return name
    return None


def extract_detail_price(soup):
    text = soup.get_text("\n", strip=True)
    return clean_price(text)


def is_probable_detail_page(url, soup):
    if re.search(r"/analiz|/analysis|/product|/item|/price", url.lower()):
        name = extract_detail_name(soup)
        price = extract_detail_price(soup)
        return bool(name and price)
    return False


def parse_detail_page(url, soup, category):
    name = extract_detail_name(soup)
    price = extract_detail_price(soup)
    if not name or not price:
        return None
    if is_bad_name(name):
        return None
    return {
        "lab": LAB_NAME,
        "city": CITY_NAME,
        "category": category,
        "analysis_name": name,
        "price": price,
        "url": url,
    }


def crawl():
    queue = deque(START_URLS)
    visited = set()
    dataset = {}
    pages = 0

    while queue and pages < MAX_PAGES:
        url = clean_url(queue.popleft())
        if url in visited:
            continue
        visited.add(url)

        try:
            html = fetch(url)
        except Exception:
            continue

        pages += 1
        soup = BeautifulSoup(html, "html.parser")
        category = detect_category(soup)

        if is_probable_detail_page(url, soup):
            row = parse_detail_page(url, soup, category)
            if row:
                key = row["analysis_name"].lower()
                if key not in dataset or row["price"] < dataset[key]["price"]:
                    dataset[key] = row

        pairs = candidate_name_price_pairs_from_page(soup)
        if pairs:
            for name, price in pairs:
                if is_bad_name(name):
                    continue
                key = name.lower()
                row = {
                    "lab": LAB_NAME,
                    "city": CITY_NAME,
                    "category": category,
                    "analysis_name": name,
                    "price": price,
                    "url": url,
                }
                if key not in dataset or row["price"] < dataset[key]["price"]:
                    dataset[key] = row

        for next_url in extract_links(soup, url):
            if next_url not in visited:
                queue.append(next_url)

    rows = list(dataset.values())
    rows.sort(key=lambda x: (x["category"], x["analysis_name"]))
    return rows


def save_csv(rows):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["lab", "city", "category", "analysis_name", "price", "url"],
        )
        writer.writeheader()
        writer.writerows(rows)


def save_xlsx(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "kislorod"
    headers = ["lab", "city", "category", "analysis_name", "price", "url"]
    ws.append(headers)
    for row in rows:
        ws.append([row[h] for h in headers])
    wb.save(OUTPUT_XLSX)


def main():
    ensure_output_dir()
    rows = crawl()
    save_csv(rows)
    save_xlsx(rows)
    print(f"done: {len(rows)} rows")
    print(OUTPUT_CSV)
    print(OUTPUT_XLSX)


if __name__ == "__main__":
    main()
