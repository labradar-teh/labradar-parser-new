# parser_kislorod.py

import csv
import os
import re
import time
from collections import OrderedDict, deque
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://kislorod-doctor.ru"
START_URL = f"{BASE_URL}/analyzes/"

LAB_NAME = "Кислород"
CITY_NAME = "Иваново"

OUTPUT_DIR = "output"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "kislorod_ivanovo.csv")
OUTPUT_XLSX = os.path.join(OUTPUT_DIR, "kislorod_ivanovo.xlsx")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": BASE_URL,
}

TIMEOUT = 30
MAX_RETRIES = 4
SLEEP = 0.08
MAX_PAGES = 10000

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
    r"\bврач",
    r"\bмассаж\b",
    r"\bманипуляц",
    r"\bпроцедур",
    r"\bкапельниц",
    r"\bчекап\b",
    r"\bcheck[- ]?up\b",
    r"\bкомплекс\b",
    r"\bпрофиль\b",
    r"\bпанел",
]

BAD_URL_PARTS = [
    "/services",
    "/service",
    "/doctors",
    "/doctor",
    "/news",
    "/contacts",
    "/about",
    "/vacancy",
    "/articles",
    "/blog",
    "/reviews",
    "/akts",
    "/sale",
    "/discount",
    "/uzi",
    "/ekg",
]

session = requests.Session()
session.headers.update(HEADERS)


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_space(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def clean_url(url):
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not path:
        path = "/"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def same_domain(url):
    return urlparse(url).netloc == urlparse(BASE_URL).netloc


def fetch(url):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            time.sleep(SLEEP)
            return resp.text
        except Exception as e:
            last_error = e
            time.sleep(min(attempt * 2, 6))
    return None


def soupify(url):
    html = fetch(url)
    if not html:
        return None
    return BeautifulSoup(html, "html.parser")


def clean_price(text):
    if not text:
        return None
    digits = re.findall(r"\d[\d\s]*", text.replace("\xa0", " "))
    if not digits:
        return None
    value = int(re.sub(r"[^\d]", "", max(digits, key=len)))
    if value <= 1:
        return None
    return value


def is_bad_name(name):
    low = (name or "").lower()
    return any(re.search(pattern, low) for pattern in BAD_NAME_PATTERNS)


def is_valid_analysis_name(name):
    if not name:
        return False
    name = normalize_space(name)
    if len(name) < 3:
        return False
    if is_bad_name(name):
        return False
    return True


def is_category_or_detail_url(url):
    if not url or not same_domain(url):
        return False
    low = url.lower()
    if any(x in low for x in BAD_URL_PARTS):
        return False
    return "/analyzes" in low


def extract_links(soup, current_url):
    links = OrderedDict()
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        abs_url = clean_url(urljoin(current_url, href))
        if is_category_or_detail_url(abs_url):
            links[abs_url] = True
    return list(links.keys())


def extract_h1(soup):
    h1 = soup.find("h1")
    if not h1:
        return None
    return normalize_space(h1.get_text(" ", strip=True))


def extract_breadcrumbs(soup):
    crumbs = []
    selectors = [
        ".breadcrumb a",
        ".breadcrumbs a",
        "nav.breadcrumbs a",
        "nav[aria-label='breadcrumb'] a",
        "[typeof='BreadcrumbList'] a",
        "a.breadcrumbs__link",
    ]
    for selector in selectors:
        for a in soup.select(selector):
            txt = normalize_space(a.get_text(" ", strip=True))
            if txt:
                crumbs.append(txt)
    cleaned = []
    for c in crumbs:
        if c not in cleaned:
            cleaned.append(c)
    return cleaned


def extract_category(soup):
    crumbs = extract_breadcrumbs(soup)
    filtered = [x for x in crumbs if x.lower() not in {"главная", "анализы"}]
    if filtered:
        return filtered[-1]
    title = extract_h1(soup)
    return title or "Анализы"


def extract_article_code(text):
    if not text:
        return None
    m = re.search(r"\b(\d{2}-\d{3})\b", text)
    return m.group(1) if m else None


def is_complex_code(code):
    return bool(code and code.startswith("40-"))


def extract_detail_price(soup):
    text = soup.get_text("\n", strip=True)

    patterns = [
        r"Cтоимость анализа\s*([\d\s]+)\s*руб",
        r"Стоимость анализа\s*([\d\s]+)\s*руб",
        r"Cтоимость анализа\s*([\d\s]+)\s*₽",
        r"Стоимость анализа\s*([\d\s]+)\s*₽",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            return clean_price(m.group(1))

    prices = re.findall(r"([\d\s]{2,})\s*(?:руб|₽)", text, flags=re.I)
    prices = [clean_price(x) for x in prices]
    prices = [x for x in prices if x]
    return prices[0] if prices else None


def is_detail_page(url, soup):
    if "/analyzes/" not in url:
        return False
    crumbs = extract_breadcrumbs(soup)
    title = extract_h1(soup)
    page_text = soup.get_text("\n", strip=True)
    code = extract_article_code(page_text)
    price = extract_detail_price(soup)
    return bool(title and len(crumbs) >= 2 and code and price)


def parse_detail_page(url, soup):
    title = extract_h1(soup)
    if not is_valid_analysis_name(title):
        return None

    page_text = soup.get_text("\n", strip=True)
    code = extract_article_code(page_text)
    if not code or is_complex_code(code):
        return None

    price = extract_detail_price(soup)
    if not price:
        return None

    crumbs = extract_breadcrumbs(soup)
    category = "Анализы"
    if len(crumbs) >= 2:
        category = crumbs[-2]
    elif crumbs:
        category = crumbs[-1]

    if not category or category.lower() == title.lower():
        category = "Анализы"

    if is_bad_name(category):
        category = "Анализы"

    return {
        "lab": LAB_NAME,
        "city": CITY_NAME,
        "category": category,
        "analysis_name": title,
        "price": price,
        "url": url,
        "_code": code,
    }


def parse_category_listing_page(url, soup):
    category = extract_category(soup)
    rows = []

    text_nodes = soup.select("li, .elementor-widget-container li, .analysis-item, .catalog-item, .price-item, .product-card, tr")
    for node in text_nodes:
        text = normalize_space(node.get_text(" ", strip=True))
        if len(text) < 5:
            continue

        price = clean_price(text)
        if not price:
            continue

        name = None
        for sel in ["a", "strong", "b", "h2", "h3", "h4", "td:first-child", ".title", ".name"]:
            el = node.select_one(sel)
            if el:
                candidate = normalize_space(el.get_text(" ", strip=True))
                if candidate:
                    name = candidate
                    break

        if not name:
            parts = re.split(r"\s+\d[\d\s]*(?:руб|₽)", text, maxsplit=1, flags=re.I)
            if parts:
                name = normalize_space(parts[0])

        if not is_valid_analysis_name(name):
            continue

        rows.append({
            "lab": LAB_NAME,
            "city": CITY_NAME,
            "category": category,
            "analysis_name": name,
            "price": price,
            "url": url,
            "_code": None,
        })

    dedup = OrderedDict()
    for row in rows:
        key = row["analysis_name"].lower()
        if key not in dedup or row["price"] < dedup[key]["price"]:
            dedup[key] = row
    return list(dedup.values())


def crawl():
    queue = deque([START_URL])
    visited = set()
    dataset_by_code = OrderedDict()
    dataset_by_name = OrderedDict()

    pages = 0

    while queue and pages < MAX_PAGES:
        current = clean_url(queue.popleft())
        if current in visited:
            continue
        visited.add(current)
        pages += 1

        soup = soupify(current)
        if not soup:
            continue

        if is_detail_page(current, soup):
            row = parse_detail_page(current, soup)
            if row:
                code = row["_code"]
                if code not in dataset_by_code or row["price"] < dataset_by_code[code]["price"]:
                    dataset_by_code[code] = row
        else:
            for row in parse_category_listing_page(current, soup):
                key = row["analysis_name"].lower()
                if key not in dataset_by_name or row["price"] < dataset_by_name[key]["price"]:
                    dataset_by_name[key] = row

        for nxt in extract_links(soup, current):
            if nxt not in visited:
                queue.append(nxt)

    final_rows = OrderedDict()

    for row in dataset_by_name.values():
        key = row["analysis_name"].lower()
        final_rows[key] = {
            "lab": row["lab"],
            "city": row["city"],
            "category": row["category"],
            "analysis_name": row["analysis_name"],
            "price": row["price"],
            "url": row["url"],
        }

    for row in dataset_by_code.values():
        key = row["analysis_name"].lower()
        final_rows[key] = {
            "lab": row["lab"],
            "city": row["city"],
            "category": row["category"],
            "analysis_name": row["analysis_name"],
            "price": row["price"],
            "url": row["url"],
        }

    rows = list(final_rows.values())
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
    rows = []
    try:
        rows = crawl()
    except Exception as e:
        print(f"crawl_error: {e}")

    save_csv(rows)
    save_xlsx(rows)

    print(f"done: {len(rows)} rows")
    print(OUTPUT_CSV)
    print(OUTPUT_XLSX)


if __name__ == "__main__":
    main()
