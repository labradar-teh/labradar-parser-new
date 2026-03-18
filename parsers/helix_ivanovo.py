# parser_helix.py

import csv
import os
import re
import sys
import time
from collections import OrderedDict
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://helix.ru"
CITY_SLUG = "ivanovo"
CITY_NAME = "Иваново"
LAB_NAME = "Хеликс"

START_URL = BASE_URL + "/" + CITY_SLUG + "/catalog/190-vse-analizy"

OUTPUT_DIR = "output"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "helix_ivanovo.csv")
OUTPUT_XLSX = os.path.join(OUTPUT_DIR, "helix_ivanovo.xlsx")

TIMEOUT = 30
RETRIES = 5
SLEEP = 0.10

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": BASE_URL + "/" + CITY_SLUG + "/",
}

SKIP_SECTION_TITLES = {
    "Врачебные услуги",
    "УЗИ (ультразвуковые исследования)",
    "ЭКГ (электрокардиограмма)",
    "Комплексы анализов",
    "Популярные анализы",
}

BAD_NAME_PATTERNS = [
    r"\bакц",
    r"\bскид",
    r"\bвзятие\b",
    r"\bбиоматериал",
    r"\bзабор\b",
    r"\bуслуг",
    r"\bуслуга\b",
    r"\bприем\b",
    r"\bприём\b",
    r"\bконсультац",
    r"\bузи\b",
    r"\bэкг\b",
    r"\bcheck[- ]?up\b",
    r"\bчекап\b",
    r"\bкомплекс\b",
]

BAD_SECTION_URL_PARTS = [
    "vrach",
    "uzi",
    "ekg",
    "kompleks",
    "akts",
    "sale",
    "promo",
]

session = requests.Session()
session.headers.update(HEADERS)


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_space(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def fetch(url):
    last_error = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            time.sleep(SLEEP)
            return resp.text
        except Exception as e:
            last_error = e
            time.sleep(min(attempt * 2, 6))
    raise RuntimeError("GET failed: " + str(url) + " :: " + str(last_error))


def soupify(url):
    html = fetch(url)
    return BeautifulSoup(html, "html.parser")


def absolute_url(href):
    if not href:
        return None
    return urljoin(BASE_URL, href)


def clean_price(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    value = int(digits)
    if value <= 1:
        return None
    return value


def extract_code_from_url(url):
    m = re.search(r"/catalog/item/(\d{2}-\d{3})", url or "")
    return m.group(1) if m else None


def is_analysis_item_url(url):
    return bool(re.search(r"/catalog/item/\d{2}-\d{3}", url or ""))


def is_complex_code(code):
    return bool(code and code.startswith("40-"))


def is_bad_name(name):
    low = (name or "").lower()
    for pattern in BAD_NAME_PATTERNS:
        if re.search(pattern, low):
            return True
    return False


def is_bad_section_url(url):
    low = (url or "").lower()
    for part in BAD_SECTION_URL_PARTS:
        if part in low:
            return True
    return False


def canonical_page_url(url, page_num):
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["page"] = [str(page_num)]
    new_query = urlencode(query, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def extract_last_page(soup):
    max_page = 1
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
        txt = normalize_space(a.get_text(" ", strip=True))
        if txt.isdigit():
            max_page = max(max_page, int(txt))
    return max_page


def extract_category_links():
    soup = soupify(START_URL)
    links = OrderedDict()

    for a in soup.select("a[href]"):
        href = absolute_url(a.get("href"))
        if not href:
            continue
        if "/" + CITY_SLUG + "/catalog/" not in href:
            continue
        if "/catalog/item/" in href:
            continue
        if is_bad_section_url(href):
            continue

        m = re.search(r"/" + CITY_SLUG + r"/catalog/(\d+)-", href)
        if not m:
            continue

        cat_id = m.group(1)
        if cat_id == "190":
            continue

        title = normalize_space(a.get_text(" ", strip=True))
        if not title:
            title = href.rsplit("/", 1)[-1]
        if title in SKIP_SECTION_TITLES:
            continue

        links[href] = title

    links[START_URL] = "Все анализы"
    return links


def extract_title(soup):
    h1 = soup.find("h1")
    if not h1:
        return None
    title = normalize_space(h1.get_text(" ", strip=True))
    title = re.sub(r"\s+в\s+" + re.escape(CITY_NAME) + r"\s*$", "", title, flags=re.I)
    return normalize_space(title)


def extract_category_from_breadcrumbs(soup):
    crumbs = []
    selectors = [
        "nav a[href]",
        ".breadcrumb a[href]",
        ".breadcrumbs a[href]",
        "[itemprop='itemListElement'] a[href]",
    ]

    for selector in selectors:
        for a in soup.select(selector):
            href = a.get("href", "")
            text = normalize_space(a.get_text(" ", strip=True))
            if not text:
                continue
            if "/catalog/" in href and "/catalog/item/" not in href:
                crumbs.append(text)

    for text in reversed(crumbs):
        if text not in ("Главная", "Сдать анализы", "Анализы"):
            return text

    return None


def extract_price(soup):
    text = soup.get_text("\n", strip=True)

    patterns = [
        r"Стоимость:\s*([\d\s]+)\s*₽",
        r"Стоимость:\s*([\d\s]+)\s*руб",
        r"Цена:\s*([\d\s]+)\s*₽",
        r"Цена:\s*([\d\s]+)\s*руб",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I | re.S)
        if m:
            price = clean_price(m.group(1))
            if price:
                return price

    prices = re.findall(r"([\d\s]{2,})\s*(?:₽|руб)", text, flags=re.I)
    cleaned = []
    for p in prices:
        val = clean_price(p)
        if val:
            cleaned.append(val)
    if cleaned:
        return cleaned[0]

    return None


def parse_item_page(item_url, fallback_category):
    soup = soupify(item_url)

    code = extract_code_from_url(item_url)
    if not code:
        return None
    if is_complex_code(code):
        return None

    title = extract_title(soup)
    if not title:
        return None
    if is_bad_name(title):
        return None

    category = extract_category_from_breadcrumbs(soup)
    if not category:
        category = fallback_category or LAB_NAME
    if category in SKIP_SECTION_TITLES:
        return None

    price = extract_price(soup)
    if not price:
        return None

    return {
        "lab": LAB_NAME,
        "city": CITY_NAME,
        "category": category,
        "analysis_name": title,
        "price": price,
        "url": item_url,
        "_code": code,
    }


def parse_listing_page(listing_url, fallback_category):
    soup = soupify(listing_url)
    item_urls = OrderedDict()

    for a in soup.select("a[href]"):
        href = absolute_url(a.get("href"))
        if not href:
            continue
        if not is_analysis_item_url(href):
            continue

        code = extract_code_from_url(href)
        if not code:
            continue
        if is_complex_code(code):
            continue

        item_urls[href] = True

    rows = []
    for item_url in item_urls.keys():
        try:
            row = parse_item_page(item_url, fallback_category)
            if row:
                rows.append(row)
        except Exception:
            continue

    return rows, extract_last_page(soup)


def collect_all():
    rows_by_code = OrderedDict()
    category_links = extract_category_links()

    for category_url, category_name in category_links.items():
        try:
            first_page_soup = soupify(category_url)
            last_page = extract_last_page(first_page_soup)
        except Exception:
            continue

        if last_page < 1:
            last_page = 1

        for page_num in range(1, last_page + 1):
            if page_num == 1:
                page_url = category_url
            else:
                page_url = canonical_page_url(category_url, page_num)

            try:
                rows, _ = parse_listing_page(page_url, category_name)
            except Exception:
                continue

            for row in rows:
                code = row["_code"]
                if code not in rows_by_code:
                    rows_by_code[code] = row

    final_rows = []
    for row in rows_by_code.values():
        if is_bad_name(row["analysis_name"]):
            continue
        if row["category"] in SKIP_SECTION_TITLES:
            continue
        final_rows.append({
            "lab": row["lab"],
            "city": row["city"],
            "category": row["category"],
            "analysis_name": row["analysis_name"],
            "price": row["price"],
            "url": row["url"],
        })

    final_rows.sort(key=lambda x: (x["category"], x["analysis_name"]))
    return final_rows


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
    ws.title = "helix"
    headers = ["lab", "city", "category", "analysis_name", "price", "url"]
    ws.append(headers)
    for row in rows:
        ws.append([row[h] for h in headers])
    wb.save(OUTPUT_XLSX)


def main():
    ensure_output_dir()
    rows = []
    try:
        rows = collect_all()
    except Exception as e:
        print("crawl_error:", str(e))

    save_csv(rows)
    save_xlsx(rows)

    print("done:", len(rows), "rows")
    print(OUTPUT_CSV)
    print(OUTPUT_XLSX)


if __name__ == "__main__":
    main()
