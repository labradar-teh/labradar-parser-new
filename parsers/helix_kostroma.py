# parser_helix.py  — СТАБИЛЬНЫЙ, ВСЕГДА СОЗДАЁТ CSV/XLSX

import csv
import os
import re
import time
from collections import OrderedDict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://helix.ru"
START_URL = "https://helix.ru/kostroma/catalog/190-vse-analizy"

LAB_NAME = "Хеликс"
CITY_NAME = "Кострома"

OUTPUT_DIR = "output"
CSV_PATH = os.path.join(OUTPUT_DIR, "helix_kostroma.csv")
XLSX_PATH = os.path.join(OUTPUT_DIR, "helix_kostroma.xlsx")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)


def ensure_output():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch(url):
    try:
        r = session.get(url, timeout=25)
        r.raise_for_status()
        time.sleep(0.05)
        return r.text
    except:
        return None


def clean_price(text):
    if not text:
        return None
    nums = re.findall(r"\d+", text.replace(" ", ""))
    if not nums:
        return None
    val = int(nums[0])
    if val <= 1:
        return None
    return val


def is_valid(name):
    if not name:
        return False
    name = name.lower()
    bad = [
        "акц", "скид", "прием", "приём", "услуг", "забор",
        "биоматериал", "узи", "экг", "врач", "консультац",
        "чекап", "комплекс", "взятие"
    ]
    return not any(x in name for x in bad)


def extract_title_from_item_page(soup):
    h1 = soup.find("h1")
    if not h1:
        return None
    title = " ".join(h1.get_text(" ", strip=True).split())
    title = re.sub(r"\s+в\s+Костроме\s*$", "", title, flags=re.I)
    return title.strip()


def extract_price_from_item_page(soup):
    text = soup.get_text(" ", strip=True)

    patterns = [
        r"Стоимость[:\s]+([\d\s]+)\s*₽",
        r"Стоимость[:\s]+([\d\s]+)\s*руб",
        r"Цена[:\s]+([\d\s]+)\s*₽",
        r"Цена[:\s]+([\d\s]+)\s*руб",
        r"([\d\s]+)\s*₽"
    ]

    for p in patterns:
        m = re.search(p, text, flags=re.I)
        if m:
            price = clean_price(m.group(1))
            if price:
                return price
    return None


def extract_category(soup):
    crumbs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = " ".join(a.get_text(" ", strip=True).split())
        if "/catalog/" in href and "/catalog/item/" not in href and txt:
            crumbs.append(txt)

    for txt in reversed(crumbs):
        if txt.lower() not in ["главная", "сдать анализы", "анализы", "все анализы"]:
            return txt

    return "Анализы"


def parse_item_page(url):
    html = fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    title = extract_title_from_item_page(soup)
    if not is_valid(title):
        return None

    price = extract_price_from_item_page(soup)
    if not price:
        return None

    code_match = re.search(r"/catalog/item/(\d{2}-\d{3})", url)
    code = code_match.group(1) if code_match else None
    if not code:
        return None

    if code.startswith("40-"):
        return None

    category = extract_category(soup)

    return {
        "_code": code,
        "lab": LAB_NAME,
        "city": CITY_NAME,
        "category": category,
        "analysis_name": title,
        "price": price,
        "url": url
    }


def parse_listing_page(url):
    html = fetch(url)
    if not html:
        return [], []

    soup = BeautifulSoup(html, "html.parser")

    item_links = OrderedDict()
    next_links = OrderedDict()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = urljoin(BASE_URL, href)

        if "/catalog/item/" in full:
            item_links[full] = True

        if "/ivanovo/catalog/" in full and "/catalog/item/" not in full:
            next_links[full] = True

        if "?page=" in full:
            next_links[full] = True

    return list(item_links.keys()), list(next_links.keys())


def crawl():
    visited_pages = set()
    queue = [START_URL]
    data = OrderedDict()

    while queue:
        page_url = queue.pop(0)

        if page_url in visited_pages:
            continue

        visited_pages.add(page_url)

        item_links, next_links = parse_listing_page(page_url)

        for item_url in item_links:
            row = parse_item_page(item_url)
            if not row:
                continue
            key = row["_code"]
            if key not in data:
                data[key] = row

        for nxt in next_links:
            if nxt not in visited_pages:
                queue.append(nxt)

        if len(visited_pages) > 3000:
            break

    rows = []
    for row in data.values():
        rows.append({
            "lab": row["lab"],
            "city": row["city"],
            "category": row["category"],
            "analysis_name": row["analysis_name"],
            "price": row["price"],
            "url": row["url"]
        })

    rows.sort(key=lambda x: (x["category"], x["analysis_name"]))
    return rows


def save_csv(rows):
    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["lab", "city", "category", "analysis_name", "price", "url"]
        )
        writer.writeheader()
        writer.writerows(rows)


def save_xlsx(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "helix"
    ws.append(["lab", "city", "category", "analysis_name", "price", "url"])

    for r in rows:
        ws.append([r["lab"], r["city"], r["category"], r["analysis_name"], r["price"], r["url"]])

    wb.save(XLSX_PATH)


def main():
    ensure_output()

    rows = []
    try:
        rows = crawl()
    except Exception as e:
        print("ERROR:", e)

    save_csv(rows)
    save_xlsx(rows)

    print("DONE:", len(rows))


if __name__ == "__main__":
    main()
