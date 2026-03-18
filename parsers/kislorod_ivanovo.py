# parser_kislorod.py

import csv
import os
import re
import time

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://kislorod-doctor.ru"
START_URL = "https://kislorod-doctor.ru/analyzes/"

LAB_NAME = "Кислород"
CITY_NAME = "Иваново"

OUTPUT_DIR = "output"
CSV_PATH = os.path.join(OUTPUT_DIR, "kislorod_ivanovo.csv")
XLSX_PATH = os.path.join(OUTPUT_DIR, "kislorod_ivanovo.xlsx")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": BASE_URL,
}

BAD_NAME_PATTERNS = [
    r"\bвзятие\b",
    r"\bбиоматериал",
    r"\bзабор\b",
    r"\bуслуг",
    r"\bуслуга\b",
    r"\bприем\b",
    r"\bприём\b",
    r"\bконсультац",
    r"\bакц",
    r"\bскид",
    r"\bузи\b",
    r"\bэкг\b",
    r"\bээг\b",
    r"\bврач",
    r"\bчек[\-\s]?ап\b",
    r"\bcheck[\-\s]?up\b",
    r"\bкомплекс\b",
    r"\bпанел",
    r"\bалгоритм",
    r"\bродства\b",
    r"\bкорпоративн",
    r"\bнаркотическ",
]

SKIP_CATEGORY_PATTERNS = [
    r"взятие биоматериала",
    r"комплексы анализов",
    r"чек-ап",
    r"чекап",
    r"панели тестов",
    r"алгоритмы исследований",
    r"генетическое установление родства",
    r"индивидуальные исследования корпоративных клиентов",
    r"наркотические вещества",
]

MAX_PAGES = 223
SLEEP = 0.05
TIMEOUT = 25

session = requests.Session()
session.headers.update(HEADERS)


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch(url):
    try:
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        time.sleep(SLEEP)
        return r.text
    except Exception:
        return None


def normalize_space(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def clean_price(text):
    if not text:
        return None
    digits = re.findall(r"\d[\d\s]*", text.replace("\xa0", " "))
    if not digits:
        return None
    value = int(re.sub(r"[^\d]", "", digits[0]))
    if value <= 1:
        return None
    return value


def is_bad_name(name):
    low = (name or "").lower()
    return any(re.search(p, low) for p in BAD_NAME_PATTERNS)


def is_skip_category(name):
    low = (name or "").lower()
    return any(re.search(p, low) for p in SKIP_CATEGORY_PATTERNS)


def parse_categories(soup):
    category_map = {}
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = normalize_space(a.get_text(" ", strip=True))
        if not text:
            continue
        if "/analyzes/" not in href and href != START_URL and not href.endswith("/analyzes"):
            continue
        if is_skip_category(text):
            continue
        category_map[href] = text
    return category_map


def parse_page(page_num):
    url = START_URL if page_num == 1 else f"{START_URL}?PAGEN_1={page_num}"
    html = fetch(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = normalize_space(a.get_text(" ", strip=True))

        if not text:
            continue
        if "руб." not in text and "руб" not in text:
            continue

        full_url = requests.compat.urljoin(BASE_URL, href)

        m = re.match(r"^(.*?)\s+(\d[\d\s]*)\s*руб\.?$", text, flags=re.I)
        if not m:
            m = re.match(r"^(.*?)\s+(\d[\d\s]*)\s*₽$", text, flags=re.I)
        if not m:
            continue

        name = normalize_space(m.group(1))
        price = clean_price(m.group(2))

        if not name or not price:
            continue
        if is_bad_name(name):
            continue

        category = "Анализы"
        rows.append({
            "lab": LAB_NAME,
            "city": CITY_NAME,
            "category": category,
            "analysis_name": name,
            "price": price,
            "url": full_url if full_url.startswith(BASE_URL) else url,
        })

    return rows


def collect_all():
    data = {}

    for page_num in range(1, MAX_PAGES + 1):
        rows = parse_page(page_num)
        if not rows and page_num > 3:
            continue

        for row in rows:
            key = row["analysis_name"].lower()
            if key not in data or row["price"] < data[key]["price"]:
                data[key] = row

    final_rows = list(data.values())
    final_rows.sort(key=lambda x: x["analysis_name"].lower())
    return final_rows


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
    ws.title = "kislorod"
    ws.append(["lab", "city", "category", "analysis_name", "price", "url"])

    for row in rows:
        ws.append([
            row["lab"],
            row["city"],
            row["category"],
            row["analysis_name"],
            row["price"],
            row["url"],
        ])

    wb.save(XLSX_PATH)


def main():
    ensure_output_dir()

    rows = []
    try:
        rows = collect_all()
    except Exception as e:
        print("ERROR:", str(e))

    save_csv(rows)
    save_xlsx(rows)

    print("DONE:", len(rows))
    print(CSV_PATH)
    print(XLSX_PATH)


if __name__ == "__main__":
    main()
