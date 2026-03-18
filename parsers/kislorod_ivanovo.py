# parser_kislorod.py  (СТАБИЛЬНЫЙ — НЕ ПАДАЕТ, ВСЕГДА СОЗДАЁТ ФАЙЛЫ)

import csv
import os
import re
import time
from collections import OrderedDict
from urllib.parse import urljoin

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
    "Accept-Language": "ru-RU,ru;q=0.9"
}

session = requests.Session()
session.headers.update(HEADERS)


def ensure_output():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch(url):
    try:
        r = session.get(url, timeout=20)
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
        "чекап", "комплекс"
    ]
    return not any(x in name for x in bad)


def parse_page(url):
    html = fetch(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for el in soup.find_all(["li", "tr", "div"]):
        text = el.get_text(" ", strip=True)

        if len(text) < 5:
            continue

        price = clean_price(text)
        if not price:
            continue

        name = text.split(str(price))[0].strip()

        if not is_valid(name):
            continue

        rows.append({
            "lab": LAB_NAME,
            "city": CITY_NAME,
            "category": "Анализы",
            "analysis_name": name,
            "price": price,
            "url": url
        })

    return rows


def crawl():
    visited = set()
    queue = [START_URL]
    data = OrderedDict()

    while queue:
        url = queue.pop(0)

        if url in visited:
            continue

        visited.add(url)

        html = fetch(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # парсим данные
        for row in parse_page(url):
            key = row["analysis_name"].lower()
            if key not in data or row["price"] < data[key]["price"]:
                data[key] = row

        # собираем ссылки
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(BASE_URL, href)

            if "/analyzes" in full and full not in visited:
                queue.append(full)

        if len(visited) > 2000:
            break

    return list(data.values())


def save_csv(rows):
    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["lab","city","category","analysis_name","price","url"])
        writer.writeheader()
        writer.writerows(rows)


def save_xlsx(rows):
    wb = Workbook()
    ws = wb.active
    ws.append(["lab","city","category","analysis_name","price","url"])

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

    # КРИТИЧНО — файлы создаются ВСЕГДА
    save_csv(rows)
    save_xlsx(rows)

    print("DONE:", len(rows))


if __name__ == "__main__":
    main()
