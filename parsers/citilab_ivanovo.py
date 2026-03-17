import re
import time
import os
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


CITY = "Иваново"
LAB = "citilab"
START_URL = "https://citilab.ru/ivanovo/catalog/"
OUTPUT_FILE = "out/citilab_ivanovo.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
    except:
        return None
    return None


def clean(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def extract_price(text):
    m = re.search(r"(\d[\d\s]+)\s*(?:₽|руб)", text)
    if m:
        return int(re.sub(r"\D", "", m.group(1)))
    return None


def is_valid_catalog(url):
    path = urlparse(url).path
    return path.startswith("/ivanovo/catalog/") and path.count("/") < 6
    # 🔥 ограничение глубины — убирает цикл


def parse_page(url, category):
    soup = fetch(url)
    if not soup:
        return [], []

    rows = []
    next_links = []

    cards = soup.select("a[href*='/catalog/']")

    for a in cards:
        href = urljoin(url, a.get("href"))

        if not is_valid_catalog(href):
            continue

        text = clean(a.get_text())

        # если это анализ (есть цена)
        parent = a.parent
        full_text = clean(parent.get_text())

        price = extract_price(full_text)

        if price:
            rows.append({
                "lab": LAB,
                "city": CITY,
                "category": category,
                "analysis_name": text,
                "price": price,
                "url": href
            })
        else:
            next_links.append((href, text or category))

    return rows, next_links


def main():
    visited = set()
    queue = [(START_URL, "Каталог")]
    all_rows = []

    while queue:
        url, category = queue.pop(0)

        if url in visited:
            continue
        visited.add(url)

        rows, links = parse_page(url, category)
        print(f"{url}: rows={len(rows)} queue={len(queue)}")

        all_rows.extend(rows)

        for link_url, link_category in links:
            if link_url not in visited:
                queue.append((link_url, link_category))

        # 🔥 ускорение
        time.sleep(0.1)

        # 🔥 защита от бесконечного цикла
        if len(visited) > 1500:
            print("STOP: too many pages")
            break

    os.makedirs("out", exist_ok=True)

    df = pd.DataFrame(all_rows)

    df = df.drop_duplicates(subset=["analysis_name", "price"])
    df = df.sort_values(["category", "analysis_name"])

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"Rows: {len(df)}")
    print(f"Saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
