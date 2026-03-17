import re
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


CITY = "Иваново"
LAB = "INVITRO"
START_URL = "https://www.invitro.ru/analizes/for-doctors/ivanovo/"
OUTPUT_FILE = "out/invitro_ivanovo.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


def fetch(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None
    return None


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def extract_price(text: str):
    matches = re.findall(r"(\d[\d ]{0,12})\s*(?:₽|руб)", text, flags=re.I)
    nums = []
    for m in matches:
        v = re.sub(r"[^\d]", "", m)
        if v:
            nums.append(int(v))
    return nums[-1] if nums else None


def collect_category_links():
    soup = fetch(START_URL)
    if not soup:
        return []

    links = []
    seen = set()

    for a in soup.select('a[href*="/analizes/for-doctors/ivanovo/"]'):
        href = a.get("href")
        name = clean_text(a.get_text(" ", strip=True))
        if not href or not name:
            continue

        full = urljoin(START_URL, href)
        path = urlparse(full).path.rstrip("/")
        parts = [p for p in path.split("/") if p]

        if len(parts) < 4:
            continue

        key = (name.lower(), full)
        if key in seen:
            continue
        seen.add(key)
        links.append((name, full))

    return links


def parse_category(category_name: str, url: str):
    soup = fetch(url)
    if not soup:
        return []

    rows = []
    cards = soup.select(".catalog-item, .analyzes-item, .search-item, .price-item, .test-item, article, li")

    for card in cards:
        name = ""
        for sel in [".catalog-item__title", ".analyzes-item__title", ".title", "h3", "h2", "a[href]"]:
            node = card.select_one(sel)
            if node:
                name = clean_text(node.get_text(" ", strip=True))
                if name:
                    break

        if not name:
            continue

        price = None
        for sel in [".price", ".catalog-item__price", ".price-item__value", ".cost", "[class*='price']"]:
            node = card.select_one(sel)
            if node:
                price = extract_price(clean_text(node.get_text(" ", strip=True)))
                if price is not None:
                    break

        if price is None:
            price = extract_price(clean_text(card.get_text(" ", strip=True)))

        if price is None:
            continue

        link_node = card.select_one("a[href]")
        link = urljoin(url, link_node.get("href")) if link_node and link_node.get("href") else url

        rows.append({
            "lab": LAB,
            "city": CITY,
            "category": category_name,
            "analysis_name": name,
            "price": price,
            "url": link,
        })

    return rows


def main():
    categories = collect_category_links()
    if not categories:
        categories = [("Все анализы", START_URL)]

    all_rows = []
    for category_name, url in categories:
        rows = parse_category(category_name, url)
        print(f"{category_name}: {len(rows)}")
        all_rows.extend(rows)
        time.sleep(0.5)

    df = pd.DataFrame(all_rows)
    if df.empty:
        raise RuntimeError("Нет данных для сохранения")

    df["analysis_name"] = df["analysis_name"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.strip()
    df = df[df["analysis_name"].str.len() > 2].copy()
    df = df.drop_duplicates(subset=["lab", "city", "category", "analysis_name", "price"]).copy()
    df = df.sort_values(["category", "analysis_name"]).reset_index(drop=True)

    import os
    os.makedirs("out", exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
