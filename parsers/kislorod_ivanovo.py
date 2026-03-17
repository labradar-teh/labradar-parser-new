import re
import time
import os
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


CITY = "Иваново"
LAB = "Kislorod"
START_URL = "https://kislorod-doctor.ru/analyzes/"
OUTPUT_FILE = "out/kislorod_ivanovo.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}


def fetch(url: str):
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


def parse_page(url: str):
    soup = fetch(url)
    if not soup:
        return []

    rows = []
    cards = soup.select("article, li, .products li, .product, .post, .item")

    for card in cards:
        name = ""
        for sel in [
            "h1",
            "h2",
            "h3",
            ".woocommerce-loop-product__title",
            "[class*='title']",
            "[class*='name']",
            "a[href]",
        ]:
            node = card.select_one(sel)
            if node:
                name = clean_text(node.get_text(" ", strip=True))
                if name and len(name) > 2:
                    break

        if not name or len(name) < 3:
            continue

        price = None
        for sel in [
            ".price",
            ".woocommerce-Price-amount",
            "[class*='price']",
            "[class*='cost']",
        ]:
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
            "category": "Все анализы",
            "analysis_name": name,
            "price": price,
            "url": link,
        })

    return rows


def main():
    all_rows = []

    for page in range(1, 224):
        url = START_URL if page == 1 else f"{START_URL}page/{page}/"
        rows = parse_page(url)
        print(f"page {page}: {len(rows)}")

        if not rows and page > 10:
            break

        all_rows.extend(rows)
        time.sleep(0.5)

    os.makedirs("out", exist_ok=True)

    df = pd.DataFrame(all_rows)

    if df.empty:
        df = pd.DataFrame(columns=["lab", "city", "category", "analysis_name", "price", "url"])
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"Saved empty file: {OUTPUT_FILE}")
        return

    df["analysis_name"] = df["analysis_name"].astype(str).str.strip()
    df = df[df["analysis_name"].str.len() > 2].copy()
    df = df.drop_duplicates(subset=["lab", "city", "category", "analysis_name", "price"]).copy()
    df = df.sort_values(["analysis_name"]).reset_index(drop=True)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
