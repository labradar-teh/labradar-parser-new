import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


CITY = "Иваново"
LAB = "Kislorod"
START_URL = "https://kislorod-doctor.ru/analyzes/"
OUTPUT_FILE = "out/kislorod_ivanovo.csv"
DEBUG_HTML = "out/kislorod_debug.html"


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


def parse_html(html: str, page_url: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    cards = soup.select(
        "article, li, .products li, .product, .post, .item, "
        "[class*='product'], [class*='catalog']"
    )

    for card in cards:
        name = ""
        for sel in [
            "h1", "h2", "h3",
            ".woocommerce-loop-product__title",
            "[class*='title']",
            "[class*='name']",
            "a[href]",
        ]:
            node = card.select_one(sel)
            if node:
                name = clean_text(node.get_text(" ", strip=True))
                if len(name) > 2:
                    break

        if len(name) < 3:
            continue

        price = None
        for sel in [
            ".price",
            ".woocommerce-Price-amount",
            "[class*='price']",
            "[class*='cost']",
            "[data-price]",
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
        link = urljoin(page_url, link_node.get("href")) if link_node and link_node.get("href") else page_url

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
    os.makedirs("out", exist_ok=True)
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for page_num in range(1, 224):
            url = START_URL if page_num == 1 else f"{START_URL}page/{page_num}/"
            print(f"Open: {url}")

            try:
                page.goto(url, wait_until="networkidle", timeout=90000)
                page.wait_for_timeout(3500)
            except Exception as e:
                print(f"Open failed: {e}")
                if page_num > 10:
                    break
                continue

            html = page.content()

            if page_num == 1:
                with open(DEBUG_HTML, "w", encoding="utf-8") as f:
                    f.write(html)

            rows = parse_html(html, url)
            print(f"page {page_num}: {len(rows)}")

            if not rows and page_num > 10:
                break

            all_rows.extend(rows)
            time.sleep(1)

        browser.close()

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
