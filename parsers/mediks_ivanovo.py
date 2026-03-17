import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


CITY = "Иваново"
LAB = "Медикс Лаб"
START_URL = "https://medikslab.ru/ivanovo/analizy/uslugi"
OUTPUT_FILE = "out/mediks_ivanovo.csv"
DEBUG_HTML = "out/mediks_debug.html"


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


def parse_page_html(html: str, page_url: str, category: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    next_links = []

    cards = soup.select(
        "article, li, .catalog-item, .service-item, .product, .item, "
        "[class*='service'], [class*='catalog']"
    )

    for card in cards:
        name = ""
        for sel in [
            "h1", "h2", "h3",
            "[class*='title']",
            "[class*='name']",
            "a[href]",
        ]:
            node = card.select_one(sel)
            if node:
                name = clean_text(node.get_text(" ", strip=True))
                if len(name) > 3:
                    break

        if len(name) < 4:
            continue

        price = None
        for sel in [
            "[class*='price']",
            ".price",
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
            "category": category,
            "analysis_name": name,
            "price": price,
            "url": link,
        })

    for a in soup.select("a[href]"):
        href = a.get("href")
        text = clean_text(a.get_text(" ", strip=True))
        full = urljoin(page_url, href) if href else ""

        if "/ivanovo/analizy" not in full:
            continue

        next_links.append((full, text or category))

    return rows, next_links


def main():
    os.makedirs("out", exist_ok=True)
    visited = set()
    queue = [(START_URL, "Каталог")]
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        first = True

        while queue:
            url, category = queue.pop(0)

            if url in visited:
                continue
            visited.add(url)

            print(f"Open: {url}")

            try:
                page.goto(url, wait_until="networkidle", timeout=90000)
                page.wait_for_timeout(3500)
            except Exception as e:
                print(f"Open failed: {e}")
                continue

            html = page.content()

            if first:
                with open(DEBUG_HTML, "w", encoding="utf-8") as f:
                    f.write(html)
                first = False

            rows, links = parse_page_html(html, url, category)
            print(f"{url}: {len(rows)}")

            all_rows.extend(rows)

            for link_url, link_category in links:
                if link_url not in visited:
                    queue.append((link_url, link_category))

            time.sleep(1)

        browser.close()

    df = pd.DataFrame(all_rows)

    if df.empty:
        df = pd.DataFrame(columns=["lab", "city", "category", "analysis_name", "price", "url"])
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"Saved empty file: {OUTPUT_FILE}")
        return

    df["analysis_name"] = df["analysis_name"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.strip()
    df = df[df["analysis_name"].str.len() > 2].copy()
    df = df.drop_duplicates(subset=["lab", "city", "category", "analysis_name", "price"]).copy()
    df = df.sort_values(["category", "analysis_name"]).reset_index(drop=True)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
