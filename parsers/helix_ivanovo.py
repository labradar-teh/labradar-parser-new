#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


LAB = "helix"
CITY = "Иваново"
START_URL = "https://helix.ru/ivanovo/catalog/190-vse-analizy"


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


def extract_links(html):
    soup = BeautifulSoup(html, "lxml")
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/catalog/item/" in href:
            if href.startswith("/"):
                href = "https://helix.ru" + href
            links.add(href)

    return list(links)


def extract_price(html):
    text = clean(BeautifulSoup(html, "lxml").get_text())

    m = re.search(r"(Стоимость|Цена).*?(\d[\d\s]+)\s*₽", text)
    if m:
        return int(re.sub(r"\D", "", m.group(2)))

    return ""


def extract_name(html):
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    if h1:
        return clean(h1.text)
    return None


def parse_item(page, url):
    try:
        page.goto(url, timeout=60000)
        page.wait_for_timeout(300)
        html = page.content()
    except:
        return None

    name = extract_name(html)
    if not name:
        return None

    price = extract_price(html)

    return {
        "lab": LAB,
        "city": CITY,
        "category": "Анализы",
        "analysis_name": name,
        "price": price,
        "url": url
    }


def scroll_full(page):
    last_count = 0

    for i in range(60):  # 🔥 ключевое — много прокруток
        page.mouse.wheel(0, 8000)
        page.wait_for_timeout(500)

        html = page.content()
        links = extract_links(html)

        print(f"[scroll] step={i} links={len(links)}")

        if len(links) == last_count:
            print("[scroll] остановка — больше не грузится")
            break

        last_count = len(links)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="output")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(START_URL)

        # 🔥 ключ — доскролл до конца
        scroll_full(page)

        html = page.content()
        links = extract_links(html)

        print(f"[helix] TOTAL LINKS: {len(links)}")

        rows = []
        for i, url in enumerate(links, 1):
            data = parse_item(page, url)
            if data:
                rows.append(data)

            if i % 100 == 0:
                print(f"{i}/{len(links)} parsed")

        browser.close()

    df = pd.DataFrame(rows)

    df.to_csv(outdir / "helix_ivanovo.csv", index=False)
    df.to_excel(outdir / "helix_ivanovo.xlsx", index=False)

    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
