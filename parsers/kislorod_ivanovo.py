#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


LAB = "kislorod"
CITY = "Иваново"
BASE_URL = "https://kislorod-doctor.ru"
START_URL = f"{BASE_URL}/analizy/"
DELAY = 0.1


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.text


def parse_links(html):
    soup = BeautifulSoup(html, "lxml")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/analizy/" not in href:
            continue

        if any(x in href for x in ["#", "javascript", "tel:", "mailto:"]):
            continue

        full = urljoin(BASE_URL, href)

        if full not in links:
            links.append(full)

    return links


def parse_page(url):
    try:
        html = fetch(url)
    except:
        return None

    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    if not h1:
        return None

    name = clean(h1.get_text())

    if len(name) < 3:
        return None

    text = clean(soup.get_text())

    m = re.search(r"([\d\s]+)\s*(₽|руб)", text)
    price = ""

    if m:
        price = int(re.sub(r"\D", "", m.group(1)))

    return {
        "lab": LAB,
        "city": CITY,
        "category": "Анализы",
        "analysis_name": name,
        "price": price,
        "url": url,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="out")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    html = fetch(START_URL)
    links = parse_links(html)

    print(f"links found: {len(links)}")

    rows = []

    for i, url in enumerate(links, 1):
        row = parse_page(url)

        if row:
            rows.append(row)

        if i % 50 == 0:
            print(f"parsed {i}/{len(links)} rows={len(rows)}")

        time.sleep(DELAY)

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["url"])

    csv_path = outdir / "kislorod_ivanovo.csv"
    xlsx_path = outdir / "kislorod_ivanovo.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)

    print(f"rows: {len(df)}")


if __name__ == "__main__":
    main()
