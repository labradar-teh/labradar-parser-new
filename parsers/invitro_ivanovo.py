#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import time
from collections import OrderedDict
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


LAB = "invitro"
CITY = "Иваново"
BASE_URL = "https://www.invitro.ru"
START_URL = f"{BASE_URL}/analizes/for-doctors/ivanovo/"


HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


def normalize(url):
    return BASE_URL + url if url.startswith("/") else url


def is_valid_analysis(url):
    # только реальные анализы
    if "/docs/" in url:
        return False
    if "clear_cache" in url:
        return False
    if not re.search(r"/\d+/\d+/", url):
        return False
    return True


def extract_price(text):
    m = re.search(r"(\d[\d\s]+)\s*₽", text)
    if m:
        return int(re.sub(r"\D", "", m.group(1)))
    return ""


def parse_page(html):
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for a in soup.find_all("a", href=True):
        url = normalize(a["href"])

        if not is_valid_analysis(url):
            continue

        name = clean(a.text)
        if not name or len(name) < 3:
            continue

        parent_text = clean(a.parent.get_text(" ", strip=True))
        price = extract_price(parent_text)

        rows.append({
            "lab": LAB,
            "city": CITY,
            "category": "Анализы",
            "analysis_name": name,
            "price": price,
            "url": url
        })

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="output")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True)

    r = requests.get(START_URL, headers=HEADERS)
    rows = parse_page(r.text)

    # удаляем дубли
    dedup = OrderedDict()
    for row in rows:
        key = row["url"]
        dedup[key] = row

    df = pd.DataFrame(list(dedup.values()))

    df.to_csv(outdir / "invitro_ivanovo.csv", index=False)
    df.to_excel(outdir / "invitro_ivanovo.xlsx", index=False)

    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
