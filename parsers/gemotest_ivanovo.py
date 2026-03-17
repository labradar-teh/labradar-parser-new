#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

LAB = "gemotest"
CITY = "Иваново"
BASE_URL = "https://gemotest.ru"
START_URL = f"{BASE_URL}/ivanovo/analyzes/"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

DELAY = 0.2


# ------------------------
# utils
# ------------------------
def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def clean_text(text):
    return re.sub(r"\s+", " ", text).strip()


def parse_price(text):
    if not text:
        return None
    text = text.replace("\xa0", " ")
    m = re.search(r"([\d\s]+)\s*₽", text)
    if not m:
        return None
    return int(re.sub(r"\D", "", m.group(1)))


# ------------------------
# сбор ссылок
# ------------------------
def collect_analysis_links():
    print("[gemotest] collecting links...")

    soup = get_soup(START_URL)

    links = set()

    # ссылки на категории
    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/analyzes/" in href and not href.endswith("/analyzes/"):
            full = urljoin(BASE_URL, href)
            links.add(full)

    print(f"[gemotest] found category links: {len(links)}")

    analysis_links = set()

    # идем по категориям
    for url in links:
        try:
            soup = get_soup(url)

            for a in soup.find_all("a", href=True):
                href = a["href"]

                # ссылка на анализ
                if re.search(r"/analyzes/.+/.+/", href):
                    full = urljoin(BASE_URL, href)
                    analysis_links.add(full)

            time.sleep(DELAY)

        except Exception as e:
            print(f"[error] {url}: {e}")

    print(f"[gemotest] total analysis links: {len(analysis_links)}")

    return list(analysis_links)


# ------------------------
# парсинг страницы анализа
# ------------------------
def parse_analysis(url):
    soup = get_soup(url)

    # название
    h1 = soup.find("h1")
    if not h1:
        return None

    name = clean_text(h1.get_text())

    # цена — ищем ВЕСЬ текст страницы
    full_text = clean_text(soup.get_text(" "))

    price = parse_price(full_text)

    # категория
    category = None
    breadcrumbs = soup.select("nav a")
    if breadcrumbs:
        category = clean_text(breadcrumbs[-2].get_text())

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": name,
        "price": price,
        "url": url
    }


# ------------------------
# main
# ------------------------
def main():
    outdir = Path("output")
    outdir.mkdir(exist_ok=True)

    links = collect_analysis_links()

    rows = []

    for i, url in enumerate(links, 1):
        try:
            data = parse_analysis(url)

            if not data or not data["price"]:
                continue

            rows.append(data)

            if i % 100 == 0:
                print(f"[gemotest] {i}/{len(links)}")

            time.sleep(DELAY)

        except Exception as e:
            print(f"[error] {url}: {e}")

    # дедуп
    unique = {}
    for r in rows:
        key = (r["analysis_name"], r["price"])
        unique[key] = r

    rows = list(unique.values())

    df = pd.DataFrame(rows)

    csv_path = outdir / "gemotest_ivanovo.csv"
    xlsx_path = outdir / "gemotest_ivanovo.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(xlsx_path, index=False)

    print(f"[gemotest] DONE: {len(rows)} rows")


if __name__ == "__main__":
    main()
