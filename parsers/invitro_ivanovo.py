#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


CITY = "Иваново"
LAB = "invitro"
START_URL = "https://www.invitro.ru/analizes/for-doctors/ivanovo/"
OUTPUT_FILE = "out/invitro_ivanovo.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


# ------------------------
# utils
# ------------------------
def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception:
        return None
    return None


def clean(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def parse_price(text):
    if not text:
        return None

    text = text.replace("\xa0", " ")

    m = re.search(r"(\d[\d\s]{0,15})\s*(₽|руб)", text)
    if not m:
        return None

    return int(re.sub(r"\D", "", m.group(1)))


# ------------------------
# 1. собираем ВСЕ ссылки
# ------------------------
def collect_links():
    print("[invitro] collecting links...")

    soup = fetch(START_URL)
    if not soup:
        return []

    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/analizes/for-doctors/ivanovo/" in href and len(href) > 50:
            full = urljoin(START_URL, href)
            links.add(full)

    print(f"[invitro] found links: {len(links)}")
    return list(links)


# ------------------------
# 2. парсим страницу анализа
# ------------------------
def parse_analysis(url):
    soup = fetch(url)
    if not soup:
        return None

    # название
    h1 = soup.find("h1")
    if not h1:
        return None

    name = clean(h1.get_text())

    # категория
    category = None
    breadcrumbs = soup.select("nav a")
    if breadcrumbs:
        category = clean(breadcrumbs[-2].get_text())

    # цена — ТОЛЬКО из блока стоимости
    price = None

    # ищем блок "Стоимость"
    for el in soup.find_all(text=re.compile("Стоимость", re.I)):
        parent = el.parent.get_text(" ", strip=True)
        price = parse_price(parent)
        if price:
            break

    # fallback
    if not price:
        full_text = clean(soup.get_text(" ", strip=True))
        price = parse_price(full_text)

    if not price:
        return None

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
    links = collect_links()

    rows = []

    for i, url in enumerate(links, 1):
        try:
            data = parse_analysis(url)

            if not data:
                continue

            rows.append(data)

            if i % 100 == 0:
                print(f"[invitro] {i}/{len(links)}")

            time.sleep(0.2)

        except Exception as e:
            print(f"[error] {url}: {e}")

    # дедуп
    unique = {}
    for r in rows:
        key = (r["analysis_name"], r["price"])
        unique[key] = r

    rows = list(unique.values())

    df = pd.DataFrame(rows)

    import os
    os.makedirs("out", exist_ok=True)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"[invitro] DONE: {len(rows)} rows")


if __name__ == "__main__":
    main()
