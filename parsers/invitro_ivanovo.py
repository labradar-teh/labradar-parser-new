#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
from collections import OrderedDict, deque
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CITY = "Иваново"
LAB = "invitro"
BASE_URL = "https://www.invitro.ru"
START_URL = f"{BASE_URL}/analizes/for-doctors/ivanovo/"
OUTPUT_DIR = "out"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "invitro_ivanovo.csv")
OUTPUT_XLSX = os.path.join(OUTPUT_DIR, "invitro_ivanovo.xlsx")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

DELAY = 0.15


def build_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def fetch(session, url):
    r = session.get(url, timeout=40)
    r.raise_for_status()
    return r.text


def soup_from_html(html):
    return BeautifulSoup(html, "lxml")


def clean_text(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def normalize_url(url):
    return urljoin(BASE_URL, url.split("#")[0])


def is_invitro_ivanovo_url(url):
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc not in {"www.invitro.ru", "invitro.ru"}:
        return False
    return parsed.path.startswith("/analizes/for-doctors/ivanovo/")


def is_detail_url(url):
    """
    У detail-страниц Invitro обычно длинный путь с id/slug сегментами.
    Отсекаем корневую и явные category/search страницы.
    """
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if not path.startswith("/analizes/for-doctors/ivanovo/"):
        return False

    parts = [p for p in path.split("/") if p]
    # /analizes/for-doctors/ivanovo/...
    if len(parts) < 6:
        return False

    bad_tail = {
        "prices",
        "search",
        "all",
        "aktsii",
        "about",
    }
    if parts[-1].lower() in bad_tail:
        return False

    return True


def extract_links(html, current_url):
    soup = soup_from_html(html)
    found = []

    for a in soup.find_all("a", href=True):
        href = clean_text(a.get("href"))
        if not href:
            continue
        full = normalize_url(href)
        if is_invitro_ivanovo_url(full):
            found.append(full)

    # fallback по сырому html
    for raw in re.findall(r'https?://www\.invitro\.ru/analizes/for-doctors/ivanovo/[^"\']+', html):
        full = normalize_url(raw)
        if is_invitro_ivanovo_url(full):
            found.append(full)

    for raw in re.findall(r'/analizes/for-doctors/ivanovo/[^"\']+', html):
        full = normalize_url(raw)
        if is_invitro_ivanovo_url(full):
            found.append(full)

    result = []
    seen = set()
    for link in found:
        if link not in seen:
            seen.add(link)
            result.append(link)
    return result


def extract_h1(soup):
    h1 = soup.find("h1")
    if h1:
        return clean_text(h1.get_text(" ", strip=True))
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    return clean_text(title.split("|")[0].split("—")[0])


def extract_category(soup, url):
    crumbs = []
    for el in soup.select('nav a, .breadcrumbs a, .breadcrumb a, [class*="breadcrumb"] a'):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt:
            crumbs.append(txt)

    blacklist = {
        "Главная",
        "Анализы",
        "Анализы и цены",
        CITY,
        "Для врачей",
    }
    crumbs = [c for c in crumbs if c not in blacklist]
    if crumbs:
        return crumbs[-1]

    parts = [p for p in urlparse(url).path.split("/") if p]
    if len(parts) >= 5:
        return parts[-2].replace("-", " ").strip()
    return "Без категории"


def parse_price_candidates(text):
    text = clean_text(text)
    candidates = []

    for m in re.finditer(r"(\d[\d\s]{0,20})\s*(?:₽|руб\.?)", text, flags=re.I):
        raw = re.sub(r"\D", "", m.group(1))
        if not raw:
            continue
        value = int(raw)
        if 50 <= value <= 500000:
            candidates.append(value)

    return candidates


def extract_price(soup):
    # 1. Приоритет — блоки со словом "Стоимость"
    for node in soup.find_all(string=re.compile(r"Стоимость", re.I)):
        parent_text = clean_text(node.parent.get_text(" ", strip=True) if node.parent else "")
        candidates = parse_price_candidates(parent_text)
        if candidates:
            return candidates[0]

    # 2. Блоки с class*=price
    for node in soup.select('[class*="price"], [class*="Price"], [data-testid*="price"]'):
        txt = clean_text(node.get_text(" ", strip=True))
        candidates = parse_price_candidates(txt)
        if candidates:
            return candidates[0]

    # 3. Fallback по верхней части страницы
    chunks = []
    for node in soup.find_all(["h1", "h2", "div", "span", "p", "section"], limit=200):
        txt = clean_text(node.get_text(" ", strip=True))
        if txt:
            chunks.append(txt)
    top_text = " | ".join(chunks[:80])
    candidates = parse_price_candidates(top_text)
    if candidates:
        return candidates[0]

    # 4. Последний fallback по всей странице
    full_text = clean_text(soup.get_text(" ", strip=True))
    candidates = parse_price_candidates(full_text)
    if candidates:
        return candidates[0]

    return None


def parse_detail_page(session, url):
    html = fetch(session, url)
    soup = soup_from_html(html)

    analysis_name = extract_h1(soup)
    if not analysis_name or len(analysis_name) < 3:
        return None

    price = extract_price(soup)
    if price is None:
        return None

    category = extract_category(soup, url)

    return {
        "lab": LAB,
        "city": CITY,
        "category": category,
        "analysis_name": analysis_name,
        "price": price,
        "url": url,
    }


def collect_detail_urls(session):
    queue = deque([START_URL])
    visited_pages = set()
    detail_urls = OrderedDict()

    while queue:
        url = queue.popleft()
        if url in visited_pages:
            continue
        visited_pages.add(url)

        try:
            html = fetch(session, url)
        except Exception as e:
            print(f"[crawl-error] {url}: {e}", file=sys.stderr)
            continue

        links = extract_links(html, url)

        for link in links:
            if is_detail_url(link):
                detail_urls.setdefault(link, None)
            elif link not in visited_pages:
                queue.append(link)

        print(
            f"[crawl] pages={len(visited_pages)} queue={len(queue)} detail_urls={len(detail_urls)} current={url}"
        )
        time.sleep(DELAY)

        # защита от бесконечного обхода
        if len(visited_pages) > 3000:
            print("[warn] page crawl limit reached")
            break

    return list(detail_urls.keys())


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = build_session()

    detail_urls = collect_detail_urls(session)
    print(f"[invitro] collected detail urls: {len(detail_urls)}")

    rows = []
    for i, url in enumerate(detail_urls, 1):
        try:
            data = parse_detail_page(session, url)
            if data:
                rows.append(data)

            if i % 100 == 0:
                print(f"[invitro] parsed {i}/{len(detail_urls)} | rows={len(rows)}")

            time.sleep(DELAY)
        except Exception as e:
            print(f"[parse-error] {url}: {e}", file=sys.stderr)

    if not rows:
        raise RuntimeError("Invitro parser returned 0 rows")

    dedup = OrderedDict()
    for row in rows:
        key = (row["analysis_name"].strip().lower(), row["url"])
        dedup[key] = row

    final_rows = list(dedup.values())

    df = pd.DataFrame(
        final_rows,
        columns=["lab", "city", "category", "analysis_name", "price", "url"],
    )
    df = df.sort_values(["category", "analysis_name"]).reset_index(drop=True)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_XLSX, index=False)

    print(f"Saved: {OUTPUT_CSV}")
    print(f"Saved: {OUTPUT_XLSX}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
