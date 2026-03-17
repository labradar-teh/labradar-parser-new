import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


CITY = "Иваново"
LAB = "Медикс Лаб"
START_URL = "https://medikslab.ru/ivanovo/analizy/uslugi"
OUTPUT_FILE = "out/mediks_ivanovo.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
}


def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.text
    except:
        return ""
    return ""


def clean_text(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def extract_price(text):
    matches = re.findall(r"(\d[\d ]{0,12})\s*(?:₽|руб)", text, flags=re.I)
    nums = []
    for m in matches:
        v = re.sub(r"[^\d]", "", m)
        if v:
            nums.append(int(v))
    return nums[-1] if nums else None


def parse_text(html, url):
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for a in soup.select("a"):
        text = clean_text(a.get_text(" ", strip=True))

        if not text:
            continue

        if "₽" not in text and "руб" not in text.lower():
            continue

        price = extract_price(text)
        if price is None:
            continue

        # убираем цену из текста
        name = re.sub(r"\d[\d ]{0,12}\s*(?:₽|руб)", "", text, flags=re.I)
        name = clean_text(name)

        if len(name) < 4:
            continue

        link = a.get("href")
        full_link = urljoin(url, link) if link else url

        rows.append({
            "lab": LAB,
            "city": CITY,
            "category": "Анализы",
            "analysis_name": name,
            "price": price,
            "url": full_link,
        })

    return rows


def main():
    all_rows = []

    for page in range(1, 40):
        url = START_URL if page == 1 else f"{START_URL}?PAGEN_1={page}"
        html = fetch(url)

        rows = parse_text(html, url)
        print(f"page {page}: {len(rows)}")

        if not rows and page > 5:
            break

        all_rows.extend(rows)
        time.sleep(0.5)

    os.makedirs("out", exist_ok=True)

    df = pd.DataFrame(all_rows)

    if df.empty:
        df = pd.DataFrame(columns=["lab", "city", "category", "analysis_name", "price", "url"])
        df.to_csv(OUTPUT_FILE, index=False)
        print("EMPTY RESULT")
        return

    df = df.drop_duplicates(subset=["analysis_name", "price"])
    df = df.sort_values("analysis_name").reset_index(drop=True)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"Saved: {len(df)} rows")


if __name__ == "__main__":
    main()
