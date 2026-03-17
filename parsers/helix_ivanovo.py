import os
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


CITY = "Иваново"
LAB = "Helix"
BASE_URL = "https://helix.ru/ivanovo/catalog/190-vse-analizy"
OUTPUT_CSV = "out/helix_ivanovo.csv"
OUTPUT_XLSX = "out/helix_ivanovo.xlsx"

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


def normalize_name(text: str) -> str:
    text = clean_text(text)

    # вырезаем цену
    text = re.sub(r"\d[\d ]{0,12}\s*(?:₽|руб)", " ", text, flags=re.I)

    # вырезаем сроки
    text = re.sub(r"До\s+\d+\s+суток.*", " ", text, flags=re.I)
    text = re.sub(r"До\s+\d+\s+раб\.\s*дн.*", " ", text, flags=re.I)

    # вырезаем служебный текст
    garbage_patterns = [
        r"Указанный срок не включает день взятия биоматериала.*",
        r"Заказать.*",
        r"Сдать дома.*",
        r"В корзину.*",
    ]
    for pattern in garbage_patterns:
        text = re.sub(pattern, " ", text, flags=re.I)

    # убираем лидирующие слова
    text = re.sub(r"^(Анализ|Комплекс)\s+", "", text, flags=re.I)

    text = clean_text(text)
    return text


def parse_page(url: str):
    soup = fetch(url)
    if not soup:
        return []

    rows = []

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        text = clean_text(a.get_text(" ", strip=True))

        if not text:
            continue

        if "₽" not in text and "руб" not in text.lower():
            continue

        if not href.startswith("/catalog/item/"):
            continue

        price = extract_price(text)
        if price is None:
            continue

        name = normalize_name(text)
        if len(name) < 4:
            continue

        full_url = urljoin(url, href)

        rows.append({
            "lab": LAB,
            "city": CITY,
            "category": "Анализы",
            "analysis_name": name,
            "price": price,
            "url": full_url,
        })

    return rows


def main():
    all_rows = []

    for page_num in range(1, 90):
        url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
        rows = parse_page(url)
        print(f"page {page_num}: {len(rows)}")

        if not rows and page_num > 5:
            break

        all_rows.extend(rows)
        time.sleep(0.4)

    os.makedirs("out", exist_ok=True)

    df = pd.DataFrame(all_rows)

    if df.empty:
        df = pd.DataFrame(columns=["lab", "city", "category", "analysis_name", "price", "url"])
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        df.to_excel(OUTPUT_XLSX, index=False)
        print("EMPTY RESULT")
        return

    df["analysis_name"] = df["analysis_name"].astype(str).str.strip()
    df = df[df["analysis_name"].str.len() > 2].copy()

    df = df.drop_duplicates(subset=["lab", "analysis_name", "price", "url"]).copy()
    df = df.sort_values(["analysis_name"]).reset_index(drop=True)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_XLSX, index=False)

    print(f"Saved CSV: {OUTPUT_CSV}")
    print(f"Saved XLSX: {OUTPUT_XLSX}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
