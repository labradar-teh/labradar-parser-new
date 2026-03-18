import csv
import re
import time
from collections import OrderedDict
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook


BASE_URL = "https://helix.ru"
CITY_SLUG = "ivanovo"
CITY_NAME = "Иваново"

START_URL = f"{BASE_URL}/{CITY_SLUG}/catalog/190-vse-analizy"

OUTPUT_CSV = "helix-ivanovo.csv"
OUTPUT_XLSX = "helix-ivanovo.xlsx"

REQUEST_TIMEOUT = 25
SLEEP_BETWEEN_REQUESTS = 0.15
MAX_RETRIES = 4

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": f"{BASE_URL}/{CITY_SLUG}/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


BAD_CATEGORY_PARTS = [
    "vrach",
    "uzi",
    "ekg",
    "akts",
    "promo",
    "skid",
]

BAD_NAME_PATTERNS = [
    r"\bвзятие\b",
    r"\bбиоматериал",
    r"\bзабор\b",
    r"\bуслуг",
    r"\bуслуга\b",
    r"\bприем\b",
    r"\bприём\b",
    r"\bконсультац",
    r"\bакц",
    r"\bскид",
    r"\bcheck[- ]?up\b",
    r"\bчекап\b",
    r"\bкомплекс\b",
    r"\bузи\b",
    r"\bэкг\b",
]

SKIP_SECTION_TITLES = {
    "Врачебные услуги",
    "УЗИ (ультразвуковые исследования)",
    "ЭКГ (электрокардиограмма)",
    "Комплексы анализов",
    "Популярные анализы",
}


def fetch(url, *, allow_404=False):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            if allow_404 and resp.status_code == 404:
                return None
            resp.raise_for_status()
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return resp
        except Exception as e:
            last_err = e
            time.sleep(min(2 * attempt, 6))
    raise RuntimeError(f"GET failed: {url} :: {last_err}")


def soupify(url):
    resp = fetch(url)
    return BeautifulSoup(resp.text, "html.parser")


def normalize_space(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def clean_price(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def abs_url(href):
    if not href:
        return None
    return urljoin(BASE_URL, href)


def canonical_page_url(url, page):
    parsed = urlparse(url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    q["page"] = [str(page)]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def extract_item_code_from_url(url):
    m = re.search(r"/catalog/item/(\d{2}-\d{3})", url)
    return m.group(1) if m else None


def is_analysis_item_url(url):
    return bool(re.search(r"/catalog/item/\d{2}-\d{3}", url or ""))


def is_complex_code(code):
    return bool(code and code.startswith("40-"))


def bad_name(name):
    low = (name or "").lower()
    for pat in BAD_NAME_PATTERNS:
        if re.search(pat, low):
            return True
    return False


def bad_category_url(url):
    low = (url or "").lower()
    return any(x in low for x in BAD_CATEGORY_PARTS)


def get_catalog_category_links():
    soup = soupify(START_URL)
    links = OrderedDict()

    for a in soup.find_all("a", href=True):
        href = abs_url(a["href"])
        text = normalize_space(a.get_text(" ", strip=True))

        if not href:
            continue
        if f"/{CITY_SLUG}/catalog/" not in href:
            continue
        if "/catalog/item/" in href:
            continue
        if bad_category_url(href):
            continue
        if text in SKIP_SECTION_TITLES:
            continue

        m = re.search(rf"/{CITY_SLUG}/catalog/(\d+)-", href)
        if not m:
            continue

        cat_id = m.group(1)
        if cat_id == "190":
            continue

        links[href] = text or href.rsplit("/", 1)[-1]

    return links


def find_last_page(soup):
    max_page = 1
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = normalize_space(a.get_text(" ", strip=True))

        if "?page=" in href:
            m = re.search(r"[?&]page=(\d+)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))

        if text.isdigit():
            try:
                max_page = max(max_page, int(text))
            except:
                pass

    return max_page


def parse_listing_page(listing_url, default_category):
    soup = soupify(listing_url)
    rows = []
    item_links = OrderedDict()

    for a in soup.find_all("a", href=True):
        href = abs_url(a["href"])
        if not is_analysis_item_url(href):
            continue

        code = extract_item_code_from_url(href)
        if not code or is_complex_code(code):
            continue

        text = normalize_space(a.get_text(" ", strip=True))
        if not text:
            continue

        item_links[href] = text

    for item_url in item_links.keys():
        row = parse_item_page(item_url, default_category)
        if row:
            rows.append(row)

    return rows, find_last_page(soup)


def extract_h1(soup):
    h1 = soup.find("h1")
    if h1:
        return normalize_space(h1.get_text(" ", strip=True))
    return None


def extract_breadcrumb_category(soup):
    texts = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = normalize_space(a.get_text(" ", strip=True))
        if not text:
            continue
        if "/catalog/" in href and "/catalog/item/" not in href:
            texts.append(text)

    # берем самый осмысленный раздел, не "Главная" и не "Сдать анализы"
    for t in reversed(texts):
        if t not in {"Главная", "Сдать анализы", "Анализы"}:
            return t
    return None


def extract_main_price(soup):
    text = soup.get_text("\n", strip=True)

    # Приоритетно "Стоимость:"
    m = re.search(r"Стоимость:\s*([\d\s]+)\s*₽", text, flags=re.S)
    if m:
        return clean_price(m.group(1))

    # fallback
    prices = re.findall(r"([\d\s]{2,})\s*₽", text)
    prices = [clean_price(p) for p in prices]
    prices = [p for p in prices if p]
    return prices[0] if prices else None


def parse_item_page(item_url, default_category):
    try:
        soup = soupify(item_url)
    except Exception:
        return None

    code = extract_item_code_from_url(item_url)
    if not code or is_complex_code(code):
        return None

    title = extract_h1(soup)
    if not title:
        return None

    # На странице h1 обычно вида "Название в Иваново" — убираем хвост города
    title = re.sub(rf"\s+в\s+{re.escape(CITY_NAME)}\s*$", "", title, flags=re.I).strip()

    if bad_name(title):
        return None

    category = extract_breadcrumb_category(soup) or default_category or "Хеликс"

    if category in SKIP_SECTION_TITLES:
        return None

    price = extract_main_price(soup)
    if not price:
        return None

    return {
        "lab": "Хеликс",
        "city": CITY_NAME,
        "category": category,
        "analysis_name": title,
        "price": price,
        "url": item_url,
        "_code": code,
    }


def collect_all():
    all_rows = OrderedDict()

    category_links = get_catalog_category_links()

    # если меню вдруг недоотдало ссылки — подстраховка стартовой веткой
    category_links[START_URL] = "Все анализы"

    for cat_url, cat_name in category_links.items():
        if cat_name in SKIP_SECTION_TITLES:
            continue

        try:
            page1_soup = soupify(cat_url)
        except Exception:
            continue

        last_page = find_last_page(page1_soup)
        if last_page < 1:
            last_page = 1

        for page in range(1, last_page + 1):
            page_url = cat_url if page == 1 else canonical_page_url(cat_url, page)

            try:
                rows, _ = parse_listing_page(page_url, cat_name)
            except Exception:
                continue

            for row in rows:
                code = row["_code"]
                if code not in all_rows:
                    all_rows[code] = row

    # финальная чистка
    cleaned = []
    for code, row in all_rows.items():
        name = row["analysis_name"]
        category = row["category"]

        if bad_name(name):
            continue
        if category in SKIP_SECTION_TITLES:
            continue
        if row["price"] is None:
            continue

        cleaned.append({
            "lab": row["lab"],
            "city": row["city"],
            "category": row["category"],
            "analysis_name": row["analysis_name"],
            "price": row["price"],
            "url": row["url"],
        })

    cleaned.sort(key=lambda x: (x["category"], x["analysis_name"]))
    return cleaned


def save_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["lab", "city", "category", "analysis_name", "price", "url"]
        )
        writer.writeheader()
        writer.writerows(rows)


def save_xlsx(rows, path):
    wb = Workbook()
    ws = wb.active
    ws.title = "helix"

    headers = ["lab", "city", "category", "analysis_name", "price", "url"]
    ws.append(headers)

    for row in rows:
        ws.append([row[h] for h in headers])

    wb.save(path)


def main():
    rows = collect_all()
    save_csv(rows, OUTPUT_CSV)
    save_xlsx(rows, OUTPUT_XLSX)
    print(f"done: {len(rows)} rows")
    print(OUTPUT_CSV)
    print(OUTPUT_XLSX)


if __name__ == "__main__":
    main()
