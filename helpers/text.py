"""
Утилиты нормализации текста и цены.
"""
import re
from typing import Optional


def clean_text(text: str) -> str:
    """Убирает лишние пробелы, неразрывные пробелы."""
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ").replace("\u200b", "")).strip()


def normalize_url(base: str, url: str) -> str:
    from urllib.parse import urljoin
    return urljoin(base, url.split("#")[0])


def clean_price(text: str) -> Optional[int]:
    """
    Извлекает первое число из строки и возвращает int, если оно в диапазоне 50–500 000.
    Иначе None.
    """
    if not text:
        return None
    text = text.replace("\xa0", " ").replace("\u202f", " ")
    m = re.search(r"(\d[\d\s]{0,10}\d|\d)", text)
    if not m:
        return None
    raw = re.sub(r"\D", "", m.group(0))
    if not raw:
        return None
    val = int(raw)
    if 50 <= val <= 500_000:
        return val
    return None


def extract_price_from_text(text: str) -> Optional[int]:
    """
    Ищет паттерны «число ₽» / «число руб» и возвращает первое валидное значение.
    """
    text = clean_text(text)
    patterns = [
        r"(\d[\d\s]{0,10})₽",
        r"(\d[\d\s]{0,10})\s*руб\.?",
    ]
    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            val = clean_price(m.group(1))
            if val is not None:
                return val
    return None


def title_from_soup(soup) -> Optional[str]:
    """Берёт h1, затем fallback на <title>."""
    h1 = soup.find("h1")
    if h1:
        txt = clean_text(h1.get_text(" ", strip=True))
        if len(txt) >= 3:
            return txt
    if soup.title:
        txt = soup.title.get_text(" ", strip=True)
        # убираем суффиксы вида «— Ситилаб» / «| Гемотест»
        txt = re.split(r"\s*[|—\-]\s*", txt)[0]
        txt = clean_text(txt)
        if len(txt) >= 3:
            return txt
    return None


def category_from_breadcrumbs(soup, blacklist: set) -> str:
    """Достаёт последний значимый элемент хлебных крошек."""
    crumbs = []
    for el in soup.select(
        'nav a, .breadcrumb a, .breadcrumbs a, '
        '[class*="breadcrumb"] a, [class*="Breadcrumb"] a, '
        '[aria-label="breadcrumb"] a'
    ):
        txt = clean_text(el.get_text(" ", strip=True))
        if txt and txt not in blacklist:
            crumbs.append(txt)
    if crumbs:
        return crumbs[-1]
    return ""
