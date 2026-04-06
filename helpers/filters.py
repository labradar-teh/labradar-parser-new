"""
Фильтрация мусора из результатов.
Логика: отсекаем нецелевые услуги, но НЕ выбрасываем реальные анализы.
Спорные случаи логируются через stats.
"""
import re
from typing import Optional

# Стоп-паттерны для названий — ТОЛЬКО явный мусор
# Используем границы слов там, где нужна точность
TRASH_NAME_PATTERNS = [
    r"\bакция\b",
    r"\bскидк",
    r"\bвыезд\s+медсестры\b",
    r"\bвзятие\s+биоматериала\b",
    r"\bзабор\s+крови\b",
    r"\bзабор\s+биоматериала\b",
    r"\bуслуга\s+забора\b",
    r"\bвыезд\s+на\s+дом\b",
    r"\bприём\s+врача\b",
    r"\bприем\s+врача\b",
    r"\bконсультация\s+врача\b",
    r"\bконсультация\s+специалиста\b",
    r"узи\b",
    r"\bэкг\b",
    r"\bэхо?кг\b",
    r"\bфлюорограф",
    r"\bрентген\b",
    r"\bмрт\b",
    r"\bкт\b(?!\s*\()",  # КТ как метод, но не «КТ (...)»
    r"\bэндоскоп",
    r"\bгастроскоп",
    r"\bкорпоративн",
    r"\bчек[\-\s]?ап\b",
    r"\bcheck[\-\s]?up\b",
    r"\bпрограмм",           # маркетинговые комплексы «Программа Женское здоровье»
    r"комплексн",            # «Комплексный анализ» — убираем
    r"\bпакет\s+анализов\b",
]

# Компилируем паттерны один раз
_COMPILED = [re.compile(p, re.IGNORECASE) for p in TRASH_NAME_PATTERNS]

# Стоп-фрагменты URL — явный нецелевой контент
TRASH_URL_PARTS = {
    "/promo", "/akts", "/akcii", "/news", "/novost",
    "/doctors", "/adres", "/contacts", "/about",
    "/corporate", "/vacanc", "/franch",
    "/dopolnitel", "/skidki", "/discount",
}


def is_trash_name(name: str) -> Optional[str]:
    """
    Если название — мусор, возвращает строку-причину.
    Если нормальное — возвращает None.
    """
    if not name:
        return "пустое название"
    low = name.lower()
    for pattern in _COMPILED:
        if pattern.search(low):
            return f"паттерн: {pattern.pattern}"
    return None


def is_trash_url(url: str) -> Optional[str]:
    low = url.lower()
    for part in TRASH_URL_PARTS:
        if part in low:
            return f"url-часть: {part}"
    return None


def is_suspicious_price(price) -> bool:
    """Цена <= 1 — явный мусор или ошибка."""
    try:
        return int(price) <= 1
    except (TypeError, ValueError):
        return False
