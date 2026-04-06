"""
Сбор статистики запуска для контроля качества.
"""
import sys
from collections import Counter
from typing import List


class RunStats:
    def __init__(self, label: str):
        self.label = label
        self.urls_visited = 0
        self.cards_found = 0
        self.rows_saved = 0
        self.rows_filtered = 0
        self.pages_errored = 0
        self.filter_reasons: Counter = Counter()
        self.error_urls: List[str] = []

    def page_ok(self):
        self.urls_visited += 1

    def page_err(self, url: str):
        self.pages_errored += 1
        self.error_urls.append(url)

    def card_found(self):
        self.cards_found += 1

    def row_saved(self):
        self.rows_saved += 1

    def row_filtered(self, reason: str = ""):
        self.rows_filtered += 1
        if reason:
            self.filter_reasons[reason] += 1

    def print_summary(self):
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"[{self.label}] ИТОГ:", file=sys.stderr)
        print(f"  URL обошли:          {self.urls_visited}", file=sys.stderr)
        print(f"  Карточек нашли:      {self.cards_found}", file=sys.stderr)
        print(f"  Валидных строк:      {self.rows_saved}", file=sys.stderr)
        print(f"  Отфильтровано мусора:{self.rows_filtered}", file=sys.stderr)
        print(f"  Страниц с ошибкой:   {self.pages_errored}", file=sys.stderr)
        if self.filter_reasons:
            print(f"  Топ причин отбраковки:", file=sys.stderr)
            for reason, cnt in self.filter_reasons.most_common(5):
                print(f"    {reason}: {cnt}", file=sys.stderr)
        if self.error_urls:
            print(f"  Примеры проблемных URL ({min(3, len(self.error_urls))}):", file=sys.stderr)
            for u in self.error_urls[:3]:
                print(f"    {u}", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)
