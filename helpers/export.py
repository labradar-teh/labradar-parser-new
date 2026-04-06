"""
Экспорт результатов в CSV и XLSX.
"""
import csv
from pathlib import Path
from typing import List, Dict

import pandas as pd

COLUMNS = ["lab", "city", "category", "analysis_name", "price", "url"]
OPTIONAL_COLUMNS = ["analysis_code", "updated_at", "source_region_url"]


def export_rows(rows: List[Dict], out_csv: Path, out_xlsx: Path) -> int:
    """
    Сохраняет строки в CSV и XLSX.
    Дедуплицирует по URL, сортирует по (category, analysis_name).
    Возвращает финальное количество строк.
    """
    if not rows:
        # Создаём пустые файлы, чтобы артефакт не сломался
        df = pd.DataFrame(columns=COLUMNS)
    else:
        all_cols = COLUMNS + [c for c in OPTIONAL_COLUMNS if c in rows[0]]
        df = pd.DataFrame(rows, columns=[c for c in all_cols if c in rows[0]])

    # Привести колонки к стандарту
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df.drop_duplicates(subset=["url"]).sort_values(
        ["category", "analysis_name"], na_position="last"
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df.to_excel(out_xlsx, index=False, engine="openpyxl")

    return len(df)
