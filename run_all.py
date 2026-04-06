#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запускает все парсеры для заданных регионов и лабораторий.
Используется для локального запуска и в master GitHub Actions workflow.

Каждый парсер запускается в отдельном subprocess, чтобы ошибка одного
не ронял остальные.
"""

import argparse
import subprocess
import sys
from pathlib import Path

# lab -> (python_module, available_regions)
PARSERS = {
    "citilab":  ("parsers/citilab.py",  ["ivanovo", "kostroma"]),
    "gemotest": ("parsers/gemotest.py", ["ivanovo", "kostroma"]),
    "helix":    ("parsers/helix.py",    ["ivanovo", "kostroma"]),
    "invitro":  ("parsers/invitro.py",  ["ivanovo", "kostroma"]),
    "kislorod": ("parsers/kislorod.py", ["ivanovo", "kostroma"]),
    "mediks":   ("parsers/mediks.py",   ["ivanovo", "kostroma"]),
}

ALL_LABS = list(PARSERS.keys())
ALL_REGIONS = ["ivanovo", "kostroma"]


def run_one(lab: str, region: str, outdir: str, delay: float) -> bool:
    """Запускает один парсер. Возвращает True если успешно."""
    module, available_regions = PARSERS[lab]
    script = Path(__file__).parent / module

    cmd = [
        sys.executable, str(script),
        "--region", region,
        "--outdir", outdir,
        "--delay", str(delay),
    ]
    print(f"\n{'='*60}", flush=True)
    print(f"[run_all] Starting {lab}/{region}", flush=True)
    print(f"[run_all] cmd: {' '.join(cmd)}", flush=True)

    result = subprocess.run(cmd, check=False)

    if result.returncode != 0:
        print(
            f"[run_all][FAIL] {lab}/{region} exited with code {result.returncode}",
            file=sys.stderr,
        )
        return False

    print(f"[run_all][OK] {lab}/{region}", flush=True)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Run all LabRadar parsers")
    parser.add_argument(
        "--labs",
        nargs="+",
        default=ALL_LABS,
        choices=ALL_LABS + ["all"],
        help="Какие лаборатории запускать (по умолчанию: все)",
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        default=ALL_REGIONS,
        choices=ALL_REGIONS + ["all"],
        help="Какие регионы запускать (по умолчанию: оба)",
    )
    parser.add_argument("--outdir", default="output", help="Папка для выгрузки")
    parser.add_argument("--delay", type=float, default=0.15, help="Задержка между запросами")
    args = parser.parse_args()

    labs = ALL_LABS if "all" in args.labs else args.labs
    regions = ALL_REGIONS if "all" in args.regions else args.regions

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    results = {}
    for lab in labs:
        for region in regions:
            ok = run_one(lab, region, args.outdir, args.delay)
            results[f"{lab}/{region}"] = ok

    print(f"\n{'='*60}")
    print("[run_all] РЕЗУЛЬТАТЫ:")
    failed = []
    for key, ok in results.items():
        status = "✓ OK" if ok else "✗ FAIL"
        print(f"  {status}  {key}")
        if not ok:
            failed.append(key)

    if failed:
        print(f"\n[run_all] Провалились: {', '.join(failed)}", file=sys.stderr)
        return 1

    print("\n[run_all] Все завершены успешно.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
