#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl.py — собирает data/{city}.json из свежих Excel парсеров.

Запуск:
  python etl.py --outdir output --datadir data
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

# ── Маппинг lab-ключей ────────────────────────────────────────────────────
LAB_KEY_MAP = {
    'mediks':   'medikslab',
    'citilab':  'citilab',
    'gemotest': 'gemotest',
    'helix':    'helix',
    'invitro':  'invitro',
    'kislorod': 'kislorod',
}

CITY_MAP = {
    'ivanovo':  'Иваново',
    'kostroma': 'Кострома',
}

# ── Canonical маппинг: slug → regex-паттерны ─────────────────────────────
CANONICAL = {
    'oak': [
        r'^общий анализ крови без лейкоцитарной формулы',
        r'^клинический анализ крови \(с лейкоцит',
        r'^клинический анализ крови \(c лейкоцит',
        r'^общий анализ крови с лейкоцитарной формулой и соэ',
        r'^общий анализ крови \(cbc/diff',
        r'^анализ крови\. общий анализ крови \(без лейкоц',
        r'^общий анализ крови \(без лейкоцитарной формулы',
        r'^общий анализ крови с лейкоцитарной формулой \+',
    ],
    'esr':        [r'^соэ\b', r'^скорость оседания эритроцитов'],
    'urinalysis': [
        r'^общий анализ мочи с микроскопией',
        r'^общий анализ мочи \(с микроскопией',
        r'^анализ мочи общий',
        r'^общий анализ мочи в ',
        r'^общий анализ мочи$',
    ],
    'glucose': [
        r'^глюкоза \(в крови\)',
        r'^глюкоза \(вен', r'^глюкоза \(кап',
        r'^глюкоза в ', r'^глюкоза$',
    ],
    'kreatinin': [
        r'^креатинин \(в крови\)',
        r'^креатинин \(вен', r'^креатинин \(кап',
        r'^креатинин в сыворотке',
        r'^креатинин в ', r'^креатинин$',
    ],
    'urea':              [r'^мочевина \(вен', r'^мочевина \(кап', r'^мочевина в ', r'^мочевина$'],
    'mochevaya_kislota': [r'^мочевая кислота \(вен', r'^мочевая кислота в ', r'^мочевая кислота$'],
    'alt': [r'^алт \(', r'^алт в ', r'^алт$', r'^аланинаминотрансфераза \(алт'],
    'ast': [r'^аст \(', r'^аст в ', r'^аст$', r'^аспартатаминотрансфераза \(аст'],
    'bilirubin_total':  [r'^билирубин общий \(', r'^билирубин общий в ', r'^билирубин общий$'],
    'bilirubin_direct': [r'^билирубин прямой \(', r'^билирубин прямой в ', r'^билирубин прямой$'],
    'albumin':       [r'^альбумин \(вен', r'^альбумин \(кап', r'^альбумин в ', r'^альбумин$'],
    'belok_obschiy': [r'^общий белок \(вен', r'^общий белок \(кап', r'^общий белок в ', r'^общий белок$', r'^белок общий'],
    'ggt':               [r'^гамма-гт\b', r'^ггт\b', r'^гамма-глутамилтрансфераза'],
    'schelochnaya_fosfataza': [r'^щелочная фосфатаза \(', r'^щелочная фосфатаза в ', r'^щелочная фосфатаза$'],
    'amilaza':  [r'^амилаза \(вен', r'^амилаза общая', r'^амилаза в ', r'^амилаза$'],
    'lipaza':   [r'^липаза \(', r'^липаза в ', r'^липаза$'],
    'ldg':      [r'^лдг\b', r'^лактатдегидрогеназа'],
    'kfk':      [r'^кфк\b(?! мв| mb)', r'^креатинкиназа \(вен', r'^креатинкиназа в ', r'^креатинкиназа$'],
    'cholesterol_total': [
        r'^холестерин общий \(', r'^холестерин общий в ', r'^холестерин общий$',
        r'^холестерол общий',
    ],
    'ldl': [
        r'^холестерин липопротеинов низкой плотности \(лпнп\)',
        r'^холестерол – липопротеины низкой плотности',
        r'^холестерол - липопротеины низкой плотности',
        r'^липопротеины низкой плотности \(лпнп,ldl',
        r'^лпнп в ', r'^лпнп$',
    ],
    'hdl': [
        r'^холестерин липопротеинов высокой плотности \(лпвп\)',
        r'^холестерин-лпвп\b',
        r'^холестерол – липопротеины высокой плотности',
        r'^холестерол - липопротеины высокой плотности',
        r'^липопротеины высокой плотности \(лпвп',
        r'^лпвп в ', r'^лпвп$',
    ],
    'triglycerides':     [r'^триглицериды \(вен', r'^триглицериды \(кап', r'^триглицериды в ', r'^триглицериды$'],
    'ferritin':          [r'^ферритин \(', r'^ферритин в ', r'^ферритин$'],
    'iron_serum':        [r'^железо сывороточное', r'^железо \(вен', r'^железо \(кап', r'^железо в ', r'^железо$'],
    'transferrin':       [r'^трансферрин \(', r'^трансферрин в ', r'^трансферрин$'],
    'ttg': [
        r'^ттг чувствительный',
        r'^ттг \(', r'^тиреотропный гормон \(ттг',
        r'^ттг в ', r'^ттг$',
    ],
    't4_free':   [r'^т4 свободный \(', r'^тироксин свободный', r'^свободный т4', r'^т4 свободный в ', r'^т4 свободный$'],
    't3_free':   [r'^т3 свободный \(', r'^трийодтиронин свободный', r'^т3 свободный в ', r'^т3 свободный$'],
    'at_k_tpo':  [r'^антитела к тпо\b', r'^ат к тпо\b', r'^антитела к тиреопероксид'],
    'tireoglobulin': [r'^тиреоглобулин \(', r'^тиреоглобулин в ', r'^тиреоглобулин$'],
    'vitd': [
        r'^25-гидроксивитамин d\b',
        r'^витамин d, 25-гидрокси \(кальциферол\)$',
        r'^витамин d \(25-oh\)',
        r'^25-oh витамин d\b',
        r'^витамин d в ', r'^витамин d$',
    ],
    'vitamin_b12':     [r'^витамин b12\b', r'^витамин в12\b', r'^цианокобаламин'],
    'folievaya_kislota': [r'^фолиевая кислота \(', r'^фолаты\b', r'^фолиевая кислота в ', r'^фолиевая кислота$'],
    'kaltsiy':  [r'^кальций \(вен', r'^кальций \(кап', r'^кальций общий', r'^кальций в ', r'^кальций$'],
    'magniy':   [r'^магний \(вен', r'^магний \(кап', r'^магний в ', r'^магний$'],
    'kaliy':    [r'^калий \(вен', r'^калий \(кап', r'^калий в ', r'^калий$'],
    'natriy':   [r'^натрий \(вен', r'^натрий \(кап', r'^натрий в ', r'^натрий$'],
    'fosfor':   [r'^фосфор \(вен', r'^фосфор неорганический \(', r'^фосфор в ', r'^фосфор$'],
    'gomotsistein': [r'^гомоцистеин \(', r'^гомоцистеин в ', r'^гомоцистеин$'],
    'crp': [
        r'^с-реактивный белок \(срб\) в ',
        r'^с-реактивный белок \(срб\)$',
        r'^с-реактивный белок \(срб, crp\)',
    ],
    'rf':            [r'^ревматоидный фактор \(', r'^ревматоидный фактор в ', r'^ревматоидный фактор$'],
    'aslo':          [r'^асло\b', r'^антистрептолизин'],
    'prokaltsitonin':[r'^прокальцитонин \(', r'^прокальцитонин в ', r'^прокальцитонин$'],
    'd_dimer':       [r'^д-димер\b', r'^d-dimer\b'],
    'fibrinogen':    [r'^фибриноген \(', r'^фибриноген в ', r'^фибриноген$'],
    'achtv':         [r'^ачтв\b', r'^активированное частичное'],
    'protrombin':    [r'^протромбин \(мно\)', r'^мно \(', r'^протромбиновое время', r'^протромбин в ', r'^протромбин$'],
    'vich':          [r'^вич 1/2\b', r'^вич-1/2\b', r'^антитела к вич'],
    'hbsag':         [r'^hbsag\b', r'^поверхностный антиген гепатита b'],
    'anti_hcv':      [r'^anti-hcv\b', r'^антитела к вирусу гепатита c'],
    'sifilis':       [r'^сифилис\b', r'^рпр\b'],
    'psa_obschiy':   [r'^пса общий\b', r'^пса \(общий\)', r'^простатический специфический антиген \(пса\) общий'],
    'psa_svobodnyy': [r'^пса свободный\b', r'^пса \(свободный\)'],
    'rea':           [r'^рэа\b', r'^раково-эмбриональный антиген'],
    'sa_125':        [r'^ca 125\b', r'^са-125\b', r'^ca-125\b'],
    'sa_19_9':       [r'^ca 19-9\b', r'^са 19-9\b'],
    'afp':           [r'^афп\b', r'^альфа-фетопротеин'],
    'prolactin':     [r'^пролактин \(', r'^пролактин в ', r'^пролактин$'],
    'kortizol':      [r'^кортизол \(', r'^кортизол в ', r'^кортизол$'],
    'insulin':       [r'^инсулин \(', r'^инсулин в ', r'^инсулин$'],
    'estradiol':     [r'^эстрадиол \(', r'^эстрадиол в ', r'^эстрадиол$'],
    'testosterone':  [r'^тестостерон общий', r'^тестостерон \(вен', r'^тестостерон в ', r'^тестостерон$'],
    'progesterone':  [r'^прогестерон \(', r'^прогестерон в ', r'^прогестерон$'],
    'fsh':           [r'^фсг\b', r'^фолликулостимулирующий гормон'],
    'lh':            [r'^лг\b(?! \+)', r'^лютеинизирующий гормон', r'^лг в ', r'^лг$'],
    'dgea_s':        [r'^дгэа-с\b', r'^дгэа сульфат', r'^дгэа-сульфат'],
    'paratgormon':   [r'^паратгормон \(', r'^паратиреоидный гормон', r'^птг\b', r'^паратгормон в ', r'^паратгормон$'],
    'hba1c':         [r'^гликированный гемоглобин', r'^hba1c\b', r'^гликогемоглобин'],
    'nechiporenko':  [r'^анализ мочи по нечипоренко'],
    'troponin_i':    [r'^тропонин i\b'],
    'khgch':         [r'^хгч\b', r'^хорионический гонадотропин человека', r'^хгч в ', r'^хгч$'],
    'amg':           [r'^амг\b', r'^антимюллеров гормон'],
    'at_k_tireoglobulinu': [r'^антитела к тиреоглобулину', r'^ат к тиреоглобулину'],
    'kaltsitonin':   [r'^кальцитонин \(', r'^кальцитонин в ', r'^кальцитонин$'],
    'aldosteron':    [r'^альдостерон \(', r'^альдостерон в ', r'^альдостерон$'],
    'leptin':        [r'^лептин \(', r'^лептин в ', r'^лептин$'],
}


def match_slug(name: str) -> str | None:
    n = re.sub(r'\s+в\s+(иваново|костроме|иванове|москве|питере|санкт-петербурге|\w+е|\w+е)$',
               '', name.lower().strip())
    for slug, patterns in CANONICAL.items():
        for pat in patterns:
            if re.search(pat, n, re.IGNORECASE):
                return slug
    return None


def best_price(prices: list) -> int | None:
    valid = [p for p in prices if pd.notna(p) and str(p) not in ('', 'nan')]
    if not valid:
        return None
    try:
        return int(min(float(p) for p in valid))
    except Exception:
        return None


def process_city(city_slug: str, city_name: str, xlsx_files: list[Path]) -> dict:
    """Собирает цены по одному городу из списка xlsx файлов."""
    prices: dict[str, dict[str, int]] = defaultdict(dict)

    for f in xlsx_files:
        try:
            df = pd.read_excel(f)
        except Exception as e:
            print(f"[etl] WARN: не удалось прочитать {f.name}: {e}", file=sys.stderr)
            continue

        # Определяем колонки
        nc = next((c for c in df.columns if 'name' in c.lower() or 'анализ' in c.lower()), None)
        pc = next((c for c in df.columns if 'price' in c.lower() or 'цен' in c.lower()), None)
        if nc is None or pc is None:
            print(f"[etl] WARN: не найдены колонки в {f.name}", file=sys.stderr)
            continue

        # Определяем лабораторию из имени файла: {lab}_{region}.xlsx
        parts = f.stem.split('_')
        raw_lab = parts[0]
        lab_key = LAB_KEY_MAP.get(raw_lab, raw_lab)

        # Группируем slug → список цен
        lab_prices: dict[str, list] = defaultdict(list)
        for _, row in df.iterrows():
            slug = match_slug(str(row[nc]))
            if slug:
                lab_prices[slug].append(row[pc])

        for slug, price_list in lab_prices.items():
            p = best_price(price_list)
            if p:
                prices[slug][lab_key] = p

    # Определяем список активных лабораторий
    labs: set[str] = set()
    for lab_prices in prices.values():
        labs.update(lab_prices.keys())

    from datetime import date
    return {
        'city':    city_name,
        'slug':    city_slug,
        'updated': date.today().isoformat(),
        'labs':    sorted(labs),
        'prices':  dict(prices),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='LabRadar ETL — собирает per-city JSON')
    parser.add_argument('--outdir',  default='output', help='Папка с Excel от парсеров')
    parser.add_argument('--datadir', default='data',   help='Куда сохранять {city}.json')
    args = parser.parse_args()

    outdir  = Path(args.outdir)
    datadir = Path(args.datadir)
    datadir.mkdir(parents=True, exist_ok=True)

    # Собираем все xlsx
    all_xlsx = list(outdir.glob('*.xlsx'))
    if not all_xlsx:
        print('[etl] ERROR: xlsx файлы не найдены в', outdir, file=sys.stderr)
        return 1

    print(f'[etl] найдено xlsx: {len(all_xlsx)}')

    # Группируем по региону
    region_files: dict[str, list[Path]] = defaultdict(list)
    for f in all_xlsx:
        parts = f.stem.split('_')
        if len(parts) >= 2:
            region_slug = parts[1]  # ivanovo, kostroma, moscow, ...
            region_files[region_slug].append(f)

    # Обрабатываем каждый регион
    for region_slug, files in sorted(region_files.items()):
        city_name = CITY_MAP.get(region_slug, region_slug.capitalize())
        print(f'[etl] обрабатываю {region_slug} ({len(files)} файлов)...')

        result = process_city(region_slug, city_name, files)

        out_path = datadir / f'{region_slug}.json'
        with open(out_path, 'w', encoding='utf-8') as fh:
            json.dump(result, fh, ensure_ascii=False, separators=(',', ':'))

        n_slugs = len(result['prices'])
        n_cells = sum(len(v) for v in result['prices'].values())
        print(f'[etl] {region_slug}: {n_slugs} анализов, {n_cells} ячеек → {out_path}')

    print('[etl] готово')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
