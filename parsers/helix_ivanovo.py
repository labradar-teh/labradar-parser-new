name: Helix Ivanovo Parser

on:
  workflow_dispatch:
  schedule:
    - cron: "15 3 * * 1"

jobs:
  helix:
    runs-on: ubuntu-latest
    timeout-minutes: 240

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests beautifulsoup4 pandas openpyxl lxml urllib3

      - name: Create output dir
        run: mkdir -p output

      - name: Run Helix parser
        run: |
          set -euxo pipefail
          python parsers/helix_ivanovo.py --outdir output

      - name: Debug output
        if: always()
        run: |
          echo "FILES:"
          ls -lah output || true

          python - << 'PY'
          from pathlib import Path
          import pandas as pd

          csv_file = Path("output/helix_ivanovo.csv")
          if csv_file.exists():
              try:
                  df = pd.read_csv(csv_file)
                  print(f"Rows: {len(df)}")
                  if len(df) > 0:
                      print(df.head(20).to_string(index=False))
                      print("\nUnique URLs:", df["url"].nunique() if "url" in df.columns else "no url column")
                      print("Unique names:", df["analysis_name"].nunique() if "analysis_name" in df.columns else "no analysis_name column")
                  else:
                      print("CSV exists but empty")
              except Exception as e:
                  print(f"CSV read error: {e}")
          else:
              print("CSV not found")
          PY

      - name: Upload CSV artifact
        uses: actions/upload-artifact@v4
        with:
          name: helix-ivanovo-csv
          path: output/helix_ivanovo.csv
          if-no-files-found: error

      - name: Upload XLSX artifact
        uses: actions/upload-artifact@v4
        with:
          name: helix-ivanovo-xlsx
          path: output/helix_ivanovo.xlsx
          if-no-files-found: error
