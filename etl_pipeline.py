#!/usr/bin/env python3
"""
Upgraded ETL Pipeline
- OS-agnostic paths & CLI flags
- Optional .env support (if python-dotenv installed)
- Robust Excel handling (.xlsx; .xls if xlrd installed)
- Case-insensitive sheet/key detection
- Graceful MySQL optional step (--skip-mysql)
- Better logging (file + console)
- Safer charting (headless backend, NaN tolerant)

Usage examples:
  python etl_pipeline.py \
    --input ./input \
    --out-data ./output/data \
    --out-plots ./output/plots \
    --out-final ./output/final \
    --sheet-keys Reg Em \
    --pattern "*.xlsx" \
    --skip-mysql

You can also set env vars (take precedence order: CLI > env > defaults):
  MYSQL_HOST, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD
  INPUT_DIR, OUTPUT_DATA_DIR, OUTPUT_PLOTS_DIR, OUTPUT_FINAL_DIR
"""

from __future__ import annotations
import os
import re
import sys
import glob
import time
import math
import logging
import argparse
from dataclasses import dataclass
from datetime import date as Date
from pathlib import Path
from typing import List, Tuple, Iterable

import numpy as np
import pandas as pd
# Use a headless backend so it works anywhere
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from PyPDF2 import PdfMerger

# Try to load .env if available
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return False

# ---------- Defaults ----------
DEFAULTS = {
    "MYSQL_HOST": "127.0.0.1",
    "MYSQL_DB": "data",
    "MYSQL_USER": "root",
    "MYSQL_PASSWORD": "",
    "INPUT_DIR": str(Path.cwd() / "input"),
    "OUTPUT_DATA_DIR": str(Path.cwd() / "output" / "data"),
    "OUTPUT_PLOTS_DIR": str(Path.cwd() / "output" / "plots"),
    "OUTPUT_FINAL_DIR": str(Path.cwd() / "output" / "final"),
}

TODAY = Date.today()
STAMP = TODAY.strftime("%Y%m%d")

# Excel column letters (A..Z)
COLA = {i: chr(64 + i) for i in range(1, 27)}  # 1->A ... 26->Z

@dataclass
class ExtractedRow:
    run_date: Date
    ticker: str
    sheet_type: str  # subtype parsed from sheet name
    quarter: str
    year: str
    est_total_sold: float | str | None
    est_sold_min: float | str | None
    est_sold_max: float | str | None
    fc_wo_sa_actual: float | str | None
    fc_wo_sa_min: float | str | None
    fc_wo_sa_max: float | str | None


# ---------- Config / CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the ETL pipeline")
    p.add_argument("--input", dest="input_dir", default=None, help="Directory with Excel files")
    p.add_argument("--out-data", dest="out_data", default=None, help="Output folder for tidy Excel")
    p.add_argument("--out-plots", dest="out_plots", default=None, help="Output folder for per-row PDFs")
    p.add_argument("--out-final", dest="out_final", default=None, help="Output folder for merged Graphs.pdf")
    p.add_argument("--pattern", default="*.xlsx", help="Glob pattern for input files (e.g., '*.xlsx')")
    p.add_argument("--sheet-keys", nargs="*", default=["Reg", "Em"], help="Keywords to detect sheets")
    p.add_argument("--skip-mysql", action="store_true", help="Skip writing to MySQL")
    p.add_argument("--mysql-host", default=None)
    p.add_argument("--mysql-db", default=None)
    p.add_argument("--mysql-user", default=None)
    p.add_argument("--mysql-password", default=None)
    return p.parse_args()


def resolve_cfg(args: argparse.Namespace) -> dict:
    load_dotenv()  # load .env if present (no-op otherwise)

    env = {k: os.environ.get(k, DEFAULTS[k]) for k in DEFAULTS}

    # CLI overrides
    if args.input_dir:
        env["INPUT_DIR"] = args.input_dir
    if args.out_data:
        env["OUTPUT_DATA_DIR"] = args.out_data
    if args.out_plots:
        env["OUTPUT_PLOTS_DIR"] = args.out_plots
    if args.out_final:
        env["OUTPUT_FINAL_DIR"] = args.out_final

    if args.mysql_host:
        env["MYSQL_HOST"] = args.mysql_host
    if args.mysql_db:
        env["MYSQL_DB"] = args.mysql_db
    if args.mysql_user:
        env["MYSQL_USER"] = args.mysql_user
    if args.mysql_password is not None:
        env["MYSQL_PASSWORD"] = args.mysql_password

    # Normalize to POSIX-style paths for consistency (Path will handle OS specifics)
    for k in ("INPUT_DIR", "OUTPUT_DATA_DIR", "OUTPUT_PLOTS_DIR", "OUTPUT_FINAL_DIR"):
        env[k] = str(Path(env[k]).expanduser().resolve())

    return env


# ---------- Logging ----------

def setup_logging(out_data_dir: str) -> None:
    log_path = Path(out_data_dir) / "etl_time.log"
    Path(out_data_dir).mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(str(log_path), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ---------- Helpers ----------

def number_or_nan(x):
    try:
        v = float(x)
        if math.isfinite(v):
            return round(v, 1)
        return np.nan
    except Exception:
        return np.nan


def find_min_cell(df: pd.DataFrame) -> Tuple[int, int]:
    """Return (row_idx, col_idx) where cell equals 'Min' (case-insensitive)."""
    arr = df.to_numpy()
    # vectorized compare for speed/robustness
    matches = np.argwhere(np.vectorize(lambda v: str(v).strip().lower() == "min")(arr))
    if matches.size == 0:
        raise IndexError("'Min' marker not found")
    r, c = matches[0]
    return int(r), int(c)


def load_sheet_frame(xls_path: Path, sheet_name: str, usecol_letter: str | None = None) -> pd.DataFrame:
    # engine auto selection by pandas works; force openpyxl for xlsx
    engine = "openpyxl" if xls_path.suffix.lower() == ".xlsx" else None
    if usecol_letter is None:
        return pd.read_excel(xls_path, sheet_name=sheet_name, engine=engine, dtype=str)
    return pd.read_excel(xls_path, sheet_name=sheet_name, engine=engine, dtype=str, usecols=usecol_letter)


def parse_quarter_year(df: pd.DataFrame, min_row: int) -> Tuple[str, str]:
    hdr_cell = str(df.iloc[max(min_row - 2, 0), 0])
    m = re.search(r"(Q[1-4]).*?(20\d{2}|\d{2})", hdr_cell, flags=re.IGNORECASE)
    if not m:
        return "Q?", "20??"
    qtr = m.group(1).upper()
    year_token = m.group(2)
    year = year_token if len(year_token) == 4 else f"20{year_token}"
    return qtr, year


def parse_subtype(sheet_name: str, prefix_keywords: Iterable[str]) -> str:
    s = sheet_name.strip()
    for kw in prefix_keywords:
        if kw.lower() in s.lower():
            # split on '-' if present to get subtype (right part)
            if '-' in s:
                return s.split('-', 1)[1].strip() or "NULL"
            return kw
    return "NULL"


def extract_from_workbook(xls_path: Path, sheet_keys: List[str]) -> List[ExtractedRow]:
    t0 = time.time()
    try:
        xls = pd.ExcelFile(xls_path)
    except Exception as e:
        logging.warning("Skip %s (unreadable): %s", xls_path.name, e)
        return []

    # case-insensitive match of any keyword
    def match_sheet(s: str) -> bool:
        s_lower = s.lower()
        return any(k.lower() in s_lower for k in sheet_keys)

    target_sheets = [s for s in xls.sheet_names if match_sheet(s)]
    if not target_sheets:
        logging.info("No target sheets in %s", xls_path.name)
        return []

    rows: List[ExtractedRow] = []

    ticker_guess = xls_path.stem.upper()
    m = re.search(r"\b([A-Z]{3,6})\b", ticker_guess)
    ticker = m.group(1) if m else ticker_guess[:6]

    # Use first matching sheet to anchor quarter/year and Min position per unique key group
    # Group by first keyword hit to keep semantics (e.g., Reg vs Em)
    grouped: dict[str, List[str]] = {}
    for s in target_sheets:
        for k in sheet_keys:
            if k.lower() in s.lower():
                grouped.setdefault(k, []).append(s)
                break

    quarter, year = "Q?", "20??"

    # Collect values per group
    values_by_group: dict[str, List[tuple]] = {}
    ref_positions: dict[str, tuple[int, int]] = {}

    for key, sheets in grouped.items():
        try:
            df0 = load_sheet_frame(xls_path, sheets[0])
            r, c = find_min_cell(df0)
            ref_positions[key] = (r, c)
            if quarter == "Q?":
                quarter, year = parse_quarter_year(df0, r)
        except Exception as e:
            logging.warning("'%s' group in %s has no 'Min' marker: %s", key, xls_path.name, e)
            continue

        vals: List[tuple] = []
        for s in sheets:
            try:
                col_letter = COLA.get(ref_positions[key][1] + 1 + 1)  # two to the right
                df = load_sheet_frame(xls_path, s, col_letter)
                series = df.iloc[:, 0]
                # Safely read rows (r-2, r-1, r)
                idxs = [max(ref_positions[key][0] - 2, 0), max(ref_positions[key][0] - 1, 0), max(ref_positions[key][0], 0)]
                a, b, cval = (number_or_nan(series.iloc[i]) if i < len(series) else np.nan for i in idxs)
                vals.append((a, b, cval))
            except Exception as e:
                logging.warning("Failed reading sheet '%s' in %s: %s", s, xls_path.name, e)
                vals.append((np.nan, np.nan, np.nan))
        values_by_group[key] = vals

    # Harmonize lengths
    n = max((len(v) for v in values_by_group.values()), default=0)
    if n == 0:
        logging.info("No values extracted from %s", xls_path.name)
        return []

    # Build rows aligning by index across groups (e.g., first Reg with first Em)
    for i in range(n):
        # defaults as NaN
        reg_a = reg_b = reg_c = np.nan
        em_a = em_b = em_c = np.nan

        # Map group keys flexibly by name
        for key, vals in values_by_group.items():
            a, b, cval = vals[i] if i < len(vals) else (np.nan, np.nan, np.nan)
            if key.lower().startswith("reg"):
                reg_a, reg_b, reg_c = a, b, cval
            elif key.lower().startswith("em"):
                em_a, em_b, em_c = a, b, cval

        # subtype from whichever sheet exists at this index (best-effort)
        subtype = "NULL"
        for key in ("Reg", "Em"):
            sheets = grouped.get(key, [])
            if i < len(sheets):
                subtype = parse_subtype(sheets[i], [key])
                break

        rows.append(
            ExtractedRow(
                run_date=TODAY,
                ticker=ticker,
                sheet_type=subtype,
                quarter=quarter,
                year=year,
                est_total_sold=em_a,
                est_sold_min=em_c,
                est_sold_max=em_b,
                fc_wo_sa_actual=reg_a,
                fc_wo_sa_min=reg_c,
                fc_wo_sa_max=reg_b,
            )
        )

    logging.info("Processed %s in %.2fs (groups=%d)", xls_path.name, time.time() - t0, len(grouped))
    return rows


# ---------- Output ----------

def make_charts_pdf(df: pd.DataFrame, plots_dir: Path, final_pdf: Path) -> None:
    t0 = time.time()
    tmp_pdfs: List[Path] = []

    for _, r in df.iterrows():
        fig = plt.figure(figsize=(10, 5))
        names1 = ["Estimated Total Sold", "Estimated Sold Max", "Estimated Sold Min"]
        values1 = [r["Estimated Total Sold"], r["Estimated Sold Max"], r["Estimated Sold Min"]]
        names2 = ["Forecast w/o SA Actual", "Forecast w/o SA Max", "Forecast w/o SA Min"]
        values2 = [r["Forecast w/o SA Actual"], r["Forecast w/o SA Max"], r["Forecast w/o SA Min"]]

        # Convert to floats with NaN where needed
        v1 = [number_or_nan(v) for v in values1]
        v2 = [number_or_nan(v) for v in values2]
        values = v1 + v2

        # bar chart
        plt.bar(range(len(v1)), v1, width=0.3)
        plt.bar(range(len(v1), len(values)), v2, width=0.3)
        plt.xlabel(f"Estimated Sold vs Forecast (type: {r['Type']})")
        plt.ylabel("Values")
        plt.title(f"{r['Ticker']} — Year {r['Year']} ({r['Quarter']})")
        for i, v in enumerate(values):
            if not np.isnan(v):
                plt.text(i, v, str(v), ha="center")
        out_pdf = plots_dir / f"{r['Ticker']}_type({r['Type']}).pdf"
        fig.savefig(out_pdf, format="pdf")
        plt.close(fig)
        tmp_pdfs.append(out_pdf)

    if not tmp_pdfs:
        logging.info("No charts to merge.")
        return

    merger = PdfMerger(strict=False)
    for p in tmp_pdfs:
        try:
            merger.append(str(p))
        except Exception as e:
            logging.warning("Failed to append %s: %s", p.name, e)
    with final_pdf.open("wb") as f:
        merger.write(f)
    merger.close()

    # Clean up individual PDFs (optional)
    for p in tmp_pdfs:
        try:
            p.unlink()
        except Exception:
            pass

    logging.info("Created graphs PDF at %s in %.2fs", final_pdf, time.time() - t0)


def load_to_mysql(df: pd.DataFrame, cfg: dict, skip_mysql: bool) -> None:
    if skip_mysql:
        logging.info("MySQL step skipped by flag.")
        return
    url = f"mysql+pymysql://{cfg['MYSQL_USER']}:{cfg['MYSQL_PASSWORD']}@{cfg['MYSQL_HOST']}/{cfg['MYSQL_DB']}"
    t0 = time.time()
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.begin() as conn:
            conn.exec_driver_sql("DROP TABLE IF EXISTS analystdata")
            df.to_sql("analystdata", conn, index=False, method=None)
        logging.info("Wrote analystdata to MySQL in %.2fs", time.time() - t0)
    except SQLAlchemyError as e:
        logging.warning("MySQL step failed: %s", e)


# ---------- Main ----------

def main():
    args = parse_args()
    cfg = resolve_cfg(args)

    # Ensure directories exist
    in_dir = Path(cfg["INPUT_DIR"]) ; out_data = Path(cfg["OUTPUT_DATA_DIR"]) ; out_plots = Path(cfg["OUTPUT_PLOTS_DIR"]) ; out_final = Path(cfg["OUTPUT_FINAL_DIR"]) 
    for p in (in_dir, out_data, out_plots, out_final):
        p.mkdir(parents=True, exist_ok=True)

    setup_logging(str(out_data))

    model_xlsx = out_data / f"Model_{STAMP}.xlsx"
    graphs_pdf = out_final / "Graphs.pdf"

    # Remove prior model file if present
    try:
        if model_xlsx.exists():
            model_xlsx.unlink()
    except Exception:
        pass

    # Gather inputs
    patterns = args.pattern.split(";") if ";" in args.pattern else [args.pattern]
    files: List[Path] = []
    for pat in patterns:
        files.extend([Path(p) for p in glob.glob(str(in_dir / pat))])
    files = [f for f in files if f.is_file() and not f.name.startswith(".")]

    if not files:
        print("No input Excel files found. Put .xlsx files in:", in_dir)
        return

    all_rows: List[ExtractedRow] = []
    for xls in files:
        all_rows.extend(extract_from_workbook(xls, sheet_keys=args.sheet_keys))

    if not all_rows:
        print("No rows extracted — check sheet names (keywords) and 'Min' positions.")
        return

    data = pd.DataFrame([{
        "Date": r.run_date,
        "Ticker": r.ticker,
        "Type": r.sheet_type,
        "Quarter": r.quarter,
        "Year": r.year,
        "Estimated Total Sold": r.est_total_sold,
        "Estimated Sold Max": r.est_sold_max,
        "Estimated Sold Min": r.est_sold_min,
        "Forecast w/o SA Actual": r.fc_wo_sa_actual,
        "Forecast w/o SA Max": r.fc_wo_sa_max,
        "Forecast w/o SA Min": r.fc_wo_sa_min,
    } for r in all_rows])

    # Write tidy Excel
    data.to_excel(model_xlsx, index=False)
    logging.info("Wrote %s", model_xlsx)

    # MySQL load (optional)
    load_to_mysql(data, cfg, skip_mysql=args.skip_mysql)

    # Charts -> merged PDF
    make_charts_pdf(data, plots_dir=out_plots, final_pdf=graphs_pdf)

    print(
        "\n✅ Done.\n" \
        f"- Data: {model_xlsx}\n" \
        f"- MySQL table (if enabled): analystdata\n" \
        f"- Graphs: {graphs_pdf}\n"
    )


if __name__ == "__main__":
    main()
