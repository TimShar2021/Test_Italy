from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_HOME = Path(os.getenv("PROJECT_HOME", Path(__file__).resolve().parents[1]))
SOURCE_XLSX_PATH = Path(os.getenv("SOURCE_XLSX_PATH", PROJECT_HOME / "data/input/italy_test_task.xlsx"))
POS_SOURCE_DB_PATH = Path(os.getenv("POS_SOURCE_DB_PATH", PROJECT_HOME / "data/runtime/pos_source.duckdb"))
DWH_DB_PATH = Path(os.getenv("DWH_DB_PATH", PROJECT_HOME / "data/runtime/dwh.duckdb"))

FACT_COLUMNS_RU_TO_EN = {
    "Учетный день": "accounting_date",
    "Номер чека": "check_id",
    "Время открытия": "open_ts",
    "Час открытия": "open_hour",
    "Время закрытия": "close_ts",
    "Час закрытия": "close_hour",
    "Блюдо": "dish_name",
    "Категория блюда": "dish_category",
    "Тип заказа": "order_type",
    "Количество блюд": "qty",
    "Количество гостей": "guest_count",
    "Сумма со скидкой, р.": "amount_discount_rub",
}

FACT_SHEETS = {
    "факт 02 2026": "2026-02",
    "факт 02 2025": "2025-02",
}

PLAN_SHEET = "план 02 2026"


def ensure_runtime_dir() -> None:
    (PROJECT_HOME / "data/runtime").mkdir(parents=True, exist_ok=True)


def normalize_datetime(series: pd.Series) -> pd.Series:
    """Robust datetime conversion for Excel cells and already parsed datetimes."""
    return pd.to_datetime(series, errors="coerce")


def normalize_numeric(series: pd.Series, *, as_int: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if as_int:
        return values.astype("Int64")
    return values.astype("float64")


def normalize_text(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def read_fact_sheet(sheet_name: str, source_period: str) -> pd.DataFrame:
    df = pd.read_excel(SOURCE_XLSX_PATH, sheet_name=sheet_name, engine="openpyxl")
    df = df.rename(columns=FACT_COLUMNS_RU_TO_EN)
    missing = sorted(set(FACT_COLUMNS_RU_TO_EN.values()) - set(df.columns))
    if missing:
        raise ValueError(f"Missing expected columns in {sheet_name}: {missing}")

    df = df[list(FACT_COLUMNS_RU_TO_EN.values())].copy()
    df["accounting_date"] = normalize_datetime(df["accounting_date"]).dt.date
    df["open_ts"] = normalize_datetime(df["open_ts"])
    df["close_ts"] = normalize_datetime(df["close_ts"])
    df["check_id"] = normalize_numeric(df["check_id"], as_int=True)
    df["open_hour"] = normalize_numeric(df["open_hour"], as_int=True)
    df["close_hour"] = normalize_numeric(df["close_hour"], as_int=True)
    df["qty"] = normalize_numeric(df["qty"])
    df["guest_count"] = normalize_numeric(df["guest_count"], as_int=True)
    df["amount_discount_rub"] = normalize_numeric(df["amount_discount_rub"])

    for col in ["dish_name", "dish_category", "order_type"]:
        df[col] = df[col].map(normalize_text)

    df = df.dropna(subset=["accounting_date", "check_id"])
    df["source_system"] = "pos_excel_simulated_db"
    df["source_file"] = SOURCE_XLSX_PATH.name
    df["source_sheet"] = sheet_name
    df["source_period"] = source_period
    df["source_row_number"] = df.index + 2  # Excel row number, header is row 1.
    return df


def read_plan_sheet() -> pd.DataFrame:
    # Row 1 contains group headers, row 2 contains the actual column names.
    df = pd.read_excel(SOURCE_XLSX_PATH, sheet_name=PLAN_SHEET, skiprows=1, engine="openpyxl")
    keep_and_rename = {
        "Учетный день": "plan_date",
        "План на день": "plan_total_amount",
        "Ресторан": "plan_restaurant_amount",
        "БАНКЕТ НАШ": "plan_banquet_own_amount",
        "Банкет C&B": "plan_banquet_cb_amount",
        "Агрегатор": "plan_aggregator_amount",
        "Самовывоз": "plan_pickup_amount",
        "Доставка": "plan_delivery_amount",
        "Ресторан.1": "plan_avg_check_restaurant",
        "Доставка.1": "plan_avg_check_delivery",
        "Ресторан.2": "plan_avg_guest_restaurant",
        "Доставка.2": "plan_avg_guest_delivery",
        "Банкет": "plan_avg_guest_banquet",
    }
    df = df[list(keep_and_rename.keys())].rename(columns=keep_and_rename)
    df["plan_date"] = normalize_datetime(df["plan_date"]).dt.date
    numeric_cols = [col for col in df.columns if col != "plan_date"]
    for col in numeric_cols:
        df[col] = normalize_numeric(df[col])
    df = df.dropna(subset=["plan_date"])
    df["source_file"] = SOURCE_XLSX_PATH.name
    df["source_sheet"] = PLAN_SHEET
    df["loaded_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return df
