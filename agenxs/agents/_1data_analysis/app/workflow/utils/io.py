from __future__ import annotations

import os
from typing import Optional, Tuple

import polars as pl


def detect_file_type(file_path: str) -> str:
    ext = os.path.splitext(file_path.lower())[1]
    if ext == ".csv":
        return "csv"
    if ext in [".xlsx", ".xlsm", ".xls"]:
        return "xlsx"
    raise ValueError(f"Unsupported file type: {ext}")


def load_dataframe(file_path: str, file_type: str, sheet_name: Optional[str] = None) -> pl.DataFrame:
    if file_type == "csv":
        # infer schema length to handle wide data better
        return pl.read_csv(file_path, infer_schema_length=1000, ignore_errors=True)
    if file_type == "xlsx":
        # Polars has read_excel (depends on engine); fallback to pandas if needed.
        try:
            return pl.read_excel(file_path, sheet_name=sheet_name)
        except Exception:
            import pandas as pd
            dfp = pd.read_excel(file_path, sheet_name=sheet_name)
            return pl.from_pandas(dfp)
    raise ValueError(f"Unsupported file_type: {file_type}")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_dataframe(df: pl.DataFrame, out_path: str) -> str:
    ext = os.path.splitext(out_path.lower())[1]
    if ext == ".csv":
        df.write_csv(out_path)
        return out_path
    if ext == ".parquet":
        df.write_parquet(out_path)
        return out_path
    raise ValueError("Supported outputs: .csv, .parquet")


def sample_dataframe(df: pl.DataFrame, n: int = 50) -> Tuple[pl.DataFrame, int]:
    total = df.height
    if total <= n:
        return df, total
    return df.head(n), total