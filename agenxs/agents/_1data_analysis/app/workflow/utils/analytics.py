from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import polars as pl


def detect_date_column(df: pl.DataFrame) -> Optional[str]:
    # heuristic: pick first Date/Datetime column
    for col, dtype in df.schema.items():
        if dtype == pl.Date or dtype == pl.Datetime:
            return col
    # also try strings that look like dates is more complex; skip v1
    return None


def detect_numeric_column(df: pl.DataFrame) -> Optional[str]:
    for col, dtype in df.schema.items():
        if dtype.is_numeric():
            return col
    return None


def basic_kpis(df: pl.DataFrame) -> Dict[str, Any]:
    numeric_cols = [c for c, t in df.schema.items() if t.is_numeric()]
    kpis: Dict[str, Any] = {
        "rows": df.height,
        "cols": df.width,
        "numeric_cols": numeric_cols,
    }

    # basic stats for up to 10 numeric cols
    for col in numeric_cols[:10]:
        try:
            stats = df.select(
                pl.col(col).mean().alias("mean"),
                pl.col(col).median().alias("median"),
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
            ).to_dicts()[0]
            kpis[f"stats_{col}"] = {k: (float(v) if v is not None else None) for k, v in stats.items()}
        except Exception:
            continue
    return kpis


def time_series_summary(df: pl.DataFrame, date_col: str, value_col: str) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    # monthly aggregation (safe default)
    ts = (
        df
        .with_columns(pl.col(date_col).dt.truncate("1mo").alias("_month"))
        .group_by("_month")
        .agg(pl.col(value_col).sum().alias("sum_value"), pl.count().alias("rows"))
        .sort("_month")
    )
    meta = {
        "date_col": date_col,
        "value_col": value_col,
        "granularity": "month",
    }
    return ts, meta