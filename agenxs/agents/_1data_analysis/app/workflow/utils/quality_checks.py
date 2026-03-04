from __future__ import annotations

from typing import Any, Dict, List

import polars as pl


def find_duplicates(df: pl.DataFrame, max_preview: int = 5) -> Dict[str, Any]:
    if df.height == 0:
        return {"duplicate_rows": 0, "preview": []}
    dup_count = int(df.is_duplicated().sum())
    preview = []
    if dup_count > 0:
        preview_df = df.filter(df.is_duplicated()).head(max_preview)
        preview = preview_df.to_dicts()
    return {"duplicate_rows": dup_count, "preview": preview}


def detect_outliers_iqr(df: pl.DataFrame, max_cols: int = 15) -> List[Dict[str, Any]]:
    """
    Simple IQR outlier counts for numeric columns.
    """
    issues: List[Dict[str, Any]] = []
    numeric_cols = [c for c, t in df.schema.items() if t.is_numeric()]
    numeric_cols = numeric_cols[:max_cols]

    for col in numeric_cols:
        try:
            q1 = df.select(pl.col(col).quantile(0.25)).item()
            q3 = df.select(pl.col(col).quantile(0.75)).item()
            if q1 is None or q3 is None:
                continue
            iqr = q3 - q1
            if iqr == 0:
                continue
            lo = q1 - 1.5 * iqr
            hi = q3 + 1.5 * iqr
            out_count = int(df.select(((pl.col(col) < lo) | (pl.col(col) > hi)).sum()).item())
            if out_count > 0:
                issues.append({"type": "outliers_iqr", "column": col, "count": out_count, "bounds": {"lo": lo, "hi": hi}})
        except Exception:
            continue
    return issues


def basic_quality_checks(df: pl.DataFrame, profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []

    # missing threshold
    for col, pct in profile.get("null_pct", {}).items():
        if pct >= 0.3:
            issues.append({"type": "high_missingness", "column": col, "missing_pct": float(pct)})

    # duplicates
    dup = find_duplicates(df)
    if dup["duplicate_rows"] > 0:
        issues.append({"type": "duplicate_rows", "count": dup["duplicate_rows"], "preview": dup["preview"]})

    # outliers
    issues.extend(detect_outliers_iqr(df))

    return issues