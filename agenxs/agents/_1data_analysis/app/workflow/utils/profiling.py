from __future__ import annotations

from typing import Any, Dict

import polars as pl


def profile_dataframe(df: pl.DataFrame) -> Dict[str, Any]:
    schema = {k: str(v) for k, v in df.schema.items()}
    row_count = df.height
    col_count = df.width

    null_counts = df.null_count().to_dict(as_series=False)
    # null_count() returns {col: [count]}
    null_counts = {k: int(v[0]) for k, v in null_counts.items()}
    null_pct = {k: (null_counts[k] / row_count if row_count else 0.0) for k in df.columns}

    # cardinality (unique counts) can be expensive; do for smaller columns only or sample
    unique_counts: Dict[str, int] = {}
    for col in df.columns:
        try:
            unique_counts[col] = int(df.select(pl.col(col).n_unique()).item())
        except Exception:
            unique_counts[col] = -1

    dtypes_summary = {}
    for col, dtype in df.schema.items():
        dtypes_summary.setdefault(str(dtype), 0)
        dtypes_summary[str(dtype)] += 1

    return {
        "row_count": row_count,
        "col_count": col_count,
        "schema": schema,
        "null_counts": null_counts,
        "null_pct": null_pct,
        "unique_counts": unique_counts,
        "dtype_summary": dtypes_summary,
        "columns": df.columns,
    }