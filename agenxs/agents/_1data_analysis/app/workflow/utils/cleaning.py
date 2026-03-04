from __future__ import annotations

from typing import Optional

import polars as pl

from ..state import CleaningSuggestion


def _cast_expr(col: str, target: str, date_format: Optional[str]) -> pl.Expr:
    if target == "int":
        return pl.col(col).cast(pl.Int64, strict=False)
    if target == "float":
        return pl.col(col).cast(pl.Float64, strict=False)
    if target == "str":
        return pl.col(col).cast(pl.Utf8, strict=False)
    if target == "bool":
        return pl.col(col).cast(pl.Boolean, strict=False)
    if target == "date":
        # parse strings to Date
        if date_format:
            return pl.col(col).str.strptime(pl.Date, format=date_format, strict=False)
        return pl.col(col).str.strptime(pl.Date, strict=False)
    if target == "datetime":
        if date_format:
            return pl.col(col).str.strptime(pl.Datetime, format=date_format, strict=False)
        return pl.col(col).str.strptime(pl.Datetime, strict=False)
    return pl.col(col)


def apply_cleaning(df: pl.DataFrame, cfg: CleaningSuggestion) -> pl.DataFrame:
    # rename
    if cfg.rename_columns:
        df = df.rename(cfg.rename_columns)

    # drop cols that exist
    drop_cols = [c for c in cfg.drop_columns if c in df.columns]
    if drop_cols:
        df = df.drop(drop_cols)

    # trim strings
    trim_cols = [c for c in cfg.trim_strings if c in df.columns]
    if trim_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8, strict=False).str.strip_chars().alias(c) for c in trim_cols])

    # casts
    if cfg.cast_columns:
        exprs = []
        for col, target in cfg.cast_columns.items():
            if col not in df.columns:
                continue
            fmt = cfg.date_formats.get(col)
            exprs.append(_cast_expr(col, target, fmt).alias(col))
        if exprs:
            df = df.with_columns(exprs)

    # fill missing
    if cfg.fill_missing:
        exprs = []
        for col, rule in cfg.fill_missing.items():
            if col not in df.columns:
                continue
            if rule.strategy == "mean":
                exprs.append(pl.col(col).fill_null(pl.col(col).mean()).alias(col))
            elif rule.strategy == "median":
                exprs.append(pl.col(col).fill_null(pl.col(col).median()).alias(col))
            elif rule.strategy == "mode":
                # mode might return multiple; take first
                exprs.append(pl.col(col).fill_null(pl.col(col).mode().first()).alias(col))
            elif rule.strategy == "constant":
                exprs.append(pl.col(col).fill_null(rule.value).alias(col))
        if exprs:
            df = df.with_columns(exprs)

    # dedupe
    if cfg.deduplicate and cfg.deduplicate.subset:
        subset = [c for c in cfg.deduplicate.subset if c in df.columns]
        if subset:
            keep = cfg.deduplicate.keep
            # polars unique keeps first/last
            df = df.unique(subset=subset, keep=keep)

    return df