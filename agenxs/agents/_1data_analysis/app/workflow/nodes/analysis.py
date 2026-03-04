from __future__ import annotations

from ..state import WorkflowState
from ..utils.io import load_dataframe
from ..utils.analytics import basic_kpis, detect_date_column, detect_numeric_column, time_series_summary


def analysis_node(state: WorkflowState) -> WorkflowState:
    try:
        if state.cleaned_path:
            df = load_dataframe(state.cleaned_path, "csv") if state.cleaned_path.endswith(".csv") else None
            # For parquet, use polars directly:
            import polars as pl
            if state.cleaned_path.endswith(".parquet"):
                df = pl.read_parquet(state.cleaned_path)
        else:
            df = load_dataframe(state.file_path, state.file_type, sheet_name=state.sheet_name)

        kpis = basic_kpis(df)

        # optional time series summary if we have date + numeric
        import polars as pl
        date_col = detect_date_column(df)
        value_col = detect_numeric_column(df)
        if date_col and value_col:
            ts, meta = time_series_summary(df, date_col, value_col)
            # store small preview in JSON
            kpis["time_series"] = {
                "meta": meta,
                "preview": ts.head(24).to_dicts(),
            }

        state.analysis_summary = kpis
        print(state)
    except Exception as e:
        state.errors.append(f"Analysis failed: {e}")
    return state