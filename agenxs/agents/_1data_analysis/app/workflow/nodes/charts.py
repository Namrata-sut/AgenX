from __future__ import annotations

import os
import polars as pl

from ..state import WorkflowState
from ..utils.io import ensure_dir
from ..utils.charts import save_line_chart


def charts_node(state: WorkflowState) -> WorkflowState:
    """
    Generates basic chart(s) when time_series preview exists.
    """
    try:
        charts = []
        ts_info = state.analysis_summary.get("time_series")
        if ts_info and "preview" in ts_info:
            preview = ts_info["preview"]
            if preview:
                ts_df = pl.DataFrame(preview)
                out_dir = os.path.join("artifacts", state.run_id, "charts")
                ensure_dir(out_dir)
                path = os.path.join(out_dir, "time_series_sum.png")
                save_line_chart(ts_df, x="_month", y="sum_value", out_path=path, title="Monthly Sum")
                charts.append({"name": "Monthly Sum", "path": path})
        state.charts = charts
    except Exception as e:
        state.errors.append(f"Charts failed: {e}")
    return state