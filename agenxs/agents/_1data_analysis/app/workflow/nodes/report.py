from __future__ import annotations

import os

from ..state import WorkflowState
from ..utils.reporting import build_markdown_report, save_report_md
from ..utils.io import ensure_dir


def report_node(state: WorkflowState) -> WorkflowState:
    try:
        out_dir = os.path.join("artifacts", state.run_id)
        ensure_dir(out_dir)
        report_md = build_markdown_report(
            run_id=state.run_id,
            profile=state.df_profile,
            quality_issues=state.quality_issues,
            cleaning_config=state.cleaning_config.model_dump() if state.cleaning_config else None,
            analysis_summary=state.analysis_summary,
            charts=state.charts,
        )
        out_path = os.path.join(out_dir, "report.md")
        save_report_md(report_md, out_path)
        state.report_path = out_path
    except Exception as e:
        state.errors.append(f"Report failed: {e}")
    return state