from __future__ import annotations

from ..state import WorkflowState
from ..utils.io import load_dataframe, sample_dataframe
from ..utils.quality_checks import basic_quality_checks
from ..utils.profiling import profile_dataframe


def quality_checks_node(state: WorkflowState) -> dict:
    if not state.file_path or not state.file_type:
        return {
            "errors": state.errors + ["Quality: missing file_path or file_type."],
            "stop_reason": "quality_failed",
        }

    try:
        df = load_dataframe(state.file_path, state.file_type, sheet_name=state.sheet_name)
        sample_df, _ = sample_dataframe(df, n=2000)

        # Use profile from state if available
        prof = state.df_profile or profile_dataframe(sample_df)

        issues = basic_quality_checks(sample_df, prof)

        return {"quality_issues": issues}

    except Exception as e:
        return {
            "errors": state.errors + [f"Quality checks failed: {e}"],
            "stop_reason": "quality_failed",
        }