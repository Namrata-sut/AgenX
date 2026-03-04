from __future__ import annotations

from ..state import WorkflowState
from ..utils.io import load_dataframe, sample_dataframe
from ..utils.profiling import profile_dataframe


def profile_node(state: WorkflowState) -> dict:
    if not state.file_path or not state.file_type:
        return {
            "errors": state.errors + ["Profile: missing file_path or file_type."],
            "stop_reason": "profile_failed",
        }

    try:
        df = load_dataframe(state.file_path, state.file_type, sheet_name=state.sheet_name)
        sample_df, total = sample_dataframe(df, n=200)

        prof = profile_dataframe(sample_df)
        prof.update(
            {
                "sampled": True,
                "sample_rows": int(sample_df.height),
                "total_rows_detected": int(total),
            }
        )

        return {"df_profile": prof}
    except Exception as e:
        return {
            "errors": state.errors + [f"Profile failed: {e}"],
            "stop_reason": "profile_failed",
        }