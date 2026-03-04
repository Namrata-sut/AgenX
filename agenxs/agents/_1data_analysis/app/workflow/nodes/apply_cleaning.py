from __future__ import annotations

import os

from ..state import WorkflowState
from ..utils.io import load_dataframe, save_dataframe, ensure_dir
from ..utils.cleaning import apply_cleaning


def apply_cleaning_node(state: WorkflowState) -> WorkflowState:
    if state.approval_status != "approved":
        state.errors.append("Apply cleaning called without approval.")
        return state
    if not state.cleaning_config:
        state.errors.append("Missing cleaning_config.")
        return state
    if not state.file_path or not state.file_type:
        state.errors.append("Missing file_path/file_type.")
        return state

    try:
        df = load_dataframe(state.file_path, state.file_type, sheet_name=state.sheet_name)
        cleaned = apply_cleaning(df, state.cleaning_config)

        out_dir = os.path.join("artifacts", state.run_id)
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "cleaned.parquet")
        save_dataframe(cleaned, out_path)
        state.cleaned_path = out_path
    except Exception as e:
        state.errors.append(f"Apply cleaning failed: {e}")
    return state