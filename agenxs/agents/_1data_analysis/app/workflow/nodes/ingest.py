from __future__ import annotations

from ..state import WorkflowState
from ..utils.io import detect_file_type


def ingest_node(state: WorkflowState) -> dict:
    # validation
    if not state.file_path:
        return {
            "errors": state.errors + ["Missing file_path."],
            "stop_reason": "ingest_failed",
        }

    try:
        ftype = detect_file_type(state.file_path)  # "csv" or "xlsx"
        return {"file_type": ftype}
    except Exception as e:
        return {
            "errors": state.errors + [f"Ingest failed: {e}"],
            "stop_reason": "ingest_failed",
        }