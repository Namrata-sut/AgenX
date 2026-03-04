from __future__ import annotations

from ..state import WorkflowState


def approval_gate_node(state: WorkflowState) -> dict:
    """
    PAUSE node.

    - approved → continue
    - rejected → stop
    - pending → stop (waiting approval)
    """

    # ✅ approved → continue to next node
    if state.approval_status == "approved":
        config = state.cleaning_config or state.cleaning_suggestions
        return {
            "cleaning_config": config,
            "stop_reason": None,
        }

    # ❌ rejected → stop
    if state.approval_status == "rejected":
        return {
            "stop_reason": "Cleaning rejected by user.",
        }

    # ⏸ pending → stop here
    return {
        "stop_reason": "Awaiting user approval.",
    }