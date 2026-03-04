from __future__ import annotations

import json
import re
from typing import Any, Dict

from ..state import WorkflowState, CleaningSuggestion
from ..utils.llm import suggest_cleaning_with_llm


def _extract_json(text: str) -> Dict[str, Any]:
    """
    Gemini sometimes wraps JSON in ```json ... ```.
    This strips fences and returns parsed dict.
    """
    t = text.strip()

    # remove ```json and ```
    t = re.sub(r"^```(?:json)?\s*", "", t)
    t = re.sub(r"\s*```$", "", t)

    # if model returns extra text, attempt to grab first JSON object
    if not t.startswith("{"):
        start = t.find("{")
        end = t.rfind("}")
        if start != -1 and end != -1 and end > start:
            t = t[start : end + 1]

    return json.loads(t)


def suggest_cleaning_node(state: WorkflowState) -> dict:
    try:
        raw = suggest_cleaning_with_llm(
            profile=state.df_profile,
            quality_issues=state.quality_issues,
        )  # string

        data = _extract_json(raw)  # dict
        suggestion = CleaningSuggestion.model_validate(data)  # model

        # RETURN DICT updates (LangGraph requirement)
        # Provide dict for cleaning_suggestions; Pydantic will coerce it into CleaningSuggestion
        return {
            "cleaning_suggestions": suggestion.model_dump(),
            "approval_status": "pending",
            "stop_reason": "Awaiting user approval.",
        }

    except Exception as e:
        return {
            "errors": state.errors + [f"Suggest cleaning failed: {e}"],
            "cleaning_suggestions": None,
            "approval_status": "pending",
            "stop_reason": "llm_failed",
        }