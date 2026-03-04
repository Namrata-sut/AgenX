from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class CleaningFillRule(BaseModel):
    strategy: Literal["mean", "median", "mode", "constant"]
    value: Optional[Any] = None  # required only for constant


class DeduplicateRule(BaseModel):
    subset: List[str] = Field(default_factory=list)
    keep: Literal["first", "last"] = "first"


class CleaningSuggestion(BaseModel):
    rename_columns: Dict[str, str] = Field(default_factory=dict)
    drop_columns: List[str] = Field(default_factory=list)
    cast_columns: Dict[str, Literal["int", "float", "str", "date", "datetime", "bool"]] = Field(default_factory=dict)
    trim_strings: List[str] = Field(default_factory=list)
    fill_missing: Dict[str, CleaningFillRule] = Field(default_factory=dict)
    deduplicate: Optional[DeduplicateRule] = None
    date_formats: Dict[str, str] = Field(default_factory=dict)


class WorkflowState(BaseModel):
    # identifiers
    run_id: str

    # input
    file_path: Optional[str] = None
    file_type: Optional[Literal["csv", "xlsx"]] = None
    sheet_name: Optional[str] = None

    # derived
    dataset_path: Optional[str] = None

    # results
    df_profile: Dict[str, Any] = Field(default_factory=dict)
    quality_issues: List[Dict[str, Any]] = Field(default_factory=list)

    # IMPORTANT: keep it as model
    cleaning_suggestions: Optional[CleaningSuggestion] = None

    approval_status: Literal["not_required", "pending", "approved", "rejected"] = "pending"
    cleaning_config: Optional[CleaningSuggestion] = None

    cleaned_path: Optional[str] = None
    analysis_summary: Dict[str, Any] = Field(default_factory=dict)
    charts: List[Dict[str, str]] = Field(default_factory=list)
    report_path: Optional[str] = None

    # control
    stop_reason: Optional[str] = None
    errors: List[str] = Field(default_factory=list)