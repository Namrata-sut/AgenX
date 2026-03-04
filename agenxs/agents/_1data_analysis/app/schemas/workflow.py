from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class WorkflowResultResponse(BaseModel):
    run_id: str
    df_profile: Dict[str, Any] = {}
    quality_issues: List[Dict[str, Any]] = []
    cleaning_suggestions: Optional[Dict[str, Any]] = None
    approval_status: str = "pending"
    cleaned_path: Optional[str] = None
    analysis_summary: Dict[str, Any] = {}
    charts: List[Dict[str, str]] = []
    report_path: Optional[str] = None
    stop_reason: Optional[str] = None
    errors: List[str] = []