from uuid import UUID
from pydantic import BaseModel
from typing import Any, Dict, Optional


class ApprovalGetResponse(BaseModel):
    run_id: UUID
    status: str
    suggestions: Optional[Dict[str, Any]] = None
    approved_config: Optional[Dict[str, Any]] = None


class ApprovalSubmitRequest(BaseModel):
    status: str  # approved|rejected
    approved_config: Optional[Dict[str, Any]] = None