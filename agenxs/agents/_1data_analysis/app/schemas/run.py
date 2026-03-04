from datetime import datetime
from uuid import UUID
from pydantic import BaseModel


class RunCreateResponse(BaseModel):
    run_id: UUID


class RunStatusResponse(BaseModel):
    run_id: UUID
    status: str
    original_filename: str | None = None
    file_type: str | None = None
    file_path: str | None = None
    state_path: str | None = None
    created_at: datetime
    updated_at: datetime