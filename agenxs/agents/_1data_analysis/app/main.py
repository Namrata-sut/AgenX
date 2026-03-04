from __future__ import annotations

import json
import os
import pdb
from uuid import UUID

from fastapi import Depends, FastAPI, File, UploadFile, HTTPException
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import Base, engine, get_db
from app.db import crud
from app.schemas.run import RunCreateResponse, RunStatusResponse
from app.schemas.approval import ApprovalGetResponse, ApprovalSubmitRequest
from app.schemas.workflow import WorkflowResultResponse

# Import your workflow state model + graph builder
from app.workflow.state import WorkflowState, CleaningSuggestion
from app.workflow.graph import build_graph
from app.workflow.utils.io import detect_file_type, ensure_dir


app = FastAPI(title=settings.APP_NAME)

# Create tables (for dev). In prod, use Alembic migrations.
Base.metadata.create_all(bind=engine)

GRAPH = build_graph()

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("langgraph").setLevel(logging.DEBUG)

def _state_path(run_id: str) -> str:
    return os.path.join(settings.ARTIFACT_DIR, run_id, "state.json")


def _save_state(state: WorkflowState) -> str:
    path = _state_path(str(state.run_id))
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(state.model_dump_json(indent=2))
    return path


def _load_state(path: str) -> WorkflowState:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return WorkflowState.model_validate(data)


@app.post("/runs", response_model=RunCreateResponse)
def create_run(db: Session = Depends(get_db)):
    run = crud.create_run(db)
    return RunCreateResponse(run_id=run.id)


@app.post("/runs/{run_id}/upload")
def upload_file(run_id: UUID, file: UploadFile = File(...), db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in [".csv", ".xlsx", ".xls", ".xlsm"]:
        raise HTTPException(status_code=400, detail="Only CSV/XLSX supported")

    run_dir = os.path.join(settings.ARTIFACT_DIR, str(run_id), "uploads")
    ensure_dir(run_dir)

    out_path = os.path.join(run_dir, file.filename)
    content = file.file.read()
    with open(out_path, "wb") as f:
        f.write(content)

    ftype = detect_file_type(out_path)
    crud.update_run_file(db, run, out_path, ftype)
    return {"ok": True, "file_path": out_path, "file_type": ftype}


@app.post("/runs/{run_id}/start")
def start_workflow(run_id: UUID, db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run or not run.file_path or not run.file_type:
        raise HTTPException(status_code=400, detail="Upload file first")

    crud.set_run_status(db, run, "running")

    state = WorkflowState(
        run_id=str(run.id),
        file_path=run.file_path,
        file_type=run.file_type,
    )

    # ✅ IMPORTANT: invoke expects dict
    out_state_dict = GRAPH.invoke(state.model_dump())

    # ✅ convert back to Pydantic if you want
    out_state = WorkflowState.model_validate(out_state_dict)

    path = _save_state(out_state)

    status = (
        "waiting_approval"
        if out_state.approval_status == "pending"
        else ("failed" if out_state.errors else "completed")
    )
    crud.set_run_status(db, run, status, state_path=path)

    if out_state.cleaning_suggestions:
        crud.upsert_approval(
            db,
            run.id,
            "pending",
            suggestions_json=out_state.cleaning_suggestions.model_dump(),
        )

    return {"ok": True, "status": status, "state_path": path, "stop_reason": out_state.stop_reason}


@app.get("/runs/{run_id}", response_model=RunStatusResponse)
def get_run_status(run_id: UUID, db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return RunStatusResponse(
        run_id=run.id,
        status=run.status,
        original_filename=run.original_filename,
        file_type=run.file_type,
        file_path=run.file_path,
        state_path=run.state_path,
        created_at=run.created_at,
        updated_at=run.updated_at,
    )


@app.get("/runs/{run_id}/result", response_model=WorkflowResultResponse)
def get_workflow_result(run_id: UUID, db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run or not run.state_path:
        raise HTTPException(status_code=404, detail="Result not found")
    state = _load_state(run.state_path)
    return WorkflowResultResponse(
        run_id=state.run_id,
        df_profile=state.df_profile,
        quality_issues=state.quality_issues,
        cleaning_suggestions=state.cleaning_suggestions.model_dump() if state.cleaning_suggestions else None,
        approval_status=state.approval_status,
        cleaned_path=state.cleaned_path,
        analysis_summary=state.analysis_summary,
        charts=state.charts,
        report_path=state.report_path,
        stop_reason=state.stop_reason,
        errors=state.errors,
    )


@app.get("/runs/{run_id}/approval", response_model=ApprovalGetResponse)
def get_approval(run_id: UUID, db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    approval = run.approvals[-1] if run.approvals else None
    if not approval:
        raise HTTPException(status_code=404, detail="No approval found")
    return ApprovalGetResponse(
        run_id=run.id,
        status=approval.status,
        suggestions=approval.suggestions_json,
        approved_config=approval.approved_config_json,
    )


@app.post("/runs/{run_id}/approve")
def submit_approval(run_id: UUID, body: ApprovalSubmitRequest, db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run or not run.state_path:
        raise HTTPException(status_code=404, detail="Run/state not found")

    state: WorkflowState = _load_state(run.state_path)

    if body.status not in ["approved", "rejected"]:
        raise HTTPException(status_code=400, detail="status must be approved or rejected")

    # -------------------------
    # REJECT
    # -------------------------
    if body.status == "rejected":
        state.approval_status = "rejected"
        state.stop_reason = "Cleaning rejected by user."

        _save_state(state)
        crud.upsert_approval(db, run.id, "rejected")
        crud.set_run_status(db, run, "completed")
        return {"ok": True, "message": "Rejected"}

    # -------------------------
    # APPROVE
    # -------------------------
    approved_cfg = body.approved_config or (
        state.cleaning_suggestions.model_dump() if state.cleaning_suggestions else {}
    )

    try:
        cfg_obj = CleaningSuggestion.model_validate(approved_cfg)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid approved_config: {e}")

    state.approval_status = "approved"
    state.cleaning_config = cfg_obj

    # Important: clear old stop_reason (it still says awaiting approval)
    state.stop_reason = None

    crud.set_run_status(db, run, "running")

    # IMPORTANT: invoke expects dict
    out_state_dict = GRAPH.invoke(state.model_dump())

    # convert back to Pydantic
    out_state = WorkflowState.model_validate(out_state_dict)

    path = _save_state(out_state)
    status = "failed" if out_state.errors else "completed"

    crud.set_run_status(db, run, status, state_path=path)
    crud.upsert_approval(db, run.id, "approved", approved_config_json=cfg_obj.model_dump())

    return {"ok": True, "status": status, "state_path": path}

@app.get("/runs/{run_id}/steps")
def get_steps(run_id: UUID, db: Session = Depends(get_db)):
    run = crud.get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    steps = crud.list_steps(db, run_id=str(run_id))
    return {"run_id": str(run_id), "steps": [crud.step_to_dict(s) for s in steps]}