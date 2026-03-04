from __future__ import annotations

from datetime import datetime
from sqlalchemy.orm import Session

from app.db import models
from app.db.models import Step


def create_run(db: Session, original_filename: str | None = None) -> models.Run:
    run = models.Run(original_filename=original_filename, status="created")
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def update_run_file(db: Session, run: models.Run, file_path: str, file_type: str) -> models.Run:
    run.file_path = file_path
    run.file_type = file_type
    run.updated_at = datetime.utcnow()
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def set_run_status(db: Session, run: models.Run, status: str, state_path: str | None = None) -> models.Run:
    run.status = status
    if state_path is not None:
        run.state_path = state_path
    run.updated_at = datetime.utcnow()
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def get_run(db: Session, run_id) -> models.Run | None:
    return db.query(models.Run).filter(models.Run.id == run_id).first()


def upsert_approval(db: Session, run_id, status: str, suggestions_json=None, approved_config_json=None) -> models.Approval:
    approval = db.query(models.Approval).filter(models.Approval.run_id == run_id).order_by(models.Approval.updated_at.desc()).first()
    if approval is None:
        approval = models.Approval(run_id=run_id)

    approval.status = status
    if suggestions_json is not None:
        approval.suggestions_json = suggestions_json
    if approved_config_json is not None:
        approval.approved_config_json = approved_config_json
    approval.updated_at = datetime.utcnow()

    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval

def create_step(db: Session, run_id, name: str) -> Step:
    step = Step(
        run_id=run_id,
        name=name,
        status="started",
        started_at=datetime.utcnow(),
    )
    db.add(step)
    db.commit()
    db.refresh(step)
    return step

def finish_step(db: Session, step_id: int, status: str, output_json=None, error_text=None):
    step = db.query(Step).filter(Step.id == step_id).first()
    if not step:
        return
    step.status = status
    step.output_json = output_json
    step.error_text = error_text
    step.ended_at = datetime.utcnow()
    db.commit()

def list_steps(db: Session, run_id: str):
    return (
        db.query(Step)
        .filter(Step.run_id == run_id)
        .order_by(Step.started_at.asc())
        .all()
    )

def step_to_dict(s: Step):
    return {
        "id": s.id,
        "name": s.name,
        "status": s.status,
        "started_at": s.started_at.isoformat() if s.started_at else None,
        "ended_at": s.ended_at.isoformat() if s.ended_at else None,
        "error_text": s.error_text,
        "output_json": s.output_json,
    }