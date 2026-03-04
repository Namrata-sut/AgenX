import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class Run(Base):
    __tablename__ = "runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(String, default="created")  # created|running|waiting_approval|completed|failed
    original_filename = Column(String, nullable=True)
    file_path = Column(Text, nullable=True)
    file_type = Column(String, nullable=True)

    state_path = Column(Text, nullable=True)  # artifacts/<run_id>/state.json
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    steps = relationship("Step", back_populates="run", cascade="all, delete-orphan")
    approvals = relationship("Approval", back_populates="run", cascade="all, delete-orphan")


class Step(Base):
    __tablename__ = "steps"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False)

    name = Column(String, nullable=False)   # ingest/profile/quality/...
    status = Column(String, default="queued")  # queued|running|completed|failed

    output_json = Column(JSON, nullable=True)
    error_text = Column(Text, nullable=True)

    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)

    run = relationship("Run", back_populates="steps")


class Approval(Base):
    __tablename__ = "approvals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False)

    status = Column(String, default="pending")  # pending|approved|rejected
    suggestions_json = Column(JSON, nullable=True)
    approved_config_json = Column(JSON, nullable=True)

    updated_at = Column(DateTime, default=datetime.utcnow)

    run = relationship("Run", back_populates="approvals")