from __future__ import annotations

from langgraph.graph import StateGraph, END
from sqlalchemy.orm import Session

from .state import WorkflowState
from .nodes.ingest import ingest_node
from .nodes.profile import profile_node
from .nodes.quality import quality_checks_node
from .nodes.suggest_cleaning import suggest_cleaning_node
from .nodes.approval_gate import approval_gate_node
from .nodes.apply_cleaning import apply_cleaning_node
from .nodes.analysis import analysis_node
from .nodes.charts import charts_node
from .nodes.report import report_node
from ..db import crud
from ..db.session import SessionLocal


def _wrap(name, fn):
    def inner(state: WorkflowState):
        db: Session = SessionLocal()
        step = None

        try:
            # -------------------------
            # 1) INSERT step row (start)
            # -------------------------
            step = crud.create_step(
                db=db,
                run_id=state.run_id,   # string UUID is fine if your model uses UUID type
                name=name,
            )

            print(f"\n=== NODE START: {name} ===")
            print(f"state.run_id={state.run_id} file_type={state.file_type} approval={state.approval_status}")

            # -------------------------
            # 2) run node
            # -------------------------
            out = fn(state)

            print(f"=== NODE END: {name} | returned {type(out)} ===")

            if not isinstance(out, dict):
                crud.finish_step(
                    db=db,
                    step_id=step.id,
                    status="failed",
                    error_text=f"Node '{name}' must return dict, got {type(out)}",
                    output_json=None,
                )
                raise TypeError(f"Node '{name}' must return dict, got {type(out)}")

            print("updates keys:", list(out.keys()))

            # -------------------------
            # 3) UPDATE step row (success)
            # store ONLY updates, not whole state
            # -------------------------
            crud.finish_step(
                db=db,
                step_id=step.id,
                status="completed",
                output_json=out,   # dict -> json
                error_text=None,
            )

            return out

        except Exception as e:
            # -------------------------
            # 4) UPDATE step row (failure)
            # -------------------------
            if step is not None:
                crud.finish_step(
                    db=db,
                    step_id=step.id,
                    status="failed",
                    output_json=None,
                    error_text=str(e),
                )
            raise

        finally:
            db.close()

    return inner

def _route_after_approval(state: WorkflowState) -> str:
    """
    Decide next node after approval_gate.
    """
    if state.approval_status == "approved":
        return "apply_cleaning"
    # if pending or rejected → stop (END)
    return END


def build_graph():
    g = StateGraph(WorkflowState)

    g.add_node("ingest", _wrap("ingest", ingest_node))
    g.add_node("profile", _wrap("profile", profile_node))
    g.add_node("quality", _wrap("quality", quality_checks_node))
    g.add_node("suggest_cleaning", _wrap("suggest_cleaning", suggest_cleaning_node))
    g.add_node("approval_gate", _wrap("approval_gate", approval_gate_node))
    g.add_node("apply_cleaning", _wrap("apply_cleaning", apply_cleaning_node))
    g.add_node("analysis", _wrap("analysis", analysis_node))
    g.add_node("generate_charts", _wrap("generate_charts", charts_node))
    g.add_node("report", _wrap("report", report_node))

    g.set_entry_point("ingest")

    g.add_edge("ingest", "profile")
    g.add_edge("profile", "quality")
    g.add_edge("quality", "suggest_cleaning")
    g.add_edge("suggest_cleaning", "approval_gate")

    g.add_conditional_edges("approval_gate", _route_after_approval)

    g.add_edge("apply_cleaning", "analysis")
    g.add_edge("analysis", "generate_charts")
    g.add_edge("generate_charts", "report")
    g.add_edge("report", END)

    return g.compile()