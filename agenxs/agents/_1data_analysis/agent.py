from __future__ import annotations
import os
import pandas as pd

from agenxs.agents.base import BaseAgent
from agenxs.api.schemas import ChatRequest, AgentResponse, Source
from agenxs.providers.llm_provider import generate_text


class DataAnalysisAgent(BaseAgent):
    name = "data_analysis"

    def run(self, req: ChatRequest) -> AgentResponse:
        if not req.files:
            return AgentResponse(
                agent_name=self.name,
                answer="Please upload a CSV file to analyze.",
                sources=[],
                metadata={"error": "no_file"},
            )

        file_path = req.files[0]
        if not os.path.exists(file_path):
            return AgentResponse(
                agent_name=self.name,
                answer=f"File not found: {file_path}",
                sources=[],
                metadata={"error": "file_not_found"},
            )

        df = pd.read_csv(file_path)

        profile = {
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
            "column_names": list(df.columns),
            "dtypes": {c: str(df[c].dtype) for c in df.columns},
            "missing_pct": {c: float(df[c].isna().mean() * 100) for c in df.columns},
        }

        # Summarize top values for up to 5 object columns
        obj_cols = [c for c in df.columns if df[c].dtype == "object"][:5]
        top_vals = {}
        for c in obj_cols:
            top_vals[c] = df[c].fillna("NULL").value_counts().head(3).to_dict()

        # LLM explains, but stats come from pandas
        system = (
            "You are a data analysis assistant for non-technical users. "
            "Explain insights in simple language and suggest 3 next steps. "
            "Do NOT invent numbers; only use the provided statistics."
        )
        user = (
            f"User question: {req.message}\n\n"
            f"Dataset profile:\n{profile}\n\n"
            f"Top values (sample):\n{top_vals}\n"
        )
        explanation = generate_text(system_prompt=system, user_prompt=user, model=req.model)

        return AgentResponse(
            agent_name=self.name,
            answer=explanation,
            sources=[Source(id="uploaded_file", title=os.path.basename(file_path), snippet="CSV uploaded by user")],
            metadata={"profile": profile},
        )