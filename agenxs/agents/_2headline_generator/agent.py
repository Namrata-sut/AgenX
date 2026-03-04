from __future__ import annotations
from agenxs.agents.base import BaseAgent
from agenxs.api.schemas import ChatRequest, AgentResponse
from agenxs.providers.llm_provider import generate_text


class HeadlineGeneratorAgent(BaseAgent):
    name = "headline_generator"

    def run(self, req: ChatRequest) -> AgentResponse:
        system = (
            "You are a marketing headline generator. "
            "Return exactly 10 headlines. Each headline must include a tone tag in brackets like [Bold] or [Friendly]. "
            "No extra commentary."
        )
        answer = generate_text(system_prompt=system, user_prompt=req.message, model=req.model)

        return AgentResponse(
            agent_name=self.name,
            answer=answer,
            sources=[],
            metadata={"format": "10_headlines_with_tone_tags"},
        )