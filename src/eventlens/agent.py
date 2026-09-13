"""LLM owns interpretation, thesis reasoning and portfolio proposals."""

from typing import Protocol

from .contracts import AgentContext, AgentDecision


class TradingAgent(Protocol):
    async def decide(self, context: AgentContext) -> AgentDecision:
        """A provider adapter calls an existing LLM and returns validated structured output.

        No local model training. Source content is untrusted data. The adapter stamps
        actual start/completion times and provider/model/prompt versions; the LLM
        cannot invent those authoritative values. Failures propagate as no decision.
        No broker tools or order credentials are available to the agent.
        """
        ...
