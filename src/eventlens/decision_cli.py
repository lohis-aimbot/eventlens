"""Offline proposal schema, pinned protocol and synthetic context validation."""

import argparse
import json
from pathlib import Path

from .contracts import AgentContext, Contract
from .decision_protocol import DecisionReplayProtocol, ResearchPositionProposal, check_proposal
from .historical_context import HistoricalContextInput, build_historical_context


class ProposalInput(Contract):
    context: AgentContext
    proposal: ResearchPositionProposal


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("schema")
    commands.add_parser("protocol")
    validate = commands.add_parser("check")
    validate.add_argument("path", type=Path)
    replay = commands.add_parser("replay-context", help="Build an explicit offline replay context")
    replay.add_argument("path", type=Path)
    args = parser.parse_args()
    if args.command == "schema":
        print(json.dumps(ResearchPositionProposal.model_json_schema(), indent=2))
    elif args.command == "protocol":
        protocol = DecisionReplayProtocol()
        print(
            json.dumps(
                {"protocol_id": protocol.protocol_id, "rules": protocol.model_dump(mode="json")},
                indent=2,
            )
        )
    elif args.command == "replay-context":
        inputs = HistoricalContextInput.model_validate_json(args.path.read_bytes())
        context = build_historical_context(inputs)
        print(
            json.dumps(
                {
                    "model_input_id": context.model_input_id,
                    **context.model_dump(mode="json"),
                },
                indent=2,
            )
        )
    else:
        payload = ProposalInput.model_validate_json(args.path.read_bytes())
        print(check_proposal(payload.proposal, payload.context).model_dump_json(indent=2))


if __name__ == "__main__":
    main()
