"""Execution extension contract. This release contains no live broker client."""

from typing import Literal, Protocol

from pydantic import Field

from .core import Model


class OrderIntent(Model):
    client_order_id: str
    instrument: Literal["EURUSD"] = "EURUSD"
    side: Literal["buy", "sell"]
    units: int = Field(gt=0)


class BrokerAdapter(Protocol):
    def submit(self, order: OrderIntent) -> str: ...
    def positions(self) -> list[dict[str, object]]: ...


class CTraderAdapter:
    def __init__(self) -> None:
        raise NotImplementedError("cTrader integration is not implemented; use offline replay")
