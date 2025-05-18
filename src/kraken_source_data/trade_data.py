import json
from typing import Dict
from pydantic import BaseModel


class Trade(BaseModel):
    trade_id: str
    symbol: str
    price: float
    qty: float
    side: str
    trade_timestamp: int

    def to_string(self) -> str:
        return json.dumps(self.model_dump())

    def to_dict(self) -> Dict[str, any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "price": self.price,
            "qty": self.qty,
            "side": self.side,
            "trade_timestamp": self.trade_timestamp,
        }
