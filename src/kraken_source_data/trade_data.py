from pydantic import BaseModel
import json
from typing import Dict


class Trade(BaseModel):
    trade_id: str
    price: float
    volume: float
    timestamp: int

    def to_string(self) -> str:
        return json.dumps(self.model_dump())

    def to_dict(self) -> Dict[str, any]:
        return {
            "trade_id": self.trade_id,
            "price": self.price,
            "volume": self.volume,
            "timestamp": self.timestamp,
        }
