import json
from typing import List, Dict
from websocket import create_connection
from dateutil.parser import isoparse
from src.kraken_source_data.trade_data import Trade
from src.logger import logger


class KrakenSourceSync:
    def __init__(self, url: str, symbols: List[str]):
        self.__url = url
        self.__symbols = symbols

    def subscribe(self):
        subscribe_payload = {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": self.__symbols, "snapshot": False},
        }

        self.ws_conn = create_connection(self.__url)
        logger.info("Successfuly Create Websocket API URL Connection!")

        # Send message to the connection
        self.ws_conn.send(json.dumps(subscribe_payload))
        logger.info("Successfuly Send Message to the Connection! ")

        # Receive the opening message for before the actual data
        _ = self.ws_conn.recv()
        _ = self.ws_conn.recv()

    def get_trades(self):
        msg = self.ws_conn.recv()
        msg_json = json.loads(msg)
        if msg_json.get("channel", {}) == "trade":
            trades = self.__get_trades(msg_json["data"])
            return trades

    def __get_trades(self, data: List[Dict]) -> List[Trade]:
        """Process messages gathered from the Kraken WebSocket Connection"""
        trades = [
            Trade(
                trade_id=str(trade["trade_id"]),
                symbol=str(trade["symbol"]),
                price=float(trade["price"]),
                qty=float(trade["qty"]),
                side=str(trade["side"]),
                trade_timestamp=int(isoparse(trade["timestamp"]).timestamp()),
            )
            for trade in data
        ]
        return trades
