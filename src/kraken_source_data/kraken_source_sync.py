import json
from typing import List
import logging
from websocket import create_connection



logger = logging.getLogger("btc-real-time-data-streaming")
logger.setLevel(logging.INFO)  # Set the logging level

# Add a console handler (prints logs to stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Ensure INFO messages are logged
logger.addHandler(console_handler)

class KrakenSourceSync:
        
    def __init__(
        self, url: str, symbols: List[str], shutdown_timeout: float = 10
    ):
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
        logger.info(self.ws_conn.recv())
        