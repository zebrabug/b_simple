import websockets
import asyncio
import logging
from algo_types import EventTypes
import time
import json


class BinanceFuturesMarket:
    def __init__(self, **kwargs):
        self._instr = kwargs.get("instr", None)
        self._ws_type = kwargs.get("ws_type", "depth20@100ms")
        self._url = kwargs.get(
            "url",
            f"wss://fstream.binance.com/stream?streams={self._instr}@{self._ws_type}",
        )
        self._event_queue = kwargs.get("event_queue")

    def run(self):
        asyncio.get_event_loop().create_task(self.connect())

    async def connect(self):
        ws = await websockets.connect(self._url)
        logging.info(f"Running {self._ws_type} receive")
        asyncio.get_event_loop().create_task(self.receive(ws))

    async def receive(self, ws):
        async for msg in ws:
            try:
                d = json.loads(msg)
                event_time = int(time.time() * 1000)
                if self._ws_type == "depth20@100ms":
                    event = {
                        "instrument": self._instr,
                        "event_type": self._ws_type,
                        "exchange_time": d["data"]["E"],
                        "event_time": event_time,
                        "data": {"asks": d["data"]["a"][0], "bids": d["data"]["b"][0]},
                    }
                elif self._ws_type == "aggTrade":
                    event = {
                        "instrument": self._instr,
                        "event_type": self._ws_type,
                        "exchange_time": d["data"]["E"],
                        "event_time": event_time,
                        "data": {
                            "price": d["data"]["p"],
                            "quantity": d["data"]["q"],
                            "is_buy": d["data"]["m"],
                        },
                    }
                else:
                    None
                await self._event_queue.put(event)
            except websockets.ConnectionClosed:
                logging.info("ConnectionClosed bf market, run again")
                self.run()
            except Exception as e:
                logging.info(f"ws {self._ws_type} exception: {e}, msg {msg}")
        logging.info(f"Closing {self._ws_type} receive")
