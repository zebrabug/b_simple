import logging
from logging.handlers import TimedRotatingFileHandler
from binance_futures import BinanceFuturesMarket
import asyncio


class Strategy:
    def __init__(self, **kwargs):
        self._event_queue = asyncio.Queue(maxsize=-1)

    def run(self):
        ob = BinanceFuturesMarket(
            instr="btcusdt", ws_type="depth20@100ms", event_queue=self._event_queue
        )
        trade = BinanceFuturesMarket(
            instr="btcusdt", ws_type="aggTrade", event_queue=self._event_queue
        )
        ob.run()
        trade.run()

        asyncio.get_event_loop().create_task(self.read_event_queue())

    async def read_event_queue(self):
        while True:
            try:
                event = await self._event_queue.get()
                if event["event_type"] == "depth20@100ms":
                    logging.info(
                        f"best_prices {event['event_time']} {event['exchange_time']} {event['instrument']} "
                        + f"{event['data']['asks'][0]} {event['data']['asks'][1]} "
                        + f"{event['data']['bids'][0]} {event['data']['bids'][1]}"
                    )
                elif event["event_type"] == "aggTrade":
                    logging.info(
                        f"trades {event['event_time']} {event['exchange_time']} "
                        + f"{event['instrument']} {event['data']['price']} {event['data']['quantity']} "
                        + f"{1 if event['data']['is_buy'] else 0}"
                    )
                else:
                    None
            except Exception as e:
                logging.info(f"read queue exception: {e}")


def main():
    # init logger
    logger = logging.getLogger()
    logger.setLevel("INFO")
    handler = TimedRotatingFileHandler(r"./logs/log.txt", "midnight")
    logger.addHandler(handler)
    logger.info("start logging")

    loop = asyncio.get_event_loop()

    Strategy().run()

    loop.run_forever()


if __name__ == "__main__":
    main()
