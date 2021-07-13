import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_GDAX:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "GDAX"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://ws-feed.pro.coinbase.com"

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.ws_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())

        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

        while True:
            time.sleep(10)

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    async def ws_listener(self):
        # GDAX Websocket Session
        symbol_ref = {"ETH-BTC": "ETH_BTC",
                      "LTC-BTC": "LTC_BTC",
                      "ZRX-BTC": "ZRX_BTC",
                      "BCH-BTC": "BCH_BTC",
                      "XRP-BTC": "XRP_BTC",
                      "EOS-BTC": "EOS_BTC",
                      "XLM-BTC": "XLM_BTC",
                      "ETH-USD": "ETH_USD",
                      "BTC-USD": "BTC_USD",
                      "ZRX-USD": "ZRX_USD",
                      "LTC-USD": "LTC_USD",
                      "BCH-USD": "BCH_USD",
                      "XRP-USD": "XRP_USD",
                      "EOS-USD": "EOS_USD",
                      "XLM-USD": "XLM_USD",
                      "BTC-USDC": "BTC_USDC",
                      "ETH-USDC": "ETH_USDC",
                      "GNT-USDC": "GNT_USDC",
                      "ZEC-USDC": "ZEC_USDC",
                      "BAT-USDC": "BAT_USDC",
                      "DAI-USDC": "DAI_USDC"}

        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        await ws.send_json({"type": "subscribe", "product_ids": list(symbol_ref.keys()),
                                            "channels": ["level2", "matches"]})
                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = json.loads(ws_msg.data)
                            # print(msg)

                            if "type" in msg:
                                if msg["type"] in ["l2update", "match", "snapshot"]:
                                    symbol = symbol_ref[msg["product_id"]]
                                    if msg["type"] == "l2update":
                                        depth_update = {"ASK": {}, "BID": {}}
                                        for data in msg["changes"]:
                                            if data[0] == "sell":
                                                depth_update["ASK"][float(data[1])] = float(data[2])
                                            elif data[0] == "buy":
                                                depth_update["BID"][float(data[1])] = float(data[2])

                                        if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                            self.__publisher.pub_depthx(symbol=symbol,
                                                                        depth_update=depth_update,
                                                                        is_snapshot=False)

                                    elif msg["type"] == "match":
                                        if msg["side"] == "sell":
                                            side = "Buy"
                                        else:
                                            side = "Sell"
                                        self.__publisher.pub_tradex(symbol=symbol,
                                                                    direction=side,
                                                                    exg_time=self.__time_convert(msg["time"]),
                                                                    px_qty=(float(msg["price"]), float(msg["size"])))

                                    elif msg["type"] == "snapshot":
                                        depth_update = {"ASK": {}, "BID": {}}
                                        for data in msg["asks"]:
                                            depth_update["ASK"][float(data[0])] = float(data[1])

                                        for data in msg["bids"]:
                                            depth_update["BID"][float(data[0])] = float(data[1])

                                        self.__publisher.pub_depthx(symbol=symbol,
                                                                    depth_update=depth_update,
                                                                    is_snapshot=True)

                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.DISCONNECTED())

            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                await asyncio.sleep(20)

    async def __aenter__(self):
        await asyncio.gather(self.ws_listener())
        return self

    def __time_convert(self, time_x: str) -> str:
        rt_time = time_x.replace("T", " ").replace("Z", "")
        return rt_time


if __name__ == '__main__':
    a = MarketData_GDAX(debug_mode=False)
