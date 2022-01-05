import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_JINNIU:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "JINNIU"
        self.__last_trade = dict()  # dict<Symbol: serialID>

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://wsapi.jinniu.pro/openapi/quote/ws/v1"
        self.__rest_depth_url = "https://api.jinniu.pro"

        self.__symbol_book = {"BTCUSDT": "BTC_USDT",

                              "ETHBTC": "ETH_BTC",
                              "ETHUSDT": "ETH_USDT",

                              "LTCBTC": "LTC_BTC",
                              "LTCUSDT": "LTC_USDT",

                              "EOSBTC": "EOS_BTC",
                              "EOSUSDT": "EOS_USDT",

                              "XRPBTC": "XRP_BTC",
                              "XRPUSDT": "XRP_USDT",

                              "BCHBTC": "BCH_BTC",
                              "BCHUSDT": "BCH_USDT",

                              "ETCBTC": "ETC_BTC",
                              "ETCUSDT": "ETC_USDT",

                              "TCUSDT": "TC_USDT",
                              }

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.ws_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())

        asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

        while True:
            time.sleep(10)

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    async def ws_listener(self):
        # JINNIU Websocket Session
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(url=self.__ws_url, heartbeat=10,
                                                     autoclose=True, headers={"User-Agent": "XCyclops"}) as ws:
                        self.__publisher.logger(level=self.__publisher.info, event=MDEvent.CONNECTED())
                        symbol_aggre = ",".join(self.__symbol_book.keys())
                        await ws.send_json({"event": "sub", "topic": "trade", "symbol": symbol_aggre})
                        await ws.send_json({"event": "sub", "topic": "depth", "symbol": symbol_aggre})

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = json.loads(ws_msg.data)

                            if "data" in msg:
                                datas = msg["data"]
                                symbol = self.__symbol_book.get(msg.get("symbol", None), None)
                                if msg.get("topic", None) == "depth":
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in datas[0]["a"]:
                                        depth_update["ASK"][float(px)] = float(qty)

                                    for px, qty in datas[0]["b"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                        self.__publisher.pub_depthx(symbol=symbol,
                                                                    depth_update=depth_update,
                                                                    is_snapshot=True)

                                elif msg.get("topic", None) == "trade":
                                    for trade in datas:
                                        serial_id = int(trade["v"])
                                        if serial_id > self.__last_trade.get(symbol, 0):
                                            self.__last_trade[symbol] = serial_id
                                            if trade["m"]:
                                                side = "Buy"
                                            else:
                                                side = "Sell"
                                            self.__publisher.pub_tradex(symbol=symbol,
                                                                        direction=side,
                                                                        exg_time=self.__time_convert(trade["t"]),
                                                                        px_qty=(float(trade["p"]), float(trade["q"])))

                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.DISCONNECTED())

            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                await asyncio.sleep(15)

    async def __aenter__(self):
        await asyncio.gather(self.ws_listener())
        return self

    def __time_convert(self, time_x: int) -> str:
        rt_time = datetime.utcfromtimestamp(time_x / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        return rt_time


if __name__ == '__main__':
    a = MarketData_JINNIU(debug_mode=False)
