import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_BINANCE:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG
            
        exchange = "BINANCE"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://stream.binance.com:9443"
        self.__rest_depth_url = "https://api.binance.com"

        self.__symbol_book = {"BTCUSDT": "BTC_USDT",
                              "BTCUSDC": "BTC_USDC",

                              "ETHBTC": "ETH_BTC",
                              "ETHUSDT": "ETH_USDT",
                              "ETHUSDC": "ETH_USDC",

                              "ZRXETH": "ZRX_ETH",
                              "ZRXBTC": "ZRX_BTC",
                              "ZRXUSDT": "ZRX_USDT",

                              "GNTETH": "GNT_ETH",
                              "GNTBTC": "GNT_BTC",

                              "VETETH": "VET_ETH",
                              "VETBTC": "VET_BTC",
                              "VETUSDT": "VET_USDT",

                              "LTCETH": "LTC_ETH",
                              "LTCBTC": "LTC_BTC",
                              "LTCUSDT": "LTC_USDT",
                              "LTCUSDC": "LTC_USDC",

                              "EOSETH": "EOS_ETH",
                              "EOSBTC": "EOS_BTC",
                              "EOSUSDC": "EOS_USDC",
                              "EOSUSDT": "EOS_USDT",

                              "XRPBTC": "XRP_BTC",
                              "XRPETH": "XRP_ETH",
                              "XRPUSDT": "XRP_USDT",
                              "XRPUSDC": "XRP_USDC",

                              "XLMBTC": "XLM_BTC",
                              "XLMETH": "XLM_ETH",
                              "XLMUSDT": "XLM_USDT",
                              "XLMUSDC": "XLM_USDC",

                              "BNBBTC": "BNB_BTC",
                              "BNBETH": "BNB_ETH",
                              "BNBUSDT": "BNB_USDT",
                              "BNBUSDC": "BNB_USDC",

                              "QTUMBTC": "QTUM_BTC",
                              "QTUMETH": "QTUM_ETH",
                              "QTUMUSDT": "QTUM_USDT",

                              "USDCUSDT": "USDC_USDT",

                              "ATOMBTC": "ATOM_BTC",
                              "ATOMUSDT": "ATOM_USDT",
                              "ATOMUSDC": "ATOM_USDC",

                              "CMTBTC": "CMT_BTC",
                              "CMTETH": "CMT_ETH",

                              "STXBTC": "STX_BTC",
                              "STXUSDT": "STX_USDT",
                              }

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.ws_loop = asyncio.new_event_loop()
        self.rest_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)
        self.executor.submit(self.asyncio_initiator, self.rest_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())

        asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)
        self.task_dispatcher()

        while True:
            time.sleep(10)

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    def task_dispatcher(self):
        for s_symbol, symbol in self.__symbol_book.items():
            asyncio.run_coroutine_threadsafe(self.depth_poller(s_symbol, symbol), self.rest_loop)

    async def ws_listener(self):
        # BINANCE Websocket Session
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    sub_string = "/".join(
                        [f"{symbol.lower()}@trade/{symbol.lower()}@depth@100ms" for symbol in self.__symbol_book.keys()])
                    async with ws_session.ws_connect(url=f"{self.__ws_url}/stream?streams={sub_string}",
                                                     heartbeat=10, autoclose=True) as ws:
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

                            if "data" in msg:
                                datas = msg["data"]
                                symbol = self.__symbol_book[datas["s"]]
                                if datas["e"] == "depthUpdate":
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in datas["a"]:
                                        depth_update["ASK"][float(px)] = float(qty)

                                    for px, qty in datas["b"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                        self.__publisher.pub_depthx(symbol=symbol,
                                                                    depth_update=depth_update,
                                                                    is_snapshot=False,
                                                                    raise_exception=False,
                                                                    exg_time=self.__time_convert(datas["E"]))

                                elif datas["e"] == "trade":
                                    if datas["m"]:
                                        side = "Sell"
                                    else:
                                        side = "Buy"
                                    self.__publisher.pub_tradex(symbol=symbol,
                                                                direction=side,
                                                                exg_time=self.__time_convert(datas["E"]),
                                                                px_qty=(float(datas["p"]), float(datas["q"])))

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

    async def depth_poller(self, s_symbol: str, symbol: str):
        symbol = symbol
        path = "/api/v1/depth"
        while True:
            async with aiohttp.ClientSession() as session:
                while True:
                    try:
                        async with session.get(url=f"{self.__rest_depth_url}{path}",
                                               params={"symbol": s_symbol, "limit": 500}) as response:
                            if response.status == 200:
                                msg = await response.json()
                                # print(msg)

                                if "lastUpdateId" in msg:
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in msg["asks"]:
                                        depth_update["ASK"][float(px)] = float(qty)

                                    for px, qty in msg["bids"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    self.__publisher.pub_depthx(symbol=symbol,
                                                                depth_update=depth_update,
                                                                is_snapshot=True,
                                                                raise_exception=False)


                    except Exception:
                        err = sys.exc_info()
                        self.__publisher.logger(level=self.__publisher.critical,
                                                event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                        await asyncio.sleep(60)

                    await asyncio.sleep(12)  # Refresh DEPTHx every 12 secs(if depth<200)

    def __time_convert(self, time_x: int) -> str:
        rt_time = datetime.utcfromtimestamp(time_x / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        return rt_time


if __name__ == '__main__':
    a = MarketData_BINANCE(debug_mode=True)
