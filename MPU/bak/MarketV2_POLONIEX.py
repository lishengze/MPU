import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_POLONIEX:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "POLONIEX"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://api2.poloniex.com"

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
        # POLONIEX Websocket Session
        symbol_ref = {148: "ETH_BTC",
                      225: "ETH_USDC",
                      149: "ETH_USDT",

                      224: "BTC_USDC",
                      121: "BTC_USDT",

                      50: "LTC_BTC",
                      123: "LTC_USDT",
                      244: "LTC_USDC",

                      226: "USDT_USDC",

                      201: "EOS_BTC",
                      202: "EOS_ETH",
                      203: "EOS_USDT",

                      192: "ZRX_BTC",
                      193: "ZRX_ETH",
                      220: "ZRX_USDT",

                      185: "GNT_BTC",
                      186: "GNT_ETH",
                      217: "GNT_USDT",

                      89: "XLM_BTC",
                      242: "XLM_USDC",
                      125: "XLM_USDT",

                      117: "XRP_BTC",
                      127: "XRP_USDT",
                      240: "XRP_USDC",

                      221: "QTUM_BTC",
                      223: "QTUM_USDT",
                      222: "QTUM_ETH",

                      253: "ATOM_BTC",
                      254: "ATOM_USDC",
                      255: "ATOM_USDT",}

        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        for ref_no in list(symbol_ref.keys()):
                            await ws.send_json({"command": "subscribe", "channel": ref_no})

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

                            if len(msg) > 2:
                                if msg[0] in symbol_ref:
                                    symbol = symbol_ref[msg[0]]
                                    data_set = msg[2]
                                    depth_update = {"ASK": {}, "BID": {}}

                                    for data in data_set:
                                        if data[0] == "o":
                                            if data[1] == 0:
                                                depth_update["ASK"][float(data[2])] = float(data[3])

                                            else:
                                                depth_update["BID"][float(data[2])] = float(data[3])

                                        elif data[0] == "t":
                                            if data[2] == 0:
                                                side = "Sell"
                                            else:
                                                side = "Buy"
                                            self.__publisher.pub_tradex(symbol=symbol,
                                                                        direction=side,
                                                                        exg_time=self.__time_convert(data[5]),
                                                                        px_qty=(float(data[3]), float(data[4])))

                                        elif data[0] == "i":
                                            depth_snapshot = {"ASK": {}, "BID": {}}
                                            for px, qty in data[1]["orderBook"][0].items():
                                                depth_snapshot["ASK"][float(px)] = float(qty)

                                            for px, qty in data[1]["orderBook"][1].items():
                                                depth_snapshot["BID"][float(px)] = float(qty)

                                            self.__publisher.pub_depthx(symbol=symbol,
                                                                        depth_update=depth_snapshot,
                                                                        is_snapshot=True)

                                    if len(depth_update["ASK"]) + len(depth_update["BID"]) > 0:
                                        self.__publisher.pub_depthx(symbol=symbol,
                                                                    depth_update=depth_update,
                                                                    is_snapshot=False)

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
        rt_time = datetime.utcfromtimestamp(time_x).strftime("%Y-%m-%d %H:%M:%S.%f")
        return rt_time


if __name__ == '__main__':
    a = MarketData_POLONIEX(debug_mode=False)
