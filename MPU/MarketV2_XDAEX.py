import asyncio
import json
import aiohttp
import sys
import hashlib
import base64
import hmac
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_XDAEX:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "XDAEX"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config,
                                     exchange_topic="XDAEX", debug_mode=debug_mode)

        self.__key = {"key": "MTU0ODIyOTI1ODY5MjAwMDAwMDAwOTk=",
                      "secret": "ZbExGIXMenjCI1DrhLMfpDKKwtbReEGicuqp4omNpXE="}

        self.__ws_url = "wss://pro.hashkey.com/APITradeWS/v1/messages"
        self.__rest_depth_url = "https://pro.hashkey.com/APITrade"

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

        self.__symbol_list = ["ETH-BTC", "ETH-USDT",
                              "ZRX-ETH", "ZRX-BTC", 
                              "GNT-ETH", "GNT-BTC",
                              "VET-ETH", "VET-BTC", 
                              "LTC-ETH", "LTC-BTC", "LTC-USDT",
                              "EOS-ETH", "EOS-BTC", "EOS-USDT",
                              "STX-BTC", "STX-USDT",
                              "XRP-BTC", "XRP-USDT",
                              "BTC-USDT",
                              "ETH-USDC", "BTC-USDC",
                              "LTC-USDC", "VET-USDC",
                              "EOS-USDC", "GNT-USDC",
                              "ZRX-USDC", "XRP-USDC",
                              "USDC-USDT", "USDT-USDC"]

        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)
        self.task_dispatcher()

        while True:
            time.sleep(10)

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    def task_dispatcher(self):
        for symbol in self.__symbol_list:
            asyncio.run_coroutine_threadsafe(self.depth_poller(symbol), self.rest_loop)

    async def ws_listener(self):
        # XDAEX Websocket Session
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        await ws.send_json({"type": "subscribe", "channel": {"ticker": self.__symbol_list}})
                        await ws.send_json({"type": "subscribe", "channel": {"level2@50": self.__symbol_list}})
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
                                symbol = self.__symbol_convert(msg["instrumentID"])
                                if msg["type"] == "level2@50_update":
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in msg["sell"]:
                                        depth_update["ASK"][float(px)] = float(qty)

                                    for px, qty in msg["buy"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    self.__publisher.pub_depthx(symbol=symbol,
                                                                depth_update=depth_update,
                                                                is_snapshot=False)

                                elif msg["type"] == "trade_detail":
                                    if msg["priceSource"] == "buy":
                                        side = "Sell"
                                    else:
                                        side = "Buy"
                                    self.__publisher.pub_tradex(symbol=symbol,
                                                                direction=side,
                                                                exg_time=self.__time_convert(msg["tradeTimestamp"]),
                                                                px_qty=(float(msg["price"]), float(msg["volume"])))

                                elif msg["type"] == "level2@50_snapshot":
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in msg["sell"]:
                                        depth_update["ASK"][float(px)] = float(qty)

                                    for px, qty in msg["buy"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    self.__publisher.pub_depthx(symbol=symbol,
                                                                depth_update=depth_update,
                                                                is_snapshot=True)

                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.DISCONNECTED())

            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                await asyncio.sleep(15)

    async def depth_poller(self, symbol: str):
        path = f"/v1/marketData/getLevel2"
        while True:
            async with aiohttp.ClientSession() as session:
                while True:
                    try:
                        header = self.get_hs256(key=self.__key["key"],
                                                secret=self.__key["secret"],
                                                method="GET",
                                                path=path)

                        async with session.get(url=f"{self.__rest_depth_url}{path}",
                                               params={"instrumentID": symbol}, headers=header) as response:
                            if response.status == 200:
                                msg = await response.json()
                                # print(msg)

                                if "buy" in msg:
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in msg["sell"]:
                                        depth_update["ASK"][float(px)] = float(qty)

                                    for px, qty in msg["buy"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    self.__publisher.pub_depthx(symbol=self.__symbol_convert(symbol),
                                                                depth_update=depth_update,
                                                                is_snapshot=True)

                    except Exception:
                        err = sys.exc_info()
                        self.__publisher.logger(level=self.__publisher.critical,
                                                event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                        await asyncio.sleep(60)

                    await asyncio.sleep(3)  # Refresh DEPTHx every 3 secs

    def __symbol_convert(self, symbol: str) -> str:
        rt_symbol = symbol.replace("-", "_")
        return rt_symbol

    def __time_convert(self, time_x: str) -> str:
        rt_time = datetime.utcfromtimestamp(int(time_x)/1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        return rt_time

    @staticmethod
    def get_hs256(key: str, secret: str, method: str, path: str, body: str = "") -> dict:
        timestamp = int((datetime.now().timestamp()) * 1000)
        sign_msg = f"{timestamp}{method}{path}{body}".replace(" ", "")

        # HMAC
        sign = hmac.new(secret.encode("utf-8"), sign_msg.encode("utf-8"), hashlib.sha256)
        signature = base64.b64encode(sign.digest()).decode("utf-8")
        return {"API-SIGNATURE": signature,
                "API-KEY": key,
                "API-TIMESTAMP": str(timestamp),
                "AUTH-TYPE": "HMAC",
                "Content-Type": "application/json"}


if __name__ == '__main__':
    a = MarketData_XDAEX(debug_mode=False)
