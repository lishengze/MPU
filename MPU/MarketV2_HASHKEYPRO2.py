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

from heapq import heappush,heappop
from collections import OrderedDict

def toTreeMap(paramMap):
    "将paramMap转换为java中的treeMap形式.将map的keys变为heapq.创建有序字典."
    keys = paramMap.keys()
    heap = []
    for item in keys:
        heappush(heap, item)

    sort = []
    while heap:
        sort.append(heappop(heap))

    resMap = OrderedDict()
    for key in sort:
        resMap[key] = paramMap.get(key)

    return resMap

class MarketData_HASHKEYPRO2:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "HASHKEYPRO2"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config,
                                     exchange_topic="HASHKEYPRO2", debug_mode=debug_mode)

        self.__key = {"key": "8292a4e5e05e4778a974da2d4e6533be",
                      "secret": "24ddfe7c6a504911af91ecf6b0959c7a"}

        self.__ws_url = "wss://sandbox-pro.hashkey.com/api/websocket-api/v1/stream"
        self.__rest_depth_url = "wss://sandbox-pro.hashkey.com/api/websocket-api/v1/stream"

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.timespan_secs = int(datetime.now().timestamp() * 1000)

        self.ws_loop = asyncio.new_event_loop()
        self.rest_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)
        self.executor.submit(self.asyncio_initiator, self.rest_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())

        self.__symbol_list = ["ETH-BTC",
                              "EOS-ETH", "EOS-BTC",
                              "ETH-USDT", "BTC-USDT"]

        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)
        # self.task_dispatcher()

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
        # HASHKEYPRO Websocket Session
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        await ws.send_json({"type": "sub", "channel": {"topic":"depth_market_data","instrumentId": ""}})
                        await ws.send_json({"type": "sub", "channel": {"topic":"trade_rtn_all","instrumentId": ""}})
                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = json.loads(ws_msg.data)
                            print("1111122222,now timestamp is ",datetime.now().timestamp())
                            print(msg)

                            if "topic" in msg:
                                if msg["topic"] == "depth_market_data":
                                    data=msg["data"]
                                    depth_update = {"ASK": {}, "BID": {}}
                                    if "ask" in data:
                                        for ask in data["ask"]:
                                            symbol = self.__symbol_convert(ask["symbol"])
                                            depth_update["ASK"][float(ask["price"])] = float(ask["amount"])
                                    if "bid" in data:
                                        for bid in data["bid"]:
                                            symbol = self.__symbol_convert(bid["symbol"])
                                            depth_update["BID"][float(bid["price"])] = float(bid["amount"])

                                    self.__publisher.pub_depthx(symbol=symbol,
                                                                depth_update=depth_update,
                                                                is_snapshot=False)

                                elif msg["topic"] == "trade_rtn_all":
                                    data = msg["data"]
                                    for dat in data:
                                        symbol = self.__symbol_convert(dat["instrumentId"])
                                        if str(dat["direction"])=="1":
                                            side = "Sell"
                                        else:
                                            side = "Buy"
                                        self.__publisher.pub_tradex(symbol=symbol,
                                                                    direction=side,
                                                                    exg_time=self.__time_convert(dat["tradeTimestamp"]),
                                                                    px_qty=(float(dat["price"]), float(dat["volume"])))


                        await asyncio.sleep(0.25)
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.DISCONNECTED())

            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                await asyncio.sleep(15)

    def __symbol_convert(self, symbol: str) -> str:
        rt_symbol = symbol.replace("-", "_")
        return rt_symbol

    def __time_convert(self, time_x: str) -> str:
        rt_time = time_x.replace("/", "-")
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

    @staticmethod
    def XDAEX_ws(key: str, secret: str, timespan_secs: float, body: dict = None) -> dict:
        if body == None:
            body = dict()
        timestamp = int((datetime.now().timestamp() + timespan_secs) * 1000)
        sign_dic = {
            "x-access-key": key,
            "x-access-timestamp": str(timestamp),
            "x-access-version": "1"}

        if len(body) > 0:
            for k, value in body.items():
                sign_dic[k] = value
        sign_dic2 = toTreeMap(sign_dic)
        sign_msg = json.dumps(sign_dic2)  # .replace(" ", "")
        # sign_msg = json.dumps(sign_dic)
        # HMAC
        sign = hmac.new(secret.encode("utf-8"), sign_msg.encode("utf-8"), hashlib.sha256)
        signature = base64.b64encode(sign.digest()).decode("utf-8")
        return {
            "x-access-sign": signature,
            "x-access-key": key,
            "x-access-timestamp": str(timestamp),
            "x-access-version": "1"}




if __name__ == '__main__':
    a = MarketData_HASHKEYPRO2(debug_mode=False)
