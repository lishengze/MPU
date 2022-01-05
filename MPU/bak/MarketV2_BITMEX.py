import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_BITMEX:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG
            
        exchange = "BITMEX"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://www.bitmex.com/realtime"

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

    @staticmethod
    def contract_combinator() -> dict:
        year_code = datetime.utcnow().year % 100
        month_codes = ["H", "M", "U", "Z"]
        symbols = {"XBT", "BCH", "EOS", "ETH", "LTC", "XRP"}

        contract_code = dict()

        for symbol in symbols:
            if symbol != "XBT":
                for month_code in month_codes:
                    contract_code[f"{symbol}{month_code}{year_code}"] = f"{symbol}_BTC@{year_code}{month_code}"
                    contract_code[f"{symbol}{month_code}{year_code + 1}"] = f"{symbol}_BTC@{year_code + 1}{month_code}"
            else:
                for month_code in month_codes:
                    contract_code[f"{symbol}{month_code}{year_code}"] = f"BTC_USD@{year_code}{month_code}"
                    contract_code[f"{symbol}{month_code}{year_code + 1}"] = f"BTC_USD@{year_code + 1}{month_code}"

        return contract_code

    async def ws_listener(self):
        # BITMEX Websocket Session
        symbol_ref = {"XBTUSD": "BTC_USD@P",
                      "ETHUSD": "ETH_USD@P"}

        symbol_ref.update(self.contract_combinator())

        orderbook_dict = dict()  # {symbol: {price_id: price}}

        for symbol in symbol_ref.keys():
            orderbook_dict.setdefault(symbol, {"Buy": {}, "Sell": {}})

        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        for symbol in symbol_ref.keys():
                            await ws.send_json(
                                {"op": "subscribe", "args": [f"orderBookL2:{symbol}", f"trade:{symbol}"]})

                        self.__publisher.logger(level=self.__publisher.info, event=MDEvent.CONNECTED())

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = json.loads(ws_msg.data)
                            # print(msg)

                            if "table" in msg:
                                if msg["table"] in ["orderBookL2", "trade"]:
                                    if msg["table"] == "orderBookL2":
                                        depth_update = {"ASK": {}, "BID": {}}
                                        symbol = None
                                        if msg["action"] == "update":
                                            for data in msg["data"]:
                                                if data["id"] in orderbook_dict[data["symbol"]][data["side"]]:
                                                    px = orderbook_dict[data["symbol"]][data["side"]][data["id"]]
                                                    if data["side"] == "Sell":
                                                        depth_update["ASK"][px] = data["size"]
                                                    elif data["side"] == "Buy":
                                                        depth_update["BID"][px] = data["size"]
                                                symbol = data["symbol"]

                                            if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                                self.__publisher.pub_depthx(symbol=symbol_ref[symbol],
                                                                            depth_update=depth_update,
                                                                            is_snapshot=False)

                                        elif msg["action"] == "insert":
                                            for data in msg["data"]:
                                                symbol = data["symbol"]

                                                if data["side"] == "Sell":
                                                    depth_update["ASK"][data["price"]] = data["size"]
                                                elif data["side"] == "Buy":
                                                    depth_update["BID"][data["price"]] = data["size"]
                                                orderbook_dict[data["symbol"]][data["side"]][data["id"]] = data["price"]

                                            if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                                self.__publisher.pub_depthx(symbol=symbol_ref[symbol],
                                                                            depth_update=depth_update,
                                                                            is_snapshot=False)

                                        elif msg["action"] == "delete":
                                            for data in msg["data"]:
                                                if data["id"] in orderbook_dict[data["symbol"]][data["side"]]:
                                                    px = orderbook_dict[data["symbol"]][data["side"]].pop(data["id"])
                                                    if data["side"] == "Sell":
                                                        depth_update["ASK"][px] = 0
                                                    elif data["side"] == "Buy":
                                                        depth_update["BID"][px] = 0
                                                symbol = data["symbol"]

                                            if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                                self.__publisher.pub_depthx(symbol=symbol_ref[symbol],
                                                                            depth_update=depth_update,
                                                                            is_snapshot=False)

                                        elif msg["action"] == "partial" and msg["data"]:
                                            orderbook_dict.pop(msg["data"][0]["symbol"])
                                            orderbook_dict.setdefault(msg["data"][0]["symbol"],
                                                                      {"Buy": {}, "Sell": {}})
                                            for data in msg["data"]:
                                                if data["side"] == "Sell":
                                                    depth_update["ASK"][data["price"]] = data["size"]
                                                elif data["side"] == "Buy":
                                                    depth_update["BID"][data["price"]] = data["size"]
                                                orderbook_dict[data["symbol"]][data["side"]][data["id"]] = data["price"]
                                                symbol = data["symbol"]

                                            self.__publisher.pub_depthx(symbol=symbol_ref[symbol],
                                                                        depth_update=depth_update,
                                                                        is_snapshot=True)

                                    elif msg["table"] == "trade" and msg["action"] == "insert":
                                        for trade in msg["data"]:
                                            self.__publisher.pub_tradex(symbol=symbol_ref[trade["symbol"]],
                                                                        direction=trade["side"],
                                                                        exg_time=self.__time_convert(
                                                                            trade["timestamp"]),
                                                                        px_qty=(trade["price"], trade["size"]))

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
    a = MarketData_BITMEX(debug_mode=False)
