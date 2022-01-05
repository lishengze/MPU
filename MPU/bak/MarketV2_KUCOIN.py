import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_KUCOIN:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "KUCOIN"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__rest_depth_url = "https://openapi-v2.kucoin.com/api/v1/market/orderbook/level2_100"
        self.__ws_url = "wss://push-private.kucoin.com/endpoint"
        self.__connection_token = ""

        self.symbol_book = {"USE-ETH": "USE_ETH",
                            "USE-BTC": "USE_BTC",
                            "ETH-BTC": "ETH_BTC",
                            "BTC-USDT": "BTC_USDT",
                            "ETH-USDT": "ETH_USDT",
                            "LTC-USDT": "LTC_USDT",
                            "BTC-USDC": "BTC_USDT",
                            "ETH-USDC": "ETH_USDC"}

        self.rest_loop = asyncio.new_event_loop()
        self.ws_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.rest_loop)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())

        self.task_dispatcher()
        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.CONNECTED())

        while True:
            time.sleep(10)

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    def task_dispatcher(self):
        for s_symbol, symbol in self.symbol_book.items():
            asyncio.run_coroutine_threadsafe(self.depth_poller(s_symbol, symbol), self.rest_loop)

    async def depth_poller(self, s_symbol: str, symbol: str):
        symbol = symbol
        session = aiohttp.ClientSession()
        while True:
            try:
                # async with aiohttp.ClientSession() as session:
                while True:
                    async with session.get(url=self.__rest_depth_url, params={"symbol": s_symbol}) as response:
                        if response.status == 200:
                            msg = await response.json()

                            if "data" in msg:
                                depth_update = {"ASK": {}, "BID": {}}
                                for px, qty in msg["data"]["asks"]:
                                    depth_update["ASK"][float(px)] = float(qty)

                                for px, qty in msg["data"]["bids"]:
                                    depth_update["BID"][float(px)] = float(qty)

                                exg_time = self.__time_convert(msg["data"]["time"])

                                self.__publisher.pub_depthx(symbol=symbol,
                                                            depth_update=depth_update,
                                                            exg_time=exg_time,
                                                            is_snapshot=True)

            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                await asyncio.sleep(60)

            await asyncio.sleep(0.5)

    async def create_connection_token(self):
        # print("create_connection_token called")
        async with aiohttp.ClientSession() as session:
            async with session.post(url="https://openapi-v2.kucoin.com/api/v1/bullet-public") as response:
                # print(response.status)
                if response.status == 200:
                    msg = await response.json()

                    if "data" in msg:
                        return msg["data"]["token"]
                    else:
                        return None
                else:
                    return None

    async def ws_listener(self):
        # KUCOIN Websocket Session
        while True:
            try:
                connection_token = None
                async with aiohttp.ClientSession() as ws_session:
                    while connection_token is None:
                        connection_token = await self.create_connection_token()
                        # print(connection_token)
                        await asyncio.sleep(0.5)

                    async with ws_session.ws_connect(url=f"{self.__ws_url}?token={connection_token}&acceptUserMessage=true",
                                                     heartbeat=10, autoclose=True) as ws:
                        await ws.send_json({"id": datetime.now().timestamp(),
                                            "type": "subscribe",
                                            "topic": f"/market/match: {','.join(self.symbol_book.keys())}".replace(" ", ""),
                                            "privateChannel": False,
                                            "response": False})

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
                                symbol = self.symbol_book[datas["symbol"]]
                                if datas["side"] == "sell":
                                    side = "Sell"
                                else:
                                    side = "Buy"
                                self.__publisher.pub_tradex(symbol=symbol,
                                                            direction=side,
                                                            exg_time=self.__time_convert(int(float(datas["time"])/1000000)),
                                                            px_qty=(float(datas["price"]), float(datas["size"])))

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
    a = MarketData_KUCOIN(debug_mode=True)
