import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_HITBTC:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "HITBTC"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://api.hitbtc.com/api/2/ws"

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
        # HITBTC Websocket Session
        symbol_ref = {"ETHBTC": "ETH_BTC",
                      "ETHUSD": "ETH_USDT",
                      "ETHUSDC": "ETH_USDC",

                      "ZRXETH": "ZRX_ETH",
                      "ZRXBTC": "ZRX_BTC",
                      "ZRXUSD": "ZRX_USDT",

                      "GNTETH": "GNT_ETH",
                      "GNTBTC": "GNT_BTC",

                      "VETETH": "VET_ETH",
                      "VETBTC": "VET_BTC",

                      "LTCETH": "LTC_ETH",
                      "LTCBTC": "LTC_BTC",
                      "LTCUSD": "LTC_USDT",

                      "XRPBTC": "XRP_BTC",
                      "XRPUSD": "XRP_USDT",

                      "XLMBTC": "XLM_BTC",
                      "XLMETH": "XLM_ETH",
                      "XLMUSD": "XLM_USDT",

                      "EOSETH": "EOS_ETH",
                      "EOSBTC": "EOS_BTC",
                      "EOSUSD": "EOS_USDT",

                      "QTUMETH": "QTUM_ETH",
                      "QTUMBTC": "QTUM_BTC",
                      "QTUMUSD": "QTUM_USDT",

                      "BTCUSD": "BTC_USDT",
                      "BTCUSDC": "BTC_USDC",

                      "USDUSDC": "USDT_USDC",
                      }

        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        for s_symbol, symbol in symbol_ref.items():
                            await ws.send_json({"method": "subscribeTrades", "params": {"symbol": s_symbol},
                                                "id": datetime.now().timestamp()})
                            await ws.send_json({"method": "subscribeOrderbook", "params": {"symbol": s_symbol},
                                                "id": datetime.now().timestamp()})

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

                            if "method" in msg:
                                symbol = symbol_ref[msg["params"]["symbol"]]
                                if msg["method"] == "updateOrderbook":
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for data in msg["params"]["ask"]:
                                        depth_update["ASK"][float(data["price"])] = float(data["size"])

                                    for data in msg["params"]["bid"]:
                                        depth_update["BID"][float(data["price"])] = float(data["size"])

                                    if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                        self.__publisher.pub_depthx(symbol=symbol,
                                                                    depth_update=depth_update,
                                                                    is_snapshot=False)

                                elif msg["method"] == "updateTrades":
                                    for trade in msg["params"]["data"]:
                                        if trade["side"] == "sell":
                                            side = "Buy"
                                        else:
                                            side = "Sell"
                                        self.__publisher.pub_tradex(symbol=symbol,
                                                                    direction=side,
                                                                    exg_time=self.__time_convert(trade["timestamp"]),
                                                                    px_qty=(
                                                                        float(trade["price"]),
                                                                        float(trade["quantity"])))

                                elif msg["method"] == "snapshotOrderbook":
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for data in msg["params"]["ask"]:
                                        depth_update["ASK"][float(data["price"])] = float(data["size"])

                                    for data in msg["params"]["bid"]:
                                        depth_update["BID"][float(data["price"])] = float(data["size"])

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

    async def __aenter__(self):
        await asyncio.gather(self.ws_listener())
        return self

    def __time_convert(self, time_x: str) -> str:
        rt_time = time_x.replace("T", " ").replace("Z", "")
        return rt_time


if __name__ == '__main__':
    a = MarketData_HITBTC(debug_mode=False)
