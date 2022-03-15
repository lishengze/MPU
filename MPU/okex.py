import asyncio
import json
import aiohttp
import sys
import hashlib
import base64
import hmac
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
import time
import zlib
from settings import REDIS_CONFIG


class MarketData_OKEX:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "OKEX"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config,
                                     exchange_topic="OKEX", debug_mode=debug_mode)

        self.__key = {"key": "MTU0ODIyOTI1ODY5MjAwMDAwMDAwOTk=",
                      "secret": "ZbExGIXMenjCI1DrhLMfpDKKwtbReEGicuqp4omNpXE="}

        self.__ws_url = "wss://real.okex.com:8443/ws/v3"
        self.__rest_depth_url = "https://www.okex.com"

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

        self.__symbol_list = {
            "BTC-USDT": "BTC_USDT",
            "ETH-USDT": "ETH_USDT",
            "LTC-USDT": "LTC_USDT",
            "OKB-USDT": "OKB_USDT",
            "OKB-BTC": "OKB_BTC",
            "OKB-ETH": "OKB_ETH",
            "ETC-USDT": "ETC_USDT",
            "BCH-USDT": "BCH_USDT",
            "EOS-USDT": "EOS_USDT",
            "XRP-USDT": "XRP_USDT",
            "TRX-USDT": "TRX_USDT",
            "BSV-USDT": "BSV_USDT",
            "DASH-USDT": "DASH_USDT",
            "QTUM-USDT": "QTUM_USDT"
        }
        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)
        self.task_dispatcher()

        while True:
            time.sleep(10)

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    def task_dispatcher(self):
        asyncio.run_coroutine_threadsafe(self.depth_poller(), self.rest_loop)

    async def ws_listener(self):
        # XDAEX Websocket Session
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        symbols = self.__symbol_list
                        print(f'{datetime.utcnow()}{symbols}')
                        depth_snaps = dict()
                        await ws.send_json({"op": "subscribe", "args": [f'spot/trade:{k}' for k, v in symbols.items()]})
                        await ws.send_json({"op": "subscribe", "args": [f'spot/depth_l2_tbt:{k}' for k, v in symbols.items()]})
                        # await ws.send_json({"op": "subscribe", "args": [f'futures/depth_l2_tbt:BTC-USD-200626']})
                        # await ws.send_json({"op": "subscribe", "args": [f'futures/trade:BTC-USD-200626']})

                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())
                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break
                            if ws_msg.type == aiohttp.WSMsgType.BINARY:
                                decompress = zlib.decompressobj(-zlib.MAX_WBITS)
                                inflated = decompress.decompress(ws_msg.data)
                                inflated += decompress.flush()
                                msg = json.loads(inflated)
                                # print(msg)
                                if "table" in msg:
                                    if msg["table"] == "spot/depth_l2_tbt":
                                        if "data" in msg:
                                            for data in msg["data"]:
                                                depth_update = {"ASK": {}, "BID": {}}
                                                for info in data["asks"]:
                                                    depth_update["ASK"][float(info[0])] = float(info[1])

                                                for info in data["bids"]:
                                                    depth_update["BID"][float(info[0])] = float(info[1])
                                                symbol = symbols[data["instrument_id"]]
                                                if symbol not in depth_snaps:
                                                    depth_snaps[symbol] = True
                                                    self.__publisher.pub_depthx(symbol=symbol,
                                                                                depth_update=depth_update,
                                                                                is_snapshot=True)
                                                else:
                                                    self.__publisher.pub_depthx(symbol=symbol,
                                                                                depth_update=depth_update,
                                                                                is_snapshot=False)

                                    elif msg["table"] == "spot/trade":
                                        if "data" in msg:
                                            for data in msg["data"]:
                                                if data["side"] == "buy":
                                                    side = "Buy"
                                                else:
                                                    side = "Sell"
                                                symbol = symbols[data["instrument_id"]]
                                                self.__publisher.pub_tradex(symbol=symbol,
                                                                            direction=side,
                                                                            exg_time=self.__time_convert(data["timestamp"]),
                                                                            px_qty=(float(data["price"]), float(data["size"])))
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.DISCONNECTED())

            except Exception:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([f'{exc_type},{fname},{exc_tb.tb_lineno}'])))

                await asyncio.sleep(15)

    async def depth_poller(self):
        while True:
            async with aiohttp.ClientSession() as session:
                while True:
                    try:
                        for symbol in list(self.__symbol_list.keys()):
                            async with session.get(url=f"https://www.okex.com/api/spot/v3/instruments/{symbol}/book",
                                                   params={"size": 200}) as response:
                                if response.status == 200:
                                    msg = await response.json()
                                    # print(msg)

                                    if "buy" in msg:
                                        depth_update = {"ASK": {}, "BID": {}}
                                        for info in msg["asks"]:
                                            depth_update["ASK"][float(info[0])] = float(info[1])

                                        for info in msg["bids"]:
                                            depth_update["BID"][float(info[0])] = float(info[1])

                                        self.__publisher.pub_depthx(symbol=self.__symbol_list[symbol],
                                                                    depth_update=depth_update,
                                                                    is_snapshot=True)
                            await asyncio.sleep(1)

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
        rt_time = time_x.replace("T", " ").replace("Z", "000")
        return rt_time

if __name__ == '__main__':
    a = MarketData_OKEX(debug_mode=False)
