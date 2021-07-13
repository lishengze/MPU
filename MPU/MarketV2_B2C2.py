import asyncio
import json
import aiohttp
import sys
import zlib
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG


class MarketData_B2C2:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:            
            redis_config = REDIS_CONFIG

        exchange = "B2C2"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://socket.b2c2.net/quotes"
        self.__token = "bf67093781746c841305d22897829ab3f3b3f87a"

        self.__symbol_book = {
                "BTCUSD.SPOT": "BTC_USD",
                "BTCUST.SPOT": "BTC_USDT",
                "ETHUSD.SPOT": "ETH_USD",
                "ETHUST.SPOT": "ETH_USDT",
                "USTUSD.SPOT": "USDT_USD"
        }

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
            time.sleep(3)

    def asyncio_initiator(self, loop):
        # Thread Worker for aio_initiator Method
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    async def ws_listener(self):
        # B2C2 Websocket Session
        while True:
            try:
                header = {'Authorization': 'Token %s' % self.__token}
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as ws_session:
                    #async with ws_session.ws_connect('wss://localhost:5000/v1/portal/ws') as ws:
                    async with ws_session.ws_connect(url=self.__ws_url, headers=header, heartbeat=10, autoclose=False) as ws:
                        response = await ws.receive()
                        #print(response)
                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())

                        data = {
                                  "event": "subscribe",
                                  "instrument": "BTCUSD.SPOT",
                                  "levels": [1,5],
                                  "tag": ""
                               }
                        await ws.send_json(data)
                        response = await ws.receive()
                        print(f"R {response}")

                        data1 = {
                                  "event": "subscribe",
                                  "instrument": "BTCUST.SPOT",
                                  "levels": [1,5],
                                  "tag": ""
                               }
                        await ws.send_json(data1)
                        response1 = await ws.receive()
                        print(f"R1 {response1}")


                        data2 = {
                                  "event": "subscribe",
                                  "instrument": "ETHUSD.SPOT",
                                  "levels": [50,250],
                                  "tag": ""
                               }
                        await ws.send_json(data2)
                        response2 = await ws.receive()
                        print(f"R2 {response2}")

                        data3 = {
                                  "event": "subscribe",
                                  "instrument": "ETHUST.SPOT",
                                  "levels": [50,250],
                                  "tag": ""
                               }
                        await ws.send_json(data3)
                        response2 = await ws.receive()
                        print(f"R3 {response2}")

                        data4 = {
                                  "event": "subscribe",
                                  "instrument": "USTUSD.SPOT",
                                  "levels": [100000,500000],
                                  "tag": ""
                               }
                        await ws.send_json(data4)
                        response2 = await ws.receive()
                        print(f"R4 {response2}")


                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = json.loads(ws_msg.data, parse_float=float)
                            if msg['event'] == "subscribe":
                                continue
                            depth_update = {"ASK": {}, "BID": {}}
                            for level in msg["levels"]["buy"]:
                                depth_update["ASK"][float(level["price"])] = float(level["quantity"])
                            for level in msg["levels"]["sell"]:
                                depth_update["BID"][float(level["price"])] = float(level["quantity"])
                            if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                self.__publisher.pub_depthx(symbol=self.__symbol_book[msg["instrument"]],
                                                            depth_update=depth_update, is_snapshot=True)

            except Exception:
                err = sys.exc_info()
                print(err)
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

            await asyncio.sleep(15)

    async def __aenter__(self):
        await asyncio.gather(self.ws_listener())
        return self

    def __time_convert(self, time_x: float) -> str:
        rt_time = datetime.utcfromtimestamp(time_x / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        return rt_time


if __name__ == '__main__':
    a = MarketData_B2C2(debug_mode=False)
