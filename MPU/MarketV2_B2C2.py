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
import hmac
import threading

g_redis_config_file_name = "./redis_config.json"

def get_redis_config():    
    json_file = open(g_redis_config_file_name,'r')
    json_dict = json.load(json_file)
    print("\n******* redis_config *******")
    print(json_dict)
    time.sleep(3)

    return json_dict

'''
"BTCUSD.SPOT": "BTC_USD",
"BTCUST.SPOT": "BTC_USDT",
"ETHUSD.SPOT": "ETH_USD",
"ETHUST.SPOT": "ETH_USDT",
"USTUSD.SPOT": "USDT_USD"
'''
class MarketData_B2C2:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:            
            redis_config = get_redis_config()

        self.__exchange_name = "B2C2"

        self.__publisher = Publisher(exchange=self.__exchange_name, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://socket.uat.b2c2.net/quotes"
        self.__token = "eabe0596c453786c0ecee81978140fad58daf881"

        self.__symbol_book = {
                "BTCUSD.SPOT" : ["BTC_USD", 1, 100],
                "BTCUST.SPOT" : ["BTC_USDT", 1, 100], 
                "ETHUSD.SPOT" : ["ETH_USD", 5, 500],
                "ETHUST.SPOT" : ["ETH_USDT", 5, 500],
                "USTUSD.SPOT" : ["USDT_USD", 100000, 500000]
        }
        self.__lever_1 = 1
        self.__lever_2 = 2

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.ws_loop = asyncio.new_event_loop()
        

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())
        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

        self._ping_secs = 10
        self._timer = threading.Timer(self._ping_secs, self.on_timer)
        self._timer.start()

        self._publish_count_dict = {
            "depth":{},
            "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }

        for symbol in self.__symbol_book:
            self._publish_count_dict["depth"][ self.__symbol_book[symbol][0]] = 0

        while True:
            time.sleep(3)

    def print_publish_info(self):
        self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
        for item in self._publish_count_dict:
            if item == "depth" or item == "trade":
                for symbol in self._publish_count_dict[item]:
                    print("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                    self._publish_count_dict[item][symbol] = 0

        self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def subscribe_symbol(self):
        for symbol in self.__symbol_book:
            data = {
                    "event": "subscribe",
                    "instrument": symbol,
                    "levels": [self.__symbol_book[symbol][1], self.__symbol_book[symbol][2]],
                    "tag": ""
                }
            # await ws.send_json(data)
            # response = await ws.receive()  
            # print(f"\nsub %s \n{response}" % (symbol))              

            self._ws.send_json(data)
            response = self._ws.receive()                
            # print(f"\nsub %s \n{response}" % (symbol))      

    def on_timer(self):
        self.subscribe_symbol()   

        self.print_publish_info()

        self._timer = threading.Timer(self._ping_secs, self.on_timer)
        self._timer.start()



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
                        self._ws = ws

                        response = await ws.receive()
                        print(response)
                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())

                        symbol_list = list(self.__symbol_book.keys())

                        for symbol in self.__symbol_book:
                            data = {
                                    "event": "subscribe",
                                    "instrument": symbol,
                                    "levels": [self.__symbol_book[symbol][1], self.__symbol_book[symbol][2]],
                                    "tag": ""
                                }
                            await ws.send_json(data)
                            response = await ws.receive()  
                            print(f"\nsub %s \n{response}" % (symbol))         

                        # data = {
                        #           "event": "subscribe",
                        #           "instrument": "BTCUST.SPOT",
                        #           "levels": [1,100],
                        #           "tag": ""
                        #        }
                        # await ws.send_json(data)
                        # response = await ws.receive()
                        # print(f"\nR \n{response}")

                        # data1 = {
                        #           "event": "subscribe",
                        #           "instrument": "BTCUSD.SPOT",
                        #           "levels": [1, 100],
                        #           "tag": ""
                        #        }
                        # await ws.send_json(data1)
                        # response1 = await ws.receive()
                        # print(f"\nR1 \n{response1}")


                        # data2 = {
                        #           "event": "subscribe",
                        #           "instrument": "ETHUSD.SPOT",
                        #           "levels": [5,500],
                        #           "tag": ""
                        #        }
                        # await ws.send_json(data2)
                        # response2 = await ws.receive()
                        # print(f"\nR2 \n{response2}")

                        # data3 = {
                        #           "event": "subscribe",
                        #           "instrument": "ETHUST.SPOT",
                        #           "levels": [5,500],
                        #           "tag": ""
                        #        }
                        # await ws.send_json(data3)
                        # response2 = await ws.receive()
                        # print(f"\nR3 \n{response2}")

                        # data4 = {
                        #           "event": "subscribe",
                        #           "instrument": "USTUSD.SPOT",
                        #           "levels": [100000,500000],
                        #           "tag": ""
                        #        }
                        # await ws.send_json(data4)
                        # response2 = await ws.receive()
                        # print(f"\nR4 \n{response2}")


                        # data5 = {
                        #           "event": "subscribe",
                        #           "instrument": "ETHBTC.SPOT",
                        #           "levels": [5,500],
                        #           "tag": ""
                        #        }
                        # await ws.send_json(data4)
                        # response2 = await ws.receive()
                        # print(f"\nR5 \n{response2}")


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

                            sys_symbol = self.__symbol_book[msg["instrument"]][0]
                            # print("\n%s.%s PUBLISH: %s" % (self.__exchange_name, sys_symbol, str(depth_update)))

                            if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                if sys_symbol in self._publish_count_dict["depth"]:
                                    self._publish_count_dict["depth"][sys_symbol] += 1
                                self.__publisher.pub_depthx(symbol=sys_symbol,
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
