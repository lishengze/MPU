import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
import websocket
import json
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
Trade InstrumentID
BTC-USDT、ETH-USDT、BTC-USD、ETH-USD、USDT-USD、ETH-BTC
'''
class MarketData_B2C2(object):
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        if redis_config is None:            
            redis_config = get_redis_config()

        self.__exchange_name = "B2C2"

        self.__publisher = Publisher(exchange=self.__exchange_name, redis_config=redis_config, debug_mode=debug_mode)

        self._ws_url = "wss://socket.uat.b2c2.net/quotes"
        self.__token = "eabe0596c453786c0ecee81978140fad58daf881"

        self.__symbol_book = {
                "BTCUSD.SPOT" : ["BTC_USD", 1, 100],
                "BTCUST.SPOT" : ["BTC_USDT", 1, 100], 
                "ETHUSD.SPOT" : ["ETH_USD", 5, 500],
                "ETHUST.SPOT" : ["ETH_USDT", 5, 500],
                "USTUSD.SPOT" : ["USDT_USD", 100000, 500000]
        }

        self._is_connnect = False
        self._ws = None
        self._ping_secs = 10
        self._reconnect_secs = 5

        self._publish_count_dict = {
            "depth":{},
            "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }

        for symbol in self.__symbol_book:
            self._publish_count_dict["depth"][ self.__symbol_book[symbol][0]] = 0

    def connect_ws_server(self, info):
        print("\n\n*****%s %s %s *****" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), info, self._ws_url))
        # websocket.enableTrace(True)
        header = {'Authorization': 'Token %s' % self.__token}
        self._ws = websocket.WebSocketApp(self._ws_url)

        self._ws.on_message = self.on_msg
        self._ws.on_error = self.on_error                                    
        self._ws.on_open = self.on_open
        self._ws.on_close = self.on_close
        self._ws.header = header

        self._ws.run_forever()

    def start_reconnect(self):
        print("\n------- %s Start Reconnect --------" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        while self._is_connnect == False:
            self.connect_ws_server("Reconnect Server")
            time.sleep(self._reconnect_secs)

    def start_timer(self):
        self._timer = threading.Timer(self._ping_secs, self.on_timer)
        self._timer.start()

    def start(self):
        self.start_timer()
        self.connect_ws_server("Start Connect")        

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
        # print("----- subscribe_symbol ------")
        for symbol in self.__symbol_book:
            data = {
                    "event": "subscribe",
                    "instrument": symbol,
                    "levels": [self.__symbol_book[symbol][1], self.__symbol_book[symbol][2]],
                    "tag": ""
                }

            sub_info_str = json.dumps(data)

            # print(sub_info_str)

            self._ws.send(sub_info_str)

    def on_msg(self, msg):
        try:
            # print("\n------- on_msg -------")
            # print(msg)
            dic = json.loads(msg)

            self.process_msg(dic)

        except Exception as e:
            print("[E] on_msg: ")
            print(e)


    def on_open(self):
        print("\nB2C2 Connect Server %s Successfully!" % (self._ws_url))
        self._is_connnect = True
        self.subscribe_symbol()

    def on_error(self):
        print("on_error")

    def on_close(self):
        print("\n******* on_close *******")

        self._is_connnect = False
        
        self.start_reconnect()

    def print_publish_info(self):
        self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
        for item in self._publish_count_dict:
            if item == "depth" or item == "trade":
                for symbol in self._publish_count_dict[item]:
                    print("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                    self._publish_count_dict[item][symbol] = 0

        self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def on_timer(self):
        if self._is_connnect:
            self.subscribe_symbol()      

        self.print_publish_info()

        self._timer = threading.Timer(self._ping_secs, self.on_timer)
        self._timer.start()

    def process_msg(self, ws_msg):
        try:
            # if ws_msg['event'] == "subscribe" or ws_msg['event'] == "tradable_instruments":
            #     return

            if ws_msg["event"] != "price":
                print(ws_msg)
                return

            msg = ws_msg

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
                        
        except Exception as e:
            print("[E] process_msg: %s" % (str(ws_msg)))
            print(e)
            print("")

if __name__ == '__main__':
    a = MarketData_B2C2(debug_mode=False)
    a.start()
