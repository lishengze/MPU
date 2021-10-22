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

from Logger import *

g_redis_config_file_name = "./redis_config.json"

def get_redis_config():    
    json_file = open(g_redis_config_file_name,'r')
    json_dict = json.load(json_file)

    time.sleep(3)
    return json_dict

'''
Trade InstrumentID
BTC-USDT、ETH-USDT、BTC-USD、ETH-USD、USDT-USD、ETH-BTC
'''
class MarketData_B2C2(object):
    def __init__(self, debug_mode: bool = True, redis_config: dict = None, env_name="-qa"):
        try:
            self.__exchange_name = "B2C2"
            self._logger = Logger(program_name="B2C2")

            if redis_config is None:            
                redis_config = get_redis_config()
                        
            self.__publisher = Publisher(exchange=self.__exchange_name, redis_config=redis_config, debug_mode=debug_mode, logger=self._logger)

            self._logger._logger.info("\n******* redis_config *******\n" + str(redis_config))

            self._ws_url = "wss://socket.uat.b2c2.net/quotes"
            if env_name == "-qa":
                self.__token = "eabe0596c453786c0ecee81978140fad58daf881"
            elif env_name == "-stg":
                self.__token = "6a0c8b12ecf5f82763d86547da61c8cfc1ebbaa3"
            elif env_name == "-prd":
                self.__token = "8f5300e0a56777cae6d2b08e46d7edfcfb7e21aa"        

            self._logger._logger.info("%s, env_name: %s, token: %s" % (str(sys._getframe().f_code.co_name), env_name, self.__token))    

            self.__symbol_book = {
                    "BTCUSD.SPOT" : ["BTC_USD", 1, 50],
                    "BTCUST.SPOT" : ["BTC_USDT", 1, 50], 
                    "ETHUSD.SPOT" : ["ETH_USD", 10, 500],
                    "ETHUST.SPOT" : ["ETH_USDT", 10, 500],
                    "USTUSD.SPOT" : ["USDT_USD", 50000, 60000]
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
        except Exception as e:
            self._logger._logger.warning("[E]%s__init__: %s" %(str(sys._getframe().f_code.co_name), str(e)))

    def connect_ws_server(self, info):
        try:
            self._logger._logger.info("\n\n*****connect_ws_server %s %s %s *****" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), info, self._ws_url))
            # websocket.enableTrace(True)
            header = {'Authorization': 'Token %s' % self.__token}
            self._ws = websocket.WebSocketApp(self._ws_url)

            self._ws.on_message = self.on_msg
            self._ws.on_error = self.on_error                                    
            self._ws.on_open = self.on_open
            self._ws.on_close = self.on_close
            self._ws.header = header

            self._ws.run_forever()
        except Exception as e:
            self._logger._logger.warning("[E]connect_ws_server: " + str(e))

    def start_reconnect(self):
        try:
            self._logger._logger.info("\n------- %s Start Reconnect --------" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            while self._is_connnect == False:
                self.connect_ws_server("Reconnect Server")
                time.sleep(self._reconnect_secs)
        except Exception as e:
            self._logger._logger.warning("[E]start_reconnect: " + str(e))

    def start_timer(self):
        try:
            self._logger._logger.info("start_timer")
            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()

            self.connect_ws_server("Start Connect")
            
        except Exception as e:
            self._logger._logger.warning("[E]start_timer: " + str(e))

    def start(self):
        try:            
            self.start_timer()            
        except Exception as e:
            self._logger._logger.warning("[E]start: " + str(e))
        
    def print_publish_info(self):
        try:
            self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._logger._logger.info("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
            for item in self._publish_count_dict:
                if item == "depth" or item == "trade":
                    for symbol in self._publish_count_dict[item]:
                        self._logger._logger.info("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                        self._publish_count_dict[item][symbol] = 0

            self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        except Exception as e:
            self._logger._logger.warning("[E]print_publish_info: " + str(e))

    def subscribe_symbol(self):
        try:
            if self._is_connnect == True:
                for symbol in self.__symbol_book:
                    data = {
                            "event": "subscribe",
                            "instrument": symbol,
                            "levels": [self.__symbol_book[symbol][1], self.__symbol_book[symbol][2]],
                            "tag": ""
                        }

                    sub_info_str = json.dumps(data)
                    self._ws.send(sub_info_str)
        except Exception as e:
            self._logger._logger.warning("[E]subscribe_symbol: " + str(e))

    def keep_alive(self):
        try:
            if self._is_connnect == True:
                symbol = "BTCUSD.SPOT"
                data = {
                        "event": "subscribe",
                        "instrument": symbol,
                        "levels": [self.__symbol_book[symbol][1], self.__symbol_book[symbol][2]],
                        "tag": ""
                    }

                sub_info_str = json.dumps(data)
                self._ws.send(sub_info_str)   
        except Exception as e:
            self._logger._logger.warning("[E]keep_alive: " + str(e))
     
    def on_msg(self, msg):
        try:
            dic = json.loads(msg)
            self.process_msg(dic)
        except Exception as e:
            self._logger._logger.warning("[E]on_msg: " + str(e))

    def on_open(self):
        try:
            self._logger._logger.info("\non_open B2C2 Connect Server %s Successfully!" % (self._ws_url))
            self._is_connnect = True
            self.subscribe_symbol()
        except Exception as e:
            self._logger._logger.warning("[E]on_open: " + str(e))

    def on_error(self):
        self._logger._logger.error("on_error")

    def on_close(self):
        try:
            self._logger._logger.warning("\n******* on_close *******")
            self._is_connnect = False
            self.start_reconnect()
        except Exception as e:
            self._logger._logger.warning("[E]on_close: " + str(e))

    def on_timer(self):
        try:
            if self._is_connnect == True:
                self.keep_alive()      

            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger._logger.warning("[E]on_timer: " + str(e))


    def process_msg(self, ws_msg):
        try:
            self._logger._debug_logger.info("process_msg: " + str(ws_msg))

            if ws_msg["event"] != "price":
                self._logger._logger.info("process_msg: " + str(ws_msg))
                return

            msg = ws_msg
            depth_update = {"ASK": {}, "BID": {}}
            for level in msg["levels"]["buy"]:
                depth_update["ASK"][float(level["price"])] = float(level["quantity"])
            for level in msg["levels"]["sell"]:
                depth_update["BID"][float(level["price"])] = float(level["quantity"])

            sys_symbol = self.__symbol_book[msg["instrument"]][0]

            if len(depth_update["ASK"]) or len(depth_update["BID"]):
                if sys_symbol in self._publish_count_dict["depth"]:
                    self._publish_count_dict["depth"][sys_symbol] += 1
                self.__publisher.pub_depthx(symbol=sys_symbol,
                                            depth_update=depth_update, is_snapshot=True)                        
        except Exception as e:
            self._logger._logger.warning("[E]process_msg: " + str(e))

if __name__ == '__main__':
    print(sys.argv)
    env_name = "-qa"
    if len(sys.argv) == 2:
        env_name = sys.argv[1]

    a = MarketData_B2C2(debug_mode=False, env_name=env_name)
    # a.start()
