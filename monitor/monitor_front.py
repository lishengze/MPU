import asyncio
import json
import aiohttp
import sys
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
import websocket
import json
import hmac
import threading
import traceback

from abc import ABC,abstractmethod
from Logger import *

import os

def get_grandfather_dir():
    parent = os.path.dirname(os.path.realpath(__file__))
    garder = os.path.dirname(parent)    
    return garder

def get_package_dir():
    garder = get_grandfather_dir()
    if garder.find('\\') != -1:
        return garder + "\package"
    else:
        return garder + "/package"

print(get_package_dir())
sys.path.append(get_package_dir())

from tool import *
from Logger import *



class WSClass(object):
    def __init__(self, ws_url: str, processor, logger):
        self._ws_url = ws_url
        self._processor = processor
        self._ws = None
        self._logger = logger
    
    def connect(self, info:str=""):
        try:
            self._logger.info("\n*****WSClass Connect %s %s %s *****" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), info, self._ws_url))
            # websocket.enableTrace(True)
            self._ws = websocket.WebSocketApp(self._ws_url)
            self._ws.on_message = self._processor.on_msg
            self._ws.on_error = self._processor.on_error                                    
            self._ws.on_open = self._processor.on_open
            self._ws.on_close = self._processor.on_close
            self._ws.run_forever()        

        except Exception as e:
            self._logger.warning("[E]connect_ws_server: " + str(e))

    def send(self, data:str):
        try:
            if self._ws != None:
                self._ws.send(data)
            else :
                self._logger.warning("[E]ws is None!")     
        except Exception as e:
            self._logger.warning("[E]send: " + str(e))   

class MonitorAtom(object):
    def __init__(self, symbol:str, data_type:str):
        self._symbol = symbol
        self._type = data_type

class MonitorFront(object):
    def __init__(self, env_name:str="local", server_type:str="new"):
        try:
            log_dir = os.path.dirname(os.path.abspath(__file__)) + get_dir_seprator() + "log" + get_dir_seprator() 

            self._exchange_name = "frontserver"
            self._logger_all = Logger(program_name=self._exchange_name, log_dir=log_dir)
            self._logger = self._logger_all._logger

            config = get_config(config_file = (os.getcwd() + get_dir_seprator() + "monitor_front_config.json"))

            self._config = config["env_name"]
            self._symbol_list = self._config["symbol"]

            self._logger.info("\n\n_symbol_dict: \n" + str(self._symbol_list))
            
            self._reconnect_secs = 5
                        
            self._error_msg_list = ["", ""]
            
            self._is_connnect = False
            self._ws = None
            self._connect_count = 0
            self._ping_secs = 15
            
            self._logger.info(str(self._symbol_list))
            
            sys_symbol_list = list()

            self._valid_trade_symbol = list()
            self._valid_depth_symbol = list()

            self._publish_count_dict = {
                "depth":{},
                "trade":{},
                "kline":{},
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            
            for item in self._symbol_list:
                sys_symbol = self._symbol_list[item]
                self._publish_count_dict["depth"][sys_symbol] = 0
                self._publish_count_dict["trade"][sys_symbol] = 0
                sys_symbol_list.append(sys_symbol)
                
            self._logger.info(str(self._publish_count_dict))
            self._logger.info(str(self._config))           
                            
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def start(self):
        try:
            if self._is_stress_test:
                self.start_exchange_moka()
            else:
                self.start_exchange_work()
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def connect_ws_server(self, info):
        try:
            self._connect_count += 1
            self._logger.info("*****connect_ws_server %s, connect_count: %d ***** \n" % (self._ws_url, self._connect_count))
            
            self._ws = WSClass(ws_url=self._ws_url, processor=self, logger= self._logger)
            self._ws.connect()

        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def start_reconnect(self):
        try:
            self._logger.warning("------- Start Reconnect -------- \n")

            while self._is_connnect == False:
                time.sleep(self._reconnect_secs)
                self._logger.warning("connect_count: " + str(self._connect_count))
                self.connect_ws_server("Reconnect Server")

            
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def start_timer(self):
        try:
            self._logger.info("start_timer\n")
            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
    

    def on_msg(self, ws = None, message=None):
        try:
            if (ws != None and message != None) or (ws == None and message != None):
                json_data = self.decode_msg(message)
            elif ws != None and message == None:
                message = ws
                json_data = self.decode_msg(message)
            else:
                self._logger.warning("ws message are all None")
                return

            self.process_msg(json_data)
        except Exception as e:
            self._logger.warning(traceback.format_exc())
                
    def decode_msg(self, msg):
        try:
            dic = json.loads(msg)
            return dic
        except Exception as e:
            self._logger.warning(traceback.format_exc())        

    def on_open(self, *t_args, **d_args):
        try:
            self._logger.info("\non_open")
            self._is_connnect = True

                                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())                

    def on_error(self, *t_args, **d_args):
        self._logger.error("on_error")


    def on_close(self, *t_args, **d_args):
        try:
            self._logger.warning("******* on_close *******\n")
            self._is_connnect = False        
            self.start_reconnect()
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def print_publish_info(self):
        try:
            self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._logger.info("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
            for item in self._publish_count_dict:
                if item == "depth" or item == "trade":
                    for symbol in self._publish_count_dict[item]:
                        self._logger.info("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                        self._publish_count_dict[item][symbol] = 0
            self._logger.info("\n")

            self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def on_timer(self):
        try:
            # if self._is_connnect:
            #     self._ws.send(self.get_ping_sub_info())        

            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
              