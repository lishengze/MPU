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
import traceback

from abc import ABC,abstractmethod

import os
# from package.data_struct import NET_SERVER_TYPE

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

sys.path.append(os.getcwd())
from Logger import *


g_redis_config_file_name = os.getcwd() + "/redis_config.json"

def get_login_info(api_key, api_secret, logger = None):
    ts = int(time.time() * 1000)

    # self._logger._logger.info("0")
    tmp_sign_origin = hmac.new(api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256')
    # self._logger._logger.info(tmp_sign_origin)
    tmp_sign_hex = tmp_sign_origin.hexdigest()
    # self._logger._logger.info(tmp_sign_hex)

    
    sub_info = {'op': 'login', 
                    'args': 
                    {
                        'key': api_key,
                        'sign': tmp_sign_hex,
                        'time': ts,
                    }
    }

    sub_info_str = json.dumps(sub_info)

    if logger is not None:
        logger._logger.info("\nsub_info: \n" + sub_info_str)
    else:
        print("\nsub_info: \n" + sub_info_str)
    return sub_info_str

def get_sub_order_info(symbol_name, logger = None):
    sub_info = {'op': 'subscribe', 
                'channel': 'orderbook', 
                'market': symbol_name}  

    sub_info_str = json.dumps(sub_info)

    if logger is not None:
        logger._logger.info("\nsub_info: \n" + sub_info_str)
    else:
        print("\nsub_info: \n" + sub_info_str)
    
    return sub_info_str           

def get_sub_trade_info(symbol_name, logger = None):
    sub_info = {'op': 'subscribe', 
                'channel': 'trades', 
                'market': symbol_name}  

    sub_info_str = json.dumps(sub_info)

    if logger is not None:
        logger._logger.info("\nsub_trade_info: \n" + sub_info_str)
    else:
        print("\nsub_trade_info: \n" + sub_info_str)
    
    return sub_info_str         

def get_ping_info():
    sub_info = {'op': 'ping'}  

    sub_info_str = json.dumps(sub_info)
    # self._logger._logger.info(sub_info_str)
    
    return sub_info_str       

'''
Trade InstrumentID
BTC-USDT、ETH-USDT、BTC-USD、ETH-USD、USDT-USD、ETH-BTC
'''
class ExchangeBase(ABC):
    def __init__(self, exchange_name:str, symbol_dict:dict, net_server_type:NET_SERVER_TYPE = NET_SERVER_TYPE.KAFKA, 
                 debug_mode: bool = True, is_test_currency:bool = False):
        try:
            self._is_test_exhange_conn = is_test_currency
            self._symbol_dict = symbol_dict
            self._net_server_type = net_server_type
            self.__exchange_name = exchange_name             
            self._is_test_currency = is_test_currency             
            
            self._logger = Logger(program_name=self.__exchange_name, log_dir=os.path.dirname(os.path.abspath(__file__)) + "/log/")
            
            self._reconnect_secs = 5
            
            if self._is_test_currency:
                self._success_log_file_name = os.getcwd() + "/log/" + self.__exchange_name + "/suceess_currency.log"
                self._success_log_file = open(self._success_log_file_name, 'a')
                
                self._failed_log_file_name = os.getcwd() + "/log/" + self.__exchange_name + "/failed_currency.log"
                self._failed_log_file = open(self._failed_log_file_name, 'a')
            
            self._error_msg_list = ["", ""]
            
            self._is_connnect = False
            self._ws = None
            self._publish_count_dict = {
                "depth":{},
                "trade":{},
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            for item in self._symbol_dict:
                sys_symbol = self._symbol_dict[item]
                if not self._is_test_currency:
                    self._publish_count_dict["depth"][sys_symbol] = 0
                self._publish_count_dict["trade"][sys_symbol] = 0
                            
            self._config = self._get_net_config(net_server_type)
                        
            if self._is_test_currency == False:                   
                self.__publisher = Publisher(exchange=self.__exchange_name, config=self._config, 
                                            net_server_type=net_server_type, debug_mode=debug_mode, 
                                            logger=self._logger._logger)
        except Exception as e:
            self._logger._logger.warning("[E]__init__: " + str(e))
            
    def set_meta(self):
        try:
            self._sub_id = 1
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())
                        
    def _get_net_config(self, net_server_type:NET_SERVER_TYPE):
        if net_server_type == NET_SERVER_TYPE.KAFKA:
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"               
        elif net_server_type == NET_SERVER_TYPE.REDIS:
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"

        return get_config(logger=self._logger, config_file=self._config_name)     
    
    def _write_successful_currency(self, symbol):
        if self._success_log_file.closed:
            self._success_log_file = open(self._success_log_file_name, 'a')
            
        self._success_log_file.write(symbol + "\n")
        self._success_log_file.close()
        
    def _write_failed_currency(self, symbol):
        if self._failed_log_file.closed:
            self._failed_log_file = open(self._failed_log_file_name, 'a')
            
        self._failed_log_file.write(symbol + "\n")
        self._failed_log_file.close()        
        
    def connect_ws_server(self, info):
        try:
            self._logger._logger.info("*****connect_ws_server %s ***** \n" % (self._ws_url))

            self._ws = websocket.WebSocketApp(self._ws_url)
            self._ws.on_message = self.on_msg
            self._ws.on_error = self.on_error                                    
            self._ws.on_open = self.on_open
            self._ws.on_close = self.on_close

            self._ws.run_forever()

        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    def start_reconnect(self):
        try:
            self._logger._logger.info("------- Start Reconnect -------- \n")

            time.sleep(self._reconnect_secs)
            self.connect_ws_server("Reconnect Server")
            
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    def start_timer(self):
        try:
            self._logger._logger.info("start_timer\n")
            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    def start(self):
        try:
            self.start_timer()
            self.connect_ws_server("Start Connect")
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    def on_msg(self, msg):
        try:
            json_data = self.decode_msg(msg)
            self.process_msg(json_data)
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())
            
    
    def decode_msg(self, msg):
        try:
            dic = json.loads(msg)
            return dic
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())        

    def on_open(self):
        try:
            self._logger._logger.info("\nftx_on_open")
            self._is_connnect = True
            self.set_meta()
            
            self.subscribe_trade()
            if not self._is_test_currency:
                self.subscribe_depth()
                    
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())                

    def on_error(self):
        self._logger._logger.error("on_error")

    def on_close(self):
        try:
            self._logger._logger.warning("\n******* on_close *******")
            self._is_connnect = False        
            self.start_reconnect()
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    def print_publish_info(self):
        try:
            self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._logger._logger.info("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
            for item in self._publish_count_dict:
                if item == "depth" or item == "trade":
                    for symbol in self._publish_count_dict[item]:
                        self._logger._logger.info("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                        self._publish_count_dict[item][symbol] = 0
            self._logger._logger.info("\n")

            self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    def on_timer(self):
        try:
            # if self._is_connnect:
            #     self._ws.send(self.get_ping_sub_info())        

            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())
            
    @abstractmethod
    def get_ping_sub_info(self):
        try:
            sub_info = {'op': 'ping'}  

            sub_info_str = json.dumps(sub_info)
            
            return sub_info_str       
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())        

    @abstractmethod
    def subscribe_depth(self):
        try:
            pass
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())        
            
    @abstractmethod
    def subscribe_trade(self):
        try:
            pass
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())    
                        
    @abstractmethod
    def process_msg(self, ws_msg):
        try:
            print(ws_msg)                              
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    @abstractmethod
    def _process_orderbook(self, symbol, msg):
        try:
            print(symbol, msg)
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())

    @abstractmethod
    def _process_trades(self, symbol, data_list):
        try:
            print(symbol, data_list)
        except Exception as e:
            self._logger._logger.warning(traceback.format_exc())            

            
                
def test_get_ori_sys_config():
    print(get_symbol_dict(os.getcwd() + "/symbol_list.json", "FTX"))
    
if __name__ == "__main__":
    # test_hmac()
    # test_websocket()
    # test_http_restful()

    # test_ftx()
    
    test_get_ori_sys_config()
