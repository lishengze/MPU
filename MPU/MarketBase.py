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
class ExchangeBase(object):
    def __init__(self, exchange_name:str, symbol_dict:dict, net_server_type:NET_SERVER_TYPE = NET_SERVER_TYPE.KAFKA, 
                 debug_mode: bool = True, is_test_currency:bool = False):
        try:
            self._is_test_exhange_conn = is_test_currency
            self._symbol_dict = symbol_dict
            self._net_server_type = net_server_type
            self.__exchange_name = exchange_name             
            self._is_test_currency = is_test_currency             
            self._logger = Logger(program_name=self.__exchange_name)
            
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
            
    def _get_net_config(self, net_server_type:NET_SERVER_TYPE):
        if net_server_type == NET_SERVER_TYPE.KAFKA:
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"               
        elif net_server_type == NET_SERVER_TYPE.REDIS:
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"

        return get_config(logger=self._logger, config_file=self._config_name)       
                
def test_get_ori_sys_config():
    print(get_symbol_dict(os.getcwd() + "/symbol_list.json", "FTX"))
    
if __name__ == "__main__":
    # test_hmac()
    # test_websocket()
    # test_http_restful()

    # test_ftx()
    
    test_get_ori_sys_config()
