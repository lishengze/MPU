import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor
from dingtalkchatbot.chatbot import DingtalkChatbot

from datetime import datetime
import time
import websocket
import json
import hmac
import threading
import traceback

from abc import ABC,abstractmethod

import os

from Logger import *


def get_config(logger = None, config_file=""):    
    json_file = open(config_file,'r')
    json_dict = json.load(json_file)
    if logger is not None:
        logger.info("\n******* config *******\n" + str(json_dict))
    else:
        print("\n******* config *******\n" + str(json_dict))
    time.sleep(3)


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

            # self._ws.on_message = self.on_msg
            # self._ws.on_error = self.on_error                                    
            # self._ws.on_open = self.on_open
            # self._ws.on_close = self.on_close

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

'''
Trade InstrumentID
BTC-USDT、ETH-USDT、BTC-USD、ETH-USD、USDT-USD、ETH-BTC
'''
class ExchangeBase(ABC):
    def __init__(self, exchange_name:str, ws_url:str, symbol_dict:dict, is_redis:bool,debug_mode: bool = True, env_type:str = "dev"):
        try:
            self._ws_url = ws_url
            self._symbol_dict = symbol_dict
            self._exchange_name = exchange_name             
            self._env_type = env_type

            self._is_connnect = False
            self._ws = None

            self._reconnect_secs = 5
            self._connect_count = 0
            self._invalid_count = 0
                        
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"                

                
            self._config = get_config(logger=self._logger, config_file=self._config_name)
            
            print(self._config_name)
            print(self._config)

            self._init_ding()
                        
            self.__publisher = Publisher(exchange=self._exchange_name, config=self._config, 
                                         is_redis=is_redis, debug_mode=debug_mode, logger=self._logger)

            self._publish_count_dict = {
                "depth":{},
                "trade":{},
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }

            for item in self._symbol_dict:
                sys_symbol = self._symbol_dict[item]
                self._publish_count_dict["depth"][sys_symbol] = 0
                self._publish_count_dict["trade"][sys_symbol] = 0            
                            
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def set_meta(self):
        try:
            self._sub_id = 1
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def send_ding_msg(self, msg):
        if self._ding != None : 
            msg = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": " + msg
            self._ding.send_text(msg, False)

    def _get_net_config(self, net_server_type:NET_SERVER_TYPE, env_type:str = "dev"):
        if net_server_type == NET_SERVER_TYPE.KAFKA:
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"               
        elif net_server_type == NET_SERVER_TYPE.REDIS:
            self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"
            
        origin_config = get_config(logger=self._logger, config_file=self._config_name, env_type=env_type)
        
        return origin_config  

    def _get_dingding_config(self, env_type:str = "dev"):
        config_name = os.path.dirname(os.path.abspath(__file__)) + "/sys_config.json"
        origin_config = get_config(logger=self._logger, config_file=config_name, env_type=env_type)
        return origin_config

    def _write_successful_currency(self, symbol):
        if self._success_log_file.closed:
            self._success_log_file = open(self._success_log_file_name, 'a')
                
        # self.send_ding_msg("info %s, sub %s, suceesslly"%(self._exchange_name, symbol))

        self._success_log_file.write(symbol + "\n")
        self._success_log_file.close()
        
    def _write_failed_currency(self, symbol):
        if self._failed_log_file.closed:
            self._failed_log_file = open(self._failed_log_file_name, 'a')
            
        self._failed_log_file.write(symbol + "\n")
        self._failed_log_file.close()        
        
    def connect_ws_server(self, info):
        try:
            self._logger.info("*****connect_ws_server %s, connect_count: %d ***** \n" % (self._ws_url, self._connect_count))
            self.send_ding_msg("Info:%s connect_ws_server %s, connect_count: %d \n" % (self._exchange_name, self._ws_url, self._connect_count))

            self._connect_count += 1
            # websocket.enableTrace(True)
            # self._ws = websocket.WebSocketApp(self._ws_url)
            # self._ws.on_message = self.on_msg
            # self._ws.on_error = self.on_error                                    
            # self._ws.on_open = self.on_open
            # self._ws.on_close = self.on_close

            # self._ws.run_forever()

            self._ws = WSClass(ws_url=self._ws_url, processor=self, logger= self._logger)
            self._ws.connect()            

        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def reset_connect(self):
        try:
            self._logger.warning("Reset Connect")
            self.send_ding_msg("%s Reset Connect "% (self._exchange_name))

            self._is_connnect = False
            self.start_reconnect()

        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def start_reconnect(self):
        try:
            self._logger.warning("\n------- Start Reconnect, Counts:%d --------" % (self._connect_count))
            self.send_ding_msg("Warn: Start Reconnect %s\n"%(self._exchange_name))

            while self._is_connnect == False:
                time.sleep(self._reconnect_secs)
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
    
    @abstractmethod
    def start_exchange_moka(self):
        print("start_exchange_moka")
        pass
    
    def start_exchange_work(self):
        try:
            self.start_timer()
            self.connect_ws_server("Start Connect")
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

            # print(json_data)
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
            self.send_ding_msg("Info %s on_open! "%(self._exchange_name))
            self._is_connnect = True
            self.set_meta()
            
            if DATA_TYPE.DEPTH in self._sub_data_type_list:
                self.subscribe_depth()
            
            if DATA_TYPE.TRADE in self._sub_data_type_list:
                self.subscribe_trade()
                                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())                

    def on_error(self, *t_args, **d_args):
        self._logger.error("on_error")


        # if error is not None:
        #     self._logger.error(error)
        # else:
        #     self._logger.error("on_error")

    def on_close(self, *t_args, **d_args):
        try:
            self._logger.warning("******* on_close *******\n")
            self.send_ding_msg("warn: %s, On Close"%(self._exchange_name))
            self._is_connnect = False    
            # time.sleep(self._reconnect_secs)    

            # # if self._restart_thread != None and self._restart_thread.is_alive():
            # #     self._restart_thread.stop()

            # restart_thread = threading.Thread(target=self.start_reconnect, )
            # restart_thread.start()
            self.start_reconnect()

        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def print_publish_info(self):
        try:
            self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._logger.info("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],
                                                                    self._publish_count_dict["end_time"] ))
            unupdate_dict = {
                "depth":list(), 
                "trade":list(),
            }

            for item in self._publish_count_dict:
                if item == "depth" or item == "trade":
                    for symbol in self._publish_count_dict[item]:
                        if self._publish_count_dict[item][symbol] == 0:
                            unupdate_dict[item].append(symbol)
                        else:
                            self._logger.info("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                        self._publish_count_dict[item][symbol] = 0
                    
                    if len(unupdate_dict[item]) > 0:
                        self._logger.info("UnUpdated Symbol Count:%d, List: %s"%(len(unupdate_dict[item]), str(unupdate_dict[item])))

            self._logger.info("\n")

            self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def is_connect_invalid(self):
        try:
            update_count = 0
            for item in self._publish_count_dict:
                if item == "depth" or item == "trade":
                    for symbol in self._publish_count_dict[item]:
                        update_count += self._publish_count_dict[item][symbol]

            self._logger.info("Sum Update Count is: " + str(update_count))        

            if update_count == 0:
                self._invalid_count += 1

                if self._invalid_count > 3:
                    return True
                else:
                    return False
            else:
                self._invalid_count = 0
                
                return False
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def on_timer(self):
        try:
            if self.is_connect_invalid():
                self.send_ding_msg("error: %s cann't receive data, reconnect!"%(self._exchange_name))
                self.reset_connect() 

            if self._is_connnect:
                self._ws.send(self.get_ping_sub_info())        

            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    @abstractmethod
    def get_ping_sub_info(self):
        try:
            sub_info = {'op': 'ping'}  

            sub_info_str = json.dumps(sub_info)
            
            return sub_info_str       
        except Exception as e:
            self._logger.warning(traceback.format_exc())        

    @abstractmethod
    def subscribe_depth(self):
        try:
            pass
        except Exception as e:
            self._logger.warning(traceback.format_exc())        
            
    @abstractmethod
    def subscribe_trade(self):
        try:
            pass
        except Exception as e:
            self._logger.warning(traceback.format_exc())    
                        
    @abstractmethod
    def process_msg(self, ws_msg):
        try:
            print(ws_msg)                              
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    @abstractmethod
    def _process_depth(self, symbol, msg):
        try:
            print(symbol, msg)
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    @abstractmethod
    def _process_trades(self, symbol, data_list):
        try:
            print(symbol, data_list)
        except Exception as e:
            self._logger.warning(traceback.format_exc())            

            
if __name__ == "__main__":
    # test_hmac()
    # test_websocket()
    # test_http_restful()

    # test_ftx()
    
    test_get_ori_sys_config()
