import asyncio
import json
import datetime
import redis
import threading
import numpy
import sys
import time
import traceback

from collections import deque
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

import os

from Logger import *

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

from package.tool import *
from package.kafka_server import *
from package.data_struct import *

def get_config(logger = None, config_file=""):    
    json_file = open(config_file,'r')
    json_dict = json.load(json_file)
    if logger is not None:
        logger.info("\n******* config *******\n" + str(json_dict))
    else:
        print("\n******* config *******\n" + str(json_dict))

    return json_dict
             
class KlineFit:
    def __init__(self, Logger=None):
        try:
            self._logger = Logger
            self.epoch = datetime.datetime(1970,1,1)
            self.klines = deque(maxlen=1440)
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def _update_klines(self, klines, trade_time, price, volume, symbol, exchange, resolution):
        try:
            if len(klines) == 0 or trade_time > klines[-1][0]:
                cur_kline = SKlineData()
                cur_kline.time = trade_time
                cur_kline.symbol = symbol
                cur_kline.exchange = exchange
                cur_kline.resolution = resolution
                cur_kline.volume = SDecimal(volume)
                cur_kline.px_open = SDecimal(price)
                cur_kline.px_high = SDecimal(price)
                cur_kline.px_low = SDecimal(price)
                cur_kline.px_close = SDecimal(price)
                
                if symbol == "BTC_USDT":
                    self._logger.info("New Kline" + cur_kline.meta_str())
                klines.append(cur_kline)
            else:
                lastest_kline = klines[-1]
                
                lastest_kline.volume = SDecimal(lastest_kline.volume.get_value() + volume)
                lastest_kline.px_close = SDecimal(price)

                if price > lastest_kline.px_high.get_value():
                    lastest_kline.px_high = SDecimal(price)
                    
                if price < lastest_kline.px_low.get_value():
                    lastest_kline.px_low = SDecimal(price)
                    
                klines[-1] = lastest_kline
                
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def new_trade(self, trade_time: datetime.datetime, price: float, volume: float, symbol:str, exchange:str):
        try:
            # 1min kline
            self._update_klines(self.klines, trade_time, price, volume, symbol, exchange, 60)         
               
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def recover_kline(self, kline_data: list, kline_type: str):
        try:
            self.klines = deque(kline_data, maxlen=1440)
        except Exception as e:
            self._logger.warning(traceback.format_exc())
              
class ProcessMain:
    def __init__(self, is_redis:bool = False, is_debug:bool = False):
        try:
            self.logger = Logger(program_name="")
            self._logger = self.logger._logger
            self._name = "ProcessMain"
            self.__kline_fit_list = dict()
            self.__verification_tag = False
                        
            self._publish_count_dict = {
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            
            self._publish_count_dict[get_out_type(KLINE_TYPE)] = {}
            self._publish_count_dict[get_in_type(TRADE_TYPE)] = {}

            self._statistic_timer_secs = 10
                                    
            if is_redis:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"    
                self._config = get_config(logger=self._logger, config_file=self._config_name)                          
                self.net_server = None 
            else:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"
                self._config = get_config(logger=self._logger, config_file=self._config_name)
                
                self.net_server = KafkaServer(config= self._config, depth_processor=self, \
                                              serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
            
            self.net_server.set_trade_meta(symbol_list=self._config["symbol_list"], exchange_list=self._config["exchange_list"])
                        
            self.net_server.start_listen_data()
            
            self.__loop = asyncio.get_event_loop()
            self.__task = asyncio.gather(self.__kline_updater(), self._log_updater())
            self.__loop.run_until_complete(self.__task)
            
            while True:
                time.sleep(3)
                
        except Exception as e:
            self._logger.warning("[E]__init__: " + traceback.format_exc())
            
    def process_trade_data(self, trade_data:STradeData):
        try:
            trade_topic = trade_data.symbol + SYMBOL_EXCHANGE_SEPARATOR + trade_data.exchange            
            
            self._update_statistic_info(get_in_type(TRADE_TYPE), trade_topic)
            
            self.update_kline(trade_topic, trade_data.time, \
                              trade_data.price, trade_data.volume, \
                              trade_data.symbol, trade_data.exchange)            
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def print_publish_info(self):
        try:
            self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._logger.info("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
            for item in self._publish_count_dict:
                if item != "start_time" and item != "end_time":
                    for symbol in self._publish_count_dict[item]:
                        self._logger.info("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                        self._publish_count_dict[item][symbol] = 1
            self._logger.info("\n")
            self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        except Exception as e:
            self._logger.warning("[E]print_publish_info: " + traceback.format_exc())

    def _update_statistic_info(self, item_type, topic):
        try:
            if item_type in self._publish_count_dict:
                if topic in self._publish_count_dict[item_type]:
                    self._publish_count_dict[item_type][topic] += 1
                else:
                    self._publish_count_dict[item_type][topic] = 1                

        except Exception as e:
            self._logger.warning("[E]_update_statistic_info: " + traceback.format_exc())
        
    def set_default_trade_topic(self, topic):
        try:
            self.__kline_fit_list.setdefault(topic, KlineFit(Logger= self._logger))                                
        except Exception as e:
            self._logger.warning("[E]set_default_trade_topic: " + traceback.format_exc())   
            
    def get_kline_atom(self, trade_topic):
        try:
            if trade_topic in self.__kline_fit_list:
                return self.__kline_fit_list[trade_topic]
            else:
                return None                              
        except Exception as e:
            self._logger.warning("[E]get_kline_atom: " + traceback.format_exc())   
            
    def update_kline(self, trade_topic, exg_time, price, volume, symbol, exchange):
        try:
            if trade_topic not in self.__kline_fit_list:
                self.__kline_fit_list.setdefault(trade_topic, KlineFit(Logger= self._logger))         
                        
            kline_fit_processor = self.__kline_fit_list[trade_topic]
            kline_fit_processor.new_trade(exg_time=exg_time, price=price, volume=volume, symbol=symbol, exchange=exchange)
                                                           
        except Exception as e:
            self._logger.warning("[E]get_kline_atom: " + traceback.format_exc())   
                            
    async def _log_updater(self):
        try:
            while True:
                self.print_publish_info()
                await asyncio.sleep(self._statistic_timer_secs)
            
        except Exception as e:
            self._logger.warning("[E]_log_updater: " + traceback.format_exc())
            
    async def __kline_updater(self):
        try:
            while True:
                now_time = datetime.datetime.now()                 
                f_now_time = float(now_time.strftime("%S.%f"))
                wait_secs = 60 - f_now_time                
                await asyncio.sleep(wait_secs)

                if self.__verification_tag is False or self.__verification_tag == now_time.minute:

                    for trade_topic in self.__kline_fit_list.keys():
                        kline_fit_processor = self.__kline_fit_list[trade_topic]
                        
                        if len(kline_fit_processor.klines) > 0:
                            lastest_kline_data = kline_fit_processor.klines[-1]
                            lastest_kline_data.time = get_utc_nano_minute()
                            self.net_server.publish_kline(lastest_kline_data)
                            self._update_statistic_info(get_out_type(KLINE_TYPE), trade_topic)
                        
                    self.__verification_tag = (now_time.minute + 1) % 60
                else:
                    self._logger.info("__verification_tag: %d" % (self.__verification_tag))
        except Exception as e:
            self._logger.warning("[E]__kline_updater: " + traceback.format_exc())

if __name__ == '__main__':
    # 运行脚本：[exe] 60 PRODUCTION
    print(sys.argv)
    
    svc = ProcessMain(is_redis=False, is_debug=False)
    
    # if len(sys.argv) == 3:
    #     svc = KLineSvc(slow_period=sys.argv[1], running_mode=sys.argv[2])
    # else:
    #     svc = KLineSvc(slow_period=2, running_mode="DEBUG")

