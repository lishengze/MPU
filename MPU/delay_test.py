import redis
import json
import operator
import logging
import requests
import traceback
import sys
import os
import threading

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
from kafka_server import *
from data_struct import *
from Logger import *

class DelayMeta:
    def __init__(self) -> None:
        self._max = -1
        self._min = -1
        self._ave = 0
        self._all = list()
        
    def update(self, new_value):
        
        if new_value > self._max or self._max == -1:
            self._max = new_value
        
        if new_value < self._min or self._min == -1:
            self._min = new_value
            
        new_sum = len(self._all) * self._ave + new_value
        self._all.append(new_value)
        
        self._ave = float(new_sum) / len(self._all)
        
    def info(self):
        return ("max: %d, min: %d, ave: %d, all.size: %d \n" % 
                (self._max, self._min, self._ave, len(self._all)))


class DelayClass:
    def __init__(self, data_type_list:list, kafka_config:dict, symbol_list:list) -> None:
        self._logger = Logger(program_name="Delay")        
        self._logger = self._logger._logger
        self._kafka_server = KafkaServer(config = kafka_config, depth_processor=self, kline_processor=self, trade_processor=self, \
                                         serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
        self._symbol_list = symbol_list
        self._exchange_list = ["FTX"]
        self._data_type_list = data_type_list
        self._kafka_server.set_meta(symbol_list=self._symbol_list, \
                                    exchange_list=self._exchange_list, \
                                    data_type=self._data_type_list)
        self._seq_no = -1
        self._ping_secs = 5
        self._delay = dict()
        
        for symbol in self._symbol_list:
            self._delay[symbol] = DelayMeta()
    
    def start(self):
        self._kafka_server.start_listen_data()
        
        self.start_timer()
    
    def check_seq(self, seq_no):
        if self._seq_no == -1:
            self._seq_no = seq_no
        elif self._seq_no + 1 != seq_no:
            print("local seq: %d, new seq: %d" % (self._seq_no, seq_no))
        
        self._seq_no = seq_no
        
    def start_timer(self):
        try:
            self._logger.info("start_timer\n")
            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def on_timer(self):
        try:
            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()

        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def print_publish_info(self):
        try:
            for symbol in self._delay:
                self._logger.info(symbol + ": " + self._delay[symbol].info())
                
        except Exception as e:
            self._logger.warning(traceback.format_exc())
                                    
                    
    def process_depth_data(self, depth_quote:SDepthQuote):
        try:    
            # self.check_seq(depth_quote.sequence_no)        
            # pass
            print(depth_quote.meta_str())
            
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def process_kline_data(self, kline_data:SKlineData):
        try:            
            print(kline_data.meta_str())
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def process_trade_data(self, trade_data:STradeData):
        try:            
            print(trade_data.meta_str())            
            cur_time = get_utc_nano_time()
            
            if trade_data.symbol in self._delay:                            
                if cur_time < trade_data.time:
                    self._logger.warning("abnormal time: " + str(trade_data.time))
                else:
                    self._delay.update(cur_time - trade_data.time)
            else:
                self._logger.warning("invalid trade data: " + trade_data.meta_str())                            
        except Exception as e:
            self._logger.warning(traceback.format_exc())                        
            
def delay_test():
    sys_config = get_config(os.getcwd() + get_dir_seprator() + "sys_config.json")
    kafka_config = get_config(os.getcwd() + get_dir_seprator() + "kafka_config.json")
    
    data_type_list = [DATA_TYPE.TRADE] 
    kafka_obj = DelayClass(data_type_list=data_type_list, kafka_config=kafka_config, symbol_list=sys_config['test_symbol_list'])
    kafka_obj.start()
        
if __name__ == "__main__":
    delay_test()