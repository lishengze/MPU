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
from redis_server import *
from data_struct import *
from Logger import *

class DelayMeta:
    def __init__(self, symbol:str="test") -> None:
        self._max = -1
        self._min = -1
        self._ave = 0
        self._cnt = 0
        self._symbol = symbol
        self._lost_cnt = 0;
        self._unsort_cnt = 0;

        self._cur_seq = -1
        self._lost_list = list()
        self._unsort_list = list()

    def update_delta_time(self, new_value):
        new_value = new_value/1000000

        if new_value > self._max or self._max == -1:
            self._max = new_value
        
        if new_value < self._min or self._min == -1:
            self._min = new_value
            
        new_sum = self._cnt * self._ave + new_value

        self._cnt = self._cnt + 1
        
        self._ave = float(new_sum) / self._cnt

    def update_sequence_no(self, sequence_no):
        if  self._cur_seq == sequence_no - 1 or self._cur_seq == -1:
            self._cur_seq = sequence_no
            return

        if self._cur_seq > sequence_no:
            self._unsort_list.append(self._cur_seq)
        
        if sequence_no in self._lost_list:
            self._lost_list.remove(sequence_no)
        else:
            cur_seq = self._cur_seq + 1
            while cur_seq < sequence_no:
                if cur_seq not in self._unsort_list:
                    self._lost_list.append(cur_seq)
                cur_seq += 1
        
        self._cur_seq = sequence_no
                            
    def update(self, new_value, sequence_no=0):
        
        self.update_delta_time(new_value)

        self.update_sequence_no(sequence_no)
        

        
    def info(self):
        return ("%s, max: %d, min: %d, ave: %f, all.size: %d, lost_all: %d, lost_ave: %f, unsort_all: %d, unsort_ave: %f \n" % 
                (self._symbol,self._max, self._min, self._ave, self._cnt, 
                len(self._lost_list), float(len(self._lost_list))/float(self._cnt), 
                len(self._unsort_cnt), float(len(self._unsort_list))/float(self._cnt)))

    def delay_info(self):
        return ("%s, max: %d, min: %d, ave: %f, all.size: %d \n" % 
                (self._symbol,self._max, self._min, self._ave, self._cnt))        


class TradeTest:
    def __init__(self, data_type_list:list, config:dict, symbol_list:list, 
                        net_server_type: NET_SERVER_TYPE =NET_SERVER_TYPE.KAFKA,) -> None:
        log_dir = os.path.dirname(os.path.abspath(__file__)) + get_dir_seprator() + "log" + get_dir_seprator() 
        self._logger = Logger(program_name="Delay", log_dir=log_dir)        
        self._logger = self._logger._logger

        if net_server_type == NET_SERVER_TYPE.KAFKA:
            self._net_server = KafkaServer(config = config, depth_processor=self, kline_processor=self, trade_processor=self, \
                                            serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
        elif net_server_type == NET_SERVER_TYPE.REDIS:
            self._net_server = RedisServer(config = config, depth_processor=self, kline_processor=self, trade_processor=self, \
                                         serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
                    

        self._symbol_list = symbol_list
        self._exchange_list = ["FTX"]
        self._data_type_list = data_type_list
        self._net_server.set_meta(symbol_list=self._symbol_list, \
                                    exchange_list=self._exchange_list, \
                                    data_type=self._data_type_list)
        self._seq_no = -1
        self._ping_secs = 5
        self._delay = dict()
        
        for symbol in self._symbol_list:
            self._delay[symbol] = DelayMeta(symbol=symbol)
        
        self._delay_all = DelayMeta(symbol="main")
        
    
    def start(self):
        self._net_server.start_listen_data()
        
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
            self.statistic_all()
            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()

        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def statistic_all(self):
        if len(self._delay) == 1:
            return

        sum_time = 0
        for symbol in self._delay:

            if self._delay_all._max == -1 or self._delay_all._max < self._delay[symbol]._max:
                self._delay_all._max = self._delay[symbol]._max

            if self._delay_all._min == -1 or self._delay_all._min > self._delay[symbol]._min:
                self._delay_all._min = self._delay[symbol]._min
            
            sum_time += self._delay[symbol]._ave * self._delay[symbol]._cnt
        
            self._delay_all._cnt +=  self._delay[symbol]._cnt

        if self._delay_all._cnt > 0:
            self._delay_all._ave = sum_time / self._delay_all._cnt   
            
    def print_publish_info(self):
        try:
            # if len(self._delay) < 15:
            for symbol in self._delay:
                if self._delay[symbol]._cnt > 0:
                    self._logger.info(symbol + ": " + self._delay[symbol].delay_info())
            
            if len(self._delay) > 1:
                self._logger.info("ALL: " + self._delay_all.delay_info())
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
            # print(trade_data.meta_str())            
            cur_time = get_utc_nano_time()
            
            if trade_data.symbol in self._delay:                            
                if cur_time < trade_data.time:
                    self._logger.warning("abnormal time: " + str(trade_data.time))
                else:
                    self._delay[trade_data.symbol].update(cur_time - trade_data.time)
            else:
                self._logger.warning("invalid trade data: " + trade_data.meta_str())                            
        except Exception as e:
            self._logger.warning(traceback.format_exc())                        
            
def trade_test():
    data_type_list = [DATA_TYPE.TRADE]    

    SYS_CONFIG = get_config(config_file = (os.getcwd() + get_dir_seprator() + "sys_config.json"))

    symbol_config = get_config(config_file = (os.getcwd() + "/symbol_list.json"))
    symbol_list = symbol_config["symbol_list"]

    if SYS_CONFIG["net_server"] == "redis":
        net_type = NET_SERVER_TYPE.REDIS
        net_config_file_name = os.getcwd() + get_dir_seprator() + "redis_config.json"
    elif SYS_CONFIG["net_server"] == "kafka":
        net_type = NET_SERVER_TYPE.KAFKA
        net_config_file_name = os.getcwd() + get_dir_seprator() + "kafka_config.json"
    else:
        print("Error!Unknown Net Type")

    net_config = get_config(config_file = net_config_file_name)

    test_obj = TradeTest(data_type_list=data_type_list, config=net_config,
                         symbol_list=symbol_config["symbol_list"], 
                        net_server_type=net_type)

    test_obj.start()

if __name__ == "__main__":
    trade_test()