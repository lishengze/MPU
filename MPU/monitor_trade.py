from grpc import server
from kafka.admin.client import KafkaAdminClient
import traceback
import sys
import os
import threading
import time

from sortedcontainers import SortedDict, sorteddict
from datetime import datetime
from collections import defaultdict

from kafka import KafkaProducer, consumer
from kafka import KafkaConsumer
from kafka import KafkaClient
from kafka import TopicPartition

from kafka.admin import KafkaAdminClient, NewTopic


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

ENV_TYPE = ""

from tool import *
from kafka_server import *
from redis_server import *
from data_struct import *

class TestKafka:
    def __init__(self, data_type_list:list) -> None:
        log_dir = os.path.dirname(os.path.abspath(__file__)) + get_dir_seprator() + "log" + get_dir_seprator() 

        self._logger = Logger(program_name="Monitor", log_dir=log_dir)
        
        self._logger = self._logger._logger
        
        server_config = get_config(config_file = (os.getcwd() + get_dir_seprator() + "kafka_config.json"), env_type=ENV_TYPE)

        print(server_config)
        self._logger.info(str(server_config))

        self._kafka_server = KafkaServer(config = server_config, depth_processor=self, kline_processor=self, trade_processor=self, \
                                         serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
        
        symbol_list_config = get_config(config_file = (os.getcwd() + get_dir_seprator() + "symbol_list.json"), env_type=ENV_TYPE)
        
        self._monitor_config = dict()
        self._monitor_config =  get_config(config_file = (os.getcwd() + get_dir_seprator() + "monitor_config.json"), env_type=ENV_TYPE)
        
        if "is_detail" not in self._monitor_config:
            self._monitor_config["is_detail"] = False


        self._symbol_list = symbol_list_config["symbol_list"]
        self._exchange_list = symbol_list_config["exchange_list"]
        
        self._data_type_list = data_type_list
        
        print(self._symbol_list)
        self._logger.info(self._symbol_list)

        print(self._data_type_list)
        self._kafka_server.set_meta(symbol_list=self._symbol_list, \
                                    exchange_list=self._exchange_list, \
                                    data_type=self._data_type_list)
        self._seq_no = -1
        
        self._trade_symbol_list = []

        self._symbol_trade_map = dict()
        self._symbol_depth_map = dict()
        self._symbol_kline_map = dict()

        for symbol in self._symbol_list:
            self._symbol_trade_map[symbol] = [0, 'Not Recorded']
            self._symbol_depth_map[symbol] = [0, 'Not Recorded']
            self._symbol_kline_map[symbol] = [0, 'Not Recorded']

        self._ping_secs = 10
        self._start_check_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self._end_check_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


    def start_timer(self):
        try:
            self._logger.info("start_timer\n")
            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
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

    def print_publish_info(self):
        try:
            self._end_check_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            self._logger.info("\nFrom %s to %s Publish Statics: "% 
                            (self._start_check_time,
                             self._end_check_time))

            for symbol in self._symbol_trade_map:
                info = "\nTrade: " + self._symbol_trade_map[symbol][1] + "." + symbol + ", time: " + get_str_time_from_nano_time(self._symbol_trade_map[symbol][0]) \
                     + "\nDepth: " + self._symbol_depth_map[symbol][1] + "." + symbol + ", time: " + get_str_time_from_nano_time(self._symbol_depth_map[symbol][0]) \
                     + "\nKline: " + self._symbol_kline_map[symbol][1] + "." + symbol + ", time: " + get_str_time_from_nano_time(self._symbol_kline_map[symbol][0])
                self._logger.info(info)

            self._logger.info("\n")

            self._start_check_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        except Exception as e:
            self._logger.warning(traceback.format_exc())            

    def start(self):
        self._kafka_server.start_listen_data()
        self.start_timer()
    
    def check_seq(self, seq_no):
        if self._seq_no == -1:
            self._seq_no = seq_no
        elif self._seq_no + 1 != seq_no:
            print("local seq: %d, new seq: %d" % (self._seq_no, seq_no))
        
        self._seq_no = seq_no
        
        
    def process_depth_data(self, depth_quote:SDepthQuote):
        try:    
            # self.check_seq(depth_quote.sequence_no)        
            # pass
            if self._monitor_config["is_detail"]:
                self._logger.info(depth_quote.meta_str())
            self._symbol_depth_map[depth_quote.symbol] = [depth_quote.origin_time, depth_quote.exchange]
            
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def process_kline_data(self, kline_data:SKlineData):
        try:            
            if self._monitor_config["is_detail"]:
                self._logger.info(kline_data.meta_str())
                
            self._symbol_kline_map[kline_data.symbol] = [kline_data.time, kline_data.exchange]
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def process_trade_data(self, trade_data:STradeData):
        try:            
            # self.check_seq(trade_data.sequence_no)
            if self._monitor_config["is_detail"]:
                self._logger.info(trade_data.meta_str())
            
            if trade_data.symbol not in self._trade_symbol_list:
                # print(trade_data.meta_str())
                self._trade_symbol_list.append(trade_data.symbol)
            
            self._symbol_trade_map[trade_data.symbol] = [trade_data.time, trade_data.exchange]

            # print(trade_data.meta_str())
            
        except Exception as e:
            self._logger.warning(traceback.format_exc())                        
            
def test_kafka():
    data_type_list = [DATA_TYPE.TRADE, DATA_TYPE.DEPTH, DATA_TYPE.KLINE] 
    kafka_obj = TestKafka(data_type_list=data_type_list)
    kafka_obj.start()
        
if __name__ == "__main__":
    if len(sys.argv) == 2:
        ENV_TYPE = sys.argv[1]
        
    print("\nENV_TYPE: " + ENV_TYPE)
        
    test_kafka()
    