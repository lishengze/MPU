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
sys.path.append(os.getcwd())
from Logger import *


from pprint import pprint

kline1_topic = "KLINE"
#kline5_topic = "KLINE5"
kline60_topic = "SLOW_KLINE"
#klineday_topic = "KLINEd"
total_kline_type = [kline1_topic, kline60_topic]

TYPE_SEPARATOR = "-"
SYMBOL_EXCHANGE_SEPARATOR = "."
DEPTH_HEAD = "DEPTHx"
DEPTH_UPDATE_HEAD = "UPDATEx"
TRADE_HEAD = "TRADEx"

g_redis_config_file_name = os.path.dirname(os.path.abspath(__file__))+ "/kline_redis_config.json"

def get_datetime_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def get_config(logger = None, config_file=""):    
    json_file = open(config_file,'r')
    json_dict = json.load(json_file)
    if logger is not None:
        logger.info("\n******* config *******\n" + str(json_dict))
    else:
        print("\n******* config *******\n" + str(json_dict))

    return json_dict
    
def to_datetime(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")

class MiddleConnector:
    def __init__(self, kline_main, config, is_redis = False , logger = None, debug=False):
        self._is_redis = is_redis
        self._logger = logger
        self._kline_main = kline_main
            
    def start_listen_data(self):
        try:            
            self._istener_thread = threading.Thread(target=self._listen_main, name="SUBCRIBERMatch")
            self._istener_thread.start()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def _listen_main(self):
        try:            
            print("")
        except Exception as e:
            self._logger.warning(traceback.format_exc())
        
    def publish(self, topic:str, msg:str):
        try:
            print("MiddleConnector publish")            
        except Exception as e:
            self._logger.warning("[E] publish: %s" % (traceback.format_exc()))    
            
    def publish_kline(self, kline_type, symbol, exchange, msg):
        try:
            print("MiddleConnector publish_kline")            
        except Exception as e:
            self._logger.warning("[E] publish_kline: %s" % (traceback.format_exc()))                                
          
class KafkaConn(MiddleConnector):
    def __init__(self, kline_main, config:dict, logger = None, debug=False):
        self._server_list = config["server_list"]
        self._logger = logger
        self._kline_main = kline_main
        self._topic_list = []
        self._producer = KafkaProducer(bootstrap_servers=self._server_list)
        self._consumer = KafkaConsumer(bootstrap_servers=self._server_list, auto_offset_reset='latest')
        self._client = KafkaAdminClient(bootstrap_servers=self._server_list, client_id='test')
                
        if self._producer.bootstrap_connected():
            self._logger.info("Producer Connect %s Successfully" % (str(self._server_list)))
        else:
             self._logger.warning("Producer Not Connected %s" % (str(self._server_list)))
             
        if self._consumer.bootstrap_connected():
            self._logger.info("Consumer Connect %s Successfully" % (str(self._server_list)))
        else:
             self._logger.warning("Consumer Not Connected %s" % (str(self._server_list)))     

    def create_topic(self, topic):
        try:
    
            self._logger.info("Original TopicList: \n%s" % (str(self.get_created_topic())))
    
            topic_list = []
            topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=3))
            self._client.create_topics(new_topics=topic_list, validate_only=False)
        
        
            self._logger.info("After Create Topic %s, TopicList: \n%s" % (topic, str(self.get_created_topic())))                
            return self._consumer.topics()
        except Exception as e:
            self._logger.warning("[E] create_topic: \n%s" % (traceback.format_exc()))   
                     
    def get_created_topic(self):
        try:
            return self._consumer.topics()
        except Exception as e:
            self._logger.warning("[E] get_created_topic: \n%s" % (traceback.format_exc()))            
        
    def check_topic(self, topic):
        try:
            if topic in self._topic_list:
                return True
            else:
                create_topics = self.get_created_topic()
                if topic not in create_topics:
                    self.create_topic(topic)   
                else:
                    self._topic_list.append(topic) 
        except Exception as e:
            self._logger.warning("[E] check_topic: \n%s" % (traceback.format_exc()))  
                         
    def get_trade_topics(self):
        try:
            all_topics = self.get_created_topic()   
            
            # self._logger.info("All Topics: %s " % (str(all_topics)))         
            trade_topics = []
            
            for topic in all_topics:
                if TRADE_HEAD in topic:
                    trade_topics.append(topic)
                    
            return trade_topics
        except Exception as e:
            self._logger.warning("[E] get_trade_topics: \n%s" % (traceback.format_exc()))         
                         
    def _listen_main(self):
        try:            
            self._logger.info("------ listen begin -------")
            while True:
                try:  
                    trade_topics = self.get_trade_topics()
                    if len(trade_topics) !=0:
                        # self._logger.info("Trade Topics: \n%s" % (str(self.get_trade_topics())))
                        self._consumer.subscribe(topics=self.get_trade_topics())
                    else:
                        self._logger.info("Trade Topics Is Empty!")
                        
                    for msg in self._consumer:

                        # self._logger.info(msg.value)

                        trade_data =  json.loads(msg.value)
                        symbol = trade_data["Symbol"]
                        exchange = trade_data["Exchange"]
                        trade_topic = symbol+ SYMBOL_EXCHANGE_SEPARATOR + exchange
                        
                        self._kline_main.update_kline(trade_topic, to_datetime(trade_data["Time"]), 
                                                      float(trade_data["LastPx"]), float(trade_data["Qty"]), 
                                                      symbol, exchange)

                except Exception as e:
                    self._logger.warning(traceback.format_exc())
                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def publish(self, topic:str, msg:str):
        try:
            self.check_topic(topic)
            self._producer.send(topic, value=bytes(msg.encode()))
        except Exception as e:
            self._logger.warning("[E] publish: \n%s" % (traceback.format_exc()))    

    def publish_kline(self, kline_type, symbol, exchange, msg):
        try:            
            topic = kline_type + TYPE_SEPARATOR + symbol + SYMBOL_EXCHANGE_SEPARATOR + exchange
            self._logger.info( get_datetime_str() + "  " + topic + "\n" + msg) 
            self.publish(topic, msg)                 
        except Exception as e:
            self._logger.warning("[E] KafkaConn publish_kline: %s" % (traceback.format_exc()))                                
                        
class RedisConn(MiddleConnector):
    def __init__(self, kline_main, config:dict, logger = None, debug=False):
        self.__debug = debug
        self._kline_main = kline_main
        self._server_list = config["server_list"]
        self.__redis_conn = redis.Redis(host=config["HOST"],
                                        port=config["PORT"],
                                        password=config["PWD"])
        self.__redis_pubsub = self.__redis_conn.pubsub()
        self._logger = logger
            
    def _listen_main(self):
        try:
            while True:
                try:
                    __pubsub_marketdata = self.__redis_conn.pubsub()
                    __pubsub_marketdata.psubscribe("TRADEx*")

                    for marketdata in __pubsub_marketdata.listen():
                        # self._logger.info("marketdata: ")
                        # self._logger.info(marketdata)

                        if marketdata["type"] == "pmessage":
                            # MarketMatch Resolution
                            trade_topic = marketdata["channel"].decode().split("|")[1]
                            trade_data = json.loads(marketdata["data"])
                            self._kline_main.__topic_list.setdefault(trade_topic, KLine(slow_period=self.__slow_period, Logger= self._logger))

                            symbol = trade_data["Symbol"]
                            exchange = trade_data["Exchange"]
                        
                            # Update Local KLine Service
                            kline = self._kline_main.__topic_list[trade_topic]
                            kline.new_trade(exg_time=to_datetime(trade_data["Time"]), price=float(trade_data["LastPx"]), volume=float(trade_data["Qty"]), symbol=symbol, exchange=exchange)

                except Exception as e:
                    self._logger.warning(traceback.format_exc())
        except Exception as e:
            self._logger.warning(traceback.format_exc())        
                     
    def publish(self, topic: str, message):
        try:
            if not self.__debug:
                self.__redis_conn.publish(channel=topic, message=message)
            else:
                self._logger.info(f"{topic}\n{message}")
        except Exception as e:
            self._logger.warning("[E] __publish: %s" % (traceback.format_exc()))

    def publish_kline(self, kline_type, symbol, exchange, msg):
        try:
            self.publish(channel=f"{kline_type}x|{symbol}.{exchange}", message = msg)

            print("RedisConn publish_kline")            
        except Exception as e:
            self._logger.warning("[E] RedisConn publish_kline: %s" % (traceback.format_exc()))                                
            
class KLine:
    def __init__(self, slow_period: int = 60, Logger=None):
        try:
            self._logger = Logger
            self.epoch = datetime.datetime(1970,1,1)
            # T: Time/ O: Open/ H: High/ L: Low/ C: Close/ V: Volume/ Q: Quotes
            self.klines = {}
            for kline_type in total_kline_type:
                self.klines[kline_type] = deque(maxlen=1440)
        except Exception as e:
            self._logger.warning("[E]KLine: " + traceback.format_exc())

    def _update_klines(self, klines, klinetime, price, volume, symbol, exchange):
        try:
            ts = int((klinetime - self.epoch).total_seconds())
            if len(klines) == 0 or ts > klines[-1][0]:
                # new kline
                kline = [ts, price, price, price, price, volume, 1.0, symbol, exchange]
                klines.append(kline)
            else:
                kline = klines[-1]
                kline[4] = price
                kline[5] += volume
                kline[6] += 1.0
                if price > kline[2]:
                    kline[2] = price
                if price < kline[3]:
                    kline[3] = price
                klines[-1] = kline
        except Exception as e:
            self._logger.warning("[E]_update_klines: " + traceback.format_exc())

    def new_trade(self, exg_time: datetime.datetime, price: float, volume: float, symbol:str, exchange:str):
        try:
            # 1min kline
            wait_secs = float(exg_time.strftime("%S.%f"))
            tval = exg_time - datetime.timedelta(seconds=wait_secs)
            self._update_klines(self.klines[kline1_topic], tval, price, volume, symbol, exchange)

            # 60min kline
            wait_mins = float(tval.strftime("%M"))
            tval = tval - datetime.timedelta(minutes=wait_mins)
            self._update_klines(self.klines[kline60_topic], tval, price, volume, symbol, exchange)
            
        except Exception as e:
            self._logger.warning("[E]new_trade: " + traceback.format_exc())

    def recover_kline(self, kline_data: list, kline_type: str):
        try:
            assert kline_type in total_kline_type
            self.klines[kline_type] = deque(kline_data, maxlen=1440)
        except Exception as e:
            self._logger.warning("[E]recover_kline: " + traceback.format_exc())
              
class KLineSvc:
    def __init__(self, slow_period: int = 60, running_mode: str = "DEBUG", is_redis:bool = False, is_debug:bool = False):
        try:
            self.logger = Logger(program_name="")
            self._logger = self.logger._logger
            self._logger.info("slow_period: %d, running_mode: %s" % (int(slow_period), running_mode))    
            self._is_debug = is_debug 
            self._name = "KlineSvc"
            self.__topic_list = dict()
            self.__slow_period = int(slow_period)
            self.__mode = running_mode
            self.__verification_tag = False
                        
            self._publish_count_dict = {
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }

            for kline_type in total_kline_type:
                self._publish_count_dict[kline_type] = {}

            self._timer_secs = 10
                                    
            if is_redis:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"                
            else:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"
            self._config = get_config(logger=self._logger, config_file=self._config_name)
            
            if is_redis:
                self._connector = RedisConn(self, self._config, self._logger, self._is_debug)
            else:
                self._connector = KafkaConn(self, self._config, self._logger, self._is_debug)
                               
            self._connector.start_listen_data()
            self.__loop = asyncio.get_event_loop()
            self.__task = asyncio.gather(self.__kline_updater(), self._log_updater())
            self.__loop.run_until_complete(self.__task)
            
            while True:
                time.sleep(3)
                
        except Exception as e:
            self._logger.warning("[E]__init__: " + traceback.format_exc())

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

    def _update_statistic_info(self, kline_type, topic):
        try:
            if kline_type in self._publish_count_dict:
                if topic in self._publish_count_dict[kline_type]:
                    self._publish_count_dict[kline_type][topic] += 1
                else:
                    self._publish_count_dict[kline_type][topic] = 1                

        except Exception as e:
            self._logger.warning("[E]_update_statistic_info: " + traceback.format_exc())
        
    def set_default_trade_topic(self, topic):
        try:
            self.__topic_list.setdefault(topic, KLine(slow_period=self.__slow_period, Logger= self._logger))                                
        except Exception as e:
            self._logger.warning("[E]set_default_trade_topic: " + traceback.format_exc())   
            
    def get_kline_atom(self, trade_topic):
        try:
            if trade_topic in self.__topic_list:
                return self.__topic_list[trade_topic]
            else:
                return None                              
        except Exception as e:
            self._logger.warning("[E]get_kline_atom: " + traceback.format_exc())   
            
    def update_kline(self, trade_topic, exg_time, price, volume, symbol, exchange):
        try:
            if trade_topic not in self.__topic_list:
                self.__topic_list.setdefault(trade_topic, KLine(slow_period=self.__slow_period, Logger= self._logger))         
                        
            kline = self.__topic_list[trade_topic]
            kline.new_trade(exg_time=exg_time, price=price, volume=volume, symbol=symbol, exchange=exchange)
                                                           
        except Exception as e:
            self._logger.warning("[E]get_kline_atom: " + traceback.format_exc())   
            
                    
                            
        
    async def _log_updater(self):
        try:
            while True:
                # self.print_publish_info()
                await asyncio.sleep(self._timer_secs)
            
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

                    topic_key_list = list(self.__topic_list.keys())
                    for topic in topic_key_list:
                        
                        kline = self.__topic_list[topic]
                        
                        topic_atom_list = topic.split(SYMBOL_EXCHANGE_SEPARATOR)
                        symbol = topic_atom_list[0]
                        exchange = topic_atom_list[1]
                        
                        for kline_type, klines in kline.klines.items():
                            self._connector.publish_kline(kline_type, symbol, exchange, json.dumps(list(klines)[-1:]))
                            
                            self._update_statistic_info(kline_type, topic)

                    self.__verification_tag = (now_time.minute + 1) % 60
                else:
                    self._logger.info("__verification_tag: %d" % (self.__verification_tag))
        except Exception as e:
            self._logger.warning("[E]__kline_updater: " + traceback.format_exc())

if __name__ == '__main__':
    # 运行脚本：[exe] 60 PRODUCTION
    print(sys.argv)
    
    svc = KLineSvc(slow_period=60, running_mode="PRODUCTION", is_redis=False, is_debug=False)
    
    # if len(sys.argv) == 3:
    #     svc = KLineSvc(slow_period=sys.argv[1], running_mode=sys.argv[2])
    # else:
    #     svc = KLineSvc(slow_period=2, running_mode="DEBUG")

