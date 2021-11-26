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

import os
sys.path.append(os.getcwd())
from Logger import *


from pprint import pprint

kline1_topic = "KLINE"
#kline5_topic = "KLINE5"
kline60_topic = "SLOW_KLINE"
#klineday_topic = "KLINEd"
total_kline_type = [kline1_topic, kline60_topic]

g_redis_config_file_name = os.path.dirname(os.path.abspath(__file__))+ "/kline_redis_config.json"

def get_config(logger = None, config_file=""):    
    json_file = open(config_file,'r')
    json_dict = json.load(json_file)
    if logger is not None:
        logger._logger.info("\n******* config *******\n" + str(json_dict))
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
            
    def publish_kline(self, kline_type, topic, msg):
        try:
            print("MiddleConnector publish_kline")            
        except Exception as e:
            self._logger.warning("[E] publish_kline: %s" % (traceback.format_exc()))                                
          
class KafkaConn(MiddleConnector):
    def __init__(self, kline_main, config:dict, logger = None, debug=False):
        self._server_list = config["server_list"]
        self._logger = logger
        self._kline_main = kline_main
        self._producer = KafkaProducer(bootstrap_servers=self._server_list)
        self._consumer = KafkaConsumer(bootstrap_servers=self._server_list, auto_offset_reset='latest')
                
        if self._producer.bootstrap_connected():
            self._logger.info("Producer Connect %s Successfully" % (str(self._server_list)))
        else:
             self._logger.warning("Producer Not Connected %s" % (str(self._server_list)))
             
        if self._consumer.bootstrap_connected():
            self._logger.info("Consumer Connect %s Successfully" % (str(self._server_list)))
        else:
             self._logger.warning("Consumer Not Connected %s" % (str(self._server_list)))             
                         
    def _listen_main(self):
        try:            
            while True:
                try:  
                    self._consumer.subscribe(topics="trade")
                    for msg in self._consumer:

                        self._logger.info(msg.value)

                        trade_data =  json.loads(msg.value)
                        symbol = trade_data["Symbol"]
                        exchange = trade_data["Exchange"]
                        trade_topic = symbol+ "_" + exchange
                        
                        self._kline_main.__topic_list.setdefault(trade_topic, KLine(slow_period=self.__slow_period, Logger= self._logger))                    
                        
                        # Update Local KLine Service
                        kline = self._kline_main.__topic_list[trade_topic]
                        kline.new_trade(exg_time=to_datetime(trade_data["Time"]), price=float(trade_data["LastPx"]), volume=float(trade_data["Qty"]))

                except Exception as e:
                    self._logger.warning(traceback.format_exc())
                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def publish(self, topic:str, msg:str):
        try:
            if self._producer.bootstrap_connected() or True:
                self._producer.send(topic, value=bytes(msg.encode()))
                # self._logger.info(topic + " " + msg)
            else:
                self._logger.warning("Producer Not Connected %s, %s " % (str(self._server_list), topic))
        except Exception as e:
            self._logger.warning("[E] publish_msg: \n%s" % (traceback.format_exc()))    

    def publish_kline(self, kline_type, topic, msg):
        try:
            self._logger.info(msg)   
            self.publish("trade", msg)                 
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

                            # Update Local KLine Service
                            kline = self._kline_main.__topic_list[trade_topic]
                            kline.new_trade(exg_time=to_datetime(trade_data["Time"]), price=float(trade_data["LastPx"]), volume=float(trade_data["Qty"]))

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

    def publish_kline(self, kline_type, topic, msg):
        try:
            self.publish(channel=f"{kline_type}x|{topic}", message = msg)

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

    def _update_klines(self, klines, klinetime, price, volume):
        try:
            ts = int((klinetime - self.epoch).total_seconds())
            if len(klines) == 0 or ts > klines[-1][0]:
                # new kline
                kline = [ts, price, price, price, price, volume, 1.0]
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

    def new_trade(self, exg_time: datetime.datetime, price: float, volume: float):
        try:
            # 1min kline
            wait_secs = float(exg_time.strftime("%S.%f"))
            tval = exg_time - datetime.timedelta(seconds=wait_secs)
            self._update_klines(self.klines[kline1_topic], tval, price, volume)

            # 60min kline
            wait_mins = float(tval.strftime("%M"))
            tval = tval - datetime.timedelta(minutes=wait_mins)
            self._update_klines(self.klines[kline60_topic], tval, price, volume)
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
            
            if is_redis:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"                
            else:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"
            self._config = get_config(logger=self._logger, config_file=self._config_name)
            
            if is_redis:
                self._connector = RedisConn(self, self._config, is_redis, self._logger, self._is_debug)
            else:
                self._connector = KafkaConn(self, self._config, is_redis, self._logger, self._is_debug)
                               
            self.__slow_period = int(slow_period)
            self.__mode = running_mode
            self.__verification_tag = False
            self.__topic_list = dict()

            self._publish_count_dict = {
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            
            self._connector.start_listen_data()

            for kline_type in total_kline_type:
                self._publish_count_dict[kline_type] = {}

            self._timer_secs = 10
            # self.__data_recover()

            self.__loop = asyncio.get_event_loop()
            # self.__task = asyncio.gather(self.__kline_updater(), self.__auto_delist(), self._log_updater())
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

    # def __data_recover(self):
    #     try:
    #         kline_keys = self.__svc_marketdata.hkeys(kline1_topic)
    #         for kline_type in total_kline_type:
    #             pipeline = self.__svc_marketdata.pipeline(False)

    #             for key in kline_keys:
    #                 self.__topic_list.setdefault(key.decode(), KLine(slow_period=self.__slow_period, Logger= self._logger))
    #                 pipeline.hget(kline_type, key=key.decode())

    #             datas = pipeline.execute()

    #             index = 0
    #             for data in datas:
    #                 kline_obj = self.__topic_list[kline_keys[index].decode()]
    #                 if data:
    #                     kline_obj.recover_kline(kline_data=json.loads(data), kline_type=kline_type)
    #                 index += 1
    #     except Exception as e:
    #         self._logger.warning("[E]__data_recover: " + traceback.format_exc())
            
    # def __match_listener(self):
    #     while True:
    #         try:
    #             __pubsub_marketdata = self.__svc_marketdata.pubsub()
    #             __pubsub_marketdata.psubscribe("TRADEx*")

    #             for marketdata in __pubsub_marketdata.listen():
    #                 # self._logger.info("marketdata: ")
    #                 # self._logger.info(marketdata)

    #                 if marketdata["type"] == "pmessage":
    #                     # MarketMatch Resolution
    #                     trade_topic = marketdata["channel"].decode().split("|")[1]
    #                     trade_data = json.loads(marketdata["data"])
    #                     self.__topic_list.setdefault(trade_topic, KLine(slow_period=self.__slow_period, Logger= self._logger))

    #                     # Update Local KLine Service
    #                     kline = self.__topic_list[trade_topic]
    #                     kline.new_trade(exg_time=to_datetime(trade_data["Time"]), price=float(trade_data["LastPx"]), volume=float(trade_data["Qty"]))

    #         except Exception as e:
    #             self._logger.warning("[E]__match_listener: " + traceback.format_exc())

    def _update_statistic_info(self, kline_type, topic):
        try:
            if kline_type in self._publish_count_dict:
                if topic in self._publish_count_dict[kline_type]:
                    self._publish_count_dict[kline_type][topic] += 1
                else:
                    self._publish_count_dict[kline_type][topic] = 1                

        except Exception as e:
            self._logger.warning("[E]_update_statistic_info: " + traceback.format_exc())
        

    # def __redis_hmset(self, marketdata_pipe: redis.client.Pipeline, data: dict, kline_type: str):
    #     try:
    #         if self.__mode == "PRODUCTION":
    #             marketdata_pipe.hmset(kline_type, data)
    #         else:
    #             for topic, kline in data.items():
    #                 self._logger.info(f"{kline_type}/{topic}\n" f" {kline}")

    #     except Exception as e:
    #         self._logger.warning("[E]__redis_hmset: " + traceback.format_exc())

    # async def __auto_delist(self):
    #     try:
    #         while True:
    #             exist_keys = self.__svc_marketdata.keys("DEPTHx*")
    #             live_topics = set()
    #             for key in exist_keys:
    #                 live_topics.add(key.decode().replace("DEPTHx|", ""))
    #             now_topics = set(self.__topic_list.keys())

    #             pipeline = self.__svc_marketdata.pipeline(False)
    #             for expire_key in now_topics.difference(live_topics):
    #                 if self.__mode == "PRODUCTION":
    #                     for kline_type in total_kline_type:
    #                         pipeline.hdel(kline_type, expire_key)
    #                 else:
    #                     self._logger.info(f"KLINE/{expire_key} Expired")

    #                 self.__topic_list.pop(expire_key, None)

    #             pipeline.execute(False)

    #             await asyncio.sleep(3600)  # Scan every hour
    #     except Exception as e:
    #         self._logger.warning("[E]__auto_delist: " + traceback.format_exc())
    
    async def _log_updater(self):
        try:
            while True:
                self.print_publish_info()
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

                    # Create pipeline of redis
                    # pipeline = self.__svc_marketdata.pipeline(False)

                    topic_key_list = list(self.__topic_list.keys())
                    for topic in topic_key_list:
                        
                        kline = self.__topic_list[topic]
                        
                        for kline_type, klines in kline.klines.items():
                            self._connector.publish_kline(kline_type, topic, json.dumps(list(klines)[-1:]))
                            
                            self._update_statistic_info(kline_type, topic)

                    self.__verification_tag = (now_time.minute + 1) % 60
                else:
                    self._logger.info("__verification_tag: %d" % (self.__verification_tag))
        except Exception as e:
            self._logger.warning("[E]__kline_updater: " + traceback.format_exc())

if __name__ == '__main__':
    # 运行脚本：[exe] 60 PRODUCTION
    print(sys.argv)
    
    svc = KLineSvc(slow_period=60, running_mode=sys.argv[2], is_redis=False, is_debug=False)
    
    if len(sys.argv) == 3:
        svc = KLineSvc(slow_period=sys.argv[1], running_mode=sys.argv[2])
    else:
        svc = KLineSvc(slow_period=2, running_mode="DEBUG")

