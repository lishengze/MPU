
import numpy
import sys
import time
import traceback
import datetime

from logging import log
from net_server import *
from data_struct import *
from tool import *

from kafka.admin.client import KafkaAdminClient
from kafka.admin import KafkaAdminClient, NewTopic

from kafka import KafkaProducer, consumer
from kafka import KafkaConsumer
from kafka import KafkaClient
from kafka import TopicPartition

from Logger import *

class KafkaServer(NetServer):
    def __init__(self, config:dict,  depth_processor=None, kline_processor=None, trade_processor=None,serializer_type: SERIALIXER_TYPE = SERIALIXER_TYPE.PROTOBUF, logger=None, debug=False):
        try:        
            super().__init__(depth_processor, kline_processor, trade_processor, serializer_type=serializer_type, logger=logger, debug=debug)

            self._server_list = config["server_list"]     
            
            self._kafka_depth_update_count = config["depth_update_count"]
            self._kafka_curr_pubed_update_count = {}    
                               
            self._client = KafkaAdminClient(bootstrap_servers=self._server_list, client_id='test')
                        
            self._producer = KafkaProducer(bootstrap_servers=self._server_list)        
            if self._producer.bootstrap_connected():
                self._logger.info("Producer Connect %s Successfully" % (str(self._server_list)))
            else:
                self._logger.warning("Producer Not Connected %s" % (str(self._server_list)))    
                        
            self._consumer = KafkaConsumer(group_id='test', bootstrap_servers=self._server_list)
            if self._consumer.bootstrap_connected():
                self._logger.info("Consumer Connect %s Successfully" % (str(self._server_list)))
            else:
                self._logger.warning("Consumer Not Connected %s" % (str(self._server_list)))              
            
            self._topic_list = []

            self._sub_topics = list()
                        
        except Exception as e:
            if self._logger:
                self._logger.warning("[E] __init__: \n%s" % (traceback.format_exc()))   

    def subcribe_topics(self):
        try:
            created_topics = self.get_created_topic()
            

            sub_topics = list()
            
            for topic in self._sub_topics:
                if created_topics.find(topic) != -1:
                    sub_topics.append(topic)

            self._logger.info("create topics: %s" % (str(created_topics)))
            self._logger.info("req  sub topics: %s" % (str(self._sub_topics)))
            self._logger.info("real sub topics: %s" % (str(sub_topics)))                    
            
            if len(sub_topics) > 0:
                self._logger.info("sub_topics: %s\n" % (str(sub_topics)))
                self._consumer.subscribe(topics=sub_topics)
                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())        

    def set_all_meta(self, symbol_list:list, exchange_list):
        try:
            for symbol in symbol_list:
                for exchange in exchange_list:
                    self._sub_topics.append(self._get_depth_topic(symbol, exchange))
                    self._sub_topics.append(self._get_kline_topic(symbol, exchange))
                    self._sub_topics.append(self._get_trade_topic(symbol, exchange))
                    
            self.subcribe_topics()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def set_meta(self, symbol_list:list, exchange_list:list, data_type:list):
        try:
            for symbol in symbol_list:
                for exchange in exchange_list:
                    if DATA_TYPE.DEPTH in data_type:                        
                        self._sub_topics.append(self._get_depth_topic(symbol, exchange))
                        
                    if DATA_TYPE.KLINE in data_type:  
                        self._sub_topics.append(self._get_kline_topic(symbol, exchange))
                        
                    if DATA_TYPE.TRADE in data_type:  
                        self._sub_topics.append(self._get_trade_topic(symbol, exchange))
                    
            self.subcribe_topics()
        except Exception as e:
            self._logger.warning(traceback.format_exc())        
            
    def set_depth_meta(self, symbol_list:list, exchange_list):
        try:
            for symbol in symbol_list:
                for exchange in exchange_list:
                    self._sub_topics.append(self._get_depth_topic(symbol, exchange))
                    
            self.subcribe_topics()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
        
    def set_kline_meta(self, symbol_list:list, exchange_list):
        try:
            for symbol in symbol_list:
                for exchange in exchange_list:
                    self._sub_topics.append(self._get_kline_topic(symbol, exchange))
                    
            self.subcribe_topics()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
    
    def set_trade_meta(self, symbol_list:list, exchange_list):
        try:
            for symbol in symbol_list:
                for exchange in exchange_list:
                    self._sub_topics.append(self._get_trade_topic(symbol, exchange))
                    
            self.subcribe_topics()
        except Exception as e:
            self._logger.warning(traceback.format_exc())
    
    def _listen_main(self):
        try:            
            self._logger.info("------ listen begin -------")
            while True:
                try:
                    for msg in self._consumer:
                        print(msg)
                        # data_type = self._get_data_type(msg.topic)
                        
                        # if data_type == DEPTH_TYPE:
                        #     self.process_depth(msg)
                            
                        # if data_type == KLINE_TYPE:
                        #     self.process_kline(msg)
                            
                        # if data_type == TRADE_TYPE:
                        #     self.process_trade(msg)                                                        

                except Exception as e:
                    self._logger.warning(traceback.format_exc())
                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())
    
    def process_depth(self, msg):
        try:
            depth_quote = self.serializer.decode_depth(msg)
            
            if self._depth_processor:
                self._depth_processor.process_depth_data(depth_quote)
                                                 
        except Exception as e:
            self._logger.warning(traceback.format_exc())                                  

    def process_kline(self, msg):
        try:
            kline_data = self.serializer.decode_kline(msg)
            
            if self._kline_processor:
                self._kline_processor.process_kline_data(kline_data)
        except Exception as e:
            self._logger.warning(traceback.format_exc())  
    
    def process_trade(self, msg):
        try:
            trade_data = self.serializer.decode_trade(msg)
            
            if self._trade_processor:
                self._trade_processor.process_trade_data(trade_data)
                                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())         
              
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
            
    def _publish_msg(self, topic:str, msg:str):
        try:
            if self._producer.bootstrap_connected() or True:
                self.check_topic(topic)
                
                # print(topic)
                # print(msg)
                
                if type(msg) == str:
                    msg = bytes(msg.encode())
                self._producer.send(topic, value=msg)
                # self._logger.info(topic + " " + msg)
            else:
                self._logger.warning("Producer Not Connected %s, %s " % (str(self._server_list), topic))
        except Exception as e:
            self._logger.warning("[E] publish_msg: \n%s" % (traceback.format_exc()))    
            
    def _get_depth_topic(self, symbol, exchange):
        try:
            return DEPTH_TYPE + TYPE_SEPARATOR + symbol+ SYMBOL_EXCHANGE_SEPARATOR  + exchange
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def _get_kline_topic(self, symbol, exchange):
        try:
            return KLINE_TYPE + TYPE_SEPARATOR + symbol + SYMBOL_EXCHANGE_SEPARATOR + exchange            
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def _get_trade_topic(self, symbol, exchange):
        try:
            return TRADE_TYPE + TYPE_SEPARATOR + symbol
        except Exception as e:
            self._logger.warning(traceback.format_exc())
                                
    def _get_data_type(self, topic):
        try:
            if topic.find(DEPTH_TYPE) != -1:
                return DEPTH_TYPE
            elif topic.find(KLINE_TYPE) != -1:
                return KLINE_TYPE
            elif topic.find(TRADE_TYPE) != -1:
                return TRADE_TYPE
            else:
                return None
        except :
            self._logger.warning(traceback.format_exc())

    def publish_depth(self, snap_quote:SDepthQuote, update_quote:SDepthQuote):   
        try:
            symbol = snap_quote.symbol
            
            if symbol not in self._kafka_curr_pubed_update_count:
                self._kafka_curr_pubed_update_count[symbol] = self._kafka_depth_update_count
                
            if self._kafka_curr_pubed_update_count[symbol] < self._kafka_depth_update_count:                
                self._publish_msg(topic=self._get_depth_topic(update_quote.symbol, update_quote.exchange), \
                                  msg=self.serializer.encode_depth(update_quote))
                self._kafka_curr_pubed_update_count[symbol] += 1
            else:
                if snap_quote:
                    self._publish_msg(topic=self._get_depth_topic(snap_quote.symbol, snap_quote.exchange),\
                                      msg=self.serializer.encode_depth(snap_quote))
                    self._kafka_curr_pubed_update_count[symbol] = 0
                else:
                    self._logger.info("snap_quote is None")   
                    
        except Exception as e:
            self._logger.warning(traceback.format_exc())                              
    
    def publish_trade(self, trade:STradeData):
        try:
            self._publish_msg(topic=self._get_trade_topic(trade.symbol, trade.exchange), \
                                        msg=self.serializer.encode_trade(trade))
        except Exception as e:
            self._logger.warning(traceback.format_exc())            

    def publish_kline(self, kline:SKlineData):
        try:
            self._publish_msg(topic=self._get_kline_topic(kline.symbol, kline.exchange), \
                              msg=self.serializer.encode_kline(kline))
        except Exception as e:
            self._logger.warning(traceback.format_exc())     
            
class TestKafka:
    def __init__(self, data_type_list:list) -> None:
        self._server_address = ["127.0.0.1:9117"]
        self._logger = Logger(program_name="")
        
        self._logger = self._logger._logger
        
        self._config = {
            "server_list": ["127.0.0.1:9117"],
            "depth_update_count":5
        }
        self._kafka_server = KafkaServer(config = self._config, depth_processor=self, kline_processor=self, trade_processor=self, \
                                         serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
        self._symbol_list = ["BTC_USDT"]
        self._exchange_list = ["FTX"]
        self._data_type_list = data_type_list
        self._kafka_server.set_meta(symbol_list=self._symbol_list, \
                                    exchange_list=self._exchange_list, \
                                    data_type=self._data_type_list)
    
    def start(self):
        self._kafka_server.start_listen_data()
        
    def process_depth_data(self, depth_quote:SDepthQuote):
        try:            
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
        except Exception as e:
            self._logger.warning(traceback.format_exc())                        
            
def test_kafka():
    data_type_list = [TRADE_TYPE] 
    kafka_obj = TestKafka(data_type_list=data_type_list)
    kafka_obj.start()
        
if __name__ == "__main__":
    test_kafka()
    