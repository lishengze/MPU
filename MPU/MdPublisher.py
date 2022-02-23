"""
MdPublisher & MarketData Service:
    1. Define MarketData Service Standard:
        if raw Depth & Update cover full orderbook:
            Update anytime
        if raw Depth is limited and Update covers full orderbook:
            Update if in range(WORSTASK, WORSTBID)
        if raw Depth & Update both limited:
            Update if in range(WORSTASK, WORSTBID)
    2. Every MPU should refresh market depth data while len(ASK) or len(BID)<25
    3. Set All DEPTHx value expired after 2 days

    Version 1.1.2 | 2019.09.25
        Revised: Quality Control Mechanism(Raise Exception after message published)
        
    Version 1.1.1 | 2019.09.19
        Fix: Revised Update added to UPDATEx message
        Fix: Minor change for null situation handle(update dict)

    Version 1.1.0 | 2019.09.15
        Revised: Use SortedDict to improve performance(in AskDepth/BidDepth)

    Version 1.0.2 | 2019.09.14
        Revised: Full Depth while crossing with trade event

    Version 1.0.1 | 2019.09.05
        Add: Price-level identify mechanism(Quality Control)
        Revised: Improved Performance while combine DEPTHx

"""
from kafka.admin.client import KafkaAdminClient
import redis
import json
import operator
import logging
import requests
import traceback
import sys
from sortedcontainers import SortedDict, sorteddict
from datetime import datetime
from collections import defaultdict

from kafka import KafkaProducer, consumer
from kafka import KafkaConsumer
from kafka import KafkaClient
from kafka import TopicPartition

from kafka.admin import KafkaAdminClient, NewTopic

def get_depth_topic(symbol, exchange, logger=None):
    try:
        return f"DEPTHx|{symbol}.{exchange}"
    except Exception as e:
        if logger:
            logger.warning("[E] get_depth_topic: \n%s" % (traceback.format_exc()))   
        
def get_depth_update_topic(symbol, exchange, logger=None):
    try:
        return f"UPDATEx|{symbol}.{exchange}"
    except Exception as e:
        if logger:
            logger.warning("[E] get_depth_topic: \n%s" % (traceback.format_exc()))           
        
def get_trade_topic(symbol, exchange, logger=None):
    try:
        return f"TRADEx|{symbol}.{exchange}"
    except Exception as e:
        if logger:
            logger.warning("[E] get_depth_topic: \n%s" % (traceback.format_exc()))   
            
class KafkaConn:
    def __init__(self, config:dict, logger = None, debug=False):
        self._server_list = config["server_list"]
        self._logger = logger
        self._client = KafkaAdminClient(bootstrap_servers=self._server_list, client_id='test')
        self._producer = KafkaProducer(bootstrap_servers=self._server_list)        
        self._consumer = KafkaConsumer(group_id='test', bootstrap_servers=['server'])
        self._topic_list = []
        
        if self._producer.bootstrap_connected():
            self._logger.info("Connect %s Successfully" % (str(self._server_list)))
        else:
             self._logger.warning("Producer Not Connected %s" % (str(self._server_list)))        
             
    def create_topic(self, topic):
        try:
    
            self._logger.info("Original TopicList: \n%s" % (str(consumer.topics())))
    
            topic_list = []
            topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=3))
            self._client.create_topics(new_topics=topic_list, validate_only=False)
        
        
            self._logger.info("After Create Topic %s, TopicList: \n%s" % (topic, str(consumer.topics())))                
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
                create_topics = self.get_created_topic
                if topic not in create_topics:
                    pass
                    self._logger.warning("Producer Not Connected %s, %s " % (str(self._server_list), topic))
                    
        except Exception as e:
            self._logger.warning("[E] check_topic: \n%s" % (traceback.format_exc()))    
            
    def publish_msg(self, topic:str, msg:str):
        try:
            if self._producer.bootstrap_connected() or True:
                self._producer.send(topic, value=bytes(msg.encode()))
                # self._logger.info(topic + " " + msg)
            else:
                self._logger.warning("Producer Not Connected %s, %s " % (str(self._server_list), topic))
        except Exception as e:
            self._logger.warning("[E] publish_msg: \n%s" % (traceback.format_exc()))    
            
class RedisConn:
    def __init__(self, config:dict, logger = None, debug=False):
        self.__debug = debug
        self.__redis_conn = redis.Redis(host=config["HOST"],
                                        port=config["PORT"],
                                        password=config["PWD"])
        self.__redis_pubsub = self.__redis_conn.pubsub()
        self._logger = logger
                 
    def publish(self, channel: str, message):
        try:
            if not self.__debug:
                if "ETH_USDT" in channel:
                    self._logger.info("Redis publish: " + channel + ", " + message)
                self.__redis_conn.publish(channel=channel, message=message)
            else:
                self._logger.info(f"{channel}\n{message}")
        except Exception as e:
            self._logger.warning("[E] __publish: %s" % (traceback.format_exc()))

    def set(self, channel: str, message):
        try:
            if not self.__debug:  # All value in redis will expire after 2 days
                
                # self._logger.info("Redis set: " + channel + ", " + message)
                self.__redis_conn.set(name=channel, value=message, ex=172800)
            else:
                self._logger.info(f"{channel}\n{message}")
        except Exception as e:
            self._logger.warning("[E] __set: %s" % (traceback.format_exc()))

    def crossing_snapshot(self, channel: str, message):
        try:
            if not self.__debug:
                self.__redis_conn.hset(name="CROSSING_SNAPSHOT", key=channel, value=message)
            else:
                self._logger.info(f"CROSSING_SNAPSHOT/{channel}\n{message}")
        except Exception as e:
            self._logger.warning("[E] crossing_snapshot: %s" % (traceback.format_exc()))
              
class MiddleConn:
    def __init__(self, config:dict, is_redis = False , logger = None, debug=False):
        self._is_redis = is_redis
        self._logger = logger
        
        

        if is_redis:
            self._redis_con = RedisConn(config, self._logger, debug)
        else:
            self._logger.info(config)
            self._kafka_con = KafkaConn(config, self._logger, debug)  
            self._kafka_depth_update_count = config["depth_update_count"]
            self._kafka_curr_pubed_update_count = {}            
        
    def publish_depth(self, symbol, book, depth_json, update_json):
        try:
            if self._is_redis:
                self._redis_publish_depth(symbol, book, depth_json, update_json)
            else:
                self._kafka_publish_depth(symbol, book, depth_json, update_json)
            
        except Exception as e:
            self._logger.warning("[E] publish_depth: %s" % (traceback.format_exc()))    
            
    def publish_trade(self, trade_json):
        try:
            if not trade_json:
                return
            
            if self._is_redis:
                self._redis_publish_trade(trade_json)
            else:
                self._kafka_publish_trade(trade_json)
        except Exception as e:
            self._logger.warning("[E] publish_trade: %s" % (traceback.format_exc()))                    

    def _redis_publish_depth(self, symbol, book, depth_json, update_json):
        try:
            if depth_json:
                self._redis_con.set(channel=get_depth_topic(depth_json["Symbol"], depth_json["Exchange"], self._logger),
                                    message=json.dumps(depth_json))
                
                # self._redis_con.crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}", message = json.dumps(depth_json))
            
            if update_json:
                self._redis_con.publish(channel=get_depth_update_topic(depth_json["Symbol"], depth_json["Exchange"], self._logger), 
                                        message=json.dumps(update_json))

            # self._process_crossing_date(symbol, depth_json, book)
            
        except Exception as e:
            self._logger.warning("[E] _redis_publish_depth: %s" % (traceback.format_exc()))
            
    def _kafka_publish_depth(self, symbol, book, depth_json, update_json):
        try:
            if symbol not in self._kafka_curr_pubed_update_count:
                self._kafka_curr_pubed_update_count[symbol] = self._kafka_depth_update_count
                
            if self._kafka_curr_pubed_update_count[symbol] < self._kafka_depth_update_count:
                if update_json:
                    self._kafka_con.publish_msg(topic=get_depth_topic(depth_json["Symbol"], depth_json["Exchange"], self._logger),  
                                                msg=json.dumps(update_json))
                    self._kafka_curr_pubed_update_count[symbol] += 1
                else:
                    self._logger.info("update_json is None")
            else:
                if depth_json:
                    self._kafka_con.publish_msg(topic=get_depth_topic(depth_json["Symbol"], depth_json["Exchange"], self._logger),  
                                                msg=json.dumps(depth_json))
                    self._kafka_curr_pubed_update_count[symbol] = 0
                else:
                    self._logger.info("depth_json is None")                    
                
        except Exception as e:
            self._logger.warning("[E] _kafka_publish_depth: %s" % (traceback.format_exc()))
                                        
    def _redis_publish_trade(self, trade_json):
        try:            
            self._redis_con.publish(channel=get_trade_topic(trade_json["Symbol"], trade_json["Exchange"], self._logger), 
                                    message=json.dumps(trade_json))
                        
        except Exception as e:
            self._logger.warning("[E] _redis_publish_trade: ")
            self._logger.warning(traceback.format_exc())    
            
    def _kafka_publish_trade(self, trade_json):
        try:
            self._kafka_con.publish_msg(topic=get_trade_topic(trade_json["Symbol"], trade_json["Exchange"], self._logger),  
                                        msg=json.dumps(trade_json))
        except Exception as e:
            self._logger.warning("[E] _kafka_publish_trade: ")
            self._logger.warning(traceback.format_exc())                       
        
class Publisher:
    def __init__(self, exchange: str, config: dict, is_redis = False , exchange_topic: str = None, debug_mode: bool = False, logger=None):
        self.__debug = debug_mode
        self.__crossing_flag = dict()  # {"Symbol": "Date"}
        self._logger = logger

        self._connector = MiddleConn(config, is_redis, logger, debug_mode)
        
        self.__exchange = exchange
        self.__msg_seq = 0
        self.__msg_seq_symbol = defaultdict(int)

        if exchange_topic:
            self.__exchange_topic = exchange_topic
        else:
            self.__exchange_topic = exchange      # 形如 "BINANCE"

        self.__orderbook = dict()

    def _is_depth_invalid(self, depth):
        try:
            result = True
            if depth and "ASK" in depth and len(depth["ASK"]) != 0:
                result = False
                
            if depth and "BID" in depth and len(depth["BID"]) != 0:
                result = False
                                
            return result
        except Exception as e:
            self._logger.warning("[E] _is_depth_invalid: \n%s" % (traceback.format_exc()))     
            return True   
        
    def _get_cached_book(self, symbol, is_snapshot, depth_update):
        try:
            if symbol not in self.__orderbook:
                if is_snapshot:
                    self.__orderbook.setdefault(symbol, {"AskDepth": SortedDict(), "BidDepth": SortedDict()})
                    for px, qty in depth_update["ASK"].items():
                        self.__orderbook[symbol]["AskDepth"][float(px)] = qty

                    for px, qty in depth_update["BID"].items():
                        self.__orderbook[symbol]["BidDepth"][float(px)] = qty   
                        
                    self._logger.info("\nInit %s, Snap: %s " %(symbol, str(self.__orderbook[symbol])))
                else:
                    return None
                            
            book = self.__orderbook[symbol]
            return book
        except Exception as e:
            self._logger.warning("[E] _get_cached_book: \n%s" % (traceback.format_exc()))      
            return None

    
    def _update_depth_volume(self, depth_update, book):
        try:
            for side in depth_update.keys():
                if side == "ASK":
                    depth_side = "AskDepth"
                else:
                    depth_side = "BidDepth"

                for px, qty in depth_update[side].items():
                    if qty == 0:
                        book[depth_side].pop(px, None)
                    else:
                        book[depth_side][px] = qty
        except Exception as e:
            self._logger.warning("[E] _update_depth_volume: \n%s" % (traceback.format_exc()))          
                        
    def _quality_control(self, book, raise_exception, depth_update):
        try:
            revised_ask = dict()
            revised_bid = dict()    
            raise_exception_flag = False        
            if len(book["AskDepth"]) and len(book["BidDepth"]):

                best_ask = book["AskDepth"].peekitem(0)
                best_bid = book["BidDepth"].peekitem(-1)
                if best_bid >= best_ask:
                    if raise_exception:
                        # Solution 1: Raise Exception force WS broken
                        raise_exception_flag = True

                    else:
                        ask_depth = book["AskDepth"]
                        bid_depth = book["BidDepth"]
                        # Solution 2: Replace Price-level which crossing
                        new_bid_update = [px for px, qty in depth_update["BID"].items() if qty > 0]
                        if len(new_bid_update):
                            max_bid_update = max(new_bid_update)
                            for px in ask_depth.keys():
                                if px <= max_bid_update:
                                    obj = ask_depth.pop(px, None)
                                    revised_ask[px] = 0
                                    # print(f"Error {obj}\n{json.dumps([depth_update])}\n{json.dumps(book)}")
                                else:
                                    break

                        new_ask_update = [px for px, qty in depth_update["ASK"].items() if qty > 0]
                        if len(new_ask_update):
                            min_ask_update = min(new_ask_update)
                            for px in bid_depth.keys()[::-1]:
                                if px >= min_ask_update:
                                    obj = bid_depth.pop(px, None)
                                    revised_bid[px] = 0
                                    # print(f"Error {obj}\n{json.dumps([depth_update])}\n{json.dumps(book)}")
                                else:
                                    break
                                
            return revised_ask, revised_bid, raise_exception_flag
        except Exception as e:
            self._logger.warning("[E] _quality_control: \n%s" % (traceback.format_exc()))              
                        
    def _update_msg_seq(self, symbol):
        try:
            self.__msg_seq += 1
            self.__msg_seq_symbol[symbol] += 1
        except Exception as e:
            self._logger.warning("[E] _update_msg_seq: \n%s" % (traceback.format_exc()))      
            
    def _get_depth_json(self, exg_time, symbol, book):
        try:
            time_arrive = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
            if not exg_time:
                exg_time = time_arrive

            depth_msg = {"Msg_seq": self.__msg_seq,
                        "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                        "Exchange": self.__exchange,
                        "Symbol": symbol,
                        "Time": exg_time,
                        "TimeArrive": time_arrive,
                        "Type": "snap",
                        "AskDepth": {px: qty for px, qty in book["AskDepth"].items()[:100]},
                        "BidDepth": {px: qty for px, qty in book["BidDepth"].items()[:-100:-1]}
                        }
            return depth_msg
        except Exception as e:
            self._logger.warning("[E] _get_depth_json: \n%s" % (traceback.format_exc()))     
    
    def _get_update_json(self, symbol, depth_update,  exg_time, time_arrive, revised_ask=None, revised_bid=None,):
        try:
            if self._is_depth_invalid(depth_update):
                return None
            
            if revised_ask:
                depth_update["ASK"].update(revised_ask)
                
            if revised_bid:
                depth_update["BID"].update(revised_bid)

            update_msg = {"Msg_seq": self.__msg_seq,
                            "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                            "Exchange": self.__exchange,
                            "Symbol": symbol,
                            "Time": exg_time,
                            "TimeArrive": time_arrive,
                            "Type": "update",
                            "AskUpdate": depth_update["ASK"],
                            "BidUpdate": depth_update["BID"]}
            return update_msg
        except Exception as e:
            self._logger.warning("[E] _get_update_json: \n%s" % (traceback.format_exc()))              
            
    def process_depth_update(self, symbol, depth_update, book, exg_time, raise_exception):
        try:
            if book is None:
                self._logger.warning("%s snapshoot was not stored, can't process update data " % (symbol))
                return
            
            self._update_depth_volume(depth_update, book)

            revised_ask, revised_bid, raise_exception_flag = self._quality_control(book, raise_exception, depth_update)

            self._update_msg_seq(symbol)

            depth_json = self._get_depth_json(exg_time, symbol, book)
            
            update_json = self._get_update_json(symbol, depth_update, depth_json["Time"], depth_json["TimeArrive"], revised_ask, revised_bid)
            
            # self._logger.info("\nupdate_json %s " % (json.dumps(update_json)))
            
            self._connector.publish_depth(symbol, book, depth_json, update_json)
            
            if raise_exception_flag:
                raise Exception(f"Ask/Bid Price Crossing, Symbol: {symbol}")
            
        except Exception as e:
            self._logger.warning("[E] process_depth_snap: \n%s" % (traceback.format_exc()))          

    def _get_update_book(self, depth_update, book):
        try:
            update_book = {"ASK": {}, "BID": {}}
            
            for side in depth_update.keys():
                if side == "ASK":
                    depth_side = "AskDepth"
                    update_side = "ASK"
                else:
                    depth_side = "BidDepth"
                    update_side = "BID"

                if not operator.eq(book[depth_side], depth_update[side]):
                    local_px_set = set(book[depth_side].keys())
                    update_px_set = set(depth_update[side].keys())
                    for px in update_px_set.difference(local_px_set):
                        update_book[update_side][px] = depth_update[side][px]

                    for px in local_px_set.difference(update_px_set):
                        update_book[update_side][px] = 0

                    for px in local_px_set.intersection(update_px_set):
                        if book[depth_side][px] != depth_update[side][px]:
                            update_book[update_side][px] = depth_update[side][px]

                    book[depth_side] = SortedDict(depth_update[side])    
                    
            return update_book     
        except Exception as e:
            self._logger.warning("[E] _get_update_book: \n%s" % (traceback.format_exc()))      
            
    def process_depth_snap(self, symbol, depth_update, book, exg_time):   
        try:
            update_book = self._get_update_book(depth_update, book)
            
            # self._logger.info("%s, update_book: %s\n" %(symbol, str(update_book)))
              
            self._update_msg_seq(symbol)
            
            depth_json = self._get_depth_json(exg_time, symbol, book)
                            
            update_json = self._get_update_json(symbol, update_book, depth_json["Time"], depth_json["TimeArrive"])
            
            # self._logger.info("\n%s, depth_json: %s" % (symbol, json.dumps(depth_json)))

            self._connector.publish_depth(symbol, book, depth_json, update_json)
                           
        except Exception as e:
            self._logger.warning("[E] process_depth_update: \n%s" % (traceback.format_exc()))   
            
    def pub_depthx(self, symbol: str, depth_update: dict, is_snapshot: bool = True, exg_time: str = None,
                   raise_exception: bool=True):
        """
        Publish Depth & Update
        :param symbol: 形如 ETH_USDT
        :param depth_update: {ASK:dict<PX:QTY>, BID:dict<PX:QTY>}
        :param is_snapshot: True=Snapshot, False=Update
        :param exg_time: Time provided by Exchange
        :param raise_exception: Quality control option, "true" raise exception while Ask/Bid crossing
        :return: Void
        """
        try:
            # self._logger.info("is_snapshot %s, depth_update: %s" % (str(is_snapshot), str(depth_update)))
            
            if self._is_depth_invalid(depth_update):
                return None
                        
            book = self._get_cached_book(symbol, is_snapshot, depth_update)

            if is_snapshot: 
                self.process_depth_snap(symbol, depth_update, book, exg_time)
            else: 
                self.process_depth_update(symbol, depth_update, book, exg_time, raise_exception)

        except Exception as e:
            self._logger.warning("[E] pub_depthx: ")
            self._logger.warning(traceback.format_exc())

    # def _process_crossing_date(self,symbol, depth_msg, book):
    #     try:
    #         if symbol in self.__crossing_flag:
    #             if depth_msg["TimeArrive"][:10] != self.__crossing_flag[symbol]:
    #                 depth_msg["AskDepth"] = book["AskDepth"]
    #                 depth_msg["BidDepth"] = book["BidDepth"]
    #                 self.self._redis_con.crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}", message=json.dumps(depth_msg))
    #                 self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

    #         else:
    #             self.__crossing_flag.setdefault(symbol, depth_msg["TimeArrive"][:10])
    #             depth_msg["AskDepth"] = book["AskDepth"]
    #             depth_msg["BidDepth"] = book["BidDepth"]
    #             self.self._redis_con.crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}", message=json.dumps(depth_msg))                                        
    #     except Exception as e:
    #         self._logger.warning("[E] _process_crossing_date: \n%s" % (traceback.format_exc()))        
    
    # def _process_crossing_date_trade(self, symbol, exg_time):
    #     try:
    #         if self._is_redis:
    #             if symbol in self.__crossing_flag:
    #                 if datetime.utcnow().strftime('%Y-%m-%d') != self.__crossing_flag[symbol]:
    #                     book = self.__orderbook[symbol]
    #                     depth_msg = {"Msg_seq": self.__msg_seq,
    #                                 "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
    #                                 "Exchange": self.__exchange,
    #                                 "Symbol": symbol,
    #                                 "Time": exg_time,
    #                                 "TimeArrive": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
    #                                 "AskDepth": book["AskDepth"],
    #                                 "BidDepth": book["BidDepth"]
    #                                 }

    #                     self._redis_con.crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}", message=json.dumps(depth_msg))
    #                     self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

    #             elif symbol in self.__orderbook:  # Data's first day(not in __crossing_flag)
    #                 book = self.__orderbook[symbol]
    #                 depth_msg = {"Msg_seq": self.__msg_seq,
    #                             "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
    #                             "Exchange": self.__exchange,
    #                             "Symbol": symbol,
    #                             "Time": exg_time,
    #                             "TimeArrive": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
    #                             "AskDepth": book["AskDepth"],
    #                             "BidDepth": book["BidDepth"]
    #                             }
    #                 self.self._redis_con.crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}", message=json.dumps(depth_msg))
    #                 self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

    #                 self.__crossing_flag.setdefault(symbol, depth_msg["TimeArrive"][:10])
    #                 self.self._redis_con.crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
    #                                         message=json.dumps(depth_msg))                                  
    #     except Exception as e:
    #         self._logger.warning("[E] _process_crossing_date_trade: \n%s" % (traceback.format_exc()))           

    def pub_tradex(self, symbol: str, direction: str, exg_time: str, px_qty: tuple):
        """
        Publish Trade record
            :param symbol:
            :param direction: Taker operation: Buy/Sell
            :param exg_time:
            :param px_qty: (PX, QTY)
            :return: Void
        """
        try:                  
            px, qty = px_qty
            trade_json = {"Exchange": self.__exchange,
                        "Symbol": symbol,
                        "Time": exg_time,
                        "TimeArrive": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                        "Direction": direction,
                        "LastPx": px,
                        "Qty": qty}
            
            # self._connector.publish_trade(trade_json)
            
        except Exception as e:
            self._logger.warning("[E] pub_tradex: ")
            self._logger.warning(traceback.format_exc())

    def ding_robot(self, level: str, msg: str = "", event=None):
        msg_body = self.__ding_msg(level=level, msg=msg, event=event)
        path = "https://oapi.dingtalk.com/robot/send?access_token=2e3c275331d63bc6fd656f638237568ab26e10f06a986d42b75aef6dcc4ced4e"
        body = {"msgtype": "markdown", "markdown": msg_body, "at": {"isAtAll": False}}
        header = {"Content-Type": "application/json"}

        try:
            resp = requests.post(url=path, json=body, headers=header)

        except Exception:
            err = sys.exc_info()
            self.logger(level=self.critical,
                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])),
                        use_dingrobot=False)

    def __ding_msg(self, level: str, msg: str, event=None) -> dict:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        status = "Online"

        if event:
            msg = f"[{event['Event']}] {event['Msg']}"
            status = event["Status"]

        if not self.__debug:
            body = {"title": f"MDService", "text": f"## **MDService-{self.__exchange_topic}** \n"
                                                   f"### **Type: {level}** \n"
                                                   f"> #### **Status: {status}** \n"
                                                   f"> ##### {msg} \n\n"
                                                   f"###### Time(UTC): {now} \n"}
        else:
            body = {"title": f"MDService", "text": f"## **MDService-{self.__exchange_topic}** \n"
                                                   f"### **Type: {level}-DebugMode** \n"
                                                   f"> #### **Status: {status}** \n"
                                                   f"> ##### {msg} \n\n"
                                                   f"###### Time(UTC): {now} \n"}
        return body
class MDEvent:
    @staticmethod
    def CONNECTIONERROR(msg: str) -> dict:
        return {"Event": "CONNECTIONERROR",
                "Status": "Offline",
                "Msg": f"MDServ is offline, can't connect to the Internet.\nExceptionMsg: {msg}"}

    @staticmethod
    def DISCONNECTED() -> dict:
        return {"Event": "DISCONNECTED",
                "Status": "Offline",
                "Msg": f"MDServ is offline, due to exchange disconnected."}

    @staticmethod
    def CONNECTED() -> dict:
        return {"Event": "CONNECTED",
                "Status": "Online",
                "Msg": f"MDServ is online."}

    @staticmethod
    def INITIALIZED() -> dict:
        return {"Event": "INITIALIZED",
                "Status": "Pre-Online",
                "Msg": f"MDServ initialized, start MDService."}

    @staticmethod
    def WSERROR(err_type) -> dict:
        return {"Event": "WSERROR",
                "Status": "Offline",
                "Msg": f"MDServ is offline, due to websocket error type-{err_type}."}
