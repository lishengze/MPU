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
import os

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

from tool import *
from kafka_server import *
from redis_server import *
from data_struct import *

# from package.tool import *
# from package.kafka_server import *
# from package.data_struct import *

class Publisher:
    def __init__(self, exchange: str, config: dict, net_server_type:NET_SERVER_TYPE, exchange_topic: str = None, debug_mode: bool = False, logger=None):
        self.__debug = debug_mode
        self.__crossing_flag = dict()  # {"Symbol": "Date"}
        self._logger = logger

        if net_server_type == NET_SERVER_TYPE.KAFKA:
            self._net_server = KafkaServer(config = config, serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
        elif net_server_type == NET_SERVER_TYPE.REDIS:
            self._net_server = RedisServer(config = config, depth_processor=self, kline_processor=self, trade_processor=self, \
                                         serializer_type=SERIALIXER_TYPE.PROTOBUF, logger=self._logger)
                    
        self._exchange = exchange
        self.__msg_seq = 0
        self.__msg_seq_symbol = defaultdict(int)

        if exchange_topic:
            self._exchange_topic = exchange_topic
        else:
            self._exchange_topic = exchange      # 形如 "BINANCE"

        self.__orderbook = dict()

        self._trade_seq = defaultdict(int)
        
        # print("create publisher for " + self._exchange)

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
            
    def _get_depth_from_book(self, book:list):
        dst_depth = []
        for price in book:
            new_depth = SDepth()
            new_depth.price = SDecimal(price)
            new_depth.volume = SDecimal(book[price])
            dst_depth.append(new_depth)
        return dst_depth
            
        print("dst_len: %d, book.len: %d" %(len(dst_depth), len(book)))        

    def _get_snap_quote(self, exg_time, symbol, book):
        try:
            if not exg_time:
                exg_time = get_nano_time()

            depth_quote = SDepthQuote()
            depth_quote.symbol =  symbol
            depth_quote.exchange = self._exchange
            depth_quote.sequence_no = self.__msg_seq_symbol[symbol]
            depth_quote.origin_time = exg_time
            depth_quote.server_time = get_nano_time()
            depth_quote.arrive_time = 0
            depth_quote.is_snap = True

            depth_quote.asks = self._get_depth_from_book(book["AskDepth"])
            depth_quote.bids = self._get_depth_from_book(book["BidDepth"])
            
            # print("ask.len: %d, bid.len: %d, AskDepth.len: %d, BidDepth.len: %d" % 
            #       (len(depth_quote.asks),len(depth_quote.bids), len(book["AskDepth"]), len(book["BidDepth"])))

            return depth_quote


        except Exception as e:
            self._logger.warning("[E] _get_snap_quote: \n%s" % (traceback.format_exc()))     
    
    def _get_update_quote(self, symbol, depth_update, revised_ask=None, revised_bid=None,):
        try:
            if self._is_depth_invalid(depth_update):
                empty_quote = SDepthQuote()
                return empty_quote
            
            if revised_ask:
                depth_update["ASK"].update(revised_ask)
                
            if revised_bid:
                depth_update["BID"].update(revised_bid)

            depth_quote = SDepthQuote()
            depth_quote.symbol =  symbol
            depth_quote.exchange = self._exchange
            depth_quote.sequence_no = self.__msg_seq_symbol[symbol]
            depth_quote.origin_time = get_nano_time()
            depth_quote.server_time = get_nano_time()
            depth_quote.arrive_time = 0
            depth_quote.is_snap = False

            depth_quote.asks = self._get_depth_from_book(depth_update["ASK"])
            depth_quote.bids = self._get_depth_from_book(depth_update["BID"])
            
            # print("ask.len: %d, bid.len: %d, AskDepth.len: %d, BidDepth.len: %d" % 
            #       (len(depth_quote.asks),len(depth_quote.bids), len(depth_update["ASK"]), len(depth_update["BID"])))
                        
            return depth_quote
        except Exception as e:
            self._logger.warning("[E] _get_update_quote: \n%s" % (traceback.format_exc()))              
            
    def process_depth_update(self, symbol, depth_update, book, exg_time, raise_exception):
        try:
            if book is None:
                self._logger.warning("%s snapshoot was not stored, can't process update data " % (symbol))
                return
            
            self._update_depth_volume(depth_update, book)

            revised_ask, revised_bid, raise_exception_flag = self._quality_control(book, raise_exception, depth_update)

            self._update_msg_seq(symbol)

            snap_quote = self._get_snap_quote(exg_time, symbol, book)
            
            update_quote = self._get_update_quote(symbol, depth_update, revised_ask, revised_bid)
            
            self._logger.info("snap: " + snap_quote.meta_str())
            self._logger.info("update: " + update_quote.meta_str())
            
            self._net_server.publish_depth(snap_quote, update_quote)
            
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
              
            self._update_msg_seq(symbol)
            
            snap_quote = self._get_snap_quote(exg_time, symbol, book)
                            
            update_quote = self._get_update_quote(symbol, update_book)
                        
            self._logger.info("snap: " + snap_quote.meta_str())
            self._logger.info("update: " + update_quote.meta_str())

            self._net_server.publish_depth(snap_quote, update_quote)
                           
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
            if symbol in self._trade_seq:
                self._trade_seq[symbol] += 1
            else:
                self._trade_seq[symbol] = 0

            px, qty = px_qty
            trade_data = STradeData()
            
            trade_data.time = get_utc_nano_time()
            trade_data.symbol = symbol
            trade_data.exchange = self._exchange
            trade_data.price = SDecimal(px)
            trade_data.volume = SDecimal(qty)
            trade_data.sequence_no = self._trade_seq[symbol]

            print(trade_data.meta_str())

            # self._logger.info(trade_data.meta_str())
                    
            self._net_server.publish_trade(trade_data)
            
        except Exception as e:
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
            body = {"title": f"MDService", "text": f"## **MDService-{self._exchange_topic}** \n"
                                                   f"### **Type: {level}** \n"
                                                   f"> #### **Status: {status}** \n"
                                                   f"> ##### {msg} \n\n"
                                                   f"###### Time(UTC): {now} \n"}
        else:
            body = {"title": f"MDService", "text": f"## **MDService-{self._exchange_topic}** \n"
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
