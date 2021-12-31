import asyncio
import json
import datetime
import redis
import threading
import numpy
import sys
import time
import traceback
from XService.XKLine import KLine
from data_struct import *

from abc import ABC,abstractmethod

class NetServer(object):
    def __init__(self) -> None:
        super().__init__()
        
class NetServer(ABC):
    def __init__(self, depth_processor=None, kline_processor=None, trade_processor=None, serializer_type:SERIALIXER_TYPE = SERIALIXER_TYPE.PROTOBUF, logger = None, debug=False):
        self._logger = logger
        
        self._depth_processor = depth_processor
        self._kline_processor = kline_processor
        self._trade_processor = trade_processor
        
        if serializer_type == SERIALIXER_TYPE.PROTOBUF:
            self.serializer = 1
        elif serializer_type == SERIALIXER_TYPE.JSON:
            self.serializer = 2
            
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
    
    @abstractmethod
    def publish_depth(self, snap_quote:SDepthQuote, update_quote:SDepthQuote):   
        self._logger.info("publish_depth") 
    
    @abstractmethod
    def publish_trade(self, trade:STradeData):
        self._logger.info("publish_trade")           

    @abstractmethod
    def publish_kline(self, kline:SKlineData):
        self._logger.info("publish_kline")     
        
    @abstractmethod
    def publish(self, topic:str, msg:str):
        try:
            print("MiddleConnector publish")            
        except Exception as e:
            self._logger.warning("[E] publish: %s" % (traceback.format_exc()))    
                       