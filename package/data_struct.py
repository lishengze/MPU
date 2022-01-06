
import os
import sys
import traceback

from enum import Enum

TYPE_SEPARATOR = "-"
SYMBOL_EXCHANGE_SEPARATOR = "."

DEPTH_TYPE = "DEPTHx"
TRADE_TYPE = "TRADEx"
KLINE_TYPE = "KLINEx"

MILLS_PER_SECS = 100
MICRO_PER_MILLS = 1000
NANO_PER_MICRO = 1000

NANO_PER_MILL = MICRO_PER_MILLS * NANO_PER_MICRO
NANO_PER_SECS = MILLS_PER_SECS * MICRO_PER_MILLS * NANO_PER_MICRO

class SERIALIXER_TYPE(Enum):
    JSON = 1
    PROTOBUF = 2
    
class NET_SERVER_TYPE(Enum):
    REDIS = 1
    KAFKA = 2
    
class DATA_TYPE(Enum):
    DEPTH = 1
    KLINE = 2
    TRADE = 3
        
class SDecimal(object):
    def __init__(self, value_:int=0, precise_:int=0):
        self.value = value_
        self.precise = precise_
        
    def __init__(self, raw:float=0.0):
        str_value = str(raw)
        self.value = 0
        self.precise = 0
        
        if 'e' in str_value:
            self.parse_e_float(raw, str_value)
        elif '.' in str_value:
            self.parse_original_float(raw, str_value=str_value)
        else:
            self.value = int(raw)
            self.precise = 0            

        
    def parse_e_float(self, raw:float, str_value:str):
        try:
            pos1 = str_value.find('e')
            
            tmp_value = float(str_value[0:pos1])
            
            self.parse_original_float(raw=tmp_value,str_value=str(tmp_value))
            
            pos2 = str_value.find('-')
            self.precise += int(str_value[pos2+1:])
        except Exception as e :
            print("str_value: %s, raw: %f \n" % (str_value, raw))
            print(traceback.format_exc())

    
    def parse_original_float(self, raw:float, str_value:str):
        try:
            pos = str_value.find('.')
            
            self.precise = len(str_value) - pos -1;
            
            tmp_value = str_value.replace('.', '')
            self.value = int(tmp_value)
        except Exception as e :
            print("str_value: %s, raw: %f \n" % (str_value, raw))
            print(traceback.format_exc())
                    
                
    def get_value(self):
        if self.precise == 0:
            return self.value
        else:
            return float(self.value) / (10**self.precise)

class SDepth(object):
    def __init__(self):
        self.price = SDecimal()
        self.volume = SDecimal()
        self.volume_by_exchanges = dict()

class SDepthQuote(object):
    def __init__(self, exchange_:str="", symbol_:str="", sequence_no_=0,\
                        origin_time_=0, arrive_time_=0, server_time_=0,\
                        price_precise_=0, volume_precise_=0, amount_precise_=0,\
                        is_snap_:bool=False, asks_:list=[], bids_:list=[]):
        self.exchange = exchange_
        self.symbol = symbol_
        self.sequence_no = sequence_no_
        self.origin_time = origin_time_
        self.arrive_time = arrive_time_
        self.server_time = server_time_
        self.price_precise = price_precise_
        self.volume_precise = volume_precise_
        self.amount_precise = amount_precise_
        self.is_snap = is_snap_
        self.asks = asks_
        self.bids = bids_
        
        # print(len(asks_), len(bids_))
        
    def meta_str(self):
        result = ("exchange: %s, symbol: %s, ask.len: %d, bid.len: %d" % \
                    (self.exchange, self.symbol, len(self.asks), len(self.bids)))

        return result

class STradeData(object):
    def __init__(self):
        self.time=0
        self.price = SDecimal()
        self.volume = SDecimal()
        self.symbol = ""
        self.exchange = ""
        
    def meta_str(self):
        result =  ("exchange: %s, symbol: %s, price: %f, volume: %f, time:%d" % \
                    (self.exchange, self.symbol, self.price.get_value(), self.volume.get_value(), self.time))
        return result
class SKlineData(object):
    def __init__(self, ):
        self.time=0
        self.symbol = ""
        self.exchange = ""
        self.resolution = 0
                
        self.px_open = SDecimal()
        self.px_high = SDecimal()
        self.px_low = SDecimal()
        self.px_close = SDecimal()
        
        self.volume = SDecimal()
 
    def meta_str(self):
        result =  ("exchange: %s, symbol: %s, px_open: %f, px_high: %f, px_low: %f, px_close: %f" % \
                    (self.exchange, self.symbol, self.px_open.get_value(), self.px_high.get_value(), \
                     self.px_low.get_value(), self.px_close.get_value()))
        return result
 
def test_decimal():
    a = 10000
    a = 10000.001
    print("original value: %f" % (a))
    
    b = SDecimal(a)
    
    b = SDecimal(25e-2)
    
    print("TestValue: %f" % (b.get_value()))
 
if __name__ == "__main__":
    # TestPrtPwd()
    test_decimal()
        