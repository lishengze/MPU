import  os
import  sys

from data_struct import *
# from proto.python.market_data_pb2 import *

def get_grandfather_dir():
    parent = os.path.dirname(os.path.realpath(__file__))
    garder = os.path.dirname(parent)    
    return garder

g_proto_dir = get_grandfather_dir() +"/proto/python"

print(g_proto_dir)
sys.path.append(g_proto_dir)

from market_data_pb2 import *


def set_proto_depth_list(dst_depth_list_proto, src_depth_list_local):
    
    for depth in src_depth_list_local:
        new_depth = Depth()
        
        new_depth.price.value = depth.price.value
        new_depth.price.precise = depth.price.precise
        
        new_depth.volume.value = depth.volume.value
        new_depth.volume.precise = depth.volume.precise      
        
        for symbol in depth.volume_by_exchanges:
            exchange_volume = Decimal()
            exchange_volume.value = depth.volume_by_exchanges[symbol].value
            exchange_volume.precise = depth.volume_by_exchanges[symbol].precise
            new_depth.volume_by_exchanges[symbol] = exchange_volume
    
        dst_depth_list_proto.append(new_depth)

class ProtoSerializer:
    def __init__(self, logger=None):
        self._logger = logger
        
    def decode_depth(self, src_str):
        pass
    
    def decode_kline(self, src_str):
        pass
    
    def decode_trade(self, src_str):
        new_trade = TradeData()
        new_trade.ParseFromString(src_str)
        
        trade_data = STradeData()        
        
        trade_data.time = new_trade.time;
        trade_data.exchange = new_trade.exchange;
        trade_data.symbol = new_trade.symbol;
        
        trade_data.price.value = new_trade.price.value
        trade_data.price.precise = new_trade.price.precise
        
        trade_data.volume.value = new_trade.volume.value
        trade_data.volume.precise = new_trade.volume.precise
        
        return trade_data
        
    def encode_depth(self, quote_src:SDepthQuote):
        new_quote = DepthQuote()
        new_quote.symbol = quote_src.symbol
        new_quote.exchange = quote_src.exchange
        new_quote.sequence_no = quote_src.sequence_no        
        new_quote.origin_time = quote_src.origin_time
        new_quote.arrive_time = quote_src.arrive_time
        new_quote.server_time = quote_src.server_time        
        new_quote.price_precise = quote_src.price_precise
        new_quote.volume_precise = quote_src.volume_precise
        new_quote.amount_precise = quote_src.amount_precise 
        
        new_quote.is_snap = quote_src.is_snap
        
        set_proto_depth_list(new_quote.asks, quote_src.asks)
        set_proto_depth_list(new_quote.bids, quote_src.bids)
                                 
        return new_quote.SerializeToString()
    
    def encode_kline(self, kline_src:SKlineData):
        new_kline = KlineData()
        new_kline.time = kline_src.time
        new_kline.symbol = kline_src.symbol
        new_kline.exchange = kline_src.exchange
        new_kline.resolution = kline_src.resolution
        
        new_kline.px_open.value = kline_src.px_open.value
        new_kline.px_open.precise = kline_src.px_open.precise
        
        new_kline.px_high.value = kline_src.px_high.value
        new_kline.px_high.precise = kline_src.px_high.precise       
        
        new_kline.px_low.value = kline_src.px_low.value
        new_kline.px_low.precise = kline_src.px_low.precise       
        
        new_kline.px_close.value = kline_src.px_close.value
        new_kline.px_close.precise = kline_src.px_close.precise    
        
        new_kline.volume.value = kline_src.volume.value
        new_kline.volume.precise = kline_src.volume.precise                               
        
        return new_kline.SerializeToString()        
    
    def encode_trade(self, trade_src:STradeData):
        new_trade = TradeData()
        new_trade.time = trade_src.time
        new_trade.symbol = trade_src.symbol
        new_trade.price.value = trade_src.price.value
        new_trade.price.precise = trade_src.price.precise
        
        new_trade.volume.value = trade_src.volume.value
        new_trade.volume.precise = trade_src.volume.precise       
        
        return new_trade.SerializeToString()
    

def test_market_data():
    new_depth = Depth()
    price = new_depth.price
    price.value = 10000
    price.precise = 4
    
    new_quote = DepthQuote()
    new_quote.asks.append(new_depth)

    se_string = new_quote.SerializeToString()

    print("se_string: %s" % (se_string))
    
    trans_quote = DepthQuote()
    trans_quote.ParseFromString(se_string)
    print(trans_quote.asks[0].price.value)
    print(trans_quote.asks[0].price.precise)



if __name__ == "__main__":
    # TestPrtPwd()
    test_market_data()