import  os
import  sys

from data_struct import *

def get_grandfather_dir():
    parent = os.path.dirname(os.path.realpath(__file__))
    garder = os.path.dirname(parent)    
    return garder

def get_proto_dir():
    garder = get_grandfather_dir()
    if garder.find('\\') != -1:
        return garder + "\proto\python"
    else:
        return garder + "/proto/python"
        
print(get_proto_dir())
sys.path.append(get_proto_dir())    

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

def get_local_depth_list(src_depth_list_proto):    
    
    dst_depth_list_local = []
    
    for depth in src_depth_list_proto:
        new_depth = SDepth()
        
        new_depth.price.value = depth.price.value
        new_depth.price.precise = depth.price.precise
        
        new_depth.volume.value = depth.volume.value
        new_depth.volume.precise = depth.volume.precise      
        
        for symbol in depth.volume_by_exchanges:
            exchange_volume = SDecimal()
            exchange_volume.value = depth.volume_by_exchanges[symbol].value
            exchange_volume.precise = depth.volume_by_exchanges[symbol].precise
            new_depth.volume_by_exchanges[symbol] = exchange_volume
    
        # print('**')
        
        dst_depth_list_local.append(new_depth)
        
    return dst_depth_list_local

    # print(len(dst_depth_list_local))
        
class ProtoSerializer:
    def __init__(self, logger=None):
        self._logger = logger
        
    def decode_depth(self, src_str):
        proto_quote = DepthQuote()
        proto_quote.ParseFromString(src_str)

        local_quote = SDepthQuote()

        local_quote.symbol = proto_quote.symbol
        local_quote.exchange = proto_quote.exchange
        local_quote.sequence_no = proto_quote.sequence_no        
        local_quote.origin_time = proto_quote.origin_time
        local_quote.arrive_time = proto_quote.arrive_time
        local_quote.server_time = proto_quote.server_time        
        local_quote.price_precise = proto_quote.price_precise
        local_quote.volume_precise = proto_quote.volume_precise
        local_quote.amount_precise = proto_quote.amount_precise 
        
        local_quote.is_snap = proto_quote.is_snap

        # print(len(local_quote.asks), len(local_quote.bids))
        
        # print("seq:%d" % (local_quote.sequence_no))

        local_quote.asks = get_local_depth_list(proto_quote.asks)
        local_quote.bids = get_local_depth_list(proto_quote.bids)

        return local_quote

    def decode_kline(self, src_str):
        proto_kline = KlineData()
        proto_kline.ParseFromString(src_str)

        local_kline = SKlineData()
    
        local_kline.time = proto_kline.time
        local_kline.symbol = proto_kline.symbol
        local_kline.exchange = proto_kline.exchange
        local_kline.resolution = proto_kline.resolution
        
        local_kline.px_open.value = proto_kline.px_open.value
        local_kline.px_open.precise = proto_kline.px_open.precise
        
        local_kline.px_high.value = proto_kline.px_high.value
        local_kline.px_high.precise = proto_kline.px_high.precise       
        
        local_kline.px_low.value = proto_kline.px_low.value
        local_kline.px_low.precise = proto_kline.px_low.precise       
        
        local_kline.px_close.value = proto_kline.px_close.value
        local_kline.px_close.precise = proto_kline.px_close.precise    
        
        local_kline.volume.value = proto_kline.volume.value
        local_kline.volume.precise = proto_kline.volume.precise    

        return local_kline
    
    def decode_trade(self, src_str):
        proto_trade = TradeData()
        proto_trade.ParseFromString(src_str)
        
        local_trade = STradeData()        
        
        local_trade.time = proto_trade.time;
        local_trade.exchange = proto_trade.exchange;
        local_trade.symbol = proto_trade.symbol;
        
        local_trade.price.value = proto_trade.price.value
        local_trade.price.precise = proto_trade.price.precise
        
        local_trade.volume.value = proto_trade.volume.value
        local_trade.volume.precise = proto_trade.volume.precise
        
        return local_trade
        
    def encode_depth(self, local_quote:SDepthQuote):
        proto_quote = DepthQuote()
        proto_quote.symbol = local_quote.symbol
        proto_quote.exchange = local_quote.exchange
        proto_quote.sequence_no = local_quote.sequence_no        
        proto_quote.origin_time = int(local_quote.origin_time)
        proto_quote.arrive_time = int(local_quote.arrive_time)
        proto_quote.server_time = int(local_quote.server_time)
        proto_quote.price_precise = local_quote.price_precise
        proto_quote.volume_precise = local_quote.volume_precise
        proto_quote.amount_precise = local_quote.amount_precise 
        
        proto_quote.is_snap = local_quote.is_snap
        
        print("seq:%d" % (local_quote.sequence_no))
        
        set_proto_depth_list(proto_quote.asks, local_quote.asks)
        set_proto_depth_list(proto_quote.bids, local_quote.bids)
                                 
        return proto_quote.SerializeToString()
    
    def encode_kline(self, local_kline:SKlineData):
        proto_kline = KlineData()
        proto_kline.time = int(local_kline.time)
        proto_kline.symbol = local_kline.symbol
        proto_kline.exchange = local_kline.exchange
        proto_kline.resolution = local_kline.resolution
        
        proto_kline.px_open.value = local_kline.px_open.value
        proto_kline.px_open.precise = local_kline.px_open.precise
        
        proto_kline.px_high.value = local_kline.px_high.value
        proto_kline.px_high.precise = local_kline.px_high.precise       
        
        proto_kline.px_low.value = local_kline.px_low.value
        proto_kline.px_low.precise = local_kline.px_low.precise       
        
        proto_kline.px_close.value = local_kline.px_close.value
        proto_kline.px_close.precise = local_kline.px_close.precise    
        
        proto_kline.volume.value = local_kline.volume.value
        proto_kline.volume.precise = local_kline.volume.precise                               
        
        return proto_kline.SerializeToString()
    
    def encode_trade(self, local_trade:STradeData):
        proto_trade = TradeData()
        proto_trade.time = int(local_trade.time)
        proto_trade.symbol = local_trade.symbol
        proto_trade.exchange = local_trade.exchange
        proto_trade.price.value = local_trade.price.value
        proto_trade.price.precise = local_trade.price.precise
        
        proto_trade.volume.value = local_trade.volume.value
        proto_trade.volume.precise = local_trade.volume.precise       
        
        return proto_trade.SerializeToString()
    

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

def get_proto_decimal_value(src:Decimal):
        if src.precise == 0:
            return src.value
        else:
            return float(src.value) / (10**src.precise)

def get_proto_quote_meta(quote):
    return ("exchange: %s, symbol: %s, ask.len: %d, bids.len: %d \n" % \
            (quote.exchange, quote.symbol, len(quote.asks), len(quote.bids)) )

def get_proto_kline_meta(kline):
    return ("exchange: %s, symbol: %s, px_open: %f, px_high: %f, px_low:%f, px_close: %f \n" % \
            (kline.exchange, kline.symbol, \
            get_proto_decimal_value(kline.px_open),  \
            get_proto_decimal_value(kline.px_high), \
            get_proto_decimal_value(kline.px_low), \
            get_proto_decimal_value(kline.px_close)))

def get_proto_trade_meta(trade):
    result =  ("exchange: %s, symbol: %s, price: %f, volume: %f" % \
                (trade.exchange, trade.symbol, \
                get_proto_decimal_value(trade.price), \
                get_proto_decimal_value(trade.volume)))

    return result
class TestMain(object):
    def __init__(self) -> None:
        self.serializer = ProtoSerializer()

        

    def test_quote(self):
        new_quote = SDepthQuote()

        new_depth = SDepth()
        price = new_depth.price
        price.value = 10000
        price.precise = 4
                
        new_quote.asks.append(new_depth)
        new_quote.bids.append(new_depth)

        new_quote.exchange = "FTX"
        new_quote.symbol = "BTC_USDT"

        se_str, new_quote = self.serializer.encode_depth(new_quote)

        print(get_proto_quote_meta(new_quote))
        print("se_str.len: %d, : %s" % (len(se_str), se_str))

        new_local_quote = self.serializer.decode_depth(se_str)
        print(new_local_quote.meta_str())
                
    def test_kline(self):
        local_kline = SKlineData()
        local_kline.exchange = "FTX"
        local_kline.symbol = "BTC_USDT"

        local_kline.px_open = SDecimal(56789.11)
        local_kline.px_high = SDecimal(55555.11)
        local_kline.px_low = SDecimal(54555.11)
        local_kline.px_close = SDecimal(57555.11)

        se_str, new_kline = self.serializer.encode_kline(local_kline)

        print(get_proto_kline_meta(new_kline))
        print("se_str.len: %d, : %s" % (len(se_str), se_str))

        new_local_kline = self.serializer.decode_kline(se_str)
        print(new_local_kline.meta_str())        


    def test_trade(self):
        local_trade = STradeData()
        local_trade.exchange = "FTX"
        local_trade.symbol = "BTC_USDT"

        local_trade.price = SDecimal(55555.1)  
        local_trade.volume = SDecimal(0.0001)        

        se_str, new_trade = self.serializer.encode_trade(local_trade)

        print("se_str.len: %d, : %s" % (len(se_str), se_str))
        print(get_proto_trade_meta(new_trade))

        local_new_trade = self.serializer.decode_trade(se_str)

        print(local_new_trade.meta_str())




if __name__ == "__main__":
    test_main = TestMain()

    test_main.test_quote()

    # test_main.test_kline()

    # test_main.test_trade()