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

# from proto.python.market_data_pb2 import Trade

def set_proto_depth_list(dst_depth_list_proto, src_depth_list_local):
    
    for depth in src_depth_list_local:
        new_depth = PriceVolume()
        
        new_depth.price = str(depth.price.get_value())
        new_depth.volume = str(depth.volume.get_value())    
            
        dst_depth_list_proto.append(new_depth)        

def get_local_depth_list(src_depth_list_proto):    
    
    dst_depth_list_local = []
    
    for depth in src_depth_list_proto:
        new_depth = SDepth()
        
        new_depth.price = SDecimal(float(depth.price))
        new_depth.volume.value = SDecimal(float(depth.volume))
        
        # for symbol in depth.volume_by_exchanges:
        #     exchange_volume = SDecimal()
        #     exchange_volume.value = depth.volume_by_exchanges[symbol].value
        #     exchange_volume.precise = depth.volume_by_exchanges[symbol].precise
        #     new_depth.volume_by_exchanges[symbol] = exchange_volume
    
        # print('**')
        
        dst_depth_list_local.append(new_depth)
        
    return dst_depth_list_local

    # print(len(dst_depth_list_local))
        
class ProtoSerializer:
    def __init__(self, logger=None):
        self._logger = logger
        
    def decode_depth(self, src_str):
        proto_quote = Depth()
        proto_quote.ParseFromString(src_str)

        local_quote = SDepthQuote()

        local_quote.symbol = proto_quote.symbol
        local_quote.exchange = proto_quote.exchange
        local_quote.sequence_no = -1        
        local_quote.origin_time = proto_quote.timestamp.ToNanoseconds()
        local_quote.mpu_timestamp = proto_quote.timestamp.ToNanoseconds()
        local_quote.mpu_timestamp = proto_quote.timestamp.ToNanoseconds()   
        local_quote.price_precise = -1
        local_quote.volume_precise = -1
        local_quote.amount_precise = -1
        
        local_quote.is_snap = True

        # print(len(local_quote.asks), len(local_quote.bids))
        
        # print("seq:%d" % (local_quote.sequence_no))

        local_quote.asks = get_local_depth_list(proto_quote.asks)
        local_quote.bids = get_local_depth_list(proto_quote.bids)

        return local_quote

    def decode_kline(self, src_str):
        proto_kline = Kline()
        proto_kline.ParseFromString(src_str)

        local_kline = SKlineData()
    
        local_kline.time = proto_kline.timestamp.ToNanoseconds()
        local_kline.symbol = proto_kline.symbol
        local_kline.exchange = proto_kline.exchange
        local_kline.resolution = proto_kline.resolution
        local_kline.sequence_no = -1
        
        local_kline.px_open = SDecimal(float(proto_kline.open))     
        local_kline.px_high = SDecimal(float(proto_kline.high))             
        local_kline.px_low = SDecimal(float(proto_kline.low))        
        local_kline.px_close = SDecimal(float(proto_kline.close))
        
        local_kline.volume = SDecimal(float(proto_kline.volume)) 

        return local_kline
    
    def decode_trade(self, src_str):
        proto_trade = Trade()
        proto_trade.ParseFromString(src_str)
        
        local_trade = STradeData()        
        
        local_trade.time = proto_trade.timestamp.ToNanoseconds();
        local_trade.exchange = proto_trade.exchange;
        local_trade.symbol = proto_trade.symbol;
        local_trade.sequence_no = -1
        
        local_trade.price = SDecimal(float(proto_trade.price))        
        local_trade.volume = SDecimal(float(proto_trade.volume))
        
        return local_trade
        
    def encode_depth(self, local_quote:SDepthQuote):
        proto_quote = Depth()
        proto_quote.symbol = local_quote.symbol
        proto_quote.exchange = local_quote.exchange     
        proto_quote.timestamp.FromNanoseconds(int(local_quote.origin_time))
        proto_quote.timestamp.FromNanoseconds(int(local_quote.arrive_time))
        
        set_proto_depth_list(proto_quote.asks, local_quote.asks)
        set_proto_depth_list(proto_quote.bids, local_quote.bids)
                                 
        return proto_quote.SerializeToString()

    def encode_trade(self, local_trade:STradeData):
        proto_trade = Trade()
        proto_trade.timestamp.FromNanoseconds(int(local_trade.time))
        proto_trade.symbol = local_trade.symbol
        proto_trade.exchange = local_trade.exchange

        proto_trade.price = str(local_trade.price.get_value())        
        proto_trade.volume = str(local_trade.volume.get_value())
        
        return proto_trade.SerializeToString()
        
    def encode_kline(self, local_kline:SKlineData):
        proto_kline = Kline()
        proto_kline.timestamp.FromNanoseconds(int(local_kline.time))
        
        proto_kline.symbol = local_kline.symbol
        proto_kline.exchange = local_kline.exchange
        proto_kline.resolution = local_kline.resolution
        
        proto_kline.open = str(local_kline.px_open.get_value())        
        proto_kline.high = str(local_kline.px_high.get_value())                 
        proto_kline.low = str(local_kline.px_low.get_value())
        proto_kline.close =  str(local_kline.px_close.get_value())       
        
        proto_kline.volume = str(local_kline.volume.get_value())
                                
        return proto_kline.SerializeToString()
    

    

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

def get_proto_decimal_value(src:SDecimal):
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

        se_str = self.serializer.encode_depth(new_quote)
        
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

        se_str = self.serializer.encode_kline(local_kline)

        print("se_str.len: %d, : %s" % (len(se_str), se_str))

        new_local_kline = self.serializer.decode_kline(se_str)
        print(new_local_kline.meta_str())        


    def test_trade(self):
        local_trade = STradeData()
        local_trade.exchange = "FTX"
        local_trade.symbol = "BTC_USDT"

        local_trade.price = SDecimal(55555.1)  
        local_trade.volume = SDecimal(0.0001)        

        se_str = self.serializer.encode_trade(local_trade)

        print("se_str.len: %d, : %s" % (len(se_str), se_str))

        local_new_trade = self.serializer.decode_trade(se_str)

        print(local_new_trade.meta_str())




if __name__ == "__main__":
    print("----------- Test Serializer! ---------")
    test_main = TestMain()

    test_main.test_quote()

    test_main.test_trade()

    # test_main.test_kline()

    # test_main.test_trade()