import asyncio
import json
import aiohttp
import sys
import zlib
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import threading

from MarketBase import ExchangeBase

# self._symbol_dict = {"BTCUSDT": "BTC_USDT",  # the exchange needs 'btcusdt'
#                       "HTUSDT": "HT_USDT",
#                       "IRISBTC": "IRIS_BTC",
#                       "IRISUSDT": "IRIS_USDT",


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

sys.path.append(os.getcwd())
from Logger import *
class HUOBI(ExchangeBase):    
    def __init__(self, symbol_dict:dict, sub_data_type_list:list, net_server_type: NET_SERVER_TYPE =NET_SERVER_TYPE.KAFKA, 
                debug_mode: bool = True, is_test_currency: bool = False):
        try:
            super().__init__(exchange_name="HUOBI", symbol_dict=symbol_dict, 
                            sub_data_type_list=sub_data_type_list, net_server_type=net_server_type,
                              debug_mode=debug_mode, is_test_currency=is_test_currency)  
                      
            self._ws_url = "wss://api.huobi.pro/ws"
            self._ping_secs = 30

            self._sub_type_list = ["trade.detail"]
            if not self._is_test_currency:
                self._sub_type_list.append("depth.step0")

                        
            self._error_msg_list = ["", ""]
            self._is_connnect = False
            self._ws = None
            self._sub_client_id = 0
            
            self._logger.info(str(self._symbol_dict))

        except Exception as e:
            self._logger.warning("[E]__init__: " + str(e))

    def on_open(self, ws = None):
        try:     
            self._is_connnect = True
            
            if DATA_TYPE.DEPTH in self._sub_data_type_list:
                self.subscribe_depth()
            
            if DATA_TYPE.TRADE in self._sub_data_type_list:
                self.subscribe_trade()
                                    
        except Exception as e:
            self._logger.warning("[E]on_open: " + str(e))

    def decode_msg(self, msg):
        try:
            msg = zlib.decompress(msg, 16 + zlib.MAX_WBITS)
            # print(msg)
            msg = json.loads(msg, parse_float=float)     
            
            return msg       
        except Exception as e:
            self._logger.warning(traceback.format_exc())      

    def get_sub_trade_info(self, symbol_name:str, logger = None):
        sub_info = {'sub': f"market.{symbol_name.lower()}.trade.detail", 
                    'channel': 'orderbook', 
                    'market': self._sub_client_id}  
        
        self._sub_client_id += 1

        sub_info_str = json.dumps(sub_info)

        if logger is not None:
            self._logger.info("\nsub_info: \n" + sub_info_str)
        else:
            print("\nsub_info: \n" + sub_info_str)
        
        return sub_info_str           

    def get_sub_order_info(self, symbol_name, logger = None):
        sub_info = {'sub': f"market.{symbol_name.lower()}.depth.step0", 
                    'channel': 'orderbook', 
                    'market': self._sub_client_id}  
        
        self._sub_client_id += 1

        sub_info_str = json.dumps(sub_info)

        if logger is not None:
            self._logger.info("\nsub_trade_info: \n" + sub_info_str)
        else:
            print("\nsub_info: \n" + sub_info_str)
        
        return sub_info_str  

    # @abstractmethod
    def subscribe_depth(self):
        try:
            for symbol in self._symbol_dict:
                self._ws.send(self.get_sub_order_info(symbol, logger=self._logger))
        except Exception as e:
            self._logger.warning(traceback.format_exc())        
            
    # @abstractmethod
    def subscribe_trade(self):
        try:
            for symbol in self._symbol_dict:
                self._ws.send(self.get_sub_trade_info(symbol, logger=self._logger))
        except Exception as e:
            self._logger.warning(traceback.format_exc())                    

    def _check_success_symbol(self, ws_json):
        if  "status" in ws_json and ws_json["status"] == "ok" and 'subbed' in ws_json:
            detail_msg = ws_json['subbed']
            msg_list = detail_msg.split('.')
            symbol = msg_list[1]
            print("[S]: " + symbol)
            self._write_successful_currency(symbol)        
    
    def _check_failed_symbol(self, ws_json):
            if "status" in ws_json and ws_json["status"] == "error":
                err_msg = ws_json["err-msg"]
                pos = err_msg.find("invalid symbol")
                if pos != -1:
                    failed_symbol = err_msg[(pos+len("invalid symbol")):]
                    print("[F]: " + failed_symbol)
                    self._write_failed_currency(failed_symbol)        

    def on_timer(self):
        try:
            # return
        
            # if self._is_connnect:
            #     self._ws.send(self.get_ping_sub_info())        

            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    # @abstractmethod
    def get_ping_sub_info(self):
        try:
            sub_info = {"action": "pong"}

            sub_info_str = json.dumps(sub_info)
            
            return sub_info_str       
        except Exception as e:
            self._logger.warning(traceback.format_exc())   
                            
    def send_ping(self):
        try:
            self._ws.send(self.get_ping_sub_info())
            self._logger.info("pong")
                
        except Exception as e:
            self._logger.warning(traceback.format_exc())   

    def process_msg(self, ws_json):
        try:
            # print(ws_json)
            
            # self._logger.info(str(ws_json))
            
            if ws_json is None:
                return
            
            if 'ping' in ws_json:
                self.send_ping()
                return
            
            # self._check_failed_symbol(ws_json)
            # self._check_success_symbol(ws_json)
            
            if 'ch' in ws_json:
                if 'trade' in ws_json['ch']:
                    self._process_trades(ws_json=ws_json)
                elif 'depth' in ws_json['ch']:
                    pass
            elif 'ping' in ws_json:
                self._send_pong(ws_json)
            
                                        
        except Exception as e:
            self._logger.warning(traceback.format_exc())
            
    def _send_pong(self, ws_json):
        try:
            data = {'pong': ws_json['ping']}
            self._ws.send(json.dumps(data))
            
        except Exception as e:
            self._logger.warning(traceback.format_exc())            
        
    # @abstractmethod
    def start_exchange_moka(self):
        print("start_exchange_moka")
        pass

    # @abstractmethod
    def _process_depth(self, symbol, ws_json):
        try:
            '''
            snap: {"channel": "orderbook", "market": "BTC/USDT", "type": "partial", "data": {
                "time": 1603430536.3420923, "checksum": 157181535, 
                "bids": [[12967.0, 0.01], [12965.5, 0.4926], [12965.0, 0.01], [12964.0, 0.2177], [12963.5, 0.5778], [12961.5, 0.05], [12960.0, 0.0804], [12959.0, 0.2412], [12958.0, 0.0402], [12957.5, 0.751], [12957.0, 1.1556], [12956.5, 0.0346], [12956.0, 0.1206], [12955.5, 0.125], [12955.0, 0.4423], [12954.0, 0.0094], [12953.5, 14.5452], [12952.0, 0.679], [12950.5, 0.0094], [12950.0, 0.0402], [12949.0, 0.1206], [12945.0, 0.0071], [12944.5, 0.46], [12932.0, 19.5214], [12929.5, 22.2086], [12918.0, 21.7227], [12902.0, 24.5751], [12901.0, 0.0027], [12900.0, 0.0714], [12895.0, 0.0015], [12885.0, 32.072], [12884.0, 0.0002], [12873.5, 0.0011], [12868.0, 23.0035], [12860.0, 0.0009], [12856.0, 33.0817], [12852.5, 34.351], [12851.0, 0.005], [12850.0, 0.002], [12848.5, 39.8868], [12844.0, 0.0015], [12842.0, 0.0001], [12838.5, 41.9776], [12833.0, 38.6363], [12823.0, 0.0413], [12811.0, 30.139], [12801.0, 0.01], [12800.0, 0.4308], [12797.0, 39.9285], [12795.0, 36.3333], [12793.0, 0.0015], [12792.0, 0.0009], [12782.5, 0.0011], [12777.5, 49.6213], [12764.5, 48.2311], [12758.0, 40.4049], [12750.0, 0.002], [12742.0, 0.0015], [12741.0, 34.8288], [12736.0, 0.0001], [12723.5, 3.0085], [12723.0, 0.0009], [12720.0, 0.0428], [12708.5, 47.093], [12700.0, 0.0716], [12692.5, 0.0011], [12691.0, 0.0015], [12655.0, 0.0009], [12650.0, 0.022], [12640.0, 0.0015], [12631.0, 0.0001], [12600.0, 0.072], [12589.0, 0.0015], [12586.0, 0.0009], [12550.0, 0.002], [12538.0, 0.0015], [12526.0, 0.0001], [12518.0, 0.0009], [12510.0, 0.4014], [12500.0, 0.1121], [12487.0, 0.0015], [12450.0, 0.0021], [12449.0, 0.0009], [12436.0, 0.0015], [12421.0, 0.0001], [12400.0, 0.0732], [12385.0, 0.0015], [12381.0, 0.0009], [12350.0, 0.0021], [12334.0, 0.0015], [12315.0, 0.0001], [12313.0, 0.0009], [12305.0, 60.4141], [12300.0, 0.0729], [12283.0, 0.0015], [12282.5, 0.0693], [12282.0, 0.1221], [12264.0, 0.1039], [12250.0, 0.0012], [12244.0, 0.0009]], 
                "asks": [[12968.0, 0.1], [12968.5, 0.07], [12970.0, 2.85], [12970.5, 0.1926], [12972.0, 0.7078], [12973.0, 11.1359], [12973.5, 0.125], [12974.0, 0.3222], [12975.0, 0.0402], [12976.0, 0.4824], [12976.5, 0.0208], [12977.0, 0.4423], [12979.0, 0.0002], [12980.5, 1.7604], [12987.0, 0.0029], [12988.0, 17.3429], [12992.5, 19.6155], [12994.5, 18.813], [12996.5, 0.0002], [12997.0, 0.0024], [13000.0, 1.1712], [13011.0, 24.2518], [13013.0, 0.0002], [13017.0, 26.5062], [13026.5, 21.439], [13028.5, 37.6109], [13030.0, 0.0002], [13030.5, 0.004], [13035.5, 27.1464], [13044.0, 0.001], [13047.0, 0.0002], [13048.0, 0.0015], [13050.0, 0.002], [13051.0, 26.6017], [13052.0, 0.0001], [13064.0, 0.0002], [13065.0, 0.0009], [13068.5, 0.0011], [13072.0, 41.2436], [13081.0, 0.0002], [13083.0, 34.9376], [13084.5, 36.7488], [13095.0, 35.1312], [13098.0, 0.0002], [13100.0, 0.0711], [13100.5, 29.4666], [13110.0, 42.1266], [13115.0, 0.0002], [13117.0, 0.001], [13132.0, 0.0002], [13134.0, 0.0009], [13134.5, 43.937], [13149.0, 0.0002], [13150.0, 0.002], [13157.0, 0.0001], [13162.0, 52.4055], [13164.5, 46.6344], [13166.0, 0.0002], [13180.0, 41.4774], [13183.5, 0.0002], [13189.0, 0.0594], [13200.0, 1.0743], [13202.0, 0.0007], [13211.5, 33.8949], [13215.5, 0.0416], [13217.0, 2.9447], [13250.0, 0.002], [13262.0, 0.001], [13263.0, 0.0001], [13270.0, 0.0009], [13285.0, 0.3333], [13289.0, 0.0532], [13300.0, 0.0791], [13303.5, 0.0623], [13334.0, 0.001], [13339.0, 0.0009], [13350.0, 0.002], [13368.0, 0.0001], [13397.0, 0.3333], [13400.0, 0.079], [13407.0, 0.0019], [13450.0, 0.0019], [13451.0, 0.0132], [13473.0, 0.0001], [13476.0, 0.0009], [13479.0, 0.001], [13500.0, 0.4116], [13544.0, 0.0009], [13550.0, 0.0011], [13552.0, 0.001], [13578.0, 0.0001], [13600.0, 0.0782], [13613.0, 0.0009], [13624.0, 0.001], [13650.0, 0.0011], [13681.0, 0.0009], [13684.0, 0.0001], [13696.0, 0.001], [13699.5, 0.0027], [13700.0, 0.0771]], 
                "action": "partial"}
            }
            update: {"channel": "orderbook", "market": "BTC/USDT", "type": "update", "data": {
                "time": 1603430536.9389837, "checksum": 1768880334, 
                "bids": [[12945.0, 0.0165], [12951.0, 0.0402], [12200.0, 0.0]], 
                "asks": [[12969.5, 3.36], [12968.5, 0.06], [13700.0, 0.0]], "action": "update"}
                }
            '''            
            pass

            # self.__publisher.pub_depthx(symbol=symbol, depth_update=depths, is_snapshot=subscribe_type=='partial')
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    # @abstractmethod
    def _process_trades(self, ws_json):
        try:
            '''
            {'ch': 'market.btcusdt.trade.detail', 'ts': 1641377078118, 
            'tick': {
                    'id': 146011879638, 'ts': 1641377078116, 
                    'data': [{'id': 146011879638447549965348259, 'ts': 1641377078116, 'tradeId': 102600033828, 
                                'amount': 0.0002, 'price': 46965.67, 'direction': 'sell'}]
                    }
            }
            '''
 
            exchange_symbol = ws_json['ch'].split('.')[1]
            
            if exchange_symbol not in self._symbol_dict:
                self._logger.warning("unkonw symbol : " + exchange_symbol)  
                return
            
            sys_symbol = self._symbol_dict[exchange_symbol]
                       
            for trade in  ws_json['tick']['data']:
                exg_time_nano = int(trade['ts']) * NANO_PER_MILL
                
                if trade['direction'] == 'buy':
                    direction = "Buy"
                else:
                    direction = "Sell"
                    
                price = float(trade["price"])
                volume = float(trade["amount"])

                if sys_symbol not in self._valid_trade_symbol:
                    self._valid_trade_symbol.append(sys_symbol)
                    self._write_successful_currency(sys_symbol)  
                    print(sys_symbol)

                self._publisher.pub_tradex(symbol=sys_symbol,
                                            direction=direction,
                                            exg_time=exg_time_nano,
                                            px_qty=(price, volume))
              
        except Exception as e:
            self._logger.warning(traceback.format_exc())

    def _process_ping(self, ws_json):
        pass     
            

def huobi_start():
    data_list = [DATA_TYPE.TRADE]
    huobi = HUOBI(symbol_dict=get_symbol_dict(os.getcwd() + "/symbol_list.json", "HUOBI"), \
                  sub_data_type_list=data_list, debug_mode=False, is_test_currency=False)
    huobi.start()
        
if __name__ == '__main__':
    huobi_start()