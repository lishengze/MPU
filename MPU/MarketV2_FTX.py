import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
import websocket
import json
import hmac
import threading

import os

sys.path.append(os.getcwd())
from Logger import *


g_redis_config_file_name = os.getcwd() + "/redis_config.json"

def get_config(logger = None, config_file=""):    
    json_file = open(config_file,'r')
    json_dict = json.load(json_file)
    if logger is not None:
        logger._logger.info("\n******* config *******\n" + str(json_dict))
    else:
        print("\n******* config *******\n" + str(json_dict))
    time.sleep(3)

    return json_dict

def get_login_info(api_key, api_secret, logger = None):
    ts = int(time.time() * 1000)

    # self._logger._logger.info("0")
    tmp_sign_origin = hmac.new(api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256')
    # self._logger._logger.info(tmp_sign_origin)
    tmp_sign_hex = tmp_sign_origin.hexdigest()
    # self._logger._logger.info(tmp_sign_hex)

    
    sub_info = {'op': 'login', 
                    'args': 
                    {
                        'key': api_key,
                        'sign': tmp_sign_hex,
                        'time': ts,
                    }
    }

    sub_info_str = json.dumps(sub_info)

    if logger is not None:
        logger._logger.info("\nsub_info: \n" + sub_info_str)
    else:
        print("\nsub_info: \n" + sub_info_str)
    return sub_info_str

def get_sub_order_info(symbol_name, logger = None):
    sub_info = {'op': 'subscribe', 
                'channel': 'orderbook', 
                'market': symbol_name}  

    sub_info_str = json.dumps(sub_info)

    if logger is not None:
        logger._logger.info("\nsub_info: \n" + sub_info_str)
    else:
        print("\nsub_info: \n" + sub_info_str)
    
    return sub_info_str           

def get_sub_trade_info(symbol_name, logger = None):
    sub_info = {'op': 'subscribe', 
                'channel': 'trades', 
                'market': symbol_name}  

    sub_info_str = json.dumps(sub_info)

    if logger is not None:
        logger._logger.info("\nsub_trade_info: \n" + sub_info_str)
    else:
        print("\nsub_trade_info: \n" + sub_info_str)
    
    return sub_info_str         

def get_ping_info():
    sub_info = {'op': 'ping'}  

    sub_info_str = json.dumps(sub_info)
    # self._logger._logger.info(sub_info_str)
    
    return sub_info_str       

'''
Trade InstrumentID
BTC-USDT、ETH-USDT、BTC-USD、ETH-USD、USDT-USD、ETH-BTC
'''
class FTX(object):
    def __init__(self, debug_mode: bool = True, is_redis:bool = False, is_test_depth=False):
        try:
            self._ws_url = "wss://ftx.com/ws/"
            self._api_key = "s8CXYtq5AGVYZFaPJLvzb0ezS1KxtwUwQTOMFBSB"
            self._api_secret = "LlGNM2EWnKghJEN_T9VCZigkHBEPu0AgoqTjXmwA"
            self._ping_secs = 10
            # self._symbol_dict = {
            #     "BTC/USDT":"BTC_USDT",
            #     "ETH/USDT":"ETH_USDT",
            #     "BTC/USD":"BTC_USD",
            #     "ETH/USD":"ETH_USD",
            #     "USDT/USD":"USDT_USD",
            #     "ETH/BTC":"ETH_BTC"                        
            # }
            
            self._symbol_dict = {
                "BTC/USDT":"BTC_USDT",
                "ETH/USDT":"ETH_USDT",
                "BTC/USD":"BTC_USD",
                "ETH/USD":"ETH_USD",
                "USDT/USD":"USDT_USD",
                "ETH/BTC":"ETH_BTC"                        
            }
                        
            self._logger = Logger(program_name="FTX")
            self._error_msg_list = ["", ""]
            self.__exchange_name = "FTX"
            self._is_connnect = False
            self._ws = None
            self._is_test_depth = is_test_depth
            
            if self._is_test_depth:
                self._moka_depth = get_config(self._logger, "moka_depth.json")
                self._moka_depth_start = 0
                self._logger._logger.info("\n----------moka_depth: \n" + str(self._moka_depth))
                print("\n----------moka_depth: \n" + str(self._moka_depth))
            
            if is_redis:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/redis_config.json"                
            else:
                self._config_name = os.path.dirname(os.path.abspath(__file__)) + "/kafka_config.json"
                
            self._config = get_config(logger=self._logger, config_file=self._config_name)
            
            print(self._config_name)
            print(self._config)
                        
            self.__publisher = Publisher(exchange=self.__exchange_name, config=self._config, 
                                         is_redis=is_redis, debug_mode=debug_mode, logger=self._logger._logger)

            self._publish_count_dict = {
                "depth":{},
                "trade":{},
                "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }

            for item in self._symbol_dict:
                sys_symbol = self._symbol_dict[item]
                self._publish_count_dict["depth"][sys_symbol] = 0
                self._publish_count_dict["trade"][sys_symbol] = 0
                
        except Exception as e:
            self._logger._logger.warning("[E]__init__: " + str(e))

    def connect_ws_server(self, info):
        try:
            self._logger._logger.info("\n*****connect_ws_server %s %s %s *****" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), info, self._ws_url))
            # websocket.enableTrace(True)
            self._ws = websocket.WebSocketApp(self._ws_url)
            self._ws.on_message = self.on_msg
            self._ws.on_error = self.on_error                                    
            self._ws.on_open = self.on_open
            self._ws.on_close = self.on_close

            self._ws.run_forever()

        except Exception as e:
            self._logger._logger.warning("[E]connect_ws_server: " + str(e))

    def start_reconnect(self):
        try:
            self._logger._logger.info("\n------- %s Start Reconnect --------" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            while self._is_connnect == False:
                self.connect_ws_server("Reconnect Server")
                time.sleep(self._reconnect_secs)
        except Exception as e:
            self._logger._logger.warning("[E]start_reconnect: " + str(e))

    def start_timer(self):
        try:
            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger._logger.warning("[E]start_timer: " + str(e))

    def start(self):
        try:
            self.start_timer()
            self.connect_ws_server("Start Connect")
        except Exception as e:
            self._logger._logger.warning("[E]start: " + str(e))

    def on_msg(self, msg):
        try:
            dic = json.loads(msg)
            self.process_msg(dic)
        except Exception as e:
            self._logger._logger.warning("[E]on_msg: " + str(e))

    def on_open(self):
        try:
            self._logger._logger.info("\nftx_on_open")
            self._is_connnect = True

            sub_info_str = get_login_info(self._api_key, self._api_secret, logger=self._logger)
            self._ws.send(sub_info_str)

            time.sleep(3)
            for symbol in self._symbol_dict:
                self._ws.send(get_sub_order_info(symbol, logger=self._logger))
                self._ws.send(get_sub_trade_info(symbol, logger=self._logger))
        except Exception as e:
            self._logger._logger.warning("[E]on_open: " + str(e))

    def on_error(self):
        self._logger.Error("on_error")

    def on_close(self):
        try:
            self._logger._logger.warning("\n******* on_close *******")
            self._is_connnect = False        
            self.start_reconnect()
        except Exception as e:
            self._logger._logger.warning("[E]on_close: " + str(e))

    def print_publish_info(self):
        try:
            self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self._logger._logger.info("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
            for item in self._publish_count_dict:
                if item == "depth" or item == "trade":
                    for symbol in self._publish_count_dict[item]:
                        self._logger._logger.info("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                        self._publish_count_dict[item][symbol] = 0
            self._logger._logger.info("\n")

            self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        except Exception as e:
            self._logger._logger.warning("[E]print_publish_info: " + str(e))

    def on_timer(self):
        try:
            if self._is_connnect:
                self._ws.send(get_ping_info())        

            self.print_publish_info()

            self._timer = threading.Timer(self._ping_secs, self.on_timer)
            self._timer.start()
        except Exception as e:
            self._logger._logger.warning("[E]on_timer: " + str(e))

    def process_msg(self, ws_msg):
        try:
            if  ws_msg["type"] == "pong" or ws_msg["type"] == "subscribed":
                return

            if "data" not in ws_msg:
                self._logger._logger.warning("ws_msg is error: " + str(ws_msg))
                return

            data = ws_msg["data"]
            ex_symbol = ws_msg["market"]
            channel_type = ws_msg["channel"]
            
            if ex_symbol in self._symbol_dict:
                sys_symbol = self._symbol_dict[ex_symbol]
            else:
                self._logger._logger.info("process_msg %s is not in symbol_dict" % (ex_symbol))
                return

            if channel_type == 'orderbook':
                if sys_symbol in self._publish_count_dict["depth"]:
                    self._publish_count_dict["depth"][sys_symbol] += 1
                self.__parse_orderbook(sys_symbol, data)
            elif channel_type == 'trades':
                if sys_symbol in self._publish_count_dict["trade"]:
                    self._publish_count_dict["trade"][sys_symbol] += 1                
                self.__parse_trades(sys_symbol, data)
            else:
                error_msg = ("\nUnknow channel_type %s, \nOriginMsg: %s" % (channel_type, str(ws_msg)))
                self._logger._logger.warning("[E]process_msg: " + error_msg)                                  
        except Exception as e:
            self._logger._logger.warning("[E] process_msg: %s" % (str(ws_msg)))
            self._logger._logger.warning(e)

    def __parse_orderbook(self, symbol, msg):
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
            data = msg

            if not data:
                return

            if 'asks' not in data and 'bids' not in data:
                return

            subscribe_type = data.get('action', '')
            if subscribe_type not in ['partial', 'update']:
                return
            
            if self._is_test_depth and symbol == self._moka_depth["symbol"]:
                if self._moka_depth_start == 0:
                    subscribe_type = 'partial'
                else:
                    subscribe_type = 'update'
                    
                data = self._moka_depth

            depths = {"ASK": {}, "BID": {}}
            for info in data.get('asks', []):
                depths["ASK"][float(info[0])] = float(info[1])
            for info in data.get('bids', []):
                depths["BID"][float(info[0])] = float(info[1])

            # if symbol == "ETH_USDT":
            #     self._logger._logger.info("%s.%s PUBLISH: %s" % (self.__exchange_name, symbol, str(depths)))

            self.__publisher.pub_depthx(symbol=symbol, depth_update=depths, is_snapshot=subscribe_type=='partial')
        except Exception as e:
            self._logger._logger.warning("[E] __parse_orderbook: ")
            self._logger._logger.warning(e)

    def __parse_trades(self, symbol, data_list):
        try:
            '''
            {"channel": "trades", "market": "BTC/USDT", "type": "update", 
                "data": [{"id": 150635418, "price": 12946.5, "size": 0.0805, "side": "sell", "liquidation": false, "time": "2020-10-23T06:09:42.187960+00:00"}, 
                {"id": 150635419, "price": 12946.5, "size": 0.0402, "side": "sell", "liquidation": false, "time": "2020-10-23T06:09:42.188834+00:00"}
            ]}
            '''
            # if 'data' not in msg:
            #     error_msg = ("__parse_trades data is not in msg:  %s" % (str(msg)))
            #     return

            for trade in data_list:
                side = trade['side']
                exg_time = trade['time'].replace('T', ' ')[:-6]
                self.__publisher.pub_tradex(symbol=symbol,
                                            direction=side,
                                            exg_time=exg_time,
                                            px_qty=(float(trade['price']), float(trade['size'])))
        except Exception as e:
            self._logger._logger.warning("[E] __parse_trades: ")
            self._logger._logger.warning(str(e))

def test_ftx():
    ftx_obj = FTX(debug_mode=False, is_redis=True, is_test_depth=True)
    ftx_obj.start()

if __name__ == "__main__":
    # test_hmac()
    # test_websocket()
    # test_http_restful()

    test_ftx()
