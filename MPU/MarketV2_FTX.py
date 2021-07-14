import asyncio
import json
import aiohttp
import sys
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG

def get_ping_info():
    sub_info = {'op': 'ping'}  

    sub_info_str = json.dumps(sub_info)
    print(sub_info_str)
    
    return sub_info_str   

def get_login_info():
    ts = int(time.time() * 1000)
    api_key = "s8CXYtq5AGVYZFaPJLvzb0ezS1KxtwUwQTOMFBSB"
    api_secret = "LlGNM2EWnKghJEN_T9VCZigkHBEPu0AgoqTjXmwA"
    # print("0")
    tmp_sign_origin = hmac.new(api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256')
    # print(tmp_sign_origin)
    tmp_sign_hex = tmp_sign_origin.hexdigest()
    # print(tmp_sign_hex)

    
    sub_info = {'op': 'login', 
                    'args': 
                    {
                        'key': api_key,
                        'sign': tmp_sign_hex,
                        'time': ts,
                    }
    }
    # print("1")

    sub_info_str = json.dumps(sub_info)
    print(sub_info_str)
    
    return sub_info_str

class MarketData_FTX:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        self.__exchange_name = "FTX"

        self.__publisher = Publisher(exchange=self.__exchange_name, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://ftx.com/ws/"

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.ws_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())

        self.__symbol_list = {
            "BTC/USDT": "BTC_USDT",
            "ETH/USDT": "ETH_USDT",
            "FTT/USDT": "FTT_USDT",
            "ETH/BTC" : "ETH_BTC",
        }


        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

        while True:
            time.sleep(10)
            self.ws.send_str(get_ping_info())

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    async def ws_listener(self):
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        self.ws = ws
                        print("FTX Connect {self.__ws_url} Success!")

                        print("Login First!")
                        ws.send_str(get_login_info())

                        for ref_no in list(self.__symbol_list.keys()):
                            await ws.send_json({'op': 'subscribe', 'channel': 'orderbook', 'market': ref_no})
                            await ws.send_json({'op': 'subscribe', 'channel': 'trades', 'market': ref_no})
                            #await ws.send_json({'op': 'subscribe', 'channel': 'markets', 'grouping': 500, 'market': ref_no})

                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = json.loads(ws_msg.data)
                            print(msg)
                            ex_symbol = msg.get('market', '')
                            channel_type = msg.get('channel', '')
                            if channel_type == 'orderbook':
                                self.__parse_orderbook(self.__symbol_list[ex_symbol], msg)
                            elif channel_type == 'trades':
                                self.__parse_trades(self.__symbol_list[ex_symbol], msg)
                            else:
                                print(msg)

                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.DISCONNECTED())

            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

                await asyncio.sleep(15)

    async def __aenter__(self):
        await asyncio.gather(self.ws_listener())
        return self

    def __parse_orderbook(self, symbol, msg):
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
        data = msg.get('data', None)
        if not data:
            return
        if 'asks' not in data and 'bids' not in data:
            return
        subscribe_type = data.get('action', '')
        if subscribe_type not in ['partial', 'update']:
            return
        depths = {"ASK": {}, "BID": {}}
        for info in data.get('asks', []):
            depths["ASK"][float(info[0])] = float(info[1])
        for info in data.get('bids', []):
            depths["BID"][float(info[0])] = float(info[1])

        print("%s PUBLISH: \n" % (__exchange_name))
        print(depths)

        self.__publisher.pub_depthx(symbol=symbol, depth_update=depths, is_snapshot=subscribe_type=='partial')

    def __parse_trades(self, symbol, msg):
        '''
        {"channel": "trades", "market": "BTC/USDT", "type": "update", 
            "data": [{"id": 150635418, "price": 12946.5, "size": 0.0805, "side": "sell", "liquidation": false, "time": "2020-10-23T06:09:42.187960+00:00"}, 
            {"id": 150635419, "price": 12946.5, "size": 0.0402, "side": "sell", "liquidation": false, "time": "2020-10-23T06:09:42.188834+00:00"}
        ]}
        '''
        if 'data' not in msg:
            return
        for trade in msg.get('data', []):
            side = trade['side']
            exg_time = trade['time'].replace('T', ' ')[:-6]
            self.__publisher.pub_tradex(symbol=symbol,
                                        direction=side,
                                        exg_time=exg_time,
                                        px_qty=(float(trade['price']), float(trade['size'])))

if __name__ == '__main__':
    a = MarketData_FTX(debug_mode=False)
