import asyncio
import json
import aiohttp
import sys
import zlib
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime
import time
from settings import REDIS_CONFIG

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
                              
class MarketData_HUOBI(ExchangeBase):
    def __init__(self, symbol_dict:dict, net_server_type: NET_SERVER_TYPE =NET_SERVER_TYPE.KAFKA, 
                debug_mode: bool = True, is_test_currency: bool = False):
        try:
            super().__init__(exchange_name="HUOBI", symbol_dict=symbol_dict, net_server_type=net_server_type,
                              debug_mode=debug_mode, is_test_currency=is_test_currency)  

            self.__ws_url = "wss://api.huobi.pro/ws"
            self.__rest_depth_url = ""

            self.ws_session = None
            self.ws_conn = None
            self.ws = None


            self._sub_type_list = ["trade.detail"]
            if not self._is_test_currency:
                self._sub_type_list.append("depth.step0")

            self.ws_loop = asyncio.new_event_loop()

            self.executor = ThreadPoolExecutor(max_workers=2)
            self.executor.submit(self.asyncio_initiator, self.ws_loop)

            # self.__publisher.logger(level=self.__publisher.info,
            #                         event=MDEvent.INITIALIZED())
            self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

            while True:
                time.sleep(10)
        except Exception as e:
            self._logger._logger.warning("[E]__init__: " + str(e))
                        

    # Thread Worker for aio_initiator Method
    def asyncio_initiator(self, loop):
        asyncio.set_event_loop(loop=loop)
        loop.run_forever()

    async def ws_listener(self):
        # HUOBI Websocket Session
        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(url=self.__ws_url, heartbeat=10, autoclose=True) as ws:
                        self.__publisher.logger(level=self.__publisher.info,
                                                event=MDEvent.CONNECTED())

                        client_id = 0
                        for chan in self._sub_type_list:
                            for pair in self._symbol_dict.keys():
                                client_id += 1
                                await ws.send_json({
                                    "sub": f"market.{pair.lower()}.{chan}",
                                    "id": client_id})

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self._logger._logger.warning(ws_msg)
                                break
                                
                                # self.__publisher.logger(level=self.__publisher.error,
                                #                         event=MDEvent.WSERROR(ws_msg.type))
                                # break

                            msg = zlib.decompress(ws_msg.data, 16 + zlib.MAX_WBITS)
                            msg = json.loads(msg, parse_float=float)
                            # print(msg)

                            # Huobi sends a ping evert 5 seconds and will disconnect us if we do not respond to it
                            if 'ping' in msg:
                                await ws.send_json({'pong': msg['ping']})
                            elif 'status' in msg and msg['status'] == 'ok':
                                pass
                            elif 'ch' in msg:
                                
                                if self._is_test_currency:
                                    pass
                                
                                symbol = self._symbol_dict[(msg['ch'].split('.')[1]).upper()]
                                if 'trade' in msg['ch']:
                                    for trade in msg['tick']['data']:
                                        direction = "Buy" if trade['direction'] == 'buy' else "Sell"
                                        self.__publisher.pub_tradex(symbol=symbol,
                                                                    direction=direction,
                                                                    exg_time=self.__time_convert(trade['ts']),
                                                                    px_qty=(
                                                                        float(trade['price']), float(trade['amount'])))
                                                                        
                                elif 'depth' in msg['ch']:
                                    data = msg['tick']
                                    depth_update = {"ASK": {}, "BID": {}}
                                    for px, qty in data["asks"]:
                                        depth_update["ASK"][float(px)] = float(qty)
                                    for px, qty in data["bids"]:
                                        depth_update["BID"][float(px)] = float(qty)

                                    if len(depth_update["ASK"]) or len(depth_update["BID"]):
                                        self.__publisher.pub_depthx(symbol=symbol, depth_update=depth_update,
                                                                    is_snapshot=True)
                                else:
                                    self.__publisher.logger(level=self.__publisher.warning,
                                                            event=MDEvent.CONNECTIONERROR(
                                                                "Invalid message type %s \n".join(msg)))
                            else:
                                pass
            except Exception:
                err = sys.exc_info()
                self.__publisher.logger(level=self.__publisher.critical,
                                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])))

            await asyncio.sleep(15)

    async def __aenter__(self):
        await asyncio.gather(self.ws_listener())
        return self

    def __time_convert(self, time_x: float) -> str:
        rt_time = datetime.utcfromtimestamp(time_x / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        return rt_time


if __name__ == '__main__':
    exchange = MarketData_HUOBI(symbol_dict=get_symbol_dict(os.getcwd() + "/symbol_list.json", "HUOBI"), \
                                debug_mode=False, is_test_currency=True)
