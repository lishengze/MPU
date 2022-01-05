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


class MarketData_HUOBI:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "HUOBI"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://api.huobi.pro/ws"
        self.__rest_depth_url = ""

        # You can hard code the pair_mapping or get all active pair mapping when you launch this script.
        # def huobi_pairs():
        #     r = requests.get('https://api.huobi.pro/v1/common/symbols').json()
        #     return {'{}_{}'.format(e['base-currency'].upper(), e['quote-currency'].upper()): '{}{}'.format(
        #         e['base-currency'], e['quote-currency']) for e in r['data']}
        self.__symbol_book = {"BTCUSDT": "BTC_USDT",  # the exchange needs 'btcusdt'
                              "HTUSDT": "HT_USDT",
                              "IRISBTC": "IRIS_BTC",
                              "IRISUSDT": "IRIS_USDT",

                              "ETHBTC": "ETH_BTC",
                              "ETHUSDT": "ETH_USDT",

                              "ZRXETH": "ZRX_ETH",
                              "ZRXBTC": "ZRX_BTC",
                              "ZRXUSDT": "ZRX_USDT",

                              "GNTETH": "GNT_ETH",
                              "GNTBTC": "GNT_BTC",
                              "GNTUSDT": "GNT_USDT",

                              "VETETH": "VET_ETH",
                              "VETBTC": "VET_BTC",
                              "VETUSDT": "VET_USDT",

                              "LTCBTC": "LTC_BTC",
                              "LTCUSDT": "LTC_USDT",

                              "EOSETH": "EOS_ETH",
                              "EOSBTC": "EOS_BTC",
                              "EOSUSDT": "EOS_USDT",

                              "XRPBTC": "XRP_BTC",
                              "XRPETH": "XRP_ETH",
                              "XRPUSDT": "XRP_USDT",

                              "XLMBTC": "XLM_BTC",
                              "XLMETH": "XLM_ETH",
                              "XLMUSDT": "XLM_USDT",

                              "QTUMBTC": "QTUM_BTC",
                              "QTUMETH": "QTUM_ETH",
                              "QTUMUSDT": "QTUM_USDT",

                              "ATOMBTC": "ATOM_BTC",
                              "ATOMUSDT": "ATOM_USDT",

                              "CMTBTC": "CMT_BTC",
                              "CMTETH": "CMT_ETH",
                              }

        self.ws_session = None
        self.ws_conn = None
        self.ws = None

        self.ws_loop = asyncio.new_event_loop()

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.asyncio_initiator, self.ws_loop)

        self.__publisher.logger(level=self.__publisher.info,
                                event=MDEvent.INITIALIZED())
        self.ws_future = asyncio.run_coroutine_threadsafe(self.ws_listener(), self.ws_loop)

        while True:
            time.sleep(10)

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
                        for chan in ["trade.detail", "depth.step0"]:
                            for pair in self.__symbol_book.keys():
                                client_id += 1
                                await ws.send_json({
                                    "sub": f"market.{pair.lower()}.{chan}",
                                    "id": client_id})

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = zlib.decompress(ws_msg.data, 16 + zlib.MAX_WBITS)
                            msg = json.loads(msg, parse_float=float)
                            # print(msg)

                            # Huobi sends a ping evert 5 seconds and will disconnect us if we do not respond to it
                            if 'ping' in msg:
                                await ws.send_json({'pong': msg['ping']})
                            elif 'status' in msg and msg['status'] == 'ok':
                                pass
                            elif 'ch' in msg:
                                symbol = self.__symbol_book[(msg['ch'].split('.')[1]).upper()]
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
                                # self.__publisher.logger(level=self.__publisher.warning,
                                #                         event=MDEvent.CONNECTIONERROR(
                                #                             "Invalid message type %s \n".join(msg)))
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
    a = MarketData_HUOBI(debug_mode=True)
