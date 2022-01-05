import asyncio
import json
import aiohttp
import sys
import zlib
from MdPublisher import *
from concurrent.futures import ThreadPoolExecutor
import uuid

from datetime import datetime
from datetime import timedelta
import time
from settings import REDIS_CONFIG


class MarketData_HUOBI_Perpetual:
    def __init__(self, debug_mode: bool = True, redis_config: dict = None):
        # Initialize REDIS Connection
        if redis_config is None:
            redis_config = REDIS_CONFIG

        exchange = "HUOBI"

        self.__publisher = Publisher(exchange=exchange, redis_config=redis_config, debug_mode=debug_mode)

        self.__ws_url = "wss://api.hbdm.com/swap-ws"
        self.__rest_depth_url = ""
        self.__symbol_book = {
                            "BTC-USD": "BTC_USD@P",
                            "LINK-USD": "LINK_USD@P",
                            "ETH-USD": "ETH_USD@P",
                            "EOS-USD": "EOS_USD@P",
                            "ADA-USD": "ADA_USD@P",
                            "LTC-USD": "LTC_USD@P",
                            "BCH-USD": "BCH_USD@P",
                            "BSV-USD": "BSV_USD@P",
                            "XRP-USD": "XRP_USD@P",
                            "TRX-USD": "TRX_USD@P"
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
                        for chan in ["trade.detail", "depth.step6"]:
                            for pair in self.__symbol_book.keys():
                                client_id += 1
                                await ws.send_json({
                                    "sub": f"market.{pair}.{chan}",
                                    "id": str(uuid.uuid1())})

                        async for ws_msg in ws:
                            if ws_msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                                # Websocket Forced Close, Break the Loop and Reconnect Websocket
                                self.__publisher.logger(level=self.__publisher.error,
                                                        event=MDEvent.WSERROR(ws_msg.type))
                                break

                            msg = zlib.decompress(ws_msg.data, 16 + zlib.MAX_WBITS)
                            msg = json.loads(msg, parse_float=float)
                            #print(msg)

                            # Huobi sends a ping evert 5 seconds and will disconnect us if we do not respond to it
                            if 'ping' in msg:
                                await ws.send_json({'pong': msg['ping']})
                            elif 'status' in msg and msg['status'] == 'ok':
                                pass
                            elif 'ch' in msg:
                                if 'trade' in msg['ch']:
                                    for trade in msg['tick']['data']:
                                        symbol = (msg['ch'].split('.')[1]).upper()
                                        # dt = datetime.utcfromtimestamp(trade['ts'] / 1000)
                                        symbol = self.__symbol_book[symbol]
                                        direction = "Buy" if trade['direction'] == 'buy' else "Sell"
                                        self.__publisher.pub_tradex(symbol=symbol,
                                                                    direction=direction,
                                                                    exg_time=self.__time_convert(trade['ts']),
                                                                    px_qty=(
                                                                        float(trade['price']), float(trade['amount'])))

                                elif 'depth' in msg['ch']:
                                    data = msg['tick']
                                    symbol = (msg['ch'].split('.')[1]).upper()
                                    symbol = self.__symbol_book[symbol]
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
                                self.__publisher.logger(level=self.__publisher.warning,
                                                        event=MDEvent.CONNECTIONERROR(
                                                            "Invalid message type %s \n".join(msg)))
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

    def lastDayOfMonth(self, year, month):
        bfMonth = month + 1
        shifYear = 0
        if bfMonth == 13:
            bfMonth = 1
            shifYear = 1
        return datetime(year=year + shifYear, month=bfMonth, day=1, hour=8, minute=0, second=0, microsecond=0) - timedelta(days=1)

    def lastFridayOfMonth(self, year, month):
        day = self.lastDayOfMonth(year, month)
        while True:
            wday = int(day.strftime("%w"))
            if wday == 5:
                return day
            day = day - timedelta(days=1)

    def shiftFriday(self, dt: datetime, shift):
        tmpDt = datetime(dt.year, dt.month, dt.day, 8, 0, 0)
        wday = int(tmpDt.strftime("%w"))
        shiftDt = tmpDt + timedelta((12 - wday) % 7 + shift * 7)
        return shiftDt

    def renameCQSymbol(self, symbol: str, updateTime: datetime):
        bq = 0
        aq = 0
        yearShift = 0
        year = int(updateTime.strftime("%Y"))
        month = int(updateTime.strftime("%m"))
        if month <= 3:
            bq = 3
            aq = 6
        elif month <= 6:
            bq = 6
            aq = 9
        elif month <= 9:
            bq = 9
            aq = 12
        elif month <= 12:
            bq = 12
            aq = 3
            yearShift = 1
        nwTime = updateTime + timedelta(days=14)
        bqTime = self.lastFridayOfMonth(year, bq)
        aqTime = self.lastFridayOfMonth(year + yearShift, aq)
        if nwTime < bqTime:
            return symbol.replace("_CQ", "_USD@") + bqTime.strftime("%y%m%d")
        else:
            return symbol.replace("_CQ", "_USD@") + aqTime.strftime("%y%m%d")

    def renameNQSymbol(self, symbol: str, updateTime: datetime):
        bq = 0
        aq = 0
        yearShift = 0
        year = int(updateTime.strftime("%Y"))
        month = int(updateTime.strftime("%m"))
        if month <= 3:
            bq = 3
            aq = 6
        elif month <= 6:
            bq = 6
            aq = 9
        elif month <= 9:
            bq = 9
            aq = 12
        elif month <= 12:
            bq = 12
            aq = 3
            yearShift = 1
        nwTime = updateTime + timedelta(days=14)
        bqTime = self.lastFridayOfMonth(year, bq)
        aqTime = self.lastFridayOfMonth(year + yearShift, aq)
        if nwTime < bqTime:
            nq = bq + 3
            if nq > 12:
                nq = 3
                yearShift += 1
            nqTime = self.lastFridayOfMonth(year + yearShift, nq)
            return symbol.replace("_NQ", "_USD@") + nqTime.strftime("%y%m%d")
        else:
            nq = aq + 3
            if nq > 12:
                nq = 3
                yearShift += 1
            nqTime = self.lastFridayOfMonth(year + yearShift, nq)
            return symbol.replace("_NQ", "_USD@") + nqTime.strftime("%y%m%d")

    def renameCWSymbol(self, symbol: str, updateTime: datetime):
        cw = self.shiftFriday(updateTime, 0)
        if cw > updateTime:
            return symbol.replace("_CW", "_USD@") + cw.strftime("%y%m%d")
        else:
            nw = self.shiftFriday(updateTime, 1)
            return symbol.replace("_CW", "_USD@") + nw.strftime("%y%m%d")

    def renameNWSymbol(self, symbol: str, updateTime: datetime):
        cw = self.shiftFriday(updateTime, 0)
        if cw > updateTime:
            nw = self.shiftFriday(updateTime, 1)
            return symbol.replace("_NW", "_USD@") + nw.strftime("%y%m%d")
        else:
            nw = self.shiftFriday(updateTime, 2)
            return symbol.replace("_NW", "_USD@") + nw.strftime("%y%m%d")




if __name__ == '__main__':
    a = MarketData_HUOBI_Future(debug_mode=True)
    a.ws_listener()
