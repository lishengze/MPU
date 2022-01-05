import re
import sys
import json
import asyncio
import redis
import multiprocessing
import threading
import logging
import logging.handlers

import time
import datetime

from MarketV2_POLONIEX import MarketData_POLONIEX
from MPU.binance import MarketData_BINANCE
from MarketV2_BINANCE_Perpetual import MarketData_BINANCE_Perpetual
from MarketV2_BITMEX import MarketData_BITMEX
from MarketV2_GDAX import MarketData_GDAX
from MarketV2_HITBTC import MarketData_HITBTC
from MarketV2_XDAEX import MarketData_XDAEX
from MPU.huobi import MarketData_HUOBI
from MarketV2_HUOBI_Future import MarketData_HUOBI_Future
from MarketV2_HUOBI_Perpetual import MarketData_HUOBI_Perpetual
from MarketV2_OKEX import MarketData_OKEX
from MarketV2_OKEX_Future import MarketData_OKEX_Future
from MarketV2_OKEX_Perpetual import MarketData_OKEX_Perpetual
from MarketV2_HASHKEYPRO import MarketData_HASHKEYPRO
from MarketV2_HASHKEYPRO2 import MarketData_HASHKEYPRO2
from MarketV2_B2C2 import MarketData_B2C2
from MPU.ftx import MarketData_FTX

g_redis_config_file_name = "../redis_config.json"

def get_redis_config():    
    json_file = open(g_redis_config_file_name,'r')
    json_dict = json.load(json_file)
    print("\n******* redis_config *******")
    print(json_dict)
    time.sleep(3)

    return json_dict

class MarketDataSurveillance:
    def __init__(self, debug_mode: bool = True):
        self.__debug_mode = debug_mode

        # Initialize Logging Module-Rotating Frequency: Everyday at midnight
        LOG_FILENAME = f"MPU_EventLog.log"
        LOG_FORMAT = logging.Formatter("[%(levelname)s] %(message)s")

        self.__logger = logging.getLogger("Entry_MarketV2")
        self.__logger.setLevel(logging.INFO)
        time_rotate_handler = logging.handlers.TimedRotatingFileHandler(filename=LOG_FILENAME, utc=True,
                                                                        when="W4", interval=1)
        time_rotate_handler.setFormatter(LOG_FORMAT)
        self.__logger.addHandler(time_rotate_handler)

        # Define variable
        self.log(level=self._INFO, topic="Initialize", msg=f"DEBUG_MODE={self.__debug_mode}")

        self.__md_svc = {
            # "BINANCE": MarketData_BINANCE,
            #"BINANCE_PERPETUAL": MarketData_BINANCE_Perpetual,
            #"GDAX": MarketData_GDAX,
            #"BITMEX": MarketData_BITMEX,
            #"HITBTC": MarketData_HITBTC,
            #"POLONIEX": MarketData_POLONIEX,
            #"XDAEX": MarketData_XDAEX,
            #"HASHKEYPRO": MarketData_HASHKEYPRO,
            # "HUOBI": MarketData_HUOBI,
            #"HUOBI_FUTURE": MarketData_HUOBI_Future,
            #"HUOBI_PERPETUAL": MarketData_HUOBI_Perpetual,
            # "OKEX": MarketData_OKEX,
            #"OKEX_FUTURE": MarketData_OKEX_Future,
            #"OKEX_PERPETUAL": MarketData_OKEX_Perpetual,
            "FTX": MarketData_FTX,
            # "B2C2": MarketData_B2C2,
            #"HASHKEYPRO2": MarketData_HASHKEYPRO2,
        }

        self.__re_md_svc = re.compile("|".join(self.__md_svc.keys()))

        self.__md_handler = self.__md_svc.fromkeys(self.__md_svc.keys())
        self.__md_connected = self.__md_svc.fromkeys(self.__md_svc.keys(), False)

        #self.__redis_config = {"HOST": "10.7.103.54",
        #                       "PORT": 6666,
        #                       "PWD": "rkqFB4,wpoMmHqT6he}r"}

        self.__redis_config = get_redis_config()

        self.__redis_conn = redis.Redis(host=self.__redis_config["HOST"],
                                        port=self.__redis_config["PORT"],
                                        password=self.__redis_config["PWD"])

        self.__redis_pubsub = self.__redis_conn.pubsub()
        self.__redis_pubsub.psubscribe("UPDATEx|*")

        self.__redis_thread = threading.Thread(target=self.redis_listener, name="REDIS_Thread", daemon=True)
        self.__redis_thread.start()

        if self.__debug_mode is False:
            for svc in self.__md_svc:
                self.start_process(svc_key=svc)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait([self.__sentinel(), self.__publish_status(), self.__auto_restart()]))

    def start_process(self, svc_key):
        try:
            process = multiprocessing.Process(target=self.__md_svc[svc_key], args=(self.__debug_mode, self.__redis_config),
                                              name=f"MarketV2_{svc_key}", daemon=True)
            process.start()
            self.log(level=self._INFO, topic="Start", msg=f"MarketData Start: {svc_key}")
            self.__md_handler[svc_key] = process
            self.__md_connected[svc_key] = False

        except Exception:
            err = sys.exc_info()
            self.log(level=self._CRITICAL, topic="Exception",
                     msg=("\n".join([str(error) for error in err])))

    def terminate_process(self, svc_key):
        if svc_key in self.__md_handler:
            self.log(level=self._DEBUG, topic="Terminate", msg=f"MarketData Terminated: {svc_key}")
            self.__md_handler[svc_key].terminate()
            self.__md_handler.pop(svc_key)

        else:
            self.log(level=self._DEBUG, topic="Terminate", msg=f"MarketData Terminated: {svc_key} not exist")

    def restart_process(self, svc_key):
        self.log(level=self._INFO, topic="Restart", msg=f"MarketData Restart: {svc_key}")
        self.terminate_process(svc_key=svc_key)
        self.start_process(svc_key=svc_key)

    def redis_listener(self):
        while True:
            for msg in self.__redis_pubsub.listen():
                if msg["type"] == "pmessage":
                    topic = msg["channel"].decode()
                    exchange = topic.split(".")[-1]
                    if exchange in self.__md_connected:
                        self.__md_connected[exchange] = True
                    if "@P" in topic:
                        exchange = f'{exchange}_PERPETUAL'
                        if exchange in self.__md_connected:
                            self.__md_connected[exchange] = True
                    elif "@" in topic:
                        exchange = f'{exchange}_FUTURE'
                        if exchange in self.__md_connected:
                            self.__md_connected[exchange] = True


    async def __sentinel(self):
        # If No Depth Update in 8 secs, Restart Process
        while True:
            await asyncio.sleep(8)

            self.log(level=self._DEBUG, topic="Sentinel", msg="Sentinel start to check")

            for exchange, connect_status in self.__md_connected.items():
                if connect_status is False:
                    self.log(level=self._WARNING, topic="Expired", msg=f"MarketData Expired: {exchange}, Restarting")
                    self.restart_process(svc_key=exchange)

                self.__md_connected[exchange] = False

    async def __auto_restart(self):
        # Restart MarketData Service at 04:00Z everyday
        while True:
            tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
            countdown = datetime.datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day,
                                          hour=4, minute=0) - datetime.datetime.utcnow()
            countdown_secs = countdown.total_seconds()

            await asyncio.sleep(countdown_secs)

            self.log(level=self._INFO, topic="Scheduled", msg="MarketData Scheduled Task: Restart")

            for svc_key in self.__md_svc:
                self.restart_process(svc_key=svc_key)

    async def __publish_status(self):
        # Update MarketData Service Status every 1 secs
        while True:
            await asyncio.sleep(0.5)

            self.log(level=self._DEBUG, topic="PublishStatus", msg=json.dumps(self.__md_connected))
            pipe = self.__redis_conn.pipeline(False)

            for exchange, status in self.__md_connected.items():
                now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
                pipe.hset("STATUS", exchange, json.dumps({"Time": now, "Connected": status}))

            pipe.expire("STATUS", 15)
            pipe.execute(False)

    def log(self, level: str, topic: str, msg: str = "", error_id: int = 0, error_msg: str = ""):
        """
        Log method
        :param level: DEBUG/INFO/WARNING/ALERT/CRITICAL
        :param topic: Log caller
        :param msg: message
        :param error_id: ErrorMsg.errorID
        :param error_msg: ErrorMsg.errorMsg

        :return: void
        """
        event_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%fZ')
        log_msg = f"{topic} | {event_time} | {msg} | ErrorID-{error_id} {error_msg}"

        if level == self._INFO:
            self.__logger.info(log_msg)
        elif level == self._WARNING:
            self.__logger.warning(log_msg)
        elif level == self._CRITICAL:
            self.__logger.critical(log_msg)
        elif level == self._ERROR:
            self.__logger.error(log_msg)
        elif level == self._DEBUG:
            self.__logger.debug(log_msg)

    @property
    def _DEBUG(self) -> str:
        return "DEBUG"

    @property
    def _INFO(self) -> str:
        return "INFO"

    @property
    def _WARNING(self) -> str:
        return "WARNING"

    @property
    def _ERROR(self) -> str:
        return "ERROR"

    @property
    def _CRITICAL(self) -> str:
        return "CRITICAL"


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == "PRODUCTION":
            obj = MarketDataSurveillance(debug_mode=False)
        else:
            obj = MarketDataSurveillance(debug_mode=True)
    else:
        obj = MarketDataSurveillance(debug_mode=True)
