import asyncio
import json
import datetime
import redis
import threading
import numpy
import sys
import time

from collections import deque

from pprint import pprint

kline1_topic = "KLINE"
#kline5_topic = "KLINE5"
kline60_topic = "SLOW_KLINE"
#klineday_topic = "KLINEd"
total_kline_type = [kline1_topic, kline60_topic]

g_redis_config_file_name = "./kline_redis_config.json"

def get_redis_config():    
    json_file = open(g_redis_config_file_name,'r')
    json_dict = json.load(json_file)
    print("\nredis_config")
    print(json_dict)
    time.sleep(3)

    return json_dict
    
def to_datetime(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")



class KLineSvc:
    def __init__(self, slow_period: int = 60, running_mode: str = "DEBUG"):
        self.__slow_period = int(slow_period)
        self.__mode = running_mode
        self.__verification_tag = False
        self.__topic_list = dict()
        self.__redis_config = get_redis_config()
        self.__svc_marketdata = redis.Redis(host=self.__redis_config["HOST"],
                                            port=self.__redis_config["PORT"],
                                            password=self.__redis_config["PWD"])

        self._publish_count_dict = {
            "start_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "end_time":time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }

        for kline_type in total_kline_type:
            self._publish_count_dict[kline_type] = {}

        self._timer_secs = 10
        self._timer = threading.Timer(self._timer_secs, self.on_timer)
        self._timer.start()

        self.__data_recover()

        self.__match_listener_thread = threading.Thread(target=self.__match_listener, name="SUBCRIBERMatch")
        self.__match_listener_thread.start()

        self.__loop = asyncio.get_event_loop()
        self.__task = asyncio.gather(self.__auto_timer(), self.__auto_delist())
        self.__loop.run_until_complete(self.__task)

        while True:
            time.sleep(3)

    def print_publish_info(self):
        self._publish_count_dict["end_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("\nFrom %s to %s Publish Statics: "% (self._publish_count_dict["start_time"],self._publish_count_dict["end_time"] ))
        for item in self._publish_count_dict:
            if item != "start_time" and item != "end_time":
                for symbol in self._publish_count_dict[item]:
                    print("%s.%s: %d" % (item, symbol, self._publish_count_dict[item][symbol]))
                    self._publish_count_dict[item][symbol] = 1

        self._publish_count_dict["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def on_timer(self):
        self.print_publish_info()

        self._timer = threading.Timer(self._timer_secs, self.on_timer)
        self._timer.start()


    def __data_recover(self):
        kline_keys = self.__svc_marketdata.hkeys(kline1_topic)

        for kline_type in total_kline_type:
            pipeline = self.__svc_marketdata.pipeline(False)

            for key in kline_keys:
                self.__topic_list.setdefault(key.decode(), KLine(slow_period=self.__slow_period))
                pipeline.hget(kline_type, key=key.decode())

            datas = pipeline.execute()

            # print("HGet Datas:")
            # print(datas)

            index = 0
            for data in datas:
                kline_obj = self.__topic_list[kline_keys[index].decode()]
                if data:
                    kline_obj.recover_kline(kline_data=json.loads(data), kline_type=kline_type)
                index += 1

    def __match_listener(self):
        while True:
            try:
                __pubsub_marketdata = self.__svc_marketdata.pubsub()
                __pubsub_marketdata.psubscribe("TRADEx*")

                for marketdata in __pubsub_marketdata.listen():
                    # print("marketdata: ")
                    # print(marketdata)

                    if marketdata["type"] == "pmessage":
                        # MarketMatch Resolution
                        trade_topic = marketdata["channel"].decode().split("|")[1]
                        trade_data = json.loads(marketdata["data"])
                        self.__topic_list.setdefault(trade_topic, KLine(slow_period=self.__slow_period))

                        # Update Local KLine Service
                        kline = self.__topic_list[trade_topic]
                        kline.new_trade(exg_time=to_datetime(trade_data["Time"]), price=float(trade_data["LastPx"]), volume=float(trade_data["Qty"]))

            except Exception as ex:
                err = sys.exc_info()
                err_msg = "   ".join([str(error) for error in err])
                print(err_msg, ex)

    def __redis_hmset(self, marketdata_pipe: redis.client.Pipeline, data: dict, kline_type: str):
        if self.__mode == "PRODUCTION":
            marketdata_pipe.hmset(kline_type, data)
        else:
            for topic, kline in data.items():
                print(f"{kline_type}/{topic}\n"
                        f" {kline}")

    async def __auto_delist(self):
        while True:
            exist_keys = self.__svc_marketdata.keys("DEPTHx*")
            live_topics = set()
            for key in exist_keys:
                live_topics.add(key.decode().replace("DEPTHx|", ""))
            now_topics = set(self.__topic_list.keys())

            pipeline = self.__svc_marketdata.pipeline(False)
            for expire_key in now_topics.difference(live_topics):
                if self.__mode == "PRODUCTION":
                    for kline_type in total_kline_type:
                        pipeline.hdel(kline_type, expire_key)
                else:
                    print(f"KLINE/{expire_key} Expired")

                self.__topic_list.pop(expire_key, None)

            pipeline.execute(False)

            await asyncio.sleep(3600)  # Scan every hour

    async def __auto_timer(self):
        while True:
            now_time = datetime.datetime.now() 
            
            f_now_time = float(now_time.strftime("%S.%f"))

            # print("now_time: %f" % (f_now_time))

            wait_secs = 60 - f_now_time

            # print("wait_secs %f" % (wait_secs))

            await asyncio.sleep(wait_secs)

            if self.__verification_tag is False or self.__verification_tag == now_time.minute:

                # Create pipeline of redis
                pipeline = self.__svc_marketdata.pipeline(False)

                temp_task_list = list(self.__topic_list.keys())
                for topic in temp_task_list:
                    # task_index += 1
                    kline = self.__topic_list[topic]
                    for kline_type, klines in kline.klines.items():
                        data = json.dumps(list(klines))

                        if kline_type in self._publish_count_dict:
                            if topic in self._publish_count_dict[kline_type]:
                                self._publish_count_dict[kline_type][topic] += 1
                            else:
                                self._publish_count_dict[kline_type][topic] = 1

                        # print("publish %s" % (f"{kline_type}x|{topic}"))
                        # print(data)

                        self.__svc_marketdata.publish(channel=f"{kline_type}x|{topic}", message=json.dumps(list(klines)[-120:]))
                        self.__redis_hmset(marketdata_pipe=pipeline, data={topic:data}, kline_type=kline_type)

                pipeline.execute(False)

                self.__verification_tag = (now_time.minute + 1) % 60
            else:
                print("__verification_tag: %d" % (self.__verification_tag))


class KLine:
    def __init__(self, slow_period: int = 60):
        self.epoch = datetime.datetime(1970,1,1)
        # T: Time/ O: Open/ H: High/ L: Low/ C: Close/ V: Volume/ Q: Quotes
        self.klines = {}
        for kline_type in total_kline_type:
            self.klines[kline_type] = deque(maxlen=1440)

    def _update_klines(self, klines, klinetime, price, volume):
        ts = int((klinetime - self.epoch).total_seconds())
        if len(klines) == 0 or ts > klines[-1][0]:
            # new kline
            kline = [ts, price, price, price, price, volume, 1.0]
            klines.append(kline)
        else:
            kline = klines[-1]
            kline[4] = price
            kline[5] += volume
            kline[6] += 1.0
            if price > kline[2]:
                kline[2] = price
            if price < kline[3]:
                kline[3] = price
            klines[-1] = kline

    def new_trade(self, exg_time: datetime.datetime, price: float, volume: float):
        # 1min kline
        wait_secs = float(exg_time.strftime("%S.%f"))
        tval = exg_time - datetime.timedelta(seconds=wait_secs)
        self._update_klines(self.klines[kline1_topic], tval, price, volume)

        # 60min kline
        wait_mins = float(tval.strftime("%M"))
        tval = tval - datetime.timedelta(minutes=wait_mins)
        self._update_klines(self.klines[kline60_topic], tval, price, volume)

    def recover_kline(self, kline_data: list, kline_type: str):
        assert kline_type in total_kline_type
        self.klines[kline_type] = deque(kline_data, maxlen=1440)


if __name__ == '__main__':
    # 运行脚本：[exe] 60 PRODUCTION
    if len(sys.argv) == 3:
        svc = KLineSvc(slow_period=sys.argv[1], running_mode=sys.argv[2])
    else:
        svc = KLineSvc(slow_period=2, running_mode="DEBUG")

