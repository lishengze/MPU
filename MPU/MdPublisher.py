"""
MdPublisher & MarketData Service:
    1. Define MarketData Service Standard:
        if raw Depth & Update cover full orderbook:
            Update anytime
        if raw Depth is limited and Update covers full orderbook:
            Update if in range(WORSTASK, WORSTBID)
        if raw Depth & Update both limited:
            Update if in range(WORSTASK, WORSTBID)
    2. Every MPU should refresh market depth data while len(ASK) or len(BID)<25
    3. Set All DEPTHx value expired after 2 days

    Version 1.1.2 | 2019.09.25
        Revised: Quality Control Mechanism(Raise Exception after message published)
        
    Version 1.1.1 | 2019.09.19
        Fix: Revised Update added to UPDATEx message
        Fix: Minor change for null situation handle(update dict)

    Version 1.1.0 | 2019.09.15
        Revised: Use SortedDict to improve performance(in AskDepth/BidDepth)

    Version 1.0.2 | 2019.09.14
        Revised: Full Depth while crossing with trade event

    Version 1.0.1 | 2019.09.05
        Add: Price-level identify mechanism(Quality Control)
        Revised: Improved Performance while combine DEPTHx

"""
import redis
import json
import operator
import logging
import requests
import sys
from sortedcontainers import SortedDict
from datetime import datetime
from collections import defaultdict


class Publisher:
    def __init__(self, exchange: str, redis_config: dict, exchange_topic: str = None, debug_mode: bool = False):
        self.__debug = debug_mode
        self.__crossing_flag = dict()  # {"Symbol": "Date"}

        if not self.__debug:
            self.__redis_conn = redis.Redis(host=redis_config["HOST"],
                                            port=redis_config["PORT"],
                                            password=redis_config["PWD"])
            self.__redis_pubsub = self.__redis_conn.pubsub()

        self.__exchange = exchange
        self.__msg_seq = 0
        self.__msg_seq_symbol = defaultdict(int)

        if exchange_topic:
            self.__exchange_topic = exchange_topic
        else:
            self.__exchange_topic = exchange      # 形如 "BINANCE"

        # Config logging
        if not self.__debug:
            logging.basicConfig(level=logging.INFO,
                                filename=f"MPUv2_Log_{self.__exchange_topic}.log",
                                format="[%(levelname)s]: %(message)s @(Local)%(asctime)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
        else:
            logging.basicConfig(level=logging.DEBUG,
                                filename=f"MPUv2_Log_{self.__exchange_topic}.log",
                                format="[%(levelname)s-DebugMode]: %(message)s @(Local)%(asctime)s",
                                datefmt="%Y-%m-%d %H:%M:%S")

        self.__logger = logging.getLogger(self.__exchange_topic)

        self.__orderbook = dict()

    @property
    def info(self) -> str:
        return "INFO"

    @property
    def warning(self) -> str:
        return "WARNING"

    @property
    def error(self) -> str:
        return "ERROR"

    @property
    def critical(self) -> str:
        return "CRITICAL"

    def pub_depthx(self, symbol: str, depth_update: dict, is_snapshot: bool = True, exg_time: str = None,
                   raise_exception: bool=True):
        """
        Publish Depth & Update
        :param symbol: 形如 ETH_USDT
        :param depth_update: {ASK:dict<PX:QTY>, BID:dict<PX:QTY>}
        :param is_snapshot: True=Snapshot, False=Update
        :param exg_time: Time provided by Exchange
        :param raise_exception: Quality control option, "true" raise exception while Ask/Bid crossing
        :return: Void
        """
        if len(depth_update["ASK"]) == 0 and len(depth_update["BID"]) == 0:
            return None

        if symbol not in self.__orderbook:
            self.__orderbook.setdefault(symbol, {"AskDepth": SortedDict(), "BidDepth": SortedDict()})
            if not self.__debug:
                cache_data = self.__redis_conn.get(f"DEPTHx|{symbol}.{self.__exchange_topic}")
            else:
                cache_data = None

            if cache_data:
                snapshot_cache = eval(cache_data)
                for px, qty in snapshot_cache["AskDepth"].items():
                    self.__orderbook[symbol]["AskDepth"][float(px)] = qty

                for px, qty in snapshot_cache["BidDepth"].items():
                    self.__orderbook[symbol]["BidDepth"][float(px)] = qty

        book = self.__orderbook[symbol]

        if not is_snapshot:  # Depth Update
            print("Is Update")
            raise_exception_flag = False
            revised_ask = dict()
            revised_bid = dict()
            for side in depth_update.keys():
                if side == "ASK":
                    depth_side = "AskDepth"
                else:
                    depth_side = "BidDepth"

                for px, qty in depth_update[side].items():
                    if qty == 0:
                        book[depth_side].pop(px, None)
                    else:
                        book[depth_side][px] = qty

            # Quality Control
            if len(book["AskDepth"]) and len(book["BidDepth"]):

                best_ask = book["AskDepth"].peekitem(0)
                best_bid = book["BidDepth"].peekitem(-1)
                if best_bid >= best_ask:
                    if raise_exception:
                        # Solution 1: Raise Exception force WS broken
                        raise_exception_flag = True

                    else:
                        ask_depth = book["AskDepth"]
                        bid_depth = book["BidDepth"]
                        # Solution 2: Replace Price-level which crossing
                        new_bid_update = [px for px, qty in depth_update["BID"].items() if qty > 0]
                        if len(new_bid_update):
                            max_bid_update = max(new_bid_update)
                            for px in ask_depth.keys():
                                if px <= max_bid_update:
                                    obj = ask_depth.pop(px, None)
                                    revised_ask[px] = 0
                                    # print(f"Error {obj}\n{json.dumps([depth_update])}\n{json.dumps(book)}")
                                else:
                                    break

                        new_ask_update = [px for px, qty in depth_update["ASK"].items() if qty > 0]
                        if len(new_ask_update):
                            min_ask_update = min(new_ask_update)
                            for px in bid_depth.keys()[::-1]:
                                if px >= min_ask_update:
                                    obj = bid_depth.pop(px, None)
                                    revised_bid[px] = 0
                                    # print(f"Error {obj}\n{json.dumps([depth_update])}\n{json.dumps(book)}")
                                else:
                                    break

            # Build msg to publish
            self.__msg_seq += 1
            self.__msg_seq_symbol[symbol] += 1

            time_arrive = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

            if not exg_time:
                exg_time = time_arrive

            # Publish Depth
            depth_msg = {"Msg_seq": self.__msg_seq,
                         "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                         "Exchange": self.__exchange,
                         "Symbol": symbol,
                         "Time": exg_time,
                         "TimeArrive": time_arrive,
                         "AskDepth": {px: qty for px, qty in book["AskDepth"].items()[:100]},
                         "BidDepth": {px: qty for px, qty in book["BidDepth"].items()[:-100:-1]}
                         }

            # "DEPTHx|ETH_USDT.BINANCE" key-value 实时更新
            self.__set(channel=f"DEPTHx|{symbol}.{self.__exchange_topic}",
                       message=json.dumps(depth_msg))

            # Publish Crossing_Snapshot while date-crossing(UTC)
            if symbol in self.__crossing_flag:
                if depth_msg["TimeArrive"][:10] != self.__crossing_flag[symbol]:
                    depth_msg["AskDepth"] = book["AskDepth"]
                    depth_msg["BidDepth"] = book["BidDepth"]
                    self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                             message=json.dumps(depth_msg))
                    self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

            else:
                self.__crossing_flag.setdefault(symbol, depth_msg["TimeArrive"][:10])
                depth_msg["AskDepth"] = book["AskDepth"]
                depth_msg["BidDepth"] = book["BidDepth"]
                self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                         message=json.dumps(depth_msg))

            # Publish Update
            depth_update["ASK"].update(revised_ask)
            depth_update["BID"].update(revised_bid)

            update_msg = {"Msg_seq": self.__msg_seq,
                          "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                          "Exchange": self.__exchange,
                          "Symbol": symbol,
                          "Time": exg_time,
                          "TimeArrive": time_arrive,
                          "AskUpdate": depth_update["ASK"],
                          "BidUpdate": depth_update["BID"]}

            # chanel "UPDATEx|ETH_USDT.BINANCE"
            self.__publish(channel=f"UPDATEx|{symbol}.{self.__exchange_topic}",
                           message=json.dumps(update_msg))

            if raise_exception_flag:
                raise Exception(f"Ask/Bid Price Crossing, Symbol: {symbol}")

        else:  # Depth Snapshot
            print("Is Snapshot")
            update_book = {"AskUpdate": {}, "BidUpdate": {}}
            for side in depth_update.keys():
                if side == "ASK":
                    depth_side = "AskDepth"
                    update_side = "AskUpdate"
                else:
                    depth_side = "BidDepth"
                    update_side = "BidUpdate"

                if not operator.eq(book[depth_side], depth_update[side]):
                    local_px_set = set(book[depth_side].keys())
                    update_px_set = set(depth_update[side].keys())
                    for px in update_px_set.difference(local_px_set):
                        update_book[update_side][px] = depth_update[side][px]

                    for px in local_px_set.difference(update_px_set):
                        update_book[update_side][px] = 0

                    for px in local_px_set.intersection(update_px_set):
                        if book[depth_side][px] != depth_update[side][px]:
                            update_book[update_side][px] = depth_update[side][px]

                    book[depth_side] = SortedDict(depth_update[side])

            # print("Build msg to publish(Publish if any update)")
            # print(update_book)

            # Build msg to publish(Publish if any update)
            if len(update_book["AskUpdate"]) or len(update_book["BidUpdate"]):
                self.__msg_seq += 1
                self.__msg_seq_symbol[symbol] += 1

                time_arrive = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
                # '2020-04-13 08:14:44.754154'

                if not exg_time:
                    exg_time = time_arrive

                # Publish Depth
                depth_msg = {"Msg_seq": self.__msg_seq,
                             "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                             "Exchange": self.__exchange,
                             "Symbol": symbol,
                             "Time": exg_time,
                             "TimeArrive": time_arrive,
                             "AskDepth": {px: qty for px, qty in book["AskDepth"].items()[:100]},
                             "BidDepth": {px: qty for px, qty in book["BidDepth"].items()[:-100:-1]}
                             }

                self.__set(channel=f"DEPTHx|{symbol}.{self.__exchange_topic}",
                           message=json.dumps(depth_msg))

                # Publish Crossing_Snapshot while date-crossing(UTC)
                if symbol in self.__crossing_flag:
                    # 前十位是 2020-04-13
                    if depth_msg["TimeArrive"][:10] != self.__crossing_flag[symbol]:
                        depth_msg["AskDepth"] = book["AskDepth"]
                        depth_msg["BidDepth"] = book["BidDepth"]
                        # CROSSING_SNAPSHOT hash key ETH_USDT.BINANCE
                        self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                                 message=json.dumps(depth_msg))
                        self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

                else:
                    self.__crossing_flag.setdefault(symbol, depth_msg["TimeArrive"][:10])
                    depth_msg["AskDepth"] = book["AskDepth"]
                    depth_msg["BidDepth"] = book["BidDepth"]
                    self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                             message=json.dumps(depth_msg))

                # Publish Update
                update_msg = {"Msg_seq": self.__msg_seq,
                              "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                              "Exchange": self.__exchange,
                              "Symbol": symbol,
                              "Time": exg_time,
                              "TimeArrive": time_arrive,
                              "AskUpdate": update_book["AskUpdate"],
                              "BidUpdate": update_book["BidUpdate"]}

                self.__publish(channel=f"UPDATEx|{symbol}.{self.__exchange_topic}",
                               message=json.dumps(update_msg))
            else:
                print("Nothing To Update")


    def pub_tradex(self, symbol: str, direction: str, exg_time: str, px_qty: tuple):
        """
        Publish Trade record
        :param symbol:
        :param direction: Taker operation: Buy/Sell
        :param exg_time:
        :param px_qty: (PX, QTY)
        :return: Void
        """
        # Publish Crossing_Snapshot while date-crossing(UTC)
        if symbol in self.__crossing_flag:
            if datetime.utcnow().strftime('%Y-%m-%d') != self.__crossing_flag[symbol]:
                book = self.__orderbook[symbol]
                depth_msg = {"Msg_seq": self.__msg_seq,
                             "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                             "Exchange": self.__exchange,
                             "Symbol": symbol,
                             "Time": exg_time,
                             "TimeArrive": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                             "AskDepth": book["AskDepth"],
                             "BidDepth": book["BidDepth"]
                             }

                self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                         message=json.dumps(depth_msg))
                self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

        elif symbol in self.__orderbook:  # Data's first day(not in __crossing_flag)
            book = self.__orderbook[symbol]
            depth_msg = {"Msg_seq": self.__msg_seq,
                         "Msg_seq_symbol": self.__msg_seq_symbol[symbol],
                         "Exchange": self.__exchange,
                         "Symbol": symbol,
                         "Time": exg_time,
                         "TimeArrive": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                         "AskDepth": book["AskDepth"],
                         "BidDepth": book["BidDepth"]
                         }
            self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                     message=json.dumps(depth_msg))
            self.__crossing_flag[symbol] = depth_msg["TimeArrive"][:10]

            self.__crossing_flag.setdefault(symbol, depth_msg["TimeArrive"][:10])
            self.__crossing_snapshot(channel=f"{symbol}.{self.__exchange_topic}",
                                     message=json.dumps(depth_msg))

        px, qty = px_qty

        msg = {"Exchange": self.__exchange,
               "Symbol": symbol,
               "Time": exg_time,
               "TimeArrive": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
               "Direction": direction,
               "LastPx": px,
               "Qty": qty}

        print("TradeInfo: ")
        print(msg)

        # channel TRADEx|ETH_USDT.BINANCE
        self.__publish(channel=f"TRADEx|{symbol}.{self.__exchange_topic}", message=json.dumps(msg))

    def __publish(self, channel: str, message):
        if not self.__debug:
            self.__redis_conn.publish(channel=channel, message=message)
        else:
            print(f"{channel}\n{message}")

    def __set(self, channel: str, message):
        if not self.__debug:  # All value in redis will expire after 2 days
            self.__redis_conn.set(name=channel, value=message, ex=172800)
        else:
            print(f"{channel}\n{message}")

    def __crossing_snapshot(self, channel: str, message):
        if not self.__debug:
            self.__redis_conn.hset(name="CROSSING_SNAPSHOT", key=channel, value=message)
        else:
            print(f"CROSSING_SNAPSHOT/{channel}\n{message}")

    def logger(self, level: str, msg: str = "", event: dict = None, use_dingrobot: bool = True):
        if event:
            event_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%fZ')
            msg = f"[{event['Event']}|{self.__exchange}] {event['Msg']}| {event_time}"

        if level == "INFO":
            self.__logger.info(msg)
        elif level == "WARNING":
            self.__logger.warning(msg)
        elif level == "ERROR":
            self.__logger.error(msg)
        elif level == "CRITICAL":
            self.__logger.critical(msg)

        if use_dingrobot:
            self.ding_robot(level, msg, event)

    def ding_robot(self, level: str, msg: str = "", event=None):
        msg_body = self.__ding_msg(level=level, msg=msg, event=event)
        path = "https://oapi.dingtalk.com/robot/send?access_token=2e3c275331d63bc6fd656f638237568ab26e10f06a986d42b75aef6dcc4ced4e"
        body = {"msgtype": "markdown", "markdown": msg_body, "at": {"isAtAll": False}}
        header = {"Content-Type": "application/json"}

        try:
            resp = requests.post(url=path, json=body, headers=header)

        except Exception:
            err = sys.exc_info()
            self.logger(level=self.critical,
                        event=MDEvent.CONNECTIONERROR("\n".join([str(error) for error in err])),
                        use_dingrobot=False)

    def __ding_msg(self, level: str, msg: str, event=None) -> dict:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        status = "Online"

        if event:
            msg = f"[{event['Event']}] {event['Msg']}"
            status = event["Status"]

        if not self.__debug:
            body = {"title": f"MDService", "text": f"## **MDService-{self.__exchange_topic}** \n"
                                                   f"### **Type: {level}** \n"
                                                   f"> #### **Status: {status}** \n"
                                                   f"> ##### {msg} \n\n"
                                                   f"###### Time(UTC): {now} \n"}
        else:
            body = {"title": f"MDService", "text": f"## **MDService-{self.__exchange_topic}** \n"
                                                   f"### **Type: {level}-DebugMode** \n"
                                                   f"> #### **Status: {status}** \n"
                                                   f"> ##### {msg} \n\n"
                                                   f"###### Time(UTC): {now} \n"}
        return body


class MDEvent:
    @staticmethod
    def CONNECTIONERROR(msg: str) -> dict:
        return {"Event": "CONNECTIONERROR",
                "Status": "Offline",
                "Msg": f"MDServ is offline, can't connect to the Internet.\nExceptionMsg: {msg}"}

    @staticmethod
    def DISCONNECTED() -> dict:
        return {"Event": "DISCONNECTED",
                "Status": "Offline",
                "Msg": f"MDServ is offline, due to exchange disconnected."}

    @staticmethod
    def CONNECTED() -> dict:
        return {"Event": "CONNECTED",
                "Status": "Online",
                "Msg": f"MDServ is online."}

    @staticmethod
    def INITIALIZED() -> dict:
        return {"Event": "INITIALIZED",
                "Status": "Pre-Online",
                "Msg": f"MDServ initialized, start MDService."}

    @staticmethod
    def WSERROR(err_type) -> dict:
        return {"Event": "WSERROR",
                "Status": "Offline",
                "Msg": f"MDServ is offline, due to websocket error type-{err_type}."}
