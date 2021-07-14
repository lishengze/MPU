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

def get_login_info(api_key, api_secret):
    ts = int(time.time() * 1000)

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

def get_sub_market_info(symbol_name):
    sub_info = {'op': 'subscribe', 
                'channel': 'orderbook', 
                'market': symbol_name}  

    sub_info_str = json.dumps(sub_info)
    print(sub_info_str)
    
    return sub_info_str           

def get_ping_info():
    sub_info = {'op': 'ping'}  

    sub_info_str = json.dumps(sub_info)
    print(sub_info_str)
    
    return sub_info_str       

class FTX(object):
    def __init__(self):
        self._ws_url = "wss://ftx.com/ws/"
        self._api_key = "s8CXYtq5AGVYZFaPJLvzb0ezS1KxtwUwQTOMFBSB"
        self._api_secret = "LlGNM2EWnKghJEN_T9VCZigkHBEPu0AgoqTjXmwA"
        self._ping_secs = 10
        self._sub_symbol_list = ["", ""]

        self._is_connnect = False
        self._ws = None

    def start(self):
        print("\n\n***** Start Connect %s *****" % (self._ws_url))
        # websocket.enableTrace(True)
        self._ws = websocket.WebSocketApp(self._ws_url)
        self._ws.on_message = self.on_msg
        self._ws.on_error = self.on_error                                    
        self._ws.on_open = self.on_open
        self._ws.on_close = self.on_close

        self._timer = threading.Timer(self._ping_secs, self.on_timer)
        self._timer.start()

        self._ws.run_forever()

    def on_msg(self, msg):
        print("on_msg")
        # print(msg)

    def on_open(self):
        print("\nftx_on_open")

        self._is_connnect = True

        sub_info_str = get_login_info(self._api_key, self._api_secret)
        self._ws.send(sub_info_str)

        time.sleep(3)
        self._ws.send(get_sub_market_info())

    def on_error(self):
        print("on_error")

    def on_close(self):
        print("on_close")

    def on_timer(self):
        print("on_timer: ")
        if self._is_connnect:
            self._ws.send(get_ping_info())        

        self._timer = threading.Timer(self._ping_secs, self.on_timer)
        self._timer.start()
                    
        # while True:
        #     time.sleep(self._ping_secs)
        #     if self._is_connnect:
        #         self._ws.send(get_ping_info())

def test_ftx():
    ftx_obj = FTX()
    ftx_obj.start()


if __name__ == "__main__":
    # test_hmac()
    # test_websocket()
    # test_http_restful()

    test_ftx()
