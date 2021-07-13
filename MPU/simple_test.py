import websocket
import json
import urllib.request
import http.client
import time
import _thread
import hmac

def process_heartbeat(ws):
    heartbeat_info = {
        "type":"heartbeat"
    }
    heartbeat_info_str = json.dumps(heartbeat_info)

    print("heartbeat_info_str: %s" % (heartbeat_info_str))
    ws.send(heartbeat_info_str)    

def get_time(secs):
    #转换成localtime
    time_local = time.localtime(secs)
    #转换成新的时间格式(2016-05-05 20:28:54)
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)   
    return dt 

def print_depth_data(dic):
    print("symbol: %s, time: %s, ask_len: %f, bid_len: %f" \
        % (dic["symbol"], get_time(int(dic["tick"]) / 1000000000), dic["ask_length"], dic["bid_length"]))

    print("asks")
    for item in dic["asks"]:
        print(item)

    print("bids")
    for item in dic["bids"]:
        print(item)        

def on_message(ws, message):
    print("on_message")
    print(message)

    dic = json.loads(message)
    if dic["type"] == "heartbeat":
        process_heartbeat(ws)
    elif dic["type"] == "symbol_update":
        pass
    elif dic["type"] == "kline_rsp":
        rsp_data = dic["data"]
        for item in rsp_data:
            print("time: %s, open: %s, high: %s, low: %s, close: %s " % \
                (get_time(float(item["tick"])), item["open"], item["high"], item["low"], item["close"]))
    elif dic["type"] == "market_data_update":
        print_depth_data(dic)
        
def on_error(ws, error):
    print("Error")
    print(error)

def on_close(ws):
    print("Server Closed")
    print("### closed ###")

def get_sub_depth_str(symbol="BTC_USDT"):
    sub_info = {
        "type":"sub_symbol",
        "symbol":[symbol]
    }
    sub_info_str = json.dumps(sub_info)
    print("sub_info_str: %s" % (sub_info_str))
    return sub_info_str

def get_sub_trade_str(symbol="BTC_USDT"):
    sub_info = {
        "type":"trade",
        "symbol":[symbol]
    }
    sub_info_str = json.dumps(sub_info)
    print("sub_info_str: %s" % (sub_info_str))
    return sub_info_str    

def get_sub_kline_str(symbl="BTC_USDT"):
    print("get_sub_kline_str")
    frequency = 60 * 5
    end_time = int(time.time())
    end_time = end_time - end_time % frequency - frequency
    start_time = end_time - 60 * 30

    # sub_info = {
    #     "type":"kline_update",
    #     "symbol":"BTC_USDT",
    #     "start_time":str(start_time),
    #     "end_time":str(end_time),
    #     "frequency":str(frequency)
    # }

    sub_info = {
        "type":"kline_update",
        "symbol":symbl,
        "data_count":str(600),
        "frequency":str(frequency)
    }

    sub_info_str = json.dumps(sub_info)
    print("\n\n\n****************************** sub_info_str: %s ****************************" % (sub_info_str))
    return sub_info_str

def sub_btc_usdt(ws, sub_symbol):
    time.sleep(5)

    # sub_info = {
    #     "type":"sub_symbol",
    #     "symbol":[sub_symbol]
    # }
    # sub_info_str = json.dumps(sub_info)
    # print("sub_info_str: %s" % (sub_info_str))

    # sub_info_str = get_sub_kline_str(sub_symbol)

    # sub_info_str = get_sub_depth_str(sub_symbol)

    sub_info_str = get_sub_trade_str(sub_symbol)

    print("\n\n\n****************************** sub_info_str: %s ****************************" % (sub_info_str))

    ws.send(sub_info_str)   

def on_open(ws):
    print("Connected")

    send_str = get_sub_depth_str()

    # send_str = get_sub_kline_str()

    # send_str = get_sub_trade_str()

    ws.send(send_str)

    # time.sleep(3)

    # send_str = get_sub_kline_str()

    # ws.send(send_str)


    # _thread.start_new_thread(sub_btc_usdt, (ws, "XRP_USDT", ) )

def ftx_on_close(ws):
    print("ftx_on_close")

def ftx_on_error(ws, error):
    print("ftx_on_error {error}")
    pass

def ftx_on_open(ws):
    print("\nftx_on_open")
    ts = int(time.time() * 1000)
    api_key = "s8CXYtq5AGVYZFaPJLvzb0ezS1KxtwUwQTOMFBSB"
    api_secret = "LlGNM2EWnKghJEN_T9VCZigkHBEPu0AgoqTjXmwA"
    print("0")
    tmp_sign_origin = hmac.new(api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256')
    print(tmp_sign_origin)
    tmp_sign_hex = tmp_sign_origin.hexdigest()
    print(tmp_sign_hex)

    
    sub_info = {'op': 'login', 
                    'args': 
                    {
                        'key': api_key,
                        'sign': tmp_sign_hex,
                        'time': ts,
                    }
    }
    print("1")

    sub_info_str = json.dumps(sub_info)

    print(sub_info_str)

    ws.send(sub_info_str)

def ftx_on_msg(ws, message):
    print("ftx_on_msg {message}")

def test_ftx_sub():
    url = "wss://ftx.com/ws/"
    print("\n\n***** Connect %s *****" % (url))
    
    ws = websocket.WebSocketApp(url,
                                on_message=ftx_on_open,
                                on_error=ftx_on_error,
                                on_close=ftx_on_close)
    ws.on_open = ftx_on_open
    ws.run_forever()

def test_websocket():
    test_ftx_sub()

def test_hmac():
    print("\nftx_on_open")
    ts = int(time.time() * 1000)
    api_key = "s8CXYtq5AGVYZFaPJLvzb0ezS1KxtwUwQTOMFBSB"
    api_secret = "LlGNM2EWnKghJEN_T9VCZigkHBEPu0AgoqTjXmwA"
    print("0")
    tmp_sign_origin = hmac.new(api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256')
    print(tmp_sign_origin)
    tmp_sign_hex = tmp_sign_origin.hexdigest()
    print(tmp_sign_hex)

    
    sub_info = {'op': 'login', 
                    'args': 
                    {
                        'key': api_key,
                        'sign': tmp_sign_hex,
                        'time': ts,
                    }
    }
    print("1")

    sub_info_str = json.dumps(sub_info)

    print(sub_info_str)


def test_http_restful():
    # test_urllib()
    test_http_client()

def get_http_kline_str():
    symbol = "BTC_USDT"
    frequency = 60 * 5
    end_time = int(time.time())
    end_time = end_time - end_time % frequency
    start_time = end_time - 60 * 30
    query_str = ("v1/kline_request/symbol=%s&start_time=%d&end_time=%d&frequency=%d" \
                % (symbol, start_time, end_time, frequency))
    return query_str  

def get_http_enquiry_str():
    symbol = "BTC_USDT"
    volume = 10
    direction_type = 0
    query_str = ("/v1/enquiry?symbol=%s&type=%d&volume=%d" \
                % (symbol, direction_type, volume))
    return query_str      

def test_http_client():
    # uri = "127.0.0.1:9115"
    uri = "36.255.220.139:9115"
    conn = http.client.HTTPConnection(uri)
    # query_str = get_http_kline_str()
    query_str = get_http_enquiry_str()

    print("query_str: %s" % (query_str))

    conn.request("GET", query_str)
    res =conn.getresponse()
    print(res.read().decode("utf-8"))
    
def test_urllib():
    uri = "http://127.0.0.1:9115"
    response = urllib.request.urlopen(uri)
    html = response.read()
    print("Http Response: \n%s" % (html))    


# class FTX(object):
#     def __init__(self):
#         self._ws_url = "wss://ftx.com/ws/"

#     def start(self):
#         pass

#     def on_msg(self, ws, msg):
#         pass

#     def on_open(self, ws):
#         pass


if __name__ == "__main__":
    # test_hmac()
    test_websocket()
    # test_http_restful()

