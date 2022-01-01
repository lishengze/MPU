import time
import datetime
import traceback
import json
import os

def get_utc_nano_time():
    ori_time = time.time()
    nano_time = ori_time - int(ori_time)

    utc_time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    time_array = time.strptime(utc_time_str,"%Y-%m-%d %H:%M:%S.%f")
    utc_time_sec_int = time.mktime(time_array)
    utc_time_sec_float = utc_time_sec_int + nano_time
    utc_time_nano = utc_time_sec_float * 1000000000

def get_nano_time():
    return time.time() * 1000000000

def get_utc_nano_minute():
    utc_time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    time_array = time.strptime(utc_time_str,"%Y-%m-%d %H:%M:%S.%f")
    utc_time_sec_int = time.mktime(time_array) 
    
    utc_time_sec_int = utc_time_sec_int - utc_time_sec_int % 60    
    utc_minute_nano = utc_time_sec_int * 1000000000
    
    return utc_minute_nano

def get_in_type(type):
    return "In_" + type;

def get_out_type(type):
    return "Out_" + type;

def get_datetime_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_config(logger = None, config_file=""):    
    json_file = open(config_file,'r')
    json_dict = json.load(json_file)
    if logger is not None:
        logger._logger.info("\n******* config *******\n" + str(json_dict))
    else:
        print("\n******* config *******\n" + str(json_dict))
    # time.sleep(3)

    return json_dict

def trans_sys_symbol_b2c2(sys_symbol):
    try:
        if sys_symbol.find("_") == -1:
            return ""
        else:
            result = sys_symbol.replace("_", "")
            
            if result.find("USDT") != -1:
                result = result.replace("USDT", "UST")
            result = result + ".SPOT"
            return result
    except Exception as e:
        print(traceback.format_exc())    

def trans_sys_symbol_binance(sys_symbol):
    try:
        if sys_symbol.find("_") == -1:
            return ""
        else:
            return sys_symbol.replace("_", "")
    except Exception as e:
        print(traceback.format_exc())    

def trans_sys_symbol_houbi(sys_symbol):
    try:
        if sys_symbol.find("_") == -1:
            return ""
        else:
            return sys_symbol.replace("_", "")
    except Exception as e:
        print(traceback.format_exc())            

def trans_sys_symbol_ftx(sys_symbol):
    try:
        if sys_symbol.find("_") == -1:
            return ""
        else:
            return sys_symbol.replace("_", "/")
    except Exception as e:
        print(traceback.format_exc())        

def trans_sys_symbol(sys_symbol:str, exchange:str):
    try:
        if exchange == "FTX":
            return trans_sys_symbol_ftx(sys_symbol)
        elif exchange == "HUOBI":
            return trans_sys_symbol_houbi(sys_symbol)
        elif exchange == "BINANCE":
            return trans_sys_symbol_binance(sys_symbol)     
        elif exchange == "B2C2":
            return trans_sys_symbol_b2c2(sys_symbol)                
    except Exception as e:
        print(traceback.format_exc())        
        

def get_exchange_sys_symbol_dict(sys_symbol_list, exchange:str):
    result = dict()
    for sys_symbol in sys_symbol_list:
        exchange_symbol = trans_sys_symbol(sys_symbol, exchange)
        
        if exchange_symbol == "":
            print("sys_symbol is error: %s" % (sys_symbol))
        else:
            result[exchange_symbol] = sys_symbol
        
    return result
    
def get_symbol_dict(config_file_name:str, exchange:str):
    config = get_config(config_file=config_file_name)
    
    exchange_sys_symbol_dict = get_exchange_sys_symbol_dict(config["symbol_list"], exchange)
    
    return exchange_sys_symbol_dict

def test_get_ori_sys_config():
    print(get_symbol_dict(os.getcwd() + "/symbol_list.json", "B2C2"))
    
if __name__ == "__main__":
    
    test_get_ori_sys_config()
