import time
import datetime
import traceback
import json
import os

def get_nano_per_sec():
    return 1000000000

def get_datetime_str():
    return datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

def get_dir_seprator():
    dir = os.getcwd()
    if dir.find('\\') != -1:
        return '\\'
    else:
        return '/'

def get_utc_nano_time():
    ori_time = time.time()
    nano_time = ori_time - int(ori_time)

    utc_time_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    time_array = time.strptime(utc_time_str,"%Y-%m-%d %H:%M:%S.%f")
    utc_time_sec_int = time.mktime(time_array)
    utc_time_sec_float = utc_time_sec_int + nano_time
    utc_time_nano = utc_time_sec_float * 1000000000
    
    return utc_time_nano

def get_str_time_from_nano_time(nano_time):

    if nano_time == 0:
        return "0"
        
    time_secs = float(nano_time) / get_nano_per_sec()
    
    _nano_secs = int(nano_time) % get_nano_per_sec()

    time_obj = time.localtime(time_secs)

    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time_obj) + "." + str(_nano_secs)

    return time_str


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

def log_info_(info, logger = None):
    if logger is not None:
        logger.info(info)
    else:
        print(info)
        
def log_warning_(logger = None, info=""):
    if logger is not None:
        logger.warning(info)
    else:
        print("[W]: " + str(info))        
        
def get_config(logger = None, config_file="", env_type:str=""):    
    print("config_file_name: " + config_file)
    json_file = open(config_file,'r')
    origin_config = json.load(json_file)
    
    log_info_("\n******* config *******\n" + str(origin_config))
    
    if env_type == "local":
        if "local" not in origin_config:
             log_warning_(logger, "local not int origin_config " + str(origin_config))
        else:
            return origin_config["local"]
    elif env_type == "dev":
        if "dev" not in origin_config:
             log_warning_(logger, "dev not int origin_config " + str(origin_config))
        else:
            return origin_config["dev"]      
    elif env_type == "qa":
        if "qa" not in origin_config:
             log_warning_(logger, "qa not int origin_config " + str(origin_config))
        else:
            return origin_config["qa"]                
    elif env_type == "stg":
        if "stg" not in origin_config:
             log_warning_(logger, "stg not int origin_config " + str(origin_config))
        else:
            return origin_config["stg"]                      
    elif env_type == "prd":
        if "prd" not in origin_config:
             log_warning_(logger, "prd not int origin_config " + str(origin_config))
        else:
            return origin_config["prd"]        
    else:
        return origin_config

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
            sys_symbol = sys_symbol.replace("_", "")
            sys_symbol = sys_symbol.lower()
            return sys_symbol
    except Exception as e:
        print(traceback.format_exc())    

def trans_sys_symbol_houbi(sys_symbol):
    try:
        if sys_symbol.find("_") == -1:
            return ""
        else:
            sys_symbol = sys_symbol.replace("_", "")
            sys_symbol = sys_symbol.lower()
            return sys_symbol
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
        
def trans_sys_symbol_okex(sys_symbol):
    try:
        if sys_symbol.find("_") == -1:
            return ""
        else:
            return sys_symbol.replace("_", "-")
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
        elif exchange == "OKEX":
            return trans_sys_symbol_okex(sys_symbol)           
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
    
def get_symbol_dict(config_file_name:str, exchange:str, env_type:str=""):
    config = get_config(config_file=config_file_name, env_type=env_type)
    
    exchange_sys_symbol_dict = get_exchange_sys_symbol_dict(config["symbol_list"], exchange)
    
    return exchange_sys_symbol_dict

def test_get_ori_sys_config():
    print(get_symbol_dict(os.getcwd() + "/symbol_list.json", "B2C2"))

def test_float_to_str():
    
    data = float(6e-5)
    print(data)
    
    # str_value = '{:.8f}'.format(6e-5)
    
    str_value = str(data)
    
    print(str_value)
    if 'e' in str_value:
        pos1 = str_value.find('e')
        value = str_value[0:pos1]
        pos2 = str_value.find('-')
        precise = str_value[pos2+1:]
    
        print(int(value), int(precise))
        
    pos = str_value.find('.')
        
    print(str_value)
    print("pos: %d " % (pos))

def get_test_symbol_list(test_count:int):
    result = []
    for i in range(0, test_count):
        result.append("A_"+ str(i) + "_USDT")
    return result

def test_nano_time():
    nano_time = get_utc_nano_time()
    print("nano_time: " + str(nano_time))

    print("time_str: " + get_str_time_from_nano_time(nano_time))

if __name__ == "__main__":
    
    # test_float_to_str()
    
    # test_get_ori_sys_config()

    test_nano_time()
