import time
import datetime

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
    utc_minute_nano = utc_time_sec_int * 
    
    return utc_minute_nano

def get_in_type(type):
    return "In_" + type;

def get_out_type(type):
    return "Out_" + type;

def get_datetime_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')