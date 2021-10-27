import logging
import logging.handlers
import datetime
import time

def get_datetime_str():
    return datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

class Logger(object):
    def __init__(self, program_name="test"):
        # LOG_FORMAT = "%(pathname)s-%(lineno)s - %(asctime)s - %(levelname)s - %(message)s"
        # DATE_FORMAT = "%Y/%m/%d %H:%M:%S"    
        # logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)

        self._debug_logger = logging.getLogger('debug_logger')
        self._debug_logger.setLevel(logging.DEBUG)     

        log_dir = "log/" 
        if program_name !="":
            log_dir = log_dir + program_name + "/"

        # debug_handler = logging.handlers.TimedRotatingFileHandler(log_dir + get_datetime_str()+"_debug.log", when='midnight', interval=1, backupCount=5, atTime=datetime.time(0, 0, 0, 0))
        debug_handler = logging.handlers.RotatingFileHandler(log_dir + get_datetime_str()+"_debug.log", maxBytes=5*1024*1024, backupCount=5)

        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(logging.Formatter(fmt="%(asctime)s-%(levelname)s-%(filename)s[:%(lineno)d]-%(message)s"))

        self._debug_logger.addHandler(debug_handler)

        self._logger = logging.getLogger('user_logger')
        self._logger.setLevel(logging.DEBUG)

        all_file_Name = log_dir+get_datetime_str()+"_info.log"
        error_file_name = log_dir+get_datetime_str()+"_warn.log"

        # detail_handler = logging.handlers.TimedRotatingFileHandler('log/all.log', when='midnight', interval=1, backupCount=7, atTime=datetime.time(0, 0, 0, 0))

        detail_handler = logging.handlers.TimedRotatingFileHandler(all_file_Name, when='midnight', interval=1, backupCount=5, atTime=datetime.time(0, 0, 0, 0))
        detail_handler.setFormatter(logging.Formatter("%(asctime)s-%(levelname)s-%(filename)s[:%(lineno)d]-%(message)s"))

        error_handler = logging.handlers.TimedRotatingFileHandler(error_file_name, when='midnight', interval=1, backupCount=5, atTime=datetime.time(0, 0, 0, 0))
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(logging.Formatter(fmt="%(asctime)s-%(levelname)s-%(filename)s[:%(lineno)d]-%(message)s"))

        self._logger.addHandler(detail_handler)
        self._logger.addHandler(error_handler)       

    def Debug(self, info):
        self._debug_logger.debug(info)

    def Info(self, info):
        self._logger.info(info)

    def Warning(self, info):
        self._logger.warning(info)

    def Error(self, info):
        self._logger.error(info)

    def Critical(self, info):
        self._logger.critical(info)        

def test_logger():
    logger = Logger();  

    logger.Debug("test debug %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    logger.Info("test info %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    logger.Warning("test warning %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    logger.Error("test error %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    logger.Critical("test critical %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

def simple_test():
    # LOG_FORMAT = "%(pathname)s-%(lineno)s - %(asctime)s - %(levelname)s - %(message)s"
    # DATE_FORMAT = "%Y/%m/%d %H:%M:%S"    
    # logging.basicConfig(format=LOG_FORMAT)

    debug_logger = logging.getLogger('debug_logger')
    debug_logger.setLevel(logging.DEBUG)         

    log_dir = "log/"
    debug_handler = logging.FileHandler(log_dir + get_datetime_str()+"_debug.log")

    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(fmt="%(asctime)s-%(levelname)s-%(filename)s[:%(lineno)d]-%(message)s"))

    debug_logger.addHandler(debug_handler)

    debug_logger.debug("test debug %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    debug_logger.info("test info %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    debug_logger.warning("test warning %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    debug_logger.error("test error %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    debug_logger.critical("test critical %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))    


def test_try():
    try:
        a = 1/0
    except Exception as e:
        print("exception: " + str(e))

def main():
    test_try()
    # test_logger()
    # simple_test()

if __name__ == '__main__':
    main()        