# !/bin/bash

PID=`ps aux|grep B2C2 |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s

rm -fr B2C2_log/

ps -aux|grep python3