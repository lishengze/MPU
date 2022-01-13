# !/bin/bash

PID=`ps aux|grep huobi |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s

rm -fr log/HUOBI/*.log.*

ps -aux|grep python3