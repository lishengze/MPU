# !/bin/bash

PID=`ps aux|grep binance |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s

rm -fr ../log/BINANCE/*.log.*

ps -aux|grep python3