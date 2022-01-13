# !/bin/bash

rm -fr log/BINANCE/*.log.*

PID=`ps aux|grep binance |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s



ps -aux|grep python3