# !/bin/bash

PID=`ps aux|grep ftx |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s

rm -fr log/FTX/*.log.*

ps -aux|grep python3