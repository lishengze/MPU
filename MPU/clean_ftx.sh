# !/bin/bash

rm -fr /mnt/market/MPU/log/FTX/*.log*

PID=`ps aux|grep MarketV2_FTX |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s

ps -aux|grep python3