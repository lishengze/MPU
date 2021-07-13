# !/bin/bash

PID=`ps aux|grep Entry_MarketV2.py |grep -v grep | awk '{print $2}'` 
kill -9 $PID

sleep 1s

ps aux|grep python3
