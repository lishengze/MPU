# !/bin/bash

PID=`ps aux|grep XKLine.py  |grep -v grep | awk '{print $2}'` 
kill -9 $PID

sleep 1s

ps aux|grep  XKLine.py 
