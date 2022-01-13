# !/bin/bash

rm -fr log/B2C2/*.log*

PID=`ps aux|grep B2C2 |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s




ps -aux|grep python3