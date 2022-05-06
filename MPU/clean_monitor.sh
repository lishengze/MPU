# !/bin/bash

rm -fr /mnt/pms_market/MPU/log/Monitor/*.log*


PID=`ps aux|grep monitor_trade |grep -v grep | awk '{print $2}'`
kill -9 $PID

sleep 2s


ps -aux|grep python3