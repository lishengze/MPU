# !/bin/bash

echo "args : $@"


nohup python3 /mnt/pms_market/MPU/ftx.py $1 > /mnt/pms_market/MPU/log/ftx.log &


sleep 2s

ps -aux|grep python3