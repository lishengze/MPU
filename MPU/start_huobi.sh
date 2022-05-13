# !/bin/bash

echo "args : $@"

nohup python3 /mnt/pms_market/MPU/huobi.py $1> /mnt/pms_market/MPU/log/huobi.log &


sleep 2s

ps -aux|grep python3