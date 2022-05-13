# !/bin/bash

echo "args : $@"

nohup python3 /mnt/pms_market/MPU/binance.py  $1> /mnt/pms_market/MPU/log/binance.log &


sleep 2s

ps -aux|grep python3