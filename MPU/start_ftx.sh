# !/bin/bash

echo "args : $@"

nohup python3 /mnt/market/MPU/MarketV2_FTX.py $1 > /mnt/market/MPU/log/ftx.log &


sleep 2s

ps -aux|grep python3