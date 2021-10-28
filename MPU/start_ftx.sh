# !/bin/bash

nohup python3 /mnt/market/MPU/MarketV2_FTX.py > /mnt/market/MPU/log/ftx.log &


sleep 2s

ps -aux|grep python3