# !/bin/bash

nohup python3 /mnt/market/MPU/MarketV2_HUOBI.py > /mnt/market/MPU/log/huobi.log &


sleep 2s

ps -aux|grep python3