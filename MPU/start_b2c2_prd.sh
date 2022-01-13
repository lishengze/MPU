# !/bin/bash

nohup python3 /mnt/market/MPU/MarketV2_B2C2.py -prd > /mnt/market/MPU/log/b2c2.log &


sleep 2s

ps -aux|grep python3