# !/bin/bash

nohup python3 /mnt/pms_market/MPU/binance.py > /mnt/pms_market/MPU/log/binance.log &


sleep 2s

ps -aux|grep python3