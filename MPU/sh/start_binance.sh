# !/bin/bash

nohup python3 /mnt/market/MPU/binance.py > /mnt/market/MPU/log/binance.log &


sleep 2s

ps -aux|grep python3