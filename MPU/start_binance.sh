# !/bin/bash

nohup python3 /mnt/market/MPU/MarketV2_BINANCE.py > /mnt/market/MPU/log/binance.log &


sleep 2s

ps -aux|grep python3