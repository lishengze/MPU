# !/bin/bash

nohup python3 /mnt/market/MPU/huobi.py > /mnt/market/MPU/log/huobi.log &


sleep 2s

ps -aux|grep python3