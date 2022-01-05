# !/bin/bash

nohup python3 /mnt/market/MPU/ftx.py > /mnt/market/MPU/log/ftx.log &


sleep 2s

ps -aux|grep python3