# !/bin/bash

nohup python3 /mnt/market/MPU/okex.py > /mnt/market/MPU/log/okex.log &


sleep 2s

ps -aux|grep python3