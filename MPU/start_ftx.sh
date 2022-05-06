# !/bin/bash

nohup python3 /mnt/pms_market/MPU/ftx.py > /mnt/pms_market/MPU/log/ftx.log &


sleep 2s

ps -aux|grep python3