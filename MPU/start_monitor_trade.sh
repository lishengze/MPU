# !/bin/bash

nohup python3 /mnt/pms_market/MPU/monitor_trade.py > /mnt/pms_market/MPU/log/monitor.log &


sleep 2s

ps -aux|grep python3