# !/bin/bash
echo "args : $@"

nohup python3 /mnt/pms_market/MPU/monitor_trade.py $1> /mnt/pms_market/MPU/log/monitor.log &


sleep 2s

ps -aux|grep python3