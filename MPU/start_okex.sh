# !/bin/bash
echo "args : $@"

nohup python3 /mnt/pms_market/MPU/okex.py  $1> /mnt/pms_market/MPU/log/okex.log &


sleep 2s

ps -aux|grep python3