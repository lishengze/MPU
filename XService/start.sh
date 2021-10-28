# !/bin/bash

nohup  python3 /mnt/market/XService/XKLine.py 60 PRODUCTION > /mnt/market/XService/log/xkline.log  &

sleep 1s

ps -aux|grep python3
