# !/bin/bash

nohup  python3 XKLine.py 60 PRODUCTION  &

sleep 1s

ps -aux|grep python3
