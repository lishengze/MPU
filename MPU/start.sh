# !/bin/bash

nohup python3 Entry_MarketV2.py PRODUCTION &

sleep 1s

ps -aux|grep python3
