# !/bin/bash

nohup python3 MarketV2_FTX.py > ftx.log &

sleep 2s

ps -aux|grep python3