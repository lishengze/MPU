# !/bin/bash

nohup python3 MarketV2_B2C2.py > b2c2.log &

sleep 2s

ps -aux|grep FTX