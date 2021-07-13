#! /bin/bash

nohup redis-server ./redis.conf > ./redis.log &

sleep 3s

ps -aux|grep redis

