#!/bin/bash

curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

sudo apt-get update
sudo apt-get install -y redis
redis-server /home/ubuntu/.local/lib/python3.8/site-packages/pyquokka/redis.conf --port 6800 --protected-mode no&

echo '* hard nofile 65536\n* soft nofile 65536\n* hard core unlimited\n* soft core unlimited' | sudo tee /etc/security/limits.conf

