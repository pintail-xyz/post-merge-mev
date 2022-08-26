import time
import json
from datetime import timedelta

import requests
from jsonrpcclient import request
from web3 import Web3

w3 = Web3(Web3.HTTPProvider("http://192.168.1.104:8545/"))

START_BLOCK = 0
END_BLOCK = 14570000

start_time = time.time()
last_update = 0

num_blocks = END_BLOCK - START_BLOCK
miners = {}

for b in range(START_BLOCK, END_BLOCK):
    miner = w3.eth.get_block(b).miner
    if miner in miners:
        miners[miner][0] += 1
        miners[miner][2] = b
    else:
        miners[miner] = [1, b, b]

    t = time.time()

    if t - last_update > 0.2:
        elapsed = timedelta(seconds = int(t - start_time))
        iteration = b - START_BLOCK + 1
        perc = 100 * iteration / num_blocks
        print(f"{elapsed} elapsed, {perc:.1f}% complete", end='\r')
        last_update = t

print()
out = [
    [k] + v for k, v in sorted(
        miners.items(), key=lambda k: k[1][0], reverse=True
    )
]

with open('miners.json', 'w') as f:
    json.dump(out, f)
