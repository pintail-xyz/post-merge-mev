import csv
import time
from datetime import datetime, timedelta

import psycopg2, psycopg2.extras
from web3 import Web3

WEI_PER_ETH = int(1e18)

connection = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        user="postgres",
        password="password",
        database="mev_inspect"
)
connection.autocommit = True
cursor = connection.cursor()

w3 = Web3(Web3.HTTPProvider("http://192.168.0.51:8545/"))

cursor.execute(
    "SELECT MIN(block_number), MAX(block_number) FROM miner_payments"
)
first_blocknum, last_blocknum = [int(b) for b in cursor.fetchone()]
num_blocks = last_blocknum - first_blocknum + 1
first_timestamp = w3.eth.getBlock(first_blocknum)['timestamp']
last_timestamp = w3.eth.getBlock(last_blocknum)['timestamp']
first_date = datetime.utcfromtimestamp(first_timestamp).date()
last_date = datetime.utcfromtimestamp(last_timestamp).date()
num_days = (last_date - first_date).days + 1

def get_history_dict(num_days):
    return {
        'block_count': [0] * num_days,
        'fee_revenue': [0] * num_days,
        'transfer_revenue': [0] * num_days,
        'basefee_cost': [0] * num_days,
        'self_fees': [0] * num_days,
        'self_transfers': [0] * num_days
    }

history = {}

start_time = time.time()
last_update = 0
for block_number in range(first_blocknum, last_blocknum+1):
    cursor.execute(
        f"SELECT coinbase_transfer, base_fee_per_gas, gas_price, gas_used, "
        f"miner_group, payer_group "
        f"FROM miner_payments WHERE block_number = {block_number}"
    )
    result = cursor.fetchall()
    if len(result) == 0:
        continue

    ts = w3.eth.getBlock(block_number)['timestamp']
    day = (datetime.utcfromtimestamp(ts).date() - first_date).days

    for coinbase, basefee, gasprice, gasused, miner, payer in result:

        if miner not in history:
            history[miner] = get_history_dict(num_days)

        history[miner]['block_count'][day] += 1
        history[miner]['basefee_cost'][day] += int(basefee * gasused)
        if miner == payer:
            history[miner]['self_fees'][day] += int(gasprice * gasused)
            history[miner]['self_transfers'][day] += int(coinbase)
        else:
            history[miner]['fee_revenue'][day] += int(gasprice * gasused)
            history[miner]['transfer_revenue'][day] += int(coinbase)

    t = time.time()
    if t - last_update > 0.1:
        elapsed = timedelta(seconds = int(t - start_time))
        perc = 100 * (block_number - first_blocknum + 1) / num_blocks
        print(f"{elapsed} / {perc:.2f}% complete", end='\r')
        last_update = t

print()

totals = get_history_dict(num_days)
for day in range(num_days):
    for item in (
        'block_count',
        'basefee_cost',
        'fee_revenue',
        'transfer_revenue',
        'self_fees',
        'self_transfers'
    ):
        totals[item][day] = sum(history[miner][item][day] for miner in history)

miner_order = sorted(
    history.keys(), key=lambda k: sum(history[k]['block_count']), reverse=True
)

with open('daily_totals.csv', 'w') as f:
    writer = csv.writer(f)
    headers = [
        'date',
        'total_block_count',
        'total_basefee_cost',
        'total_fee_revenue',
        'total_transfer_revenue',
        'total_self_fees',
        'total_self_transfers'
    ]

    for miner in miner_order:
        headers += [
            'block_count_' + miner,
            'basefee_cost_' + miner,
            'fee_revenue_' + miner,
            'transfer_revenue_' + miner,
            'self_fees_' + miner,
            'self_tranfers_' + miner
        ]
    writer.writerow(headers)

    for day in range(num_days):
        row = [
            str(first_date + timedelta(days=day)),
            totals['block_count'][day],
            totals['basefee_cost'][day] / WEI_PER_ETH,
            totals['fee_revenue'][day] / WEI_PER_ETH,
            totals['transfer_revenue'][day] / WEI_PER_ETH,
            totals['self_fees'][day] / WEI_PER_ETH,
            totals['self_transfers'][day] / WEI_PER_ETH
        ]
        for miner in miner_order:
            row += [
                history[miner]['block_count'][day],
                history[miner]['basefee_cost'][day] / WEI_PER_ETH,
                history[miner]['fee_revenue'][day] / WEI_PER_ETH,
                history[miner]['transfer_revenue'][day] / WEI_PER_ETH,
                history[miner]['self_fees'][day] / WEI_PER_ETH,
                history[miner]['self_transfers'][day] / WEI_PER_ETH,
            ]

        writer.writerow(row)
