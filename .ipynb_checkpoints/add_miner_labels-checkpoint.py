import csv
import time
from datetime import datetime, timedelta

import psycopg2, psycopg2.extras

connection = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        user="postgres",
        password="password",
        database="mev_inspect"
)
connection.autocommit = True
cursor = connection.cursor()

start_block = 12965000
end_block = 14714999

print(1, datetime.now())

cursor.execute("ALTER TABLE miner_payments DROP COLUMN IF EXISTS miner_group")
cursor.execute(
    "ALTER TABLE miner_payments ADD COLUMN miner_group VARCHAR(256)"
)

print(2, datetime.now())

cursor.execute("ALTER TABLE miner_payments DROP COLUMN IF EXISTS payer_group")
cursor.execute(
    "ALTER TABLE miner_payments ADD COLUMN payer_group VARCHAR(256)"
)

#print(3, datetime.now())

#cursor.execute(
#    "SELECT MIN(block_number), MAX(block_number) FROM miner_payments WHERE miner_group = ''"
#)
#first, last = [int(b) for b in cursor.fetchone()]

print(4, datetime.now())

miner_lookup = {}
with open("etherscan_pool_addresses.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        miner_lookup[row['address']] = row['label']

print(5, datetime.now())

with open("miner_stats.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if len(row['label']) > 0:
            miner_lookup[row['address']] = row['label']

print(6, datetime.now())

update_query = (
    "UPDATE miner_payments AS t "
    "SET miner_group = i.miner_group, payer_group = i.payer_group "
    "FROM (VALUES %s) "
    "AS i(block_number, transaction_hash, miner_group, payer_group) "
    "WHERE t.block_number = i.block_number "
    "AND t.transaction_hash = i.transaction_hash"
)
miner_payer_count = 0
start_time = time.time()
last_update = 0
for block_number in range(start_block, end_block+1):
    cursor.execute(
        f"SELECT transaction_hash, miner_address, transaction_from_address "
        f"FROM miner_payments WHERE block_number = {block_number}"
    )
    result = cursor.fetchall()
    if len(result) == 0:
        continue
    tx_hashes, miners, payers = zip(*result)
    values = []
    for tx_hash, miner, payer in zip(tx_hashes, miners, payers):
        miner_group = payer_group = ""
        if miner in miner_lookup:
            miner_group = miner_lookup[miner]
        else:
            miner_group = miner
        if payer in miner_lookup:
            payer_group = miner_lookup[payer]
            if payer_group == miner_group:
                miner_payer_count += 1
        else:
            payer_group = payer
        values += [[block_number, tx_hash, miner_group, payer_group]]

    psycopg2.extras.execute_values(cursor, update_query, values)

    t = time.time()
    if t - last_update > 0.1:
        elapsed = timedelta(seconds = int(t - start_time))
        perc = 100 * (block_number - start_block + 1) / (end_block + 1 - start_block)
        print(f"{elapsed} / {perc:.2f}% complete", end='\r')
        last_update = t

print(miner_payer_count)
