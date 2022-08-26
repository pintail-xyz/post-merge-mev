import psycopg2
from datetime import datetime, timedelta, timezone as tz
from time import time

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
num_blocks = end_block - start_block + 1

exclude_list = [
    # 7,676 ETH erroneous gas price
    '0x2c9931793876db33b1a9aad123ad4921dfb9cd5e59dbb78ce78f277759587115'
]
cursor.execute(
    "CREATE TABLE IF NOT EXISTS block_totals ("
    "   block_number numeric PRIMARY KEY, "
    "   block_timestamp timestamp NOT NULL, "
    "   base_fee numeric NOT NULL, "
    "   gas_used numeric NOT NULL, "
    "   net_coinbase_transfer numeric NOT NULL, "
    "   net_fees numeric NOT NULL, "
    "   burned_fees numeric NOT NULL, "
    "   ts_diff_1 numeric NOT NULL, "
    "   ts_diff_2 numeric NOT NULL"
    ")"
)

timestamp_sql = (
    "SELECT block_timestamp FROM blocks WHERE block_number BETWEEN %s AND %s "
    "ORDER BY block_number"
)
cursor.execute(timestamp_sql, (start_block - 2, end_block))
timestamps = [
    int(r[0].replace(tzinfo=tz.utc).timestamp()) for r in cursor.fetchall()
]
diffs = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
start_time = time()
select_sql = (
    "SELECT coinbase_transfer, base_fee_per_gas, gas_price, gas_used, "
    "miner_group, payer_group, transaction_hash "
    "FROM miner_payments WHERE block_number = %s "
)
in_sql = "INSERT INTO block_totals VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
for block_number in range(start_block, end_block + 1):
    ind = block_number - start_block

    cursor.execute(select_sql, (block_number, ))
    result = cursor.fetchall()

    total_gas_used = net_transfer = net_fees = burned_fees = 0
    for transfer, base_fee, gas_price, gas_used, miner, payer, tx_hash in result:
        if tx_hash in exclude_list:
            continue
        total_gas_used += gas_used
        burned_fees += base_fee * gas_used
        if miner != payer:
            net_transfer += transfer
            net_fees += (gas_price - base_fee) * gas_used
    dt = datetime.utcfromtimestamp(timestamps[ind + 2])
    cursor.execute(
        in_sql, (
            block_number, dt, int(base_fee) / 1e9, total_gas_used,
            int(net_transfer) / 1e18, int(net_fees) / 1e18,
            int(burned_fees) / 1e18, diffs[ind + 1], diffs[ind]
        )
    )

    elapsed = timedelta(seconds=int(time()- start_time))
    print(
        f"block {ind + 1} of {num_blocks} / "
        f"{100 * (ind + 1) / num_blocks:.2f}% / "
        f"{elapsed}", end='\r'
    )
print()
