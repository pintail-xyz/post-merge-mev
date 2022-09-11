import psycopg2
from datetime import datetime, timedelta
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

start_block = 14714998
end_block = 15449617
num_blocks = end_block - start_block + 1

sql = "SELECT COUNT(*) FROM blocks WHERE block_number = %s"

start_time = time()
last_update = 0
missing_count = 0
with open('missing', 'w') as f:
    for block in range(start_block, end_block + 1):
        cursor.execute(sql, (block,))
        num = cursor.fetchone()[0]
        if num == 0:
            f.write(f"{block}\n")
            missing_count += 1
        elif num > 1:
            print(f"/// multiple entries for block {block} ///")

        t = time()
        if t - last_update > 0.1:
            last_update = t
            elapsed = timedelta(seconds=int(t - start_time))
            perc = 100 * (block - start_block + 1) / num_blocks
            print(f"{perc:.2f}% complete / {elapsed} elapsed", end='\r')

print()
print(f'identified {missing_count} mising blocks')
