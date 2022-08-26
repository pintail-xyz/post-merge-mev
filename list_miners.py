import json
import csv

with open('miners.json') as f:
    miners = json.load(f)

total_blocks = sum([item[1] for item in miners])

cumulative = 0
with open('miners.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(
        ['addr','count','first','last','perc','cumulative','cumulative_perc']
    )
    for address, num, first, last in miners:
        cumulative += num
        perc = f"{100 * num / total_blocks:.2f}"
        cumulative_perc = f"{100 * cumulative / total_blocks:.2f}"

        writer.writerow(
            [address, num, first, last, perc, cumulative, cumulative_perc]
        )
