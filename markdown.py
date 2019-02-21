import csv
import json

from pprint import pprint

with open('markdown_map.json') as f:
    data = json.load(f)

pprint(data)

with open('data/czo.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    header =
    for row in spamreader:
        print(', '.join(row))


