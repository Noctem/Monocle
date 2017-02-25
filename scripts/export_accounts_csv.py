#!/usr/bin/env python3

from pickle import load
from pprint import pprint
from datetime import datetime
import csv, os

with open('pickles/accounts.pickle', 'rb') as f:
    accounts = load(f)

os.rename('accounts.csv', 'accounts.csv-' + datetime.now().strftime("%Y%m%d-%H%M"))

with open('accounts.csv', 'w') as csvfile:
    accfile = csv.writer(csvfile, delimiter=',')
    for account in accounts.values():
        if account.get('banned', False):
            continue
        accfile.writerow([account['username'], account['password'], account['provider']])
