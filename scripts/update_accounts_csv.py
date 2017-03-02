#!/usr/bin/env python3

from datetime import datetime
import csv, os

from monocle.shared import ACCOUNTS

os.rename('accounts.csv', 'accounts-{}.csv'.format(datetime.now().strftime("%Y%m%d-%H%M")))

with open('accounts.csv', 'wt') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    writer.writerow(('username', 'password', 'provider', 'model', 'iOS', 'id'))
    for account in ACCOUNTS.values():
        if account.get('banned', False):
            continue
        writer.writerow((account['username'],
                         account['password'],
                         account['provider'],
                         account['model'],
                         account['iOS'],
                         account['id']))
