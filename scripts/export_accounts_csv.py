#!/usr/bin/env python3

import csv

from monocle import config

if not hasattr(config, 'PASS'):
    config.PASS = None
if not hasattr(config, 'PROVIDER'):
    config.PROVIDER = None

usernames = set()

with open('accounts-exported.csv', 'wt') as f:
    writer = csv.writer(f)
    writer.writerow(('username', 'password', 'provider', 'model', 'iOS', 'id'))
    for account in config.ACCOUNTS:
        length = len(account)
        if length not in (1, 3, 4, 6):
            raise ValueError('Each account should have either 3 (account info only) or 6 values (account and device info).')
        if length in (1, 4):
            if not config.PASS or not config.PROVIDER:
                raise ValueError('No default PASS or PROVIDER are set.')
            if length == 1:
                row = account[0], config.PASS, config.PROVIDER
            else:
                row = account[0], config.PASS, config.PROVIDER, *account[1:]
        else:
            row = account
        username = row[0]
        if username in usernames:
            print('Skipping duplicate: {}'.format(username))
        else:
            usernames.append(username)
        writer.writerow(row)

print('Done!')
