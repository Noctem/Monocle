#!/usr/bin/env python3

from pickle import load
from pprint import PrettyPrinter
from pathlib import Path
from datetime import datetime

pickle_path = Path(__file__).resolve().parents[1] / 'pickles' / 'accounts.pickle'

with pickle_path.open('rb') as f:
    accounts = load(f)

for account in accounts.values():
    if 'time' in account:
        account['time'] = datetime.fromtimestamp(account['time']).strftime('%x %X')
    if 'created' in account:
        account['created'] = datetime.fromtimestamp(account['created']).strftime('%x %X')
    if 'expiry' in account:
        account['expiry'] = datetime.fromtimestamp(account['expiry']).strftime('%x %X')

pp = PrettyPrinter(indent=3)
pp.pprint(accounts)
