#!/usr/bin/env python3

from pickle import load
from pprint import PrettyPrinter
from pathlib import Path

pickle_path = Path(__file__).resolve().parents[1] / 'pickles' / 'accounts.pickle'

with pickle_path.open('rb') as f:
    accounts = load(f)

for username, account in accounts.items():
    if 'level' in account:
        print(username, '-', account['level'])
    else:
        print(username, '- unknown')
