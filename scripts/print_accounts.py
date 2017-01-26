#!/usr/bin/env python3

from pickle import load
from pprint import PrettyPrinter

with open('pickles/accounts.pickle', 'rb') as f:
    accounts = load(f)

pp = PrettyPrinter(indent=3)
pp.pprint(accounts)
