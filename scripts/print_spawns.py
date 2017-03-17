#!/usr/bin/env python3

from pickle import load
from pprint import PrettyPrinter
from pathlib import Path

pickle_path = Path(__file__).resolve().parents[1] / 'pickles' / 'spawns.pickle'

with pickle_path.open('rb') as f:
    spawns = load(f)

pp = PrettyPrinter(indent=3)
pp.pprint(spawns)
