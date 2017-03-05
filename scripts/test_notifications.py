#!/usr/bin/env python3

from asyncio import get_event_loop
from pathlib import Path

import time
import logging
import sys

monocle_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(monocle_dir))

from monocle import names, sanitized as conf

conf.ALWAYS_NOTIFY_IDS = {0}
conf.HASHTAGS = {'test'}
names.POKEMON_NAMES[0] = 'Test'

from monocle.notification import Notifier
from monocle.shared import SessionManager

root = logging.getLogger()
root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
root.addHandler(ch)

pokemon = {
    'encounter_id': 93253523,
    'spawn_id': 3502935,
    'pokemon_id': 0,
    'time_till_hidden': 89,
    'lat': 40.776714,
    'lon': -111.888558,
    'individual_attack': 15,
    'individual_defense': 15,
    'individual_stamina': 15,
    'seen': time.time(),
    'move_1': 13,
    'move_2': 14,
    'valid': True,
    'expire_timestamp': time.time() + 89
}

notifier = Notifier()

loop = get_event_loop()

if loop.run_until_complete(notifier.notify(pokemon, 2)):
    print('Success')
else:
    print('Failure')

SessionManager.close()
loop.close()
