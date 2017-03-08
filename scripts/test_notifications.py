#!/usr/bin/env python3

from asyncio import get_event_loop, set_event_loop_policy
from pathlib import Path
from random import uniform, randint, choice

try:
    from uvloop import EventLoopPolicy
    set_event_loop_policy(EventLoopPolicy())
except ImportError:
    pass

import time
import logging
import sys

monocle_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(monocle_dir))

from monocle import names, sanitized as conf

pokemon_id = randint(1, 251)
conf.ALWAYS_NOTIFY_IDS = {pokemon_id}
conf.HASHTAGS = {'test'}
names.POKEMON_NAMES[0] = 'Test'

from monocle.notification import Notifier
from monocle.shared import SessionManager
from monocle.names import POKEMON_MOVES

root = logging.getLogger()
root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
root.addHandler(ch)

MOVES = tuple(POKEMON_MOVES.keys())

try:
    lat = uniform(conf.MAP_START[0], conf.MAP_END[0])
    lon = uniform(conf.MAP_START[1], conf.MAP_END[1])
except Exception:
    lat = 40.776714
    lon = -111.888558

tth = uniform(89, 3599)
now = time.time()



pokemon = {
    'encounter_id': 93253523,
    'spawn_id': 3502935,
    'pokemon_id': pokemon_id,
    'time_till_hidden': tth,
    'lat': lat,
    'lon': lon,
    'individual_attack': randint(0, 15),
    'individual_defense': randint(0, 15),
    'individual_stamina': randint(0, 15),
    'seen': now,
    'move_1': choice(MOVES),
    'move_2': choice(MOVES),
    'valid': True,
    'expire_timestamp': now + tth
}

notifier = Notifier()

loop = get_event_loop()

if loop.run_until_complete(notifier.notify(pokemon, randint(1, 2))):
    print('Success')
else:
    print('Failure')

SessionManager.close()
loop.close()
