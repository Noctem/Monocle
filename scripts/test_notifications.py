#!/usr/bin/env python3

from asyncio import get_event_loop, set_event_loop_policy
from pathlib import Path
from random import uniform, randint, choice
from argparse import ArgumentParser

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

parser = ArgumentParser()
parser.add_argument(
    '-i', '--id',
    type=int,
    help='Pokémon ID to notify about'
)
parser.add_argument(
    '-lat', '--latitude',
    type=float,
    help='latitude for fake spawn'
)
parser.add_argument(
    '-lon', '--longitude',
    type=float,
    help='longitude for fake spawn'
)
parser.add_argument(
    '-r', '--remaining',
    type=int,
    help='seconds remaining on fake spawn'
)
parser.add_argument(
    '-u', '--unmodified',
    action='store_true',
    help="don't add ID to ALWAYS_NOTIFY_IDS"
)
args = parser.parse_args()

if args.id is not None:
    pokemon_id = args.id
    if args.id == 0:
        names.POKEMON[0] = 'Test'
else:
    pokemon_id = randint(1, 252)

if not args.unmodified:
    conf.ALWAYS_NOTIFY_IDS = {pokemon_id}

conf.HASHTAGS = {'test'}

from monocle.notification import Notifier
from monocle.shared import SessionManager
from monocle.names import MOVES

root = logging.getLogger()
root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
root.addHandler(ch)

MOVES = tuple(MOVES.keys())

if args.latitude is not None:
    lat = args.latitude
else:
    lat = uniform(conf.MAP_START[0], conf.MAP_END[0])

if args.longitude is not None:
    lon = args.longitude
else:
    lon = uniform(conf.MAP_START[1], conf.MAP_END[1])

if args.remaining:
    tth = args.remaining
else:
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
