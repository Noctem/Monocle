#!/usr/bin/env python3

from monocle import config
config.ALWAYS_NOTIFY_IDS = {71}
config.HASHTAGS = {'test'}

from monocle.names import POKEMON_NAMES
from monocle.notification import Notifier

import time

pokemon = {
    'encounter_id': 93253523,
    'spawn_id': 3502935,
    'pokemon_id': 71,
    'time_till_hidden': 89,
    'lat': 40.776714,
    'lon': -111.888558,
    'individual_attack': 15,
    'individual_defense': 15,
    'individual_stamina': 15,
    'seen': time.time(),
    'move_1': 13,
    'move_2': 14,
    'valid': True
}

notifier = Notifier()

print(notifier.notify(pokemon, 2))
