#!/usr/bin/env python3

import config
config.TIME_REQUIRED = 83
config.ALWAYS_NOTIFY_IDS = {71}

from notification import Notifier
from shared import Spawns

spawns = Spawns()

# Victreebell
pokemon = {
    'encounter_id': 93253523,
    'spawn_id': 3502935,
    'pokemon_id': 71,
    'time_till_hidden_ms': 84000,
    'lat': 40.776714,
    'lon': -111.888558,
    'individual_attack': 14,
    'individual_defense': 14,
    'individual_stamina': 14,
    'move_1': 13,
    'move_2': 14,
    'valid': True
}

notifier = Notifier(spawns)

print(notifier.notify(pokemon, 2))
