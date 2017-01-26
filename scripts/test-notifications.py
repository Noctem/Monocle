#!/usr/bin/env python3

from pokeminer import config
config.ALWAYS_NOTIFY_IDS = {71}
config.HASHTAGS = {'test'}

from pokeminer.names import POKEMON_NAMES
POKEMON_NAMES[0] = 'Test'

from pokeminer.notification import Notifier
from pokeminer.shared import Spawns


spawns = Spawns()

pokemon = {
    'encounter_id': 93253523,
    'spawn_id': 3502935,
    'pokemon_id': 71,
    'time_till_hidden_ms': 1740000,
    'lat': 40.776714,
    'lon': -111.888558,
    'individual_attack': 15,
    'individual_defense': 15,
    'individual_stamina': 15,
    'move_1': 13,
    'move_2': 14,
    'valid': True
}

notifier = Notifier(spawns)

print(notifier.notify(pokemon, 2))
