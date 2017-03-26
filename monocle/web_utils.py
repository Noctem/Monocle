from argparse import ArgumentParser
from datetime import datetime
from multiprocessing.managers import BaseManager, RemoteError
from time import time

from monocle import sanitized as conf
from monocle.db import get_forts, Pokestop, session_scope, Sighting, Spawnpoint
from monocle.utils import Units, get_address
from monocle.names import DAMAGE, MOVES, POKEMON

if conf.MAP_WORKERS:
    try:
        UNIT = getattr(Units, conf.SPEED_UNIT.lower())
        if UNIT is Units.miles:
            UNIT_STRING = "MPH"
        elif UNIT is Units.kilometers:
            UNIT_STRING = "KMH"
        elif UNIT is Units.meters:
            UNIT_STRING = "m/h"
    except AttributeError:
        UNIT_STRING = "MPH"

def get_args():
    parser = ArgumentParser()
    parser.add_argument(
        '-H',
        '--host',
        help='Set web server listening host',
        default='127.0.0.1'
    )
    parser.add_argument(
        '-P',
        '--port',
        type=int,
        help='Set web server listening port',
        default=5000
    )
    parser.add_argument(
        '-d', '--debug', help='Debug Mode', action='store_true'
    )
    parser.set_defaults(debug=False)
    return parser.parse_args()


class AccountManager(BaseManager): pass
AccountManager.register('worker_dict')


class Workers:
    def __init__(self):
        self._data = {}
        self._manager = AccountManager(address=get_address(), authkey=conf.AUTHKEY)

    def connect(self):
        try:
            self._manager.connect()
            self._data = self._manager.worker_dict()
        except (FileNotFoundError, AttributeError, RemoteError, ConnectionRefusedError, BrokenPipeError):
            print('Unable to connect to manager for worker data.')
            self._data = {}

    @property
    def data(self):
        try:
            if self._data:
                return self._data.items()
            else:
                raise ValueError
        except (FileNotFoundError, RemoteError, ConnectionRefusedError, ValueError, BrokenPipeError):
            self.connect()
            return self._data.items()


def get_worker_markers(workers):
    return [{
        'lat': lat,
        'lon': lon,
        'worker_no': worker_no,
        'time': datetime.fromtimestamp(timestamp).strftime('%I:%M:%S %p'),
        'speed': '{:.1f}{}'.format(speed, UNIT_STRING),
        'total_seen': total_seen,
        'visits': visits,
        'seen_here': seen_here
    } for worker_no, ((lat, lon), timestamp, speed, total_seen, visits, seen_here) in workers.data]


def sighting_to_marker(pokemon, names=POKEMON, moves=MOVES, damage=DAMAGE):
    pokemon_id = pokemon.pokemon_id
    marker = {
        'id': 'pokemon-' + str(pokemon.id),
        'trash': pokemon_id in conf.TRASH_IDS,
        'name': names[pokemon_id],
        'pokemon_id': pokemon_id,
        'lat': pokemon.lat,
        'lon': pokemon.lon,
        'expires_at': pokemon.expire_timestamp,
    }
    move1 = pokemon.move_1
    if pokemon.move_1:
        move2 = pokemon.move_2
        marker['atk'] = pokemon.atk_iv
        marker['def'] = pokemon.def_iv
        marker['sta'] = pokemon.sta_iv
        marker['move1'] = moves[move1]
        marker['move2'] = moves[move2]
        marker['damage1'] = damage[move1]
        marker['damage2'] = damage[move2]
    return marker


def get_pokemarkers(after_id=0):
    with session_scope() as session:
        pokemons = session.query(Sighting) \
            .filter(Sighting.expire_timestamp > time(),
                    Sighting.id > after_id)
        if conf.MAP_FILTER_IDS:
            pokemons = pokemons.filter(~Sighting.pokemon_id.in_(conf.MAP_FILTER_IDS))
        return tuple(map(sighting_to_marker, pokemons))


def get_gym_markers(names=POKEMON):
    with session_scope() as session:
        forts = get_forts(session)
    return [{
            'id': 'fort-' + str(fort['fort_id']),
            'sighting_id': fort['id'],
            'prestige': fort['prestige'],
            'pokemon_id': fort['guard_pokemon_id'],
            'pokemon_name': names[fort['guard_pokemon_id']],
            'team': fort['team'],
            'lat': fort['lat'],
            'lon': fort['lon']
    } for fort in forts]


def get_spawnpoint_markers():
    with session_scope() as session:
        spawns = session.query(Spawnpoint)
        return [{
            'spawn_id': spawn.spawn_id,
            'despawn_time': spawn.despawn_time,
            'lat': spawn.lat,
            'lon': spawn.lon,
            'duration': spawn.duration
        } for spawn in spawns]

if conf.BOUNDARIES:
    from shapely.geometry import mapping

    def get_scan_coords():
        coordinates = mapping(conf.BOUNDARIES)['coordinates']
        coords = coordinates[0]
        markers = [{
                'type': 'scanarea',
                'coords': coords
            }]
        for blacklist in coordinates[1:]:
            markers.append({
                    'type': 'scanblacklist',
                    'coords': blacklist
                })
        return markers
else:
    def get_scan_coords():
        return ({
            'type': 'scanarea',
            'coords': (conf.MAP_START, (conf.MAP_START[0], conf.MAP_END[1]),
                       conf.MAP_END, (conf.MAP_END[0], conf.MAP_START[1]), conf.MAP_START)
        },)


def get_pokestop_markers():
    with session_scope() as session:
        pokestops = session.query(Pokestop)
        return [{
            'external_id': pokestop.external_id,
            'lat': pokestop.lat,
            'lon': pokestop.lon
        } for pokestop in pokestops]


def sighting_to_report_marker(sighting):
    return {
        'icon': 'static/monocle-icons/icons/{}.png'.format(sighting.pokemon_id),
        'lat': sighting.lat,
        'lon': sighting.lon,
    }

