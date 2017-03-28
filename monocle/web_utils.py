from argparse import ArgumentParser
from datetime import datetime
from multiprocessing.managers import BaseManager, RemoteError
from contextlib import contextmanager

from monocle import db, utils, sanitized as conf
from monocle.names import DAMAGE, MOVES, POKEMON

if conf.BOUNDARIES:
    from shapely.geometry import mapping

if conf.MAP_WORKERS:
    try:
        UNIT = getattr(utils.Units, conf.SPEED_UNIT.lower())
        if UNIT is utils.Units.miles:
            UNIT_STRING = "MPH"
        elif UNIT is utils.Units.kilometers:
            UNIT_STRING = "KMH"
        elif UNIT is utils.Units.meters:
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
        self._manager = AccountManager(address=utils.get_address(), authkey=conf.AUTHKEY)

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
    markers = []

    # Worker start points
    for worker_no, data in workers.data:
        coords = data[0]
        unix_time = data[1]
        speed = '{:.1f}{}'.format(data[2], UNIT_STRING)
        total_seen = data[3]
        visits = data[4]
        seen_here = data[5]
        time = datetime.fromtimestamp(unix_time).strftime('%I:%M:%S %p').lstrip('0')
        markers.append({
            'lat': coords[0],
            'lon': coords[1],
            'worker_no': worker_no,
            'time': time,
            'speed': speed,
            'total_seen': total_seen,
            'visits': visits,
            'seen_here': seen_here
        })
    return markers


def get_pokemarkers(after_id=0):
    markers = []
    with db.session_scope() as session:
        pokemons = db.get_sightings(session, after_id)
        pokemon_names = POKEMON
        move_names = MOVES
        damage = DAMAGE
        for pokemon in pokemons:
            content = {
                'id': 'pokemon-{}'.format(pokemon.id),
                'trash': pokemon.pokemon_id in conf.TRASH_IDS,
                'name': pokemon_names[pokemon.pokemon_id],
                'pokemon_id': pokemon.pokemon_id,
                'lat': pokemon.lat,
                'lon': pokemon.lon,
                'expires_at': pokemon.expire_timestamp,
            }
            if pokemon.move_1:
                iv = {
                    'atk': pokemon.atk_iv,
                    'def': pokemon.def_iv,
                    'sta': pokemon.sta_iv,
                    'move1': move_names[pokemon.move_1],
                    'move2': move_names[pokemon.move_2],
                    'damage1': damage[pokemon.move_1],
                    'damage2': damage[pokemon.move_2],
                }
                content.update(iv)
            markers.append(content)
        return markers


def get_gym_markers():
    markers = []
    with db.session_scope() as session:
        forts = db.get_forts(session)
    pokemon_names = POKEMON
    for fort in forts:
        if fort['guard_pokemon_id']:
            pokemon_name = pokemon_names[fort['guard_pokemon_id']]
        else:
            pokemon_name = 'Empty'
        markers.append({
            'id': 'fort-{}'.format(fort['fort_id']),
            'sighting_id': fort['id'],
            'prestige': fort['prestige'],
            'pokemon_id': fort['guard_pokemon_id'],
            'pokemon_name': pokemon_name,
            'team': fort['team'],
            'lat': fort['lat'],
            'lon': fort['lon'],
        })
    return markers


def get_spawnpoint_markers():
    markers = []
    with db.session_scope() as session:
        spawns = db.get_spawn_points(session)

        for spawn in spawns:
            markers.append({
                'spawn_id': spawn.spawn_id,
                'despawn_time': spawn.despawn_time,
                'lat': spawn.lat,
                'lon': spawn.lon,
                'duration': spawn.duration
            })
        return markers

if conf.BOUNDARIES:
    def get_scan_coords():
        markers = []
        coordinates = mapping(conf.BOUNDARIES)['coordinates']
        coords = coordinates[0]
        for blacklist in coordinates[1:]:
            markers.append({
                    'type': 'scanblacklist',
                    'coords': blacklist
                })
        markers.append({
                'type': 'scanarea',
                'coords': coords
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
    markers = []
    with db.session_scope() as session:
        pokestops = db.get_pokestops(session)

        for pokestop in pokestops:
            markers.append({
                'external_id': pokestop.external_id,
                'lat': pokestop.lat,
                'lon': pokestop.lon
            })
        return markers


def sighting_to_marker(sighting):
    return {
        'icon': 'static/monocle-icons/icons/{}.png'.format(sighting.pokemon_id),
        'lat': sighting.lat,
        'lon': sighting.lon,
    }

