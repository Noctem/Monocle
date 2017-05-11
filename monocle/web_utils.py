from argparse import ArgumentParser
from datetime import datetime
from multiprocessing.managers import BaseManager, RemoteError
from time import time

from monocle import spawnid_to_coords, sanitized as conf
from monocle.db import get_forts, Pokestop, session_scope, Sighting, Spawnpoint
from monocle.utils import get_address
from monocle.names import POKEMON


def get_args():
    parser = ArgumentParser()
    parser.add_argument(
        '-H',
        '--host',
        help='Set web server listening host',
        default='0.0.0.0'
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
        'lat': location[0],
        'lon': location[1],
        'worker_no': worker_no,
        'time': datetime.fromtimestamp(timestamp).strftime('%I:%M:%S %p'),
        'speed': '{:.1f}m/s'.format(speed),
        'total_seen': total_seen,
        'visits': visits,
        'seen_here': seen_here
    } for worker_no, (location, timestamp, speed, total_seen, visits, seen_here) in workers.data]


def get_gym_markers(names=POKEMON):
    with session_scope() as session:
        forts = get_forts(session)
    return [{
            'id': 'fort-' + repr(fort['fort_id']),
            'sighting_id': fort['id'],
            'prestige': fort['prestige'],
            'pokemon_id': fort['guard_pokemon_id'],
            'pokemon_name': names[fort['guard_pokemon_id']],
            'team': fort['team'],
            'lat': fort['lat'],
            'lon': fort['lon']
    } for fort in forts]


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

