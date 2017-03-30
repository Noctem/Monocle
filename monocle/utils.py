import random
import requests
import time
import socket
import pickle

from polyline import encode as polyencode
from os import mkdir
from os.path import join, exists
from sys import platform
from asyncio import sleep
from math import sqrt
from uuid import uuid4
from enum import Enum
from logging import getLogger
from csv import DictReader

from geopy import Point
from geopy.distance import distance
from aiopogo import utilities as pgoapi_utils
from pogeo import get_distance

try:
    from numba import jit
except ImportError:
    def jit(func):
        return func

from . import bounds, sanitized as conf

IPHONES = {'iPhone5,1': 'N41AP',
           'iPhone5,2': 'N42AP',
           'iPhone5,3': 'N48AP',
           'iPhone5,4': 'N49AP',
           'iPhone6,1': 'N51AP',
           'iPhone6,2': 'N53AP',
           'iPhone7,1': 'N56AP',
           'iPhone7,2': 'N61AP',
           'iPhone8,1': 'N71AP',
           'iPhone8,2': 'N66AP',
           'iPhone8,4': 'N69AP',
           'iPhone9,1': 'D10AP',
           'iPhone9,2': 'D11AP',
           'iPhone9,3': 'D101AP',
           'iPhone9,4': 'D111AP'}


log = getLogger(__name__)


class Units(Enum):
    miles = 1
    kilometers = 2
    meters = 3


@jit
def get_start_coords(worker_no):
    """Returns center of square for given worker"""
    grid = conf.GRID
    total_workers = grid[0] * grid[1]
    per_column = int(total_workers / grid[0])

    column = worker_no % per_column
    row = int(worker_no / per_column)
    part_lat = (bounds.south - bounds.north) / grid[0]
    part_lon = (bounds.east - bounds.west) / grid[1]
    start_lat = bounds.north + part_lat * row + part_lat / 2
    start_lon = bounds.west + part_lon * column + part_lon / 2
    return start_lat, start_lon


def float_range(start, end, step):
    """xrange for floats, also capable of iterating backwards"""
    if start > end:
        while end < start:
            yield start
            start += -step
    else:
        while start < end:
            yield start
            start += step


def get_gains(dist=70):
    """Returns lat and lon gain

    Gain is space between circles.
    """
    start = Point(*bounds.center)
    base = dist * sqrt(3)
    height = base * sqrt(3) / 2
    dis_a = distance(meters=base)
    dis_h = distance(meters=height)
    lon_gain = dis_a.destination(point=start, bearing=90).longitude
    lat_gain = dis_h.destination(point=start, bearing=0).latitude
    return abs(start.latitude - lat_gain), abs(start.longitude - lon_gain)


@jit
def round_coords(point, precision):
    return round(point[0], precision), round(point[1], precision)


@jit
def random_altitude():
    altitude = random.uniform(*conf.ALT_RANGE)
    return altitude


def get_altitude(point):
    params = {
        'locations': 'enc:' + polyencode((point,)),
        'key': conf.GOOGLE_MAPS_KEY
    }
    r = requests.get('https://maps.googleapis.com/maps/api/elevation/json',
                     params=params).json()
    return r['results'][0]['elevation']


def get_altitudes(coords):
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    if len(coords) > 300:
        altitudes = {}
        for chunk in chunks(coords, 300):
            altitudes.update(get_altitudes(chunk))
        return altitudes
    else:
        try:
            params = {'locations': 'enc:' + polyencode(coords)}
            if conf.GOOGLE_MAPS_KEY:
                params['key'] = conf.GOOGLE_MAPS_KEY
            r = requests.get('https://maps.googleapis.com/maps/api/elevation/json',
                             params=params).json()

            return {round_coords((x['location']['lat'], x['location']['lng']), conf.ALT_PRECISION):
                    x['elevation'] for x in r['results']}
        except Exception:
            log.exception('Error fetching altitudes.')
            return {}


def get_all_altitudes(bound=False):
    coords = []
    precision = conf.ALT_PRECISION
    gain = 1 / (10 ** precision)
    for lat in float_range(bounds.south, bounds.north, gain):
        for lon in float_range(bounds.west, bounds.east, gain):
            point = lat, lon
            if not bound or point not in bounds:
                coords.append(round_coords(point, precision))
    return get_altitudes(coords)


def get_bootstrap_points():
    lat_gain, lon_gain = get_gains(conf.BOOTSTRAP_RADIUS)
    coords = []
    for map_row, lat in enumerate(
        float_range(bounds.south, bounds.north, lat_gain)
    ):
        row_start_lon = bounds.west
        if map_row % 2 != 0:
            row_start_lon -= 0.5 * lon_gain
        for map_col, lon in enumerate(
            float_range(row_start_lon, bounds.east, lon_gain)
        ):
            if (lat, lon) in bounds:
                coords.append((lat, lon))
    random.shuffle(coords)
    return coords


def get_device_info(account):
    device_info = {'brand': 'Apple',
                   'device': 'iPhone',
                   'manufacturer': 'Apple'}
    try:
        if account['iOS'].startswith('1'):
            device_info['product'] = 'iOS'
        else:
            device_info['product'] = 'iPhone OS'
        device_info['hardware'] = account['model'] + '\x00'
        device_info['model'] = IPHONES[account['model']] + '\x00'
    except (KeyError, AttributeError):
        account = generate_device_info(account)
        return get_device_info(account)
    device_info['version'] = account['iOS']
    device_info['device_id'] = account['id']
    return device_info


def generate_device_info(account):
    ios8 = ('8.0', '8.0.1', '8.0.2', '8.1', '8.1.1', '8.1.2', '8.1.3', '8.2', '8.3', '8.4', '8.4.1')
    ios9 = ('9.0', '9.0.1', '9.0.2', '9.1', '9.2', '9.2.1', '9.3', '9.3.1', '9.3.2', '9.3.3', '9.3.4', '9.3.5')
    ios10 = ('10.0', '10.0.1', '10.0.2', '10.0.3', '10.1', '10.1.1', '10.2', '10.2.1')

    devices = tuple(IPHONES.keys())
    account['model'] = random.choice(devices)

    account['id'] = uuid4().hex

    if account['model'] in ('iPhone9,1', 'iPhone9,2',
                            'iPhone9,3', 'iPhone9,4'):
        account['iOS'] = random.choice(ios10)
    elif account['model'] in ('iPhone8,1', 'iPhone8,2', 'iPhone8,4'):
        account['iOS'] = random.choice(ios9 + ios10)
    else:
        account['iOS'] = random.choice(ios8 + ios9 + ios10)

    return account


def create_account_dict(account):
    if isinstance(account, (tuple, list)):
        length = len(account)
    else:
        raise TypeError('Account must be a tuple or list.')

    if length not in (1, 3, 4, 6):
        raise ValueError('Each account should have either 3 (account info only) or 6 values (account and device info).')
    if length in (1, 4) and (not conf.PASS or not conf.PROVIDER):
        raise ValueError('No default PASS or PROVIDER are set.')

    entry = {}
    entry['username'] = account[0]

    if length == 1 or length == 4:
        entry['password'], entry['provider'] = conf.PASS, conf.PROVIDER
    else:
        entry['password'], entry['provider'] = account[1:3]

    if length == 4 or length == 6:
        entry['model'], entry['iOS'], entry['id'] = account[-3:]
    else:
        entry = generate_device_info(entry)

    entry['time'] = 0
    entry['captcha'] = False
    entry['banned'] = False

    return entry


def accounts_from_config(pickled_accounts=None):
    accounts = {}
    for account in conf.ACCOUNTS:
        username = account[0]
        if pickled_accounts and username in pickled_accounts:
            accounts[username] = pickled_accounts[username]
            if len(account) == 3 or len(account) == 6:
                accounts[username]['password'] = account[1]
                accounts[username]['provider'] = account[2]
        else:
            accounts[username] = create_account_dict(account)
    return accounts


def accounts_from_csv(new_accounts, pickled_accounts):
    accounts = {}
    for username, account in new_accounts.items():
        if pickled_accounts:
            pickled_account = pickled_accounts.get(username)
            if pickled_account:
                if pickled_account['password'] != account['password']:
                    del pickled_account['password']
                account.update(pickled_account)
            accounts[username] = account
            continue
        account['provider'] = account.get('provider') or 'ptc'
        if not all(account.get(x) for x in ('model', 'iOS', 'id')):
            account = generate_device_info(account)
        account['time'] = 0
        account['captcha'] = False
        account['banned'] = False
        accounts[username] = account
    return accounts


if conf.SPAWN_ID_INT:
    def get_spawn_id(pokemon):
        return int(pokemon['spawn_point_id'], 16)
else:
    def get_spawn_id(pokemon):
        return pokemon['spawn_point_id']


def get_current_hour(now=None):
    now = now or time.time()
    return round(now - (now % 3600))


def time_until_time(seconds, seen=None):
    current_seconds = seen or time.time() % 3600
    if current_seconds > seconds:
        return seconds + 3600 - current_seconds
    elif current_seconds + 3600 < seconds:
        return seconds - 3600 - current_seconds
    else:
        return seconds - current_seconds


def get_address():
    if conf.MANAGER_ADDRESS:
        return conf.MANAGER_ADDRESS
    if platform == 'win32':
        return r'\\.\pipe\monocle'
    if hasattr(socket, 'AF_UNIX'):
        return join(conf.DIRECTORY, 'monocle.sock')
    return ('127.0.0.1', 5001)


def load_pickle(name, raise_exception=False):
    location = join(conf.DIRECTORY, 'pickles', '{}.pickle'.format(name))
    try:
        with open(location, 'rb') as f:
            return pickle.load(f)
    except (FileNotFoundError, EOFError):
        if raise_exception:
            raise FileNotFoundError
        else:
            return None


def dump_pickle(name, var):
    folder = join(conf.DIRECTORY, 'pickles')
    try:
        mkdir(folder)
    except FileExistsError:
        pass
    except Exception as e:
        raise OSError("Failed to create 'pickles' folder, please create it manually") from e

    location = join(folder, '{}.pickle'.format(name))
    with open(location, 'wb') as f:
        pickle.dump(var, f, pickle.HIGHEST_PROTOCOL)


def load_accounts():
    pickled_accounts = load_pickle('accounts')

    if conf.ACCOUNTS_CSV:
        accounts = load_accounts_csv()
        if pickled_accounts and set(pickled_accounts) == set(accounts):
            return pickled_accounts
        else:
            accounts = accounts_from_csv(accounts, pickled_accounts)
    elif conf.ACCOUNTS:
        if pickled_accounts and set(pickled_accounts) == set(acc[0] for acc in conf.ACCOUNTS):
            return pickled_accounts
        else:
            accounts = accounts_from_config(pickled_accounts)
    else:
        raise ValueError('Must provide accounts in a CSV or your config file.')

    dump_pickle('accounts', accounts)
    return accounts


def load_accounts_csv():
    csv_location = join(conf.DIRECTORY, conf.ACCOUNTS_CSV)
    with open(csv_location, 'rt') as f:
        accounts = {}
        reader = DictReader(f)
        for row in reader:
            accounts[row['username']] = dict(row)
    return accounts


@jit
def randomize_point(point, amount=0.0003):
    '''Randomize point, by up to ~47 meters by default.'''
    lat, lon = point
    return (
        random.uniform(lat - amount, lat + amount),
        random.uniform(lon - amount, lon + amount)
    )
