import random
import requests
import polyline
import time
import socket
import pickle

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

try:
    from numba import jit
except ImportError:
    def jit(func):
        return func

from . import config

_optional = {
    'ALT_RANGE': (300, 400),
    'GOOGLE_MAPS_KEY': None,
    'MAP_START': None,
    'MAP_END': None,
    'BOUNDARIES': None,
    'SPAWN_ID_INT': True,
    'PASS': None,
    'PROVIDER': None,
    'MANAGER_ADDRESS': None,
    'BOOTSTRAP_RADIUS': 450,
    'DIRECTORY': None,
    'SPEED_UNIT': 'miles',
    'ACCOUNTS': None,
    'ACCOUNTS_CSV': None
}
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)
del _optional

if config.DIRECTORY is None:
    if exists(join('..', 'pickles')):
        config.DIRECTORY = '..'
    else:
        config.DIRECTORY = ''

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

if config.BOUNDARIES:
    MAP_CENTER = config.BOUNDARIES.centroid.coords[0]
    LAT_MEAN, LON_MEAN = MAP_CENTER
else:
    LAT_MEAN = (config.MAP_END[0] + config.MAP_START[0]) / 2
    LON_MEAN = (config.MAP_END[1] + config.MAP_START[1]) / 2
    MAP_CENTER = LAT_MEAN, LON_MEAN

try:
    from pogeo import get_distance

    class Units(Enum):
        miles = 1
        kilometers = 2
        meters = 3
except ImportError:
    from math import hypot, pi, cos

    LON_MULT = cos(pi * LAT_MEAN / 180)
    _lat_rad = LAT_MEAN * pi / 180
    _mult = 111132.92 + (-559.82 * cos(2 * _lat_rad)) + (1.175 * cos(4 * _lat_rad)) + (-0.0023 * cos(6 * _lat_rad))

    class Units(Enum):
        miles = _mult * 0.000621371
        kilometers = _mult / 1000
        meters = _mult

    del _lat_rad, _mult

    @jit
    def get_distance(p1, p2, mult=Units.meters.value):
        return hypot(p1[0] - p2[0], (p1[1] - p2[1]) * LON_MULT) * mult

log = getLogger(__name__)


def get_scan_area():
    """Returns the square kilometers for configured scan area"""
    width = get_distance(config.MAP_START, (config.MAP_START[0], config.MAP_END[1]), Units.kilometers.value)
    height = get_distance(config.MAP_START, (config.MAP_END[0], config.MAP_START[1]), Units.kilometers.value)
    area = round(width * height)
    return area


@jit
def get_start_coords(worker_no):
    """Returns center of square for given worker"""
    grid = config.GRID
    total_workers = grid[0] * grid[1]
    per_column = int(total_workers / grid[0])

    column = worker_no % per_column
    row = int(worker_no / per_column)
    part_lat = (config.MAP_END[0] - config.MAP_START[0]) / grid[0]
    part_lon = (config.MAP_END[1] - config.MAP_START[1]) / grid[1]
    start_lat = config.MAP_START[0] + part_lat * row + part_lat / 2
    start_lon = config.MAP_START[1] + part_lon * column + part_lon / 2
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
    start = Point(*MAP_CENTER)
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
    altitude = random.uniform(*config.ALT_RANGE)
    return altitude


def get_altitude(point):
    params = {'locations': 'enc:' + polyline.encode((point,))}
    if config.GOOGLE_MAPS_KEY:
        params['key'] = config.GOOGLE_MAPS_KEY
    r = requests.get('https://maps.googleapis.com/maps/api/elevation/json',
                     params=params).json()
    altitude = r['results'][0]['elevation']
    return altitude


def get_altitudes(coords, precision=3):
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    altitudes = dict()
    if len(coords) > 300:
        for chunk in tuple(chunks(coords, 300)):
            altitudes.update(get_altitudes(chunk, precision))
    else:
        try:
            params = {'locations': 'enc:' + polyline.encode(coords)}
            if config.GOOGLE_MAPS_KEY:
                params['key'] = config.GOOGLE_MAPS_KEY
            r = requests.get('https://maps.googleapis.com/maps/api/elevation/json',
                             params=params).json()

            for result in r['results']:
                point = (result['location']['lat'], result['location']['lng'])
                key = round_coords(point, precision)
                altitudes[key] = result['elevation']
        except Exception:
            log.exception('Error fetching altitudes.')
    return altitudes


def get_point_altitudes(precision=3):
    rounded_coords = set()
    lat_gain, lon_gain = get_gains(100)
    for map_row, lat in enumerate(
        float_range(config.MAP_START[0], config.MAP_END[0], lat_gain)
    ):
        row_start_lon = config.MAP_START[1]
        odd = map_row % 2 != 0
        if odd:
            row_start_lon -= 0.5 * lon_gain
        for map_col, lon in enumerate(
            float_range(row_start_lon, config.MAP_END[1], lon_gain)
        ):
            key = round_coords((lat, lon), precision)
            rounded_coords.add(key)
    rounded_coords = tuple(rounded_coords)
    altitudes = get_altitudes(rounded_coords, precision)
    return altitudes


def get_bootstrap_points():
    lat_gain, lon_gain = get_gains(config.BOOTSTRAP_RADIUS)
    coords = []
    for map_row, lat in enumerate(
        float_range(config.MAP_START[0], config.MAP_END[0], lat_gain)
    ):
        row_start_lon = config.MAP_START[1]
        odd = map_row % 2 != 0
        if odd:
            row_start_lon -= 0.5 * lon_gain
        for map_col, lon in enumerate(
            float_range(row_start_lon, config.MAP_END[1], lon_gain)
        ):
            coords.append([lat,lon])
    random.shuffle(coords)
    return coords


def get_device_info(account):
    device_info = {'brand': 'Apple',
                   'device': 'iPhone',
                   'manufacturer': 'Apple'}
    if account['iOS'].startswith('1'):
        device_info['product'] = 'iOS'
    else:
        device_info['product'] = 'iPhone OS'
    device_info['hardware'] = account['model']
    device_info['model'] = IPHONES[account['model']]
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
    if length in (1, 4) and (not config.PASS or not config.PROVIDER):
        raise ValueError('No default PASS or PROVIDER are set.')

    entry = {}
    entry['username'] = account[0]

    if length == 1 or length == 4:
        entry['password'], entry['provider'] = config.PASS, config.PROVIDER
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
    for account in config.ACCOUNTS:
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


def get_spawn_id(pokemon):
    if config.SPAWN_ID_INT:
        return int(pokemon['spawn_point_id'], 16)
    else:
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
    if config.MANAGER_ADDRESS:
        return config.MANAGER_ADDRESS
    if platform == 'win32':
        address = r'\\.\pipe\monocle'
    elif hasattr(socket, 'AF_UNIX'):
        address = join(config.DIRECTORY, 'monocle.sock')
    else:
        address = ('127.0.0.1', 5001)
    return address


def load_pickle(name):
    location = join(config.DIRECTORY, 'pickles', '{}.pickle'.format(name))
    try:
        with open(location, 'rb') as f:
            return pickle.load(f)
    except (FileNotFoundError, EOFError):
        return None


def dump_pickle(name, var):
    folder = join(config.DIRECTORY, 'pickles')
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

    if config.ACCOUNTS_CSV:
        accounts = load_accounts_csv()
        if pickled_accounts and set(pickled_accounts) == set(accounts):
            return pickled_accounts
        else:
            accounts = accounts_from_csv(accounts, pickled_accounts)
    elif config.ACCOUNTS:
        if pickled_accounts and set(pickled_accounts) == set(acc[0] for acc in config.ACCOUNTS):
            return pickled_accounts
        else:
            accounts = accounts_from_config(pickled_accounts)
    else:
        raise ValueError('Must provide accounts in a CSV or your config file.')

    dump_pickle('accounts', accounts)
    return accounts


def load_accounts_csv():
    csv_location = join(config.DIRECTORY, config.ACCOUNTS_CSV)
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
