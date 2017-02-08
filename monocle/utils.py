import random
import requests
import polyline
import time
import socket
import pickle
import functools

from os import mkdir
from os.path import join, exists
from math import ceil, sqrt, hypot, pi, cos
from uuid import uuid4
from geopy import Point
from geopy.distance import distance
from pogo_async import utilities as pgoapi_utils
from sys import platform
from asyncio import sleep

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
    'DIRECTORY': None
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

LAT_RAD = LAT_MEAN * pi / 180
LON_MULT = cos(pi * LAT_MEAN / 180)
METER_MULT = 111132.92 + (-559.82 * cos(2 * LAT_RAD)) + (1.175 * cos(4 * LAT_RAD)) + (-0.0023 * cos(6 * LAT_RAD))


def get_scan_area():
    """Returns the square kilometers for configured scan area"""
    width = get_distance(config.MAP_START, (config.MAP_START[0], config.MAP_END[1])) / 1000
    height = get_distance(config.MAP_START, (config.MAP_END[0], config.MAP_START[1])) / 1000
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
    try:
        params = {'locations': 'enc:' + polyline.encode((point,))}
        if config.GOOGLE_MAPS_KEY:
            params['key'] = config.GOOGLE_MAPS_KEY
        r = requests.get('https://maps.googleapis.com/maps/api/elevation/json',
                         params=params).json()
        altitude = r['results'][0]['elevation']
    except Exception:
        altitude = random_altitude()
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
            pass
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
                   'manufacturer': 'Apple',
                   'product': 'iPhone OS'}
    device_info['hardware'] = account['model']
    device_info['model'] = IPHONES[account['model']]
    device_info['version'] = account['iOS']
    device_info['device_id'] = account['id']
    return device_info


def generate_device_info():
    account = dict()
    devices = tuple(IPHONES.keys())
    ios8 = ('8.0', '8.0.1', '8.0.2', '8.1', '8.1.1', '8.1.2', '8.1.3', '8.2', '8.3', '8.4', '8.4.1')
    ios9 = ('9.0', '9.0.1', '9.0.2', '9.1', '9.2', '9.2.1', '9.3', '9.3.1', '9.3.2', '9.3.3', '9.3.4', '9.3.5')
    ios10 = ('10.0', '10.0.1', '10.0.2', '10.0.3', '10.1', '10.1.1', '10.2', '10.2.1')

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

    if not (length == 1 or length == 3 or length == 4 or length == 6):
        raise ValueError('Each account should have either 3 (account info only) or 6 values (account and device info).')
    if (length == 1 or length == 4) and (not config.PASS or not config.PROVIDER):
        raise AttributeError('No default PASS or PROVIDER are set.')

    username = account[0]
    entry = {}

    if length == 1 or length == 4:
        entry['password'], entry['provider'] = config.PASS, config.PROVIDER
    else:
        entry['password'], entry['provider'] = account[1:3]

    if length == 4 or length == 6:
        entry['model'], entry['iOS'], entry['id'] = account[-3:]
    else:
        entry.update(generate_device_info())

    entry.update({'time': 0, 'captcha': False, 'banned': False})

    return entry


def create_accounts_dict(old_accounts=None):
    accounts = {}
    for account in config.ACCOUNTS:
        username = account[0]
        if old_accounts and username in old_accounts:
            accounts[username] = old_accounts[username]
            if len(account) == 3 or len(account) == 6:
                accounts[username]['password'] = account[1]
                accounts[username]['provider'] = account[2]
        else:
            accounts[username] = create_account_dict(account)
    return accounts


def get_spawn_id(pokemon):
    if config.SPAWN_ID_INT:
        return int(pokemon['spawn_point_id'], 16)
    else:
        return pokemon['spawn_point_id']


def get_current_hour(now=None):
    now = now or time.time()
    return round(now - (now % 3600))


@jit
def get_distance(p1, p2):
    return hypot(p1[0] - p2[0], (p1[1] - p2[1]) * LON_MULT) * METER_MULT


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
    location = join(config.DIRECTORY, 'pickles', 'accounts.pickle')
    try:
        with open(location, 'rb') as f:
            accounts = pickle.load(f)
        if (config.ACCOUNTS and 
                set(accounts) != set(acc[0] for acc in config.ACCOUNTS)):
            accounts = create_accounts_dict(accounts)
            dump_pickle('accounts', accounts)
    except (FileNotFoundError, EOFError):
        if not config.ACCOUNTS:
            raise ValueError(
                'Must have accounts in config or an accounts pickle.')
        accounts = create_accounts_dict()
        dump_pickle('accounts', accounts)
    return accounts


async def random_sleep(minimum=10, maximum=13, mode=None):
    """Sleeps for a bit"""
    if mode:
        await sleep(random.triangular(minimum, maximum, mode))
    else:
        await sleep(random.uniform(minimum, maximum))
