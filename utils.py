import math
import random
import requests
import polyline
import time
import socket
import pickle

from uuid import uuid4
from geopy import distance, Point
from pgoapi import utilities as pgoapi_utils
from sys import platform
from asyncio import sleep

import config

OPTIONAL_SETTINGS = {
    'ALT_RANGE': (300, 400),
    'GOOGLE_MAPS_KEY': None,
    'MAP_START': None,
    'MAP_END': None,
    'BOUNDARIES': None,
    'SPAWN_ID_INT': True
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

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


def get_map_center():
    """Returns center of the map"""
    if config.BOUNDARIES:
        coords = config.BOUNDARIES.centroid.coords[0]
        return coords
    elif config.MAP_START and config.MAP_END:
        lat = (config.MAP_END[0] + config.MAP_START[0]) / 2
        lon = (config.MAP_END[1] + config.MAP_START[1]) / 2
        return lat, lon
    else:
        raise ValueError(
            'Must set either MAP_START/END or BOUNDARIES to get center')


def get_scan_area():
    """Returns the square kilometers for configured scan area"""
    lat1 = config.MAP_START[0]
    lat2 = config.MAP_END[0]
    lon1 = config.MAP_START[1]
    lon2 = config.MAP_END[1]
    p1 = Point(lat1, lon1)
    p2 = Point(lat1, lon2)
    p3 = Point(lat1, lon1)
    p4 = Point(lat2, lon1)

    width = distance.distance(p1, p2).kilometers
    height = distance.distance(p3, p4).kilometers
    area = int(width * height)
    return area


def get_start_coords(worker_no, altitude=False):
    """Returns center of square for given worker"""
    grid = config.GRID
    total_workers = grid[0] * grid[1]
    per_column = int(total_workers / grid[0])
    column = worker_no % per_column
    row = int(worker_no / per_column)
    part_lat = (config.MAP_END[0] - config.MAP_START[0]) / float(grid[0])
    part_lon = (config.MAP_END[1] - config.MAP_START[1]) / float(grid[1])
    start_lat = config.MAP_START[0] + part_lat * row + part_lat / 2
    start_lon = config.MAP_START[1] + part_lon * column + part_lon / 2
    if altitude:
        start_alt = get_altitude((start_lat, start_lon))
    else:
        start_alt = random.uniform(*config.ALT_RANGE)
    return start_lat, start_lon, start_alt


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


def get_gains():
    """Returns lat and lon gain

    Gain is space between circles.
    """
    start = Point(*get_map_center())
    base = config.SCAN_RADIUS * math.sqrt(3)
    height = base * math.sqrt(3) / 2
    dis_a = distance.VincentyDistance(meters=base)
    dis_h = distance.VincentyDistance(meters=height)
    lon_gain = dis_a.destination(point=start, bearing=90).longitude
    lat_gain = dis_h.destination(point=start, bearing=0).latitude
    return abs(start.latitude - lat_gain), abs(start.longitude - lon_gain)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def round_coords(point, precision=2):
    return (round(point[0], precision), round(point[1], precision))


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


def get_altitudes(coords, precision=2):
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


def get_spawn_altitudes(spawns, precision=2):
    rounded_coords = set()
    for spawn in spawns:
        key = round_coords(spawn['point'], precision)
        rounded_coords.add(key)
    rounded_coords = tuple(rounded_coords)
    altitudes = get_altitudes(rounded_coords, precision)
    return altitudes


def add_spawn_altitudes(spawns, precision=2):
    altitudes = get_spawn_altitudes(spawns, precision)
    for spawn in spawns:
        key = round_coords(spawn['point'], precision)
        altitude = altitudes.get(key, random_altitude())
        spawn['point'] = (*spawn['point'], altitude)
    return spawns


def get_point_altitudes(precision=2):
    rounded_coords = set()
    lat_gain, lon_gain = get_gains()
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


def get_points_per_worker(gen_alts=False):
    """Returns all points that should be visited for whole grid"""
    total_workers = config.GRID[0] * config.GRID[1]

    lat_gain, lon_gain = get_gains()

    points = [[] for _ in range(total_workers)]
    total_rows = math.ceil(
        abs(config.MAP_START[0] - config.MAP_END[0]) / lat_gain
    )
    total_columns = math.ceil(
        abs(config.MAP_START[1] - config.MAP_END[1]) / lon_gain
    )

    if gen_alts:
        altitudes = get_point_altitudes()

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
            # Figure out which worker this should go to
            grid_row = int(map_row / float(total_rows) * config.GRID[0])
            grid_col = int(map_col / float(total_columns) * config.GRID[1])
            if map_col >= total_columns:  # should happen only once per 2 rows
                grid_col -= 1
            worker_no = grid_row * config.GRID[1] + grid_col
            key = round_coords((lat, lon))
            if gen_alts:
                alt = altitudes.get(key, random_altitude())
                points[worker_no].append((lat, lon, alt))
            else:
                points[worker_no].append((lat, lon))
    points = [
        sort_points_for_worker(p, i)
        for i, p in enumerate(points)
    ]
    return points


def get_device_info(account):
    device_info = {'brand': 'Apple',
                   'device': 'iPhone',
                   'manufacturer': 'Apple',
                   'product': 'iPhone OS'
                   }
    device_info['hardware'] = account.get('model')
    device_info['model'] = IPHONES[account.get('model')]
    device_info['version'] = account.get('iOS')
    device_info['device_id'] = account.get('id')
    return device_info


def generate_device_info():
    account = dict()
    devices = tuple(IPHONES.keys())
    ios8 = ('8.0', '8.0.1', '8.0.2', '8.1', '8.1.1', '8.1.2', '8.1.3', '8.2', '8.3', '8.4', '8.4.1')
    ios9 = ('9.0', '9.0.1', '9.0.2', '9.1', '9.2', '9.2.1', '9.3', '9.3.1', '9.3.2', '9.3.3', '9.3.4', '9.3.5')
    ios10 = ('10.0', '10.0.1', '10.0.2', '10.0.3', '10.1', '10.1.1')

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
        raise TypeError('Account must be a tuple/list or string.')

    if not (length == 1 or length == 3 or length == 4 or length == 6):
        raise ValueError('Each account should have either 3 (account info only) or 6 values (account and device info).')
    if (length == 1 or length == 4) and (not config.PASS or not config.PROVIDER):
        raise ValueError('No default PASS or PROVIDER are set.')

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

    entry.update({'location': (0,0,0), 'time': 0, 'captcha': False, 'banned': False})

    return entry


def create_accounts_dict(old_accounts=None):
    accounts = {}
    for account in config.ACCOUNTS:
        username = account[0]
        if old_accounts and username in old_accounts:
            accounts[username] = old_accounts[username]
        else:
            accounts[username] = create_account_dict(account)
    return accounts


def sort_points_for_worker(points, worker_no):
    center = get_start_coords(worker_no)
    return sorted(points, key=lambda p: get_distance(p, center))


def get_spawn_id(pokemon):
    if config.SPAWN_ID_INT:
        return int(pokemon['spawn_point_id'], 16)
    else:
        return pokemon['spawn_point_id']


def get_current_hour():
    now = time.time()
    return round(now - (now % 3600))


def get_distance(p1, p2):
    return math.sqrt(pow(p1[0] - p2[0], 2) + pow(p1[1] - p2[1], 2))


def get_cell_ids_for_points(points):
    cell_ids = []
    for point in points:
        cell_ids.append(pgoapi_utils.get_cell_ids(point[0], point[1]))
    return cell_ids


def time_until_time(seconds):
    current_seconds = time.time() % 3600
    if current_seconds > seconds:
        return seconds + 3600 - current_seconds
    else:
        return seconds - current_seconds


def get_address():
    if platform == 'win32':
        address=r'\\.\pipe\pokeminer'
    elif hasattr(socket, 'AF_UNIX'):
        address='pokeminer.sock'
    else:
        address=('127.0.0.1', 5000)
    return address


def normalize_pokemon(raw, now):
    """Normalizes data coming from API into something acceptable by db"""
    return {
        'type': 'pokemon',
        'encounter_id': raw['encounter_id'],
        'pokemon_id': raw['pokemon_data']['pokemon_id'],
        'expire_timestamp': round((now + raw['time_till_hidden_ms']) / 1000),
        'lat': raw['latitude'],
        'lon': raw['longitude'],
        'spawn_id': get_spawn_id(raw),
        'time_till_hidden_ms': raw['time_till_hidden_ms'],
        'last_modified_timestamp_ms': raw['last_modified_timestamp_ms']
    }


def normalize_lured(raw, now):
    return {
        'type': 'pokemon',
        'encounter_id': raw['lure_info']['encounter_id'],
        'pokemon_id': raw['lure_info']['active_pokemon_id'],
        'expire_timestamp': raw['lure_info']['lure_expires_timestamp_ms'] / 1000,
        'lat': raw['latitude'],
        'lon': raw['longitude'],
        'spawn_id': -1,
        'time_till_hidden_ms': raw['lure_info']['lure_expires_timestamp_ms'] - now,
        'valid': 'pokestop'
    }


def normalize_gym(raw):
    return {
        'type': 'fort',
        'external_id': raw['id'],
        'lat': raw['latitude'],
        'lon': raw['longitude'],
        'team': raw.get('owned_by_team', 0),
        'prestige': raw.get('gym_points', 0),
        'guard_pokemon_id': raw.get('guard_pokemon_id', 0),
        'last_modified': round(raw['last_modified_timestamp_ms'] / 1000),
    }


def normalize_pokestop(raw):
    return {
        'type': 'pokestop',
        'external_id': raw['id'],
        'lat': raw['latitude'],
        'lon': raw['longitude']
    }


def load_pickle(name):
    location = 'pickles/{}.pickle'.format(name)
    try:
        with open(location, 'rb') as f:
            return pickle.load(f)
    except (FileNotFoundError, EOFError):
        return None


def dump_pickle(name, var):
    location = 'pickles/{}.pickle'.format(name)
    with open(location, 'wb') as f:
        pickle.dump(var, f, pickle.HIGHEST_PROTOCOL)


async def random_sleep(minimum=8, maximum=14, mode=10):
    """Sleeps for a bit"""
    if mode:
        await sleep(random.triangular(minimum, maximum, mode))
    else:
        await sleep(random.uniform(minimum, maximum))
