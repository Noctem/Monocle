import math
import random
import requests
import polyline

from geopy import distance, Point
from pgoapi import utilities as pgoapi_utils

import config

OPTIONAL_SETTINGS = {
    'ALT_RANGE': (300, 400),
    'GOOGLE_MAPS_KEY': None,
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)


def get_map_center():
    """Returns center of the map"""
    lat = (config.MAP_END[0] + config.MAP_START[0]) / 2
    lon = (config.MAP_END[1] + config.MAP_START[1]) / 2
    return lat, lon


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
        return start_lat, start_lon, start_alt
    else:
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


def get_altitude_key(point, precision=2):
    return (round(point[0], precision), round(point[1], precision))


def random_altitude():
    if hasattr(config, 'ALT_RANGE'):
        altitude = random.uniform(*config.ALT_RANGE)
    else:
        altitude = random.uniform(300, 400)
    return altitude


def get_altitude(point):
    try:
        params = {'locations': 'enc:' + polyline.encode((point,))}
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
            if hasattr(config, 'GOOGLE_MAPS_KEY'):
                params['key'] = config.GOOGLE_MAPS_KEY
            r = requests.get('https://maps.googleapis.com/maps/api/elevation/json',
                             params=params).json()

            for result in r['results']:
                point = (result['location']['lat'], result['location']['lng'])
                key = get_altitude_key(point, precision)
                altitudes[key] = result['elevation']
        except Exception:
            pass
    return altitudes


def get_spawn_altitudes(spawns, precision=2):
    rounded_coords = set()
    for spawn in spawns:
        key = get_altitude_key(spawn['point'], precision)
        rounded_coords.add(key)
    rounded_coords = tuple(rounded_coords)
    altitudes = get_altitudes(rounded_coords, precision)
    return altitudes


def add_spawn_altitudes(spawns, precision=2):
    altitudes = get_spawn_altitudes(spawns, precision)
    for spawn in spawns:
        key = get_altitude_key(spawn['point'], precision)
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
            key = get_altitude_key((lat, lon), precision)
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
            key = get_altitude_key((lat, lon))
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


def get_worker_device(worker_number):
    hardware = {'iPhone5,1': 'N41AP',
                'iPhone5,2': 'N42AP',
                'iPhone5,3': 'N48AP',
                'iPhone5,4': 'N49AP',
                'iPhone6,1': 'N51AP',
                'iPhone6,2': 'N53AP',
                'iPhone7,1': 'N56AP',
                'iPhone7,2': 'N61AP',
                'iPhone8,1': 'N71AP',
                'iPhone8,2': 'N66AP',
                'iPhone8,4': 'N69AP'}
    account = config.ACCOUNTS[worker_number]
    device_info = {'device_brand': 'Apple',
                   'device_model': 'iPhone',
                   'hardware_manufacturer': 'Apple',
                   'firmware_brand': 'iPhone OS'
                   }
    device_info['device_comms_model'] = account[3]
    device_info['hardware_model'] = hardware[account[3]]
    device_info['firmware_type'] = account[4]
    device_info['device_id'] = account[5]
    return device_info


def sort_points_for_worker(points, worker_no):
    center = get_start_coords(worker_no)
    return sorted(points, key=lambda p: get_distance(p, center))


def get_distance(p1, p2):
    return math.sqrt(pow(p1[0] - p2[0], 2) + pow(p1[1] - p2[1], 2))


def get_cell_ids_for_points(points):
    cell_ids = []
    for point in points:
        cell_ids.append(pgoapi_utils.get_cell_ids(point[0], point[1]))
    return cell_ids
