#!/usr/bin/env python3

from datetime import datetime
from pkg_resources import resource_filename

import argparse
import json

from flask import Flask, request, render_template, jsonify
from multiprocessing.managers import BaseManager

from pokeminer import config
from pokeminer import db
from pokeminer import utils
from pokeminer.names import POKEMON_NAMES


# Check whether config has all necessary attributes
_required = (
    'AREA_NAME',
    'GOOGLE_MAPS_KEY'
)
for setting_name in _required:
    if not hasattr(config, setting_name):
        raise RuntimeError('Please set "{}" in config'.format(setting_name))
# Set defaults for missing config options
_optional = {
    'TRASH_IDS': (),
    'MAP_PROVIDER_URL': '//{s}.tile.osm.org/{z}/{x}/{y}.png',
    'MAP_PROVIDER_ATTRIBUTION': '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
    'MAP_WORKERS': True,
    'AUTHKEY': b'm3wtw0'
}
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)
del (_required, _optional)


def get_args():
    parser = argparse.ArgumentParser()
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


app = Flask(__name__, template_folder=resource_filename('pokeminer', 'templates'), static_folder=resource_filename('pokeminer', 'static'))

class AccountManager(BaseManager): pass
AccountManager.register('worker_dict')
manager = AccountManager(address=utils.get_address(), authkey=config.AUTHKEY)
manager.connect()
worker_dict = manager.worker_dict()

@app.route('/')
def fullmap():
    map_center = utils.MAP_CENTER
    return render_template(
        'newmap.html',
        area_name=config.AREA_NAME,
        map_center=map_center,
        map_provider_url=config.MAP_PROVIDER_URL,
        map_provider_attribution=config.MAP_PROVIDER_ATTRIBUTION,
    )

@app.route('/data')
def pokemon_data():
    return jsonify(get_pokemarkers())

@app.route('/spawnpoints')
def get_spawn_points():
    return jsonify(get_spawnpointsmarkers())

@app.route('/pokestops')
def get_pokestops():
    return jsonify(get_pokestopsmarkers())

@app.route('/scan_coords')
def get_scan_coords():
    return jsonify(get_scan_coords())

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory(path)


if config.MAP_WORKERS:
    def manager_connect():
        global worker_dict
        global manager
        try:
            manager = AccountManager(address=utils.get_address(), authkey=config.AUTHKEY)
            manager.connect()
            worker_dict = manager.worker_dict()
        except (FileNotFoundError, AttributeError):
            print('Unable to connect to manager for worker data.')
            worker_dict = {}

    @app.route('/workers_data')
    def workers_data():
        return jsonify(get_worker_markers())


    @app.route('/workers')
    def workers_map():
        map_center = utils.MAP_CENTER
        return render_template(
            'workersmap.html',
            area_name=config.AREA_NAME,
            map_center=map_center,
            map_provider_url=config.MAP_PROVIDER_URL,
            map_provider_attribution=config.MAP_PROVIDER_ATTRIBUTION
        )

    def get_worker_markers():
        markers = []
        try:
            if not worker_dict:
                manager_connect()
        except FileNotFoundError:
            manager_connect()

        # Worker start points
        for worker_no, data in worker_dict.items():
            coords = data[0]
            unix_time = data[1]
            speed = '{:.1f}mph'.format(data[2])
            total_seen = data[3]
            visits = data[4]
            seen_here = data[5]
            sent_notification = data[6]
            time = datetime.fromtimestamp(unix_time).strftime('%I:%M:%S %p').lstrip('0')
            markers.append({
                'lat': coords[0],
                'lon': coords[1],
                'type': 'worker',
                'worker_no': worker_no,
                'time': time,
                'speed': speed,
                'total_seen': total_seen,
                'visits': visits,
                'seen_here': seen_here,
                'sent_notification': sent_notification
            })
        return markers


def get_pokemarkers():
    markers = []
    session = db.Session(autoflush=False)
    pokemons = db.get_sightings(session)
    forts = db.get_forts(session)
    session.close()

    for pokemon in pokemons:
        markers.append({
            'id': 'pokemon-{}'.format(pokemon.id),
            'type': 'pokemon',
            'trash': pokemon.pokemon_id in config.TRASH_IDS,
            'name': POKEMON_NAMES[pokemon.pokemon_id],
            'pokemon_id': pokemon.pokemon_id,
            'lat': pokemon.lat,
            'lon': pokemon.lon,
            'expires_at': pokemon.expire_timestamp,
        })
    for fort in forts:
        if fort['guard_pokemon_id']:
            pokemon_name = POKEMON_NAMES[fort['guard_pokemon_id']]
        else:
            pokemon_name = 'Empty'
        markers.append({
            'id': 'fort-{}'.format(fort['fort_id']),
            'sighting_id': fort['id'],
            'type': 'fort',
            'prestige': fort['prestige'],
            'pokemon_id': fort['guard_pokemon_id'],
            'pokemon_name': pokemon_name,
            'team': fort['team'],
            'lat': fort['lat'],
            'lon': fort['lon'],
        })

    if config.MAP_WORKERS:
        # Worker stats
        markers.extend(get_worker_markers())
    return markers

def get_spawnpointsmarkers():
    markers = []
    session = db.Session()
    spawns = db.get_spawn_points(session)
    session.close()

    for spawn in spawns:
        markers.append({
            'id': 'spawn-{}'.format(spawn.id),
            'type': 'spawn',
            'spawn_id': spawn.spawn_id,
            'despawn_time': spawn.despawn_time,
            'lat': spawn.lat,
            'lon': spawn.lon,
            'alt': spawn.alt,
            'duration': spawn.duration
        })
    return markers

def get_pokestopsmarkers():
    markers = []
    session = db.Session()
    pokestops = db.get_pokestops(session)
    session.close()

    for pokestop in pokestops:
        markers.append({
            'id': 'pokestop-{}'.format(pokestop.id),
            'type': 'pokestop',
            'external_id': pokestop.external_id,
            'lat': pokestop.lat,
            'lon': pokestop.lon
        })
    return markers

def get_scan_coords():
    markers = []
    if config.BOUNDARIES:
        from shapely.geometry import mapping
        mapping = mapping(config.BOUNDARIES)['coordinates']
        coords = mapping[0]
        for blacklist in mapping[1:]:
            markers.append({
                    'type': 'scanblacklist',
                    'coords': blacklist
                })
    else:
        coords = (config.MAP_START, (config.MAP_START[0], config.MAP_END[1]), config.MAP_END, (config.MAP_END[0], config.MAP_START[1]), config.MAP_START)

    markers.append({
            'type': 'scanarea',
            'coords': coords
        })
    return markers

@app.route('/report')
def report_main():
    session = db.Session(autoflush=False)
    top_pokemon = db.get_top_pokemon(session)
    bottom_pokemon = db.get_top_pokemon(session, order='ASC')
    bottom_sightings = db.get_all_sightings(
        session, [r[0] for r in bottom_pokemon]
    )
    rare_pokemon = db.get_rare_pokemon(session)
    if rare_pokemon:
        rare_sightings = db.get_all_sightings(
            session, [r[0] for r in rare_pokemon]
        )
    else:
        rare_sightings = []
    js_data = {
        'charts_data': {
            'punchcard': db.get_punch_card(session),
            'top30': [(POKEMON_NAMES[r[0]], r[1]) for r in top_pokemon],
            'bottom30': [
                (POKEMON_NAMES[r[0]], r[1]) for r in bottom_pokemon
            ],
            'rare': [
                (POKEMON_NAMES[r[0]], r[1]) for r in rare_pokemon
            ],
        },
        'maps_data': {
            'bottom30': [sighting_to_marker(s) for s in bottom_sightings],
            'rare': [sighting_to_marker(s) for s in rare_sightings],
        },
        'map_center': utils.MAP_CENTER,
        'zoom': 13,
    }
    icons = {
        'top30': [(r[0], POKEMON_NAMES[r[0]]) for r in top_pokemon],
        'bottom30': [(r[0], POKEMON_NAMES[r[0]]) for r in bottom_pokemon],
        'rare': [(r[0], POKEMON_NAMES[r[0]]) for r in rare_pokemon],
        'nonexistent': [
            (r, POKEMON_NAMES[r])
            for r in db.get_nonexistent_pokemon(session)
        ]
    }
    session_stats = db.get_session_stats(session)
    session.close()

    area = utils.get_scan_area()

    return render_template(
        'report.html',
        current_date=datetime.now(),
        area_name=config.AREA_NAME,
        area_size=area,
        total_spawn_count=session_stats['count'],
        spawns_per_hour=session_stats['per_hour'],
        session_start=session_stats['start'],
        session_end=session_stats['end'],
        session_length_hours=int(session_stats['length_hours']),
        js_data=js_data,
        icons=icons,
        google_maps_key=config.GOOGLE_MAPS_KEY,
    )


@app.route('/report/<int:pokemon_id>')
def report_single(pokemon_id):
    session = db.Session(autoflush=False)
    session_stats = db.get_session_stats(session)
    js_data = {
        'charts_data': {
            'hours': db.get_spawns_per_hour(session, pokemon_id),
        },
        'map_center': utils.MAP_CENTER,
        'zoom': 13,
    }
    session.close()
    return render_template(
        'report_single.html',
        current_date=datetime.now(),
        area_name=config.AREA_NAME,
        area_size=utils.get_scan_area(),
        pokemon_id=pokemon_id,
        pokemon_name=POKEMON_NAMES[pokemon_id],
        total_spawn_count=db.get_total_spawns_count(session, pokemon_id),
        session_start=session_stats['start'],
        session_end=session_stats['end'],
        session_length_hours=int(session_stats['length_hours']),
        google_maps_key=config.GOOGLE_MAPS_KEY,
        js_data=js_data,
    )


def sighting_to_marker(sighting):
    return {
        'icon': resource_filename('pokeminer', 'static/icons/{}.png'.format(sighting.pokemon_id)),
        'lat': sighting.lat,
        'lon': sighting.lon,
    }


@app.route('/report/heatmap')
def report_heatmap():
    session = db.Session(autoflush=False)
    pokemon_id = request.args.get('id')
    points = db.get_all_spawn_coords(session, pokemon_id=pokemon_id)
    session.close()
    return jsonify(points)


def main():
    args = get_args()
    app.run(debug=args.debug, threaded=True, host=args.host, port=args.port)

if __name__ == '__main__':
    main()
