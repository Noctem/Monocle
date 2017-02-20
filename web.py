#!/usr/bin/env python3

from datetime import datetime
from pkg_resources import resource_filename

import json

from flask import Flask, request, render_template, jsonify, Markup

from monocle import config, db, utils
from monocle.names import POKEMON_NAMES, MOVES, POKEMON_MOVES

# Set defaults for missing config options
_optional = {
    'AREA_NAME': 'area',
    'GOOGLE_MAPS_KEY': None,
    'RARE_IDS': (),
    'TRASH_IDS': (),
    'MAP_PROVIDER_URL': '//{s}.tile.osm.org/{z}/{x}/{y}.png',
    'MAP_PROVIDER_ATTRIBUTION': '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
    'MAP_WORKERS': True,
    'AUTHKEY': b'm3wtw0',
    'REPORT_MAPS': True,
    'LOAD_CUSTOM_HTML_FILE': False,
    'LOAD_CUSTOM_CSS_FILE': False,
    'LOAD_CUSTOM_JS_FILE': False,
    'FB_PAGE_ID': None,
    'TWITTER_SCREEN_NAME': None,
    'DISCORD_INVITE_ID': None,
    'TELEGRAM_USERNAME': None,
    'BOUNDARIES': None,
    'FIXED_OPACITY': False,
    'SHOW_TIMER': False
}
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)
del _optional

from monocle.web_utils import *


if not config.REPORT_MAPS:
    config.GOOGLE_MAPS_KEY = None

app = Flask(__name__, template_folder=resource_filename('monocle', 'templates'), static_folder=resource_filename('monocle', 'static'))


@app.route('/')
def fullmap():
    extra_css_js = ''
    social_links = ''
    init_js_vars = ''

    init_js_vars += "_defaultSettings['FIXED_OPACITY'] = '{}'; ".format(int(config.FIXED_OPACITY))
    init_js_vars += "_defaultSettings['SHOW_TIMER'] = '{}'; ".format(int(config.SHOW_TIMER))
    init_js_vars += "_defaultSettings['TRASH_IDS'] = [{}]; ".format(', '.join(str(p_id) for p_id in config.TRASH_IDS))

    if config.LOAD_CUSTOM_HTML_FILE:
        mapfile = 'custom.html'
    else:
        mapfile = 'newmap.html'

    if config.LOAD_CUSTOM_CSS_FILE:
        extra_css_js += '<link rel="stylesheet" href="static/css/custom.css">'

    if config.LOAD_CUSTOM_JS_FILE:
        extra_css_js += '<script type="text/javascript" src="static/js/custom.js"></script>'

    if config.FB_PAGE_ID:
        social_links += '<a class="map_btn facebook-icon" target="_blank" href="https://www.facebook.com/' + config.FB_PAGE_ID + '"></a>'

    if config.TWITTER_SCREEN_NAME:
        social_links += '<a class="map_btn twitter-icon" target="_blank" href="https://www.twitter.com/' + config.TWITTER_SCREEN_NAME + '"></a>'

    if config.DISCORD_INVITE_ID:
        social_links += '<a class="map_btn discord-icon" target="_blank" href="https://discord.gg/' + config.DISCORD_INVITE_ID + '"></a>'

    if config.TELEGRAM_USERNAME:
        social_links += '<a class="map_btn telegram-icon" target="_blank" href="https://www.telegram.me/' + config.TELEGRAM_USERNAME + '"></a>'


    return render_template(
        mapfile,
        area_name=config.AREA_NAME,
        map_center=utils.MAP_CENTER,
        map_provider_url=config.MAP_PROVIDER_URL,
        map_provider_attribution=config.MAP_PROVIDER_ATTRIBUTION,
        social_links=Markup(social_links),
        init_js_vars=Markup(init_js_vars),
        extra_css_js=Markup(extra_css_js)
    )


@app.route('/data')
def pokemon_data():
    last_id = request.args.get('last_id', 0)
    return jsonify(get_pokemarkers(last_id))


@app.route('/gym_data')
def gym_data():
    return jsonify(get_gym_markers())


@app.route('/spawnpoints')
def get_spawn_points():
    return jsonify(get_spawnpoint_markers())


@app.route('/pokestops')
def get_pokestops():
    return jsonify(get_pokestop_markers())


@app.route('/scan_coords')
def scan_coords():
    return jsonify(get_scan_coords())


if config.MAP_WORKERS:
    workers = Workers()


    @app.route('/workers_data')
    def workers_data():
        return jsonify(get_worker_markers(workers))


    @app.route('/workers')
    def workers_map():
        return render_template(
            'workersmap.html',
            area_name=config.AREA_NAME,
            map_center=utils.MAP_CENTER,
            map_provider_url=config.MAP_PROVIDER_URL,
            map_provider_attribution=config.MAP_PROVIDER_ATTRIBUTION
        )


@app.route('/report')
def report_main():
    with db.session_scope() as session:
        counts = db.get_sightings_per_pokemon(session)
        session_stats = db.get_session_stats(session)

        count = sum(counts.values())
        counts_tuple = tuple(counts.items())
        top_pokemon = list(counts_tuple[-30:])
        top_pokemon.reverse()
        bottom_pokemon = counts_tuple[:30]
        nonexistent = [(x, POKEMON_NAMES[x]) for x in range(1, 252) if x not in counts]
        rare_pokemon = [r for r in counts_tuple if r[0] in config.RARE_IDS]
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
                'rare': [sighting_to_marker(s) for s in rare_sightings],
            },
            'map_center': utils.MAP_CENTER,
            'zoom': 13,
        }
    icons = {
        'top30': [(r[0], POKEMON_NAMES[r[0]]) for r in top_pokemon],
        'bottom30': [(r[0], POKEMON_NAMES[r[0]]) for r in bottom_pokemon],
        'rare': [(r[0], POKEMON_NAMES[r[0]]) for r in rare_pokemon],
        'nonexistent': nonexistent
    }

    area = utils.get_scan_area()

    return render_template(
        'report.html',
        current_date=datetime.now(),
        area_name=config.AREA_NAME,
        area_size=area,
        total_spawn_count=count,
        spawns_per_hour=count // session_stats['length_hours'],
        session_start=session_stats['start'],
        session_end=session_stats['end'],
        session_length_hours=session_stats['length_hours'],
        js_data=js_data,
        icons=icons,
        google_maps_key=config.GOOGLE_MAPS_KEY,
    )


@app.route('/report/<int:pokemon_id>')
def report_single(pokemon_id):
    with db.session_scope() as session:
        session_stats = db.get_session_stats(session)
        js_data = {
            'charts_data': {
                'hours': db.get_spawns_per_hour(session, pokemon_id),
            },
            'map_center': utils.MAP_CENTER,
            'zoom': 13,
        }
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


@app.route('/report/heatmap')
def report_heatmap():
    pokemon_id = request.args.get('id')
    with db.session_scope() as session:
        return json.dumps(db.get_all_spawn_coords(session, pokemon_id=pokemon_id))


def main():
    args = get_args()
    app.run(debug=args.debug, threaded=True, host=args.host, port=args.port)


if __name__ == '__main__':
    main()
