#!/usr/bin/env python3

from datetime import datetime
from pkg_resources import resource_filename
from contextlib import contextmanager
from multiprocessing.managers import BaseManager, RemoteError

import argparse
import time

from sanic import Sanic
from sanic.response import html, json
from jinja2 import Environment, PackageLoader, Markup
from asyncpg import create_pool

from monocle import db, sanitized as conf
from monocle.bounds import center
from monocle.names import DAMAGE, MOVES, POKEMON
from monocle.web_utils import get_scan_coords, get_worker_markers, Workers, get_args


GOOGLE_MAPS_KEY = conf.GOOGLE_MAPS_KEY if conf.REPORT_MAPS else None
MAPFILE = 'custom.html' if conf.LOAD_CUSTOM_HTML_FILE else 'newmap.html'

CSS_JS = ''
SOCIAL_LINKS = ''
JS_VARS = Markup(
    "_defaultSettings['FIXED_OPACITY'] = '{:d}'; "
    "_defaultSettings['SHOW_TIMER'] = '{:d}'; "
    "_defaultSettings['TRASH_IDS'] = [{}]; ".format(conf.FIXED_OPACITY, conf.SHOW_TIMER, ', '.join(str(p_id) for p_id in conf.TRASH_IDS))
)
if conf.LOAD_CUSTOM_CSS_FILE:
    CSS_JS += '<link rel="stylesheet" href="static/css/custom.css">'
if conf.LOAD_CUSTOM_JS_FILE:
    CSS_JS += '<script type="text/javascript" src="static/js/custom.js"></script>'
if conf.FB_PAGE_ID:
    SOCIAL_LINKS += '<a class="map_btn facebook-icon" target="_blank" href="https://www.facebook.com/' + conf.FB_PAGE_ID + '"></a>'
if conf.TWITTER_SCREEN_NAME:
    SOCIAL_LINKS += '<a class="map_btn twitter-icon" target="_blank" href="https://www.twitter.com/' + conf.TWITTER_SCREEN_NAME + '"></a>'
if conf.DISCORD_INVITE_ID:
    SOCIAL_LINKS += '<a class="map_btn discord-icon" target="_blank" href="https://discord.gg/' + conf.DISCORD_INVITE_ID + '"></a>'
if conf.TELEGRAM_USERNAME:
    SOCIAL_LINKS += '<a class="map_btn telegram-icon" target="_blank" href="https://www.telegram.me/' + conf.TELEGRAM_USERNAME + '"></a>'
CSS_JS = Markup(CSS_JS)
SOCIAL_LINKS = Markup(SOCIAL_LINKS)

env = Environment(loader=PackageLoader('monocle', 'templates'), enable_async=True)
app = Sanic(__name__)
app.static('/static', resource_filename('monocle', 'static'))


def jsonify(records):
    """
    Parse asyncpg record response into JSON format
    """
    return [{key: value for key, value in
            zip(r.keys(), r.values())} for r in records]


@app.route('/')
async def fullmap(request):
    template = env.get_template(MAPFILE)
    html_content = await template.render_async(
        area_name=conf.AREA_NAME,
        map_center=center,
        map_provider_url=conf.MAP_PROVIDER_URL,
        map_provider_attribution=conf.MAP_PROVIDER_ATTRIBUTION,
        social_links=SOCIAL_LINKS,
        init_js_vars=JS_VARS,
        extra_css_js=CSS_JS
    )
    return html(html_content)


@app.route('/data')
async def pokemon_data(request):
    last_id = request.args.get('last_id', 0)
    return json(await get_pokemarkers_async(last_id))


@app.route('/gym_data')
async def gym_data(request):
    return json(await get_gyms_async())


@app.route('/spawnpoints')
async def get_spawn_points(request):
    return json(await get_spawnpoints_async())


@app.route('/pokestops')
async def get_pokestops(request):
    return json(await get_pokestops_async())


@app.route('/scan_coords')
async def scan_coords(request):
    return json(get_scan_coords())


if conf.MAP_WORKERS:
    workers = Workers()


    @app.route('/workers_data')
    async def workers_data(request):
        return json(get_worker_markers(workers))


    @app.route('/workers')
    async def workers_map(request):
        template = env.get_template('workersmap.html')

        html_content = await template.render_async(
            area_name=conf.AREA_NAME,
            map_center=center,
            map_provider_url=conf.MAP_PROVIDER_URL,
            map_provider_attribution=conf.MAP_PROVIDER_ATTRIBUTION
        )
        return html(html_content)


async def get_pokemarkers_async(after_id):
    markers = []
    pokemon_names = POKEMON
    damage = DAMAGE
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                results = await conn.fetch('''
                    SELECT id, pokemon_id, expire_timestamp, lat, lon, atk_iv, def_iv, sta_iv, move_1, move_2
                    FROM sightings
                    WHERE expire_timestamp > {ts} AND id > {poke_id}
                '''.format(ts=time.time(), poke_id=after_id))

                for row in results:
                    content = {
                        'id': 'pokemon-{}'.format(row[0]),
                        'trash': row[1] in conf.TRASH_IDS,
                        'name': pokemon_names[row[1]],
                        'pokemon_id': row[1],
                        'lat': row[3],
                        'lon': row[4],
                        'expires_at': row[2]
                    }
                    if row[5]:
                        content.update({
                            'atk': row[5],
                            'def': row[6],
                            'sta': row[7],
                            'move1': row[8],
                            'move2': row[9],
                            'damage1': damage[row[8]],
                            'damage2': damage[row[9]]
                        })
                    markers.append(content)
    return markers


async def get_gyms_async():
    markers = []
    pokemon_names = POKEMON
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                results = await conn.fetch('''
                    SELECT
                        fs.fort_id,
                        fs.id,
                        fs.team,
                        fs.prestige,
                        fs.guard_pokemon_id,
                        fs.last_modified,
                        f.lat,
                        f.lon
                    FROM fort_sightings fs
                    JOIN forts f ON f.id=fs.fort_id
                    WHERE (fs.fort_id, fs.last_modified) IN (
                        SELECT fort_id, MAX(last_modified)
                        FROM fort_sightings
                        GROUP BY fort_id
                    )
                ''')
                for row in results:
                    if row[4]:
                        pokemon_name = pokemon_names[row[4]]
                    else:
                        pokemon_name = 'Empty'
                    markers.append({
                        'id': 'fort-{}'.format(row[0]),
                        'sighting_id': row[1],
                        'prestige': row[3],
                        'pokemon_id': row[4],
                        'pokemon_name': pokemon_name,
                        'team': row[2],
                        'lat': row[6],
                        'lon': row[7],
                    })
    return markers


async def get_spawnpoints_async():
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                results = await conn.fetch('SELECT spawn_id, despawn_time, lat, lon, duration FROM spawnpoints')
                return jsonify(results)


async def get_pokestops_async():
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                results = await conn.fetch('SELECT external_id, lat, lon FROM pokestops')
                return jsonify(results)


def main():
    args = get_args()
    app.run(debug=args.debug, host=args.host, port=args.port)

if __name__ == '__main__':
    main()

