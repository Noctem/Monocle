#!/usr/bin/env python3

from pkg_resources import resource_filename
from time import time

from sanic import Sanic
from sanic.response import html, HTTPResponse, json
from jinja2 import Environment, PackageLoader, Markup
from asyncpg import create_pool
from pogeo.monotools.aiosightingcache import AioSightingCache

from monocle import bounds, names, sanitized as conf
from monocle.web_utils import get_worker_markers, Workers, get_args


env = Environment(loader=PackageLoader('monocle', 'templates'))
app = Sanic(__name__)
app.static('/static', resource_filename('monocle', 'static'))
_CACHE = AioSightingCache(conf, names)


def social_links():
    social_links = ''

    if conf.FB_PAGE_ID:
        social_links = '<a class="map_btn facebook-icon" target="_blank" href="https://www.facebook.com/' + conf.FB_PAGE_ID + '"></a>'
    if conf.TWITTER_SCREEN_NAME:
        social_links += '<a class="map_btn twitter-icon" target="_blank" href="https://www.twitter.com/' + conf.TWITTER_SCREEN_NAME + '"></a>'
    if conf.DISCORD_INVITE_ID:
        social_links += '<a class="map_btn discord-icon" target="_blank" href="https://discord.gg/' + conf.DISCORD_INVITE_ID + '"></a>'
    if conf.TELEGRAM_USERNAME:
        social_links += '<a class="map_btn telegram-icon" target="_blank" href="https://www.telegram.me/' + conf.TELEGRAM_USERNAME + '"></a>'

    return Markup(social_links)


def render_map():
    css_js = ''

    if conf.LOAD_CUSTOM_CSS_FILE:
        css_js = '<link rel="stylesheet" href="static/css/custom.css">'
    if conf.LOAD_CUSTOM_JS_FILE:
        css_js += '<script type="text/javascript" src="static/js/custom.js"></script>'

    js_vars = Markup(
        "_defaultSettings['FIXED_OPACITY'] = '{:d}'; "
        "_defaultSettings['SHOW_TIMER'] = '{:d}'; "
        "_defaultSettings['TRASH_IDS'] = [{}]; ".format(conf.FIXED_OPACITY, conf.SHOW_TIMER, ', '.join(str(p_id) for p_id in conf.TRASH_IDS)))

    template = env.get_template('custom.html' if conf.LOAD_CUSTOM_HTML_FILE else 'newmap.html')
    return html(template.render(
        area_name=conf.AREA_NAME,
        map_center=bounds.center,
        map_provider_url=conf.MAP_PROVIDER_URL,
        map_provider_attribution=conf.MAP_PROVIDER_ATTRIBUTION,
        social_links=social_links(),
        init_js_vars=js_vars,
        extra_css_js=Markup(css_js)
    ))


def render_worker_map():
    template = env.get_template('workersmap.html')
    return html(template.render(
        area_name=conf.AREA_NAME,
        map_center=bounds.center,
        map_provider_url=conf.MAP_PROVIDER_URL,
        map_provider_attribution=conf.MAP_PROVIDER_ATTRIBUTION,
        social_links=social_links()
    ))


@app.get('/')
async def fullmap(request, html_map=render_map()):
    return html_map


if conf.MAP_WORKERS:
    workers = Workers()


    @app.get('/workers_data')
    async def workers_data(request):
        return json(get_worker_markers(workers))


    @app.get('/workers')
    async def workers_map(request, html_map=render_worker_map()):
        return html_map


del env


@app.get('/data')
async def pokemon_data(request, _cache=_CACHE):
    try:
        compress = 'gzip' in request.headers['Accept-Encoding'].lower()
    except KeyError:
        compress = False
    body = await _cache.get_json(int(request.args.get('last_id', 0)), compress)
    return HTTPResponse(
        body_bytes=body,
        content_type='application/json',
        headers={'Content-Encoding': 'gzip'} if compress else None)



@app.get('/gym_data')
async def gym_data(request, names=names.POKEMON, _str=str):
    async with app.pool.acquire() as conn:
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
    return json([{
            'id': 'fort-' + _str(fort['fort_id']),
            'sighting_id': fort['id'],
            'prestige': fort['prestige'],
            'pokemon_id': fort['guard_pokemon_id'],
            'pokemon_name': names[fort['guard_pokemon_id']],
            'team': fort['team'],
            'lat': fort['lat'],
            'lon': fort['lon']
    } for fort in results])


@app.get('/spawnpoints')
async def spawn_points(request, _dict=dict):
    async with app.pool.acquire() as conn:
         results = await conn.fetch('SELECT spawn_id, despawn_time, lat, lon, duration FROM spawnpoints')
    return json([_dict(x) for x in results])


@app.get('/pokestops')
async def get_pokestops(request):
    async with app.pool.acquire() as conn:
        results = await conn.fetch('SELECT external_id, lat, lon FROM pokestops')
    return json(results)


@app.get('/scan_coords')
async def scan_coords(request, _response=HTTPResponse(body_bytes=bounds.json, content_type='application/json')):
    return _response


@app.listener('before_server_start')
async def register_db(app, loop):
    app.pool = await create_pool(dsn=conf.DB_ENGINE, loop=loop)
    _CACHE.initialize(loop, app.pool)


def main():
    args = get_args()
    app.run(debug=args.debug, host=args.host, port=args.port)


if __name__ == '__main__':
    main()

