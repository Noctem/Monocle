#!/usr/bin/env python3

from pkg_resources import resource_filename
from time import time

from sanic import Sanic
from sanic.response import html, json
from jinja2 import Environment, PackageLoader, Markup
from asyncpg import create_pool

from monocle import sanitized as conf
from monocle.bounds import center
from monocle.names import DAMAGE, MOVES, POKEMON
from monocle.web_utils import get_scan_coords, get_worker_markers, Workers, get_args


env = Environment(loader=PackageLoader('monocle', 'templates'))
app = Sanic(__name__)
app.static('/static', resource_filename('monocle', 'static'))


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
        map_center=center,
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
        map_center=center,
        map_provider_url=conf.MAP_PROVIDER_URL,
        map_provider_attribution=conf.MAP_PROVIDER_ATTRIBUTION,
        social_links=social_links()
    ))


@app.route('/')
async def fullmap(request, html_map=render_map()):
    return html_map


@app.route('/data')
async def pokemon_data(request):
    last_id = request.args.get('last_id', 0)
    return json(await get_pokemarkers_async(last_id))


@app.route('/gym_data')
async def gym_data(request):
    return json(await get_gyms_async())


@app.route('/spawnpoints')
async def spawn_points(request):
    return json([dict(x) for x in await get_spawnpoints_async()])


@app.route('/pokestops')
async def get_pokestops(request):
    return json([dict(x) for x in await get_pokestops_async()])


@app.route('/scan_coords')
async def scan_coords(request):
    return json(get_scan_coords())


if conf.MAP_WORKERS:
    workers = Workers()


    @app.route('/workers_data')
    async def workers_data(request):
        return json(get_worker_markers(workers))


    @app.route('/workers')
    async def workers_map(request, html_map=render_worker_map()):
        return html_map


def sighting_to_marker(pokemon, names=POKEMON, moves=MOVES, damage=DAMAGE):
    pokemon_id = pokemon['pokemon_id']
    marker = {
        'id': 'pokemon-' + str(pokemon['id']),
        'trash': pokemon_id in conf.TRASH_IDS,
        'name': names[pokemon_id],
        'pokemon_id': pokemon_id,
        'lat': pokemon['lat'],
        'lon': pokemon['lon'],
        'expires_at': pokemon['expire_timestamp'],
    }
    move1 = pokemon['move_1']
    if move1:
        move2 = pokemon['move_2']
        marker['atk'] = pokemon['atk_iv']
        marker['def'] = pokemon['def_iv']
        marker['sta'] = pokemon['sta_iv']
        marker['move1'] = moves[move1]
        marker['move2'] = moves[move2]
        marker['damage1'] = damage[move1]
        marker['damage2'] = damage[move2]
    return marker


async def get_pokemarkers_async(after_id):
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                results = await conn.fetch('''
                    SELECT id, pokemon_id, expire_timestamp, lat, lon, atk_iv, def_iv, sta_iv, move_1, move_2
                    FROM sightings
                    WHERE expire_timestamp > {ts} AND id > {poke_id}
                '''.format(ts=time(), poke_id=after_id))
                return tuple(map(sighting_to_marker, results))


async def get_gyms_async(names=POKEMON):
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
                return [{
                        'id': 'fort-' + str(fort['fort_id']),
                        'sighting_id': fort['id'],
                        'prestige': fort['prestige'],
                        'pokemon_id': fort['guard_pokemon_id'],
                        'pokemon_name': names[fort['guard_pokemon_id']],
                        'team': fort['team'],
                        'lat': fort['lat'],
                        'lon': fort['lon']
                } for fort in results]


async def get_spawnpoints_async():
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetch('SELECT spawn_id, despawn_time, lat, lon, duration FROM spawnpoints')


async def get_pokestops_async():
    async with create_pool(**conf.DB) as pool:
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetch('SELECT external_id, lat, lon FROM pokestops')


def main():
    args = get_args()
    app.run(debug=args.debug, host=args.host, port=args.port)

if __name__ == '__main__':
    main()

