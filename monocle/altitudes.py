import sys

from asyncio import gather, CancelledError
from statistics import mean

from aiohttp import ClientSession
from polyline import encode as polyencode
from aiopogo import json_loads
from cyrandom import uniform

from . import bounds, sanitized as conf
from .shared import get_logger, LOOP, run_threaded
from .utils import dump_pickle, float_range, load_pickle, round_coords


class Altitudes:
    """Manage altitudes"""
    __slots__ = ('altitudes', 'changed', 'fallback', 'log', 'mean')

    def __init__(self):
        self.log = get_logger('altitudes')
        self.changed = False
        self.load()
        if len(self.altitudes) > 5:
            self.fallback = self.average
        else:
            self.fallback = self.random

    async def get_all(self):
        self.log.info('Fetching all altitudes')

        coords = self.get_coords()

        async with ClientSession(loop=LOOP) as session:
            if len(coords) < 300:
                await self.fetch_alts(coords, session)
            else:
                tasks = [self.fetch_alts(chunk, session)
                         for chunk in self.chunks(coords)]
                await gather(*tasks, loop=LOOP)
        self.changed = True
        LOOP.create_task(run_threaded(self.pickle))

    async def fetch_alts(self, coords, session, precision=conf.ALT_PRECISION):
        try:
            async with session.get(
                    'https://maps.googleapis.com/maps/api/elevation/json',
                    params={'locations': 'enc:' + polyencode(coords),
                            'key': conf.GOOGLE_MAPS_KEY},
                    timeout=10) as resp:
                response = await resp.json(loads=json_loads)
            for r in response['results']:
                coords = round_coords((r['location']['lat'], r['location']['lng']), precision)
                self.altitudes[coords] = r['elevation']
            if not self.altitudes:
                self.log.error(response['error_message'])
        except Exception:
            self.log.exception('Error fetching altitudes.')

    def get(self, point, randomize=uniform):
        point = round_coords(point, conf.ALT_PRECISION)
        alt = self.altitudes[point]
        return randomize(alt - 2.5, alt + 2.5)

    async def fetch(self, point, key=conf.GOOGLE_MAPS_KEY):
        if not key:
            return self.fallback()
        try:
            async with ClientSession(loop=LOOP) as session:
                async with session.get(
                        'https://maps.googleapis.com/maps/api/elevation/json',
                        params={'locations': '{0[0]},{0[1]}'.format(point),
                                'key': key},
                        timeout=10) as resp:
                    response = await resp.json(loads=json_loads)
                    altitude = response['results'][0]['elevation']
                    self.altitudes[point] = altitude
                    self.changed = True
                    return altitude
        except CancelledError:
            raise
        except Exception:
            try:
                self.log.error(response['error_message'])
            except (KeyError, NameError):
                self.log.error('Error fetching altitude for {}.', point)
            return self.fallback()

    def average(self, randomize=uniform):
        self.log.info('Fell back to average altitude.')
        try:
            return randomize(self.mean - 15.0, self.mean + 15.0)
        except AttributeError:
            self.mean = mean(self.altitudes.values())
            return self.average()

    def random(self, alt_range=conf.ALT_RANGE, randomize=uniform):
        self.log.info('Fell back to random altitude.')
        return randomize(*conf.ALT_RANGE)

    def load(self):
        try:
            state = load_pickle('altitudes', raise_exception=True)
        except FileNotFoundError:
            self.log.info('No altitudes pickle found.')
            self.altitudes = {}
            LOOP.run_until_complete(self.get_all())
            return

        if state['bounds_hash'] == hash(bounds):
            if state['precision'] == conf.ALT_PRECISION and state['altitudes']:
                self.altitudes = state['altitudes']
                return
            elif state['precision'] < conf.ALT_PRECISION:
                self.altitudes = state['altitudes']
                LOOP.run_until_complete(self.get_all())
                return
        elif state['precision'] <= conf.ALT_PRECISION:
            pickled_alts = state['altitudes']

            to_remove = []
            for coords in pickled_alts.keys():
                if coords not in bounds:
                    to_remove.append(coords)
            for key in to_remove:
                del pickled_alts[key]

            self.altitudes = pickled_alts
            LOOP.run_until_complete(self.get_all())
            return
        self.altitudes = {}
        LOOP.run_until_complete(self.get_all())

    def pickle(self):
        if self.changed:
            state = {
                'altitudes': self.altitudes,
                'precision': conf.ALT_PRECISION,
                'bounds_hash': hash(bounds)
            }
            dump_pickle('altitudes', state)
            self.changed = False

    def get_coords(self, bounds=bounds, precision=conf.ALT_PRECISION):
        coords = []
        if bounds.multi:
            for b in bounds.polygons:
                coords.extend(self.get_coords(b))
            return coords
        step = 1 / (10 ** precision)
        west, east = bounds.west, bounds.east
        existing = self.altitudes.keys() if self.altitudes else False
        for lat in float_range(bounds.south, bounds.north, step):
            for lon in float_range(west, east, step):
                point = lat, lon
                if not existing or point not in existing:
                    coords.append(round_coords(point, precision))
        return coords

    @staticmethod
    def chunks(l, n=300):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]


sys.modules[__name__] = Altitudes()
