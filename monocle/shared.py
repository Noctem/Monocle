from queue import Queue
from collections import deque, OrderedDict
from logging import getLogger, LoggerAdapter
from threading import Thread
from sqlalchemy.exc import DBAPIError
from time import time
from random import shuffle
from itertools import chain

import asyncio

from .utils import dump_pickle, load_pickle, get_current_hour, time_until_time, round_coords, get_altitude, get_point_altitudes

from . import db


class Spawns:
    """Manage spawn points and times"""
    def __init__(self):
        self.spawns = OrderedDict()
        self.despawn_times = {}
        self.mysteries = set()
        self.cell_points = set()
        self.altitudes = {}
        self.known_points = set()

    def __len__(self):
        return len(self.despawn_times)

    def __bool__(self):
        return len(self.despawn_times) > 0

    def update(self, loadpickle=False):
        if loadpickle:
            try:
                self.spawns, self.despawn_times, self.mysteries, self.altitudes, self.known_points = load_pickle('spawns')
                if self.mysteries or self.despawn_times:
                    return
            except Exception:
                pass
        with db.session_scope() as session:
            self.spawns, self.despawn_times, self.mysteries, a, self.known_points = db.get_spawns(session)
        self.altitudes.update(a)
        if not self.altitudes:
            self.altitudes = get_point_altitudes()
        dump_pickle('spawns', self.pickle_objects)

    def get_altitude(self, point):
        point = round_coords(point, 3)
        alt = self.altitudes.get(point)
        if not alt:
            alt = get_altitude(point)
            self.altitudes[point] = alt
        return alt

    def items(self):
        return self.spawns.items()

    def get_mysteries(self):
        mysteries = deque(self.mysteries | self.cell_points)
        shuffle(mysteries)
        return mysteries

    def after_last(self):
        try:
            k = next(reversed(self.spawns))
            seconds = self.spawns[k][1]
            current_seconds = time() % 3600
            return current_seconds > seconds
        except (StopIteration, KeyError, TypeError):
            return False

    def add_despawn(self, spawn_id, despawn_time):
        self.despawn_times[spawn_id] = despawn_time

    def add_known(self, point):
        self.known_points.add(point)
        self.remove_mystery(point)

    def add_mystery(self, point):
        self.mysteries.add(point)
        self.cell_points.discard(point)

    def add_cell_point(self, point):
        self.cell_points.add(point)

    def remove_mystery(self, point):
        self.mysteries.discard(point)
        self.cell_points.discard(point)

    def get_despawn_seconds(self, spawn_id):
        return self.despawn_times.get(spawn_id)

    def db_has(self, point):
        return point in chain(self.known_points, self.mysteries)

    def have_point(self, point):
        return point in chain(self.cell_points, self.known_points, self.mysteries)

    def get_despawn_time(self, spawn_id, seen=None):
        now = seen or time()
        hour = get_current_hour(now=now)
        try:
            despawn_time = self.get_despawn_seconds(spawn_id) + hour
            if now > despawn_time:
                despawn_time += 3600
            return despawn_time
        except TypeError:
            return None

    def get_time_till_hidden(self, spawn_id):
        if spawn_id not in self.despawn_times:
            return None
        return time_until_time(self.despawn_times[spawn_id])

    @property
    def pickle_objects(self):
        return self.spawns, self.despawn_times, self.mysteries, self.altitudes, self.known_points

    @property
    def total_length(self):
        return len(self.despawn_times) + self.mysteries_count + self.cells_count

    @property
    def mysteries_count(self):
        return len(self.mysteries)

    @property
    def cells_count(self):
        return len(self.cell_points)


class DatabaseProcessor(Thread):

    def __init__(self):
        super().__init__()
        self.queue = Queue()
        self.log = get_logger('dbprocessor')
        self.running = True
        self.count = 0
        self._commit = False

    def stop(self):
        self.running = False

    def add(self, obj):
        self.queue.put(obj)

    def run(self):
        session = db.Session()

        while self.running or not self.queue.empty():
            try:
                item = self.queue.get()

                if item['type'] == 'pokemon':
                    db.add_sighting(session, item)
                    self.count += 1
                    if not item['inferred']:
                        db.add_spawnpoint(session, item)
                elif item['type'] == 'mystery':
                    db.add_mystery(session, item)
                    self.count += 1
                elif item['type'] == 'fort':
                    db.add_fort_sighting(session, item)
                elif item['type'] == 'pokestop':
                    db.add_pokestop(session, item)
                elif item['type'] == 'mystery-update':
                    db.update_mystery(session, item)
                elif item['type'] == 'kill':
                    break
                self.log.debug('Item saved to db')
                if self._commit:
                    session.commit()
                    self._commit = False
            except Exception as e:
                session.rollback()
                self.log.exception('A wild {} appeared in the DB processor!', e.__class__.__name__)

        db.MYSTERY_CACHE.update_db(session)
        session.close()

    def clean_cache(self):
        self._clean_cache = True

    def commit(self):
        self._commit = True


class Message:
    def __init__(self, fmt, args):
        self.fmt = fmt
        self.args = args

    def __str__(self):
        return self.fmt.format(*self.args)


class StyleAdapter(LoggerAdapter):
    def __init__(self, logger, extra=None):
        super(StyleAdapter, self).__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, Message(msg, args), (), **kwargs)


def get_logger(name=None):
    return StyleAdapter(getLogger(name))


SPAWNS = Spawns()
DB = DatabaseProcessor()
