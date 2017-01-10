from queue import Queue
from collections import deque, OrderedDict
from logging import getLogger
from threading import Thread
from sqlalchemy.exc import DBAPIError
from time import time
from random import shuffle

import asyncio

from utils import dump_pickle, load_pickle, get_current_hour, time_until_time, round_coords, get_altitude, get_point_altitudes

import db

class Spawns:
    """Manage spawn points and times"""
    session = db.Session(autoflush=False)
    spawns = OrderedDict()
    despawn_times = {}
    mysteries = set()
    altitudes = {}
    extra_mysteries = set()

    def __len__(self):
        return len(self.despawn_times)

    def __bool__(self):
        return len(self.despawn_times) > 0

    def update(self, loadpickle=False):
        if loadpickle:
            pickle = load_pickle('spawns')
            if pickle:
                self.spawns, self.despawn_times, self.mysteries, self.altitudes = pickle
            if self.spawns or self.mysteries:
                return
        self.spawns, self.despawn_times, self.mysteries, a = db.get_spawns(self.session)
        self.altitudes.update(a)
        if not self.altitudes:
            self.altitudes = get_point_altitudes()
        dump_pickle('spawns', self.pickle_objects)

    def get_altitude(self, point):
        point = round_coords(point)
        alt = self.altitudes.get(point)
        if not alt:
            alt = get_altitude(point)
            self.altitudes[point] = alt
        return alt

    def items(self):
        return self.spawns.items()

    def get_mysteries(self):
        if not self.extra_mysteries or len(self.mysteries) > 1000:
            mysteries = deque(self.mysteries)
        else:
            mysteries = deque(self.mysteries | self.extra_mysteries)
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

    def add_mystery(self, point):
        self.mysteries.add(point)

    def remove_mystery(self, point):
        self.mysteries.discard(point)

    def have_mystery(self, point):
        return point in self.mysteries

    def add_extra_mystery(self, point):
        self.extra_mysteries.add(round_coords(point, 4))

    def add_despawn(self, spawn_id, despawn_time):
        self.despawn_times[spawn_id] = despawn_time

    def get_despawn_seconds(self, spawn_id):
        return self.despawn_times.get(spawn_id)

    def get_despawn_time(self, spawn_id, seen=None):
        now = seen or time()
        hour = get_current_hour(now=now)
        try:
            despawn_time = self.get_despawn_seconds(spawn_id) + hour
            if now > despawn_time - 89:
                despawn_time += 3600
            return despawn_time
        except TypeError:
            return None

    def get_time_till_hidden(self, spawn_id):
        if spawn_id not in self:
            return None
        despawn_seconds = self.get_despawn_seconds(spawn_id)
        return time_until_time(despawn_seconds)

    @property
    def pickle_objects(self):
        return self.spawns, self.despawn_times, self.mysteries, self.altitudes

    @property
    def total_length(self):
        return len(self.despawn_times) + self.mysteries_count

    @property
    def mysteries_count(self):
        return len(self.mysteries) + len(self.extra_mysteries)


class DatabaseProcessor(Thread):
    spawns = Spawns()

    def __init__(self):
        super().__init__()
        self.queue = Queue()
        self.logger = getLogger('dbprocessor')
        self.running = True
        self._clean_cache = False
        self.count = 0
        self._commit = False

    def stop(self):
        self.running = False

    def add(self, obj):
        self.queue.put(obj)

    def run(self):
        session = db.Session()

        while self.running or not self.queue.empty():
            if self._clean_cache:
                try:
                    db.SIGHTING_CACHE.clean_expired()
                    db.MYSTERY_CACHE.clean_expired(session)
                except Exception:
                    self.logger.exception('Failed to clean cache.')
                finally:
                    self._clean_cache = False
            try:
                item = self.queue.get()

                if item['type'] == 'pokemon':
                    if item['valid']:
                        db.add_sighting(session, item)
                        if item['valid'] == True:
                            db.add_spawnpoint(session, item, self.spawns)
                    else:
                        db.add_mystery(session, item, self.spawns)
                    self.count += 1
                elif item['type'] == 'fort':
                    db.add_fort_sighting(session, item)
                elif item['type'] == 'pokestop':
                    db.add_pokestop(session, item)
                elif item['type'] == 'kill':
                    break
                self.logger.debug('Item saved to db')
                if self._commit:
                    session.commit()
                    self._commit = False
            except DBAPIError as e:
                session.rollback()
                self.logger.exception('A wild DB exception appeared!')
            except Exception:
                self.logger.exception('A wild exception appeared!')

        try:
            db.MYSTERY_CACHE.clean_expired(session)
            session.commit()
        except DBAPIError:
            session.rollback()
            self.logger.exception('A wild DB exception appeared!')
        session.close()

    def clean_cache(self):
        self._clean_cache = True

    def commit(self):
        self._commit = True
