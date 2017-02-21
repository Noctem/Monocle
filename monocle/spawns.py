from collections import deque, OrderedDict
from time import time
from random import shuffle
from itertools import chain

from .shared import get_logger

from . import db
from .utils import dump_pickle, load_pickle, get_current_hour, time_until_time, round_coords, get_altitude, get_point_altitudes, random_altitude

class Spawns:
    """Manage spawn points and times"""
    def __init__(self):
        self.spawns = OrderedDict()
        self.despawn_times = {}
        self.mysteries = set()
        self.cell_points = set()
        self.altitudes = {}
        self.known_points = set()
        self.log = get_logger('spawns')

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
        try:
            alt = self.altitudes[point]
        except KeyError:
            try:
                alt = get_altitude(point)
                self.altitudes[point] = alt
            except IndexError as e:
                self.log.warning('Empty altitude response for {}, falling back to random.', point)
                alt = random_altitude()
            except KeyError as e:
                self.log.error('Invalid altitude response for {}, falling back to random.', point)
                alt = random_altitude()
            except Exception as e:
                self.log.error('{} while fetching altitude for {}, falling back to random.', e.__class__.__name__, point)
                alt = random_altitude()
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

SPAWNS = Spawns()
