import sys

from collections import deque, OrderedDict
from time import time
from itertools import chain
from hashlib import sha256

from . import bounds, db, sanitized as conf
from .shared import get_logger
from .utils import dump_pickle, load_pickle, get_current_hour, time_until_time


class BaseSpawns:
    """Manage spawn points and times"""
    def __init__(self):
        ## Spawns with known times
        # {(lat, lon): (spawn_id, spawn_seconds)}
        self.known = OrderedDict()
        # {spawn_id: despawn_seconds}
        self.despawn_times = {}

        ## Spawns with unknown times
        # {(lat, lon)}
        self.unknown = set()

        self.class_version = 3
        self.db_hash = sha256(conf.DB_ENGINE.encode()).digest()
        self.log = get_logger('spawns')

    def __len__(self):
        return len(self.despawn_times)

    def __bool__(self):
        return len(self.despawn_times) > 0

    def update(self):
        bound = bool(bounds)
        last_migration = conf.LAST_MIGRATION

        with db.session_scope() as session:
            query = session.query(db.Spawnpoint)
            if bound or conf.STAY_WITHIN_MAP:
                query = query.filter(db.Spawnpoint.lat >= bounds.south,
                                     db.Spawnpoint.lat <= bounds.north,
                                     db.Spawnpoint.lon >= bounds.west,
                                     db.Spawnpoint.lon <= bounds.east)
            known = {}
            for spawn in query:
                point = spawn.lat, spawn.lon

                # skip if point is not within boundaries (if applicable)
                if bound and point not in bounds:
                    continue

                if not spawn.updated or spawn.updated <= last_migration:
                    self.unknown.add(point)
                    continue

                if spawn.duration == 60:
                    spawn_time = spawn.despawn_time
                else:
                    spawn_time = (spawn.despawn_time + 1800) % 3600

                self.despawn_times[spawn.spawn_id] = spawn.despawn_time
                known[point] = spawn.spawn_id, spawn_time
        self.known = OrderedDict(sorted(known.items(), key=lambda k: k[1][1]))

    def after_last(self):
        try:
            k = next(reversed(self.known))
            seconds = self.known[k][1]
            return time() % 3600 > seconds
        except (StopIteration, KeyError, TypeError):
            return False

    def get_despawn_time(self, spawn_id, seen):
        hour = get_current_hour(now=seen)
        try:
            despawn_time = self.despawn_times[spawn_id] + hour
            if seen > despawn_time:
                despawn_time += 3600
            return despawn_time
        except KeyError:
            return None

    def unpickle(self):
        try:
            state = load_pickle('spawns', raise_exception=True)
            if all((state['class_version'] == self.class_version,
                    state['db_hash'] == self.db_hash,
                    state['bounds_hash'] == hash(bounds),
                    state['last_migration'] == conf.LAST_MIGRATION)):
                self.__dict__.update(state)
                return True
            else:
                self.log.warning('Configuration changed, reloading spawns from DB.')
        except FileNotFoundError:
            self.log.warning('No spawns pickle found, will create one.')
        except (TypeError, KeyError):
            self.log.warning('Obsolete or invalid spawns pickle type, reloading from DB.')
        return False

    def pickle(self):
        state = self.__dict__.copy()
        del state['log']
        state.pop('cells_count', None)
        state['bounds_hash'] = hash(bounds)
        state['last_migration'] = conf.LAST_MIGRATION
        dump_pickle('spawns', state)

    @property
    def total_length(self):
        return len(self.despawn_times) + len(self.unknown) + self.cells_count


class Spawns(BaseSpawns):
    def __init__(self):
        super().__init__()
        self.cells_count = 0

    def items(self):
        return self.known.items()

    def add_known(self, spawn_id, despawn_time, point):
        self.despawn_times[spawn_id] = despawn_time
        self.unknown.discard(point)

    def add_unknown(self, point):
        self.unknown.add(point)

    def unpickle(self):
        result = super().unpickle()
        try:
            del self.cell_points
        except AttributeError:
            pass
        return result

    def mystery_gen(self):
        for mystery in self.unknown.copy():
            yield mystery


class MoreSpawns(BaseSpawns):
    def __init__(self):
        super().__init__()

        ## Coordinates mentioned as "spawn_points" in GetMapObjects response
        ## May or may not be actual spawn points, more research is needed.
        # {(lat, lon)}
        self.cell_points = set()

    def items(self):
        # return a copy since it may be modified
        return self.known.copy().items()

    def add_known(self, spawn_id, despawn_time, point):
        self.despawn_times[spawn_id] = despawn_time
        # add so that have_point() will be up to date
        self.known[point] = None
        self.unknown.discard(point)
        self.cell_points.discard(point)

    def add_unknown(self, point):
        self.unknown.add(point)
        self.cell_points.discard(point)

    def have_point(self, point):
        return point in chain(self.cell_points, self.known, self.unknown)

    def mystery_gen(self):
        for mystery in chain(self.unknown.copy(), self.cell_points.copy()):
            yield mystery

    @property
    def cells_count(self):
        return len(self.cell_points)

sys.modules[__name__] = MoreSpawns() if conf.MORE_POINTS else Spawns()
