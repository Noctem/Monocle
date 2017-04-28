import sys

from collections import deque, OrderedDict
from time import time

from . import bounds, db, sanitized as conf
from .db import DB_HASH, session_scope, Spawnpoint
from .shared import get_logger
from .utils import dump_pickle, load_pickle, get_current_hour, time_until_time

contains_spawn = bounds.contains_cellid if conf.SPAWN_ID_INT else bounds.contains_token


class Spawns:
    """Manage spawn points and times"""

    __spec__ = __spec__
    __slots__ = ('known', 'despawn_times', 'unknown', 'log')

    def __init__(self):
        ## Spawns with known times
        # {spawn_id: spawn_seconds}
        self.known = OrderedDict()

        # points may not be in bounds, but are visible from within bounds
        # {spawn_id: despawn_seconds}
        self.despawn_times = {}

        ## Spawns with unknown times
        # {(lat, lon)}
        self.unknown = set()

        self.log = get_logger('spawns')

    def __len__(self):
        return len(self.despawn_times)

    def __bool__(self):
        return len(self.despawn_times) > 0

    def items(self):
        return self.known.items()

    def add_known(self, spawn_id, despawn_time):
        self.despawn_times[spawn_id] = despawn_time
        self.unknown.discard(spawn_id)

    def update(self, _migration=conf.LAST_MIGRATION, _contains=contains_spawn):
        with session_scope() as session:
            query = session.query(Spawnpoint.spawn_id, Spawnpoint.despawn_time, Spawnpoint.duration, Spawnpoint.updated)
            known = {}
            for spawn_id, despawn_time, duration, updated in query:
                # skip if point is not within boundaries (if applicable)
                if not _contains(spawn_id):
                    continue

                if not updated or updated < _migration:
                    self.unknown.add(spawn_id)
                    continue

                self.despawn_times[spawn_id] = despawn_time if duration == 60 else (despawn_time + 1800) % 3600

                known[spawn_id] = spawn_time
        if known:
            self.known = OrderedDict(sorted(known.items(), key=lambda k: k[1]))

    def after_last(self):
        try:
            k = next(reversed(self.known))
            return time() % 3600 > self.known[k]
        except (StopIteration, KeyError, TypeError):
            return False

    def get_despawn_time(self, spawn_id, seen):
        try:
            despawn_time = self.despawn_times[spawn_id] + get_current_hour(now=seen)
            return despawn_time if seen < despawn_time else despawn_time + 3600
        except KeyError:
            return None

    def unpickle(self):
        try:
            state = load_pickle('spawns', raise_exception=True)
            if (state['class_version'] == 4,
                    and state['db_hash'] == DB_HASH,
                    and state['bounds_hash'] == hash(bounds),
                    and state['last_migration'] == conf.LAST_MIGRATION):
                self.despawn_times = state['despawn_times']
                self.known = state['known']
                self.unknown = state['unknown']
                return True
            else:
                self.log.warning('Configuration changed, reloading spawns from DB.')
        except FileNotFoundError:
            self.log.warning('No spawns pickle found, will create one.')
        except (TypeError, KeyError):
            self.log.warning('Obsolete or invalid spawns pickle type, reloading from DB.')
        return False

    def pickle(self):
        dump_pickle('spawns', {
            'bounds_hash': hash(bounds),
            'class_version': 4,
            'db_hash': DB_HASH,
            'despawn_times': self.despawn_times,
            'known': self.known,
            'last_migration': conf.LAST_MIGRATION,
            'unknown': self.unknown})

    @property
    def total_length(self):
        return len(self.despawn_times) + len(self.unknown)


sys.modules[__name__] = Spawns()
