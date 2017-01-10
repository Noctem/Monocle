from datetime import datetime
from collections import OrderedDict
import enum
import time

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, SmallInteger, BigInteger, ForeignKey, UniqueConstraint
from sqlalchemy.types import TypeDecorator, Numeric, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.mysql import TINYINT, MEDIUMINT, BIGINT
from sqlalchemy.exc import DBAPIError

import utils

try:
    import config
    DB_ENGINE = config.DB_ENGINE
except (ImportError, AttributeError):
    DB_ENGINE = 'sqlite:///db.sqlite'

OPTIONAL_SETTINGS = {
    'LAST_MIGRATION': 1481932800,
    'SPAWN_ID_INT': True,
    'RARE_IDS': [],
    'REPORT_SINCE': None,
    'BOUNDARIES': None,
    'STAY_WITHIN_MAP': True,
    'MORE_POINTS': True
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

if config.BOUNDARIES:
    try:
        from shapely.geometry import Polygon, Point

        if not isinstance(config.BOUNDARIES, Polygon):
            raise TypeError('BOUNDARIES must be a shapely Polygon.')
    except ImportError as e:
        raise ImportError('BOUNDARIES is set but shapely is not available.') from e

try:
    if config.LAST_MIGRATION > time.time():
        raise ValueError('LAST_MIGRATION must be a timestamp from the past.')
except TypeError as e:
    raise TypeError('LAST_MIGRATION must be a numeric timestamp.') from e


class Team(enum.Enum):
    none = 0
    mystic = 1
    valor = 2
    instict = 3


if DB_ENGINE.startswith('mysql'):
    TINY_TYPE = TINYINT(unsigned=True)          # 0 to 255
    MEDIUM_TYPE = MEDIUMINT(unsigned=True)      # 0 to 4294967295
    HUGE_TYPE = BIGINT(unsigned=True)           # 0 to 18446744073709551615
elif DB_ENGINE.startswith('postgres'):
    class NumInt(TypeDecorator):
        '''Modify Numeric type for integers'''
        impl = Numeric

        def process_bind_param(self, value, dialect):
            return int(value)

        def process_result_value(self, value, dialect):
            return int(value)

        @property
        def python_type(self):
            return int

    TINY_TYPE = SmallInteger                    # -32768 to 32767
    MEDIUM_TYPE = Integer                       # -2147483648 to 2147483647
    HUGE_TYPE = NumInt(precision=20, scale=0)   # up to 20 digits
else:
    class TextInt(TypeDecorator):
        '''Modify Text type for integers'''
        impl = Text

        def process_bind_param(self, value, dialect):
            return str(value)

        def process_result_value(self, value, dialect):
            return int(value)

    TINY_TYPE = SmallInteger
    MEDIUM_TYPE = Integer
    HUGE_TYPE = TextInt

if config.SPAWN_ID_INT:
    ID_TYPE = BigInteger
else:
    ID_TYPE = String(11)


def get_engine():
    return create_engine(DB_ENGINE)


def get_engine_name(session):
    return session.connection().engine.name


def combine_key(sighting):
    return sighting['encounter_id'], sighting['spawn_id']

Base = declarative_base()


class Bounds:
    if config.BOUNDARIES:
        boundaries = config.BOUNDARIES

        @classmethod
        def contain(cls, p):
            return cls.boundaries.contains(Point(p))
    elif config.STAY_WITHIN_MAP:
        north = max(config.MAP_START[0], config.MAP_END[0])
        south = min(config.MAP_START[0], config.MAP_END[0])
        east = max(config.MAP_START[1], config.MAP_END[1])
        west = min(config.MAP_START[1], config.MAP_END[1])

        @classmethod
        def contain(cls, p):
            lat, lon = p
            return (cls.south <= lat <= cls.north and
                    cls.west <= lon <= cls.east)
    else:
        @staticmethod
        def contain(p):
            return True


class SightingCache(object):
    """Simple cache for storing actual sightings

    It's used in order not to make as many queries to the database.
    It's also capable of purging old entries.
    """
    def __init__(self):
        self.store = {}
        self.spawns = set()

    def add(self, sighting):
        self.store[combine_key(sighting)] = sighting['expire_timestamp']
        self.spawns.add(sighting['spawn_id'])

    def __contains__(self, raw_sighting):
        expire_timestamp = self.store.get(combine_key(raw_sighting))
        if not expire_timestamp:
            return False
        within_range = (
            expire_timestamp > raw_sighting['expire_timestamp'] - 1 and
            expire_timestamp < raw_sighting['expire_timestamp'] + 1
        )
        return within_range

    def clean_expired(self):
        to_remove = []
        for key, timestamp in self.store.items():
            if time.time() > timestamp:
                to_remove.append(key)
                try:
                    self.spawns.remove(key[1])
                except KeyError:
                    pass
        for key in to_remove:
            del self.store[key]


class MysteryCache(object):
    """Simple cache for storing Pokemon with unknown expiration times

    It's used in order not to make as many queries to the database.
    It's also capable of purging old entries.
    """
    def __init__(self):
        self.store = {}

    def add(self, sighting):
        self.store[combine_key(sighting)] = [sighting['seen']] * 2

    def __contains__(self, raw_sighting):
        key = combine_key(raw_sighting)
        try:
            first, last = self.store[key]
        except (KeyError, TypeError):
            return False
        new_time = raw_sighting['seen']
        if new_time > last:
            self.store[key][1] = new_time
        return True

    def clean_expired(self, session):
        to_remove = []
        for key, times in self.store.items():
            first, last = times
            if first < time.time() - 3600:
                to_remove.append(key)
                if last == first:
                    continue
                encounter_id, spawn_id = key
                encounter = session.query(Mystery) \
                            .filter(Mystery.spawn_id == spawn_id) \
                            .filter(Mystery.encounter_id == encounter_id) \
                            .first()
                if not encounter:
                    continue
                hour = encounter.first_seen - (encounter.first_seen % 3600)
                encounter.last_seconds = last - hour
                encounter.seen_range = last - first
        if to_remove:
            try:
                session.commit()
            except DBAPIError:
                session.rollback()
        for key in to_remove:
            del self.store[key]


class FortCache(object):
    """Simple cache for storing fort sightings"""
    def __init__(self):
        self.store = {}

    def add(self, sighting):
        if sighting['type'] == 'pokestop':
            self.store[sighting['external_id']] = True
        else:
            self.store[sighting['external_id']] = (
                sighting['team'],
                sighting['prestige'],
                sighting['guard_pokemon_id'],
            )

    def __contains__(self, sighting):
        params = self.store.get(sighting['external_id'])
        if not params:
            return False
        if sighting['type'] == 'pokestop':
            return True
        is_the_same = (
            params[0] == sighting['team'] and
            params[1] == sighting['prestige'] and
            params[2] == sighting['guard_pokemon_id']
        )
        return is_the_same


SIGHTING_CACHE = SightingCache()
MYSTERY_CACHE = MysteryCache()
FORT_CACHE = FortCache()


class Sighting(Base):
    __tablename__ = 'sightings'

    id = Column(Integer, primary_key=True)
    pokemon_id = Column(TINY_TYPE)
    spawn_id = Column(ID_TYPE)
    expire_timestamp = Column(Integer, index=True)
    encounter_id = Column(HUGE_TYPE, index=True)
    normalized_timestamp = Column(Integer)
    lat = Column(Float)
    lon = Column(Float)
    atk_iv = Column(TINY_TYPE)
    def_iv = Column(TINY_TYPE)
    sta_iv = Column(TINY_TYPE)
    move_1 = Column(SmallInteger)
    move_2 = Column(SmallInteger)

    __table_args__ = (
        UniqueConstraint(
            'encounter_id',
            'expire_timestamp',
            name='timestamp_encounter_id_unique'
        ),
    )


class Mystery(Base):
    __tablename__ = 'mystery_sightings'

    id = Column(Integer, primary_key=True)
    pokemon_id = Column(TINY_TYPE)
    spawn_id = Column(ID_TYPE, index=True)
    encounter_id = Column(HUGE_TYPE, index=True)
    lat = Column(Float)
    lon = Column(Float)
    first_seen = Column(Integer, index=True)
    first_seconds = Column(SmallInteger)
    last_seconds = Column(SmallInteger)
    seen_range = Column(SmallInteger)
    atk_iv = Column(TINY_TYPE)
    def_iv = Column(TINY_TYPE)
    sta_iv = Column(TINY_TYPE)
    move_1 = Column(SmallInteger)
    move_2 = Column(SmallInteger)

    __table_args__ = (
        UniqueConstraint(
            'encounter_id',
            'spawn_id',
            name='unique_encounter'
        ),
    )


class Spawnpoint(Base):
    __tablename__ = 'spawnpoints'

    id = Column(Integer, primary_key=True)
    spawn_id = Column(ID_TYPE, unique=True, index=True)
    despawn_time = Column(SmallInteger, index=True)
    lat = Column(Float)
    lon = Column(Float)
    alt = Column(SmallInteger)
    updated = Column(Integer, index=True)
    duration = Column(TINY_TYPE)


class Fort(Base):
    __tablename__ = 'forts'

    id = Column(Integer, primary_key=True)
    external_id = Column(String(35), unique=True)
    lat = Column(Float, index=True)
    lon = Column(Float, index=True)

    sightings = relationship(
        'FortSighting',
        backref='fort',
        order_by='FortSighting.last_modified'
    )


class FortSighting(Base):
    __tablename__ = 'fort_sightings'

    id = Column(Integer, primary_key=True)
    fort_id = Column(Integer, ForeignKey('forts.id'))
    last_modified = Column(Integer)
    team = Column(TINY_TYPE)
    prestige = Column(MEDIUM_TYPE)
    guard_pokemon_id = Column(TINY_TYPE)

    __table_args__ = (
        UniqueConstraint(
            'fort_id',
            'last_modified',
            name='fort_id_last_modified_unique'
        ),
    )


class Pokestop(Base):
    __tablename__ = 'pokestops'

    id = Column(Integer, primary_key=True)
    external_id = Column(String(35), unique=True)
    lat = Column(Float, index=True)
    lon = Column(Float, index=True)


Session = sessionmaker(bind=get_engine())


def get_spawns(session):
    spawns = session.query(Spawnpoint)
    mysteries = set()
    spawns_dict = {}
    despawn_times = {}
    altitudes = {}
    for spawn in spawns:
        point = (spawn.lat, spawn.lon)

        # skip if point is not within boundaries (if applicable)
        if not Bounds.contain(point):
            continue

        rounded = utils.round_coords(point, precision=3)
        altitudes[rounded] = spawn.alt

        if not spawn.updated or spawn.updated < config.LAST_MIGRATION:
            mysteries.add(point)
            continue

        if spawn.duration == 60:
            spawn_time = spawn.despawn_time
        else:
            spawn_time = (spawn.despawn_time + 1800) % 3600

        despawn_times[spawn.spawn_id] = spawn.despawn_time
        spawns_dict[spawn.spawn_id] = (point, spawn_time)

    spawns = OrderedDict(sorted(spawns_dict.items(), key=lambda k: k[1][1]))
    return spawns, despawn_times, mysteries, altitudes


def normalize_timestamp(timestamp):
    return int(float(timestamp) / 120.0) * 120


def get_since():
    """Returns 'since' timestamp that should be used for filtering"""
    return time.mktime(config.REPORT_SINCE.timetuple())


def get_since_query_part(where=True):
    """Returns WHERE part of query filtering records before set date"""
    if config.REPORT_SINCE:
        return '{noun} expire_timestamp > {since}'.format(
            noun='WHERE' if where else 'AND',
            since=get_since(),
        )
    return ''


def add_sighting(session, pokemon):
    # Check if there isn't the same entry already
    if pokemon in SIGHTING_CACHE:
        return
    existing = session.query(Sighting) \
        .filter(Sighting.encounter_id == pokemon['encounter_id']) \
        .filter(Sighting.expire_timestamp == pokemon['expire_timestamp']) \
        .first()
    if existing:
        SIGHTING_CACHE.add(pokemon)
        return
    obj = Sighting(
        pokemon_id=pokemon['pokemon_id'],
        spawn_id=pokemon['spawn_id'],
        encounter_id=pokemon['encounter_id'],
        expire_timestamp=pokemon['expire_timestamp'],
        normalized_timestamp=normalize_timestamp(pokemon['expire_timestamp']),
        lat=pokemon['lat'],
        lon=pokemon['lon'],
        atk_iv=pokemon.get('individual_attack'),
        def_iv=pokemon.get('individual_defense'),
        sta_iv=pokemon.get('individual_stamina'),
        move_1=pokemon.get('move_1'),
        move_2=pokemon.get('move_2')
    )
    session.add(obj)
    SIGHTING_CACHE.add(pokemon)


def add_spawnpoint(session, pokemon, spawns):
    # Check if the same entry already exists
    spawn_id = pokemon['spawn_id']
    new_time = pokemon['expire_timestamp'] % 3600
    existing_time = spawns.get_despawn_seconds(spawn_id)
    point = (pokemon['lat'], pokemon['lon'])
    spawns.remove_mystery(point)
    if new_time == existing_time:
        return
    existing = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == spawn_id) \
        .first()
    now = round(time.time())
    if existing:
        existing.updated = now

        if (existing.despawn_time is None or
                existing.updated < config.LAST_MIGRATION):
            widest = get_widest_range(session, spawn_id)
            if widest and widest > 1710:
                existing.duration = 60
        elif new_time == existing.despawn_time:
            return

        existing.despawn_time = new_time
        spawns.add_despawn(spawn_id, new_time)
    else:
        altitude = spawns.get_altitude(point)
        spawns.add_despawn(spawn_id, new_time)

        widest = get_widest_range(session, spawn_id)

        if widest and widest > 1710:
            duration = 60
        else:
            duration = None

        obj = Spawnpoint(
            spawn_id=spawn_id,
            despawn_time=new_time,
            lat=pokemon['lat'],
            lon=pokemon['lon'],
            alt=altitude,
            updated=now,
            duration=duration
        )
        session.add(obj)


def add_mystery_spawnpoint(session, pokemon, spawns):
    # Check if the same entry already exists
    spawn_id = pokemon['spawn_id']
    point = (pokemon['lat'], pokemon['lon'])
    if spawns.have_mystery(point):
        return
    if spawns.get_despawn_seconds(spawn_id):
        return
    existing = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == spawn_id) \
        .first()
    if existing:
        return
    altitude = spawns.get_altitude(point)

    obj = Spawnpoint(
        spawn_id=spawn_id,
        despawn_time=None,
        lat=pokemon['lat'],
        lon=pokemon['lon'],
        alt=altitude,
        updated=0,
        duration=None
    )
    session.add(obj)

    if Bounds.contain(point):
        spawns.add_mystery(point)


def add_mystery(session, pokemon, spawns):
    if pokemon in MYSTERY_CACHE:
        return
    add_mystery_spawnpoint(session, pokemon, spawns)
    existing = session.query(Mystery) \
        .filter(Mystery.encounter_id == pokemon['encounter_id']) \
        .filter(Mystery.spawn_id == pokemon['spawn_id']) \
        .first()
    if existing:
        key = combine_key(pokemon)
        MYSTERY_CACHE.store[key] = [existing.first_seen, pokemon['seen']]
        return
    seconds = pokemon['seen'] % 3600
    obj = Mystery(
        pokemon_id=pokemon['pokemon_id'],
        spawn_id=pokemon['spawn_id'],
        encounter_id=pokemon['encounter_id'],
        lat=pokemon['lat'],
        lon=pokemon['lon'],
        first_seen=pokemon['seen'],
        first_seconds=seconds,
        last_seconds=seconds,
        seen_range=0,
        atk_iv=pokemon.get('individual_attack'),
        def_iv=pokemon.get('individual_defense'),
        sta_iv=pokemon.get('individual_stamina'),
        move_1=pokemon.get('move_1'),
        move_2=pokemon.get('move_2')
    )
    session.add(obj)
    MYSTERY_CACHE.add(pokemon)


def add_fort_sighting(session, raw_fort):
    if raw_fort in FORT_CACHE:
        return
    # Check if fort exists
    fort = session.query(Fort) \
        .filter(Fort.external_id == raw_fort['external_id']) \
        .first()
    if not fort:
        fort = Fort(
            external_id=raw_fort['external_id'],
            lat=raw_fort['lat'],
            lon=raw_fort['lon'],
        )
        session.add(fort)
    if fort.id:
        existing = session.query(FortSighting) \
            .filter(FortSighting.fort_id == fort.id) \
            .filter(FortSighting.team == raw_fort['team']) \
            .filter(FortSighting.prestige == raw_fort['prestige']) \
            .filter(FortSighting.guard_pokemon_id ==
                    raw_fort['guard_pokemon_id']) \
            .first()
        if existing:
            # Why is it not in the cache? It should be there!
            FORT_CACHE.add(raw_fort)
            return
    obj = FortSighting(
        fort=fort,
        team=raw_fort['team'],
        prestige=raw_fort['prestige'],
        guard_pokemon_id=raw_fort['guard_pokemon_id'],
        last_modified=raw_fort['last_modified'],
    )
    session.add(obj)
    FORT_CACHE.add(raw_fort)


def add_pokestop(session, raw_pokestop):
    if raw_pokestop in FORT_CACHE:
        return
    pokestop = session.query(Pokestop) \
        .filter(Pokestop.external_id == raw_pokestop['external_id']) \
        .first()
    if pokestop:
        FORT_CACHE.add(raw_pokestop)
        return

    pokestop = Pokestop(
        external_id=raw_pokestop['external_id'],
        lat=raw_pokestop['lat'],
        lon=raw_pokestop['lon'],
    )
    session.add(pokestop)
    FORT_CACHE.add(raw_pokestop)


def get_sightings(session):
    return session.query(Sighting) \
        .filter(Sighting.expire_timestamp > time.time()) \
        .all()


def get_forts(session):
    if get_engine_name(session) == 'sqlite':
        # SQLite version is slooooooooooooow when compared to MySQL
        where = '''
            WHERE fs.fort_id || '-' || fs.last_modified IN (
                SELECT fort_id || '-' || MAX(last_modified)
                FROM fort_sightings
                GROUP BY fort_id
            )
        '''
    else:
        where = '''
            WHERE (fs.fort_id, fs.last_modified) IN (
                SELECT fort_id, MAX(last_modified)
                FROM fort_sightings
                GROUP BY fort_id
            )
        '''
    query = session.execute('''
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
        {where}
    '''.format(where=where))
    return query.fetchall()


def get_session_stats(session):
    query = '''
        SELECT
            MIN(expire_timestamp) ts_min,
            MAX(expire_timestamp) ts_max,
            COUNT(*)
        FROM sightings
        {report_since}
    '''
    min_max_query = session.execute(query.format(
        report_since=get_since_query_part()
    ))
    min_max_result = min_max_query.first()
    length_hours = (min_max_result[1] - min_max_result[0]) // 3600
    if length_hours == 0:
        length_hours = 1
    # Convert to datetime
    return {
        'start': datetime.fromtimestamp(min_max_result[0]),
        'end': datetime.fromtimestamp(min_max_result[1]),
        'count': min_max_result[2],
        'length_hours': length_hours,
        'per_hour': round(min_max_result[2] / length_hours),
    }


def get_despawn_time(session, spawn_id):
    spawn = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == spawn_id) \
        .filter(Spawnpoint.updated > config.LAST_MIGRATION) \
        .first()
    if spawn:
        return spawn.despawn_time
    else:
        return None


def get_first_last(session, spawn_id):
    query = session.execute('''
        SELECT min(first_seconds) as min, max(last_seconds) as max
        FROM mystery_sightings
        WHERE spawn_id = {i}
        AND first_seen > {m}
    '''.format(i=spawn_id, m=config.LAST_MIGRATION))
    result = query.first()
    if result:
        return result
    else:
        return None, None


def get_widest_range(session, spawn_id):
    query = session.execute('''
        SELECT max(seen_range)
        FROM mystery_sightings
        WHERE spawn_id = {i}
        AND first_seen > {m}
    '''.format(i=spawn_id, m=config.LAST_MIGRATION))
    largest = None
    try:
        largest = query.first()[0]
    except TypeError:
        pass
    return largest


def estimate_remaining_time(session, spawn_id, seen=None):
    first, last = get_first_last(session, spawn_id)

    if not first:
        return 90, 1800

    if seen:
        if seen > last:
            last = seen
        elif seen < first:
            first = seen

    if last - first > 1710:
        possible = (first + 90, last + 90, first + 1800, last + 1800)
        estimates = []
        for possibility in possible:
            estimates.append(utils.time_until_time(possibility, seen))
        soonest = min(estimates)
        latest = max(estimates)
        return soonest, latest

    soonest = last + 90
    latest = first + 1800
    soonest = utils.time_until_time(soonest, seen)
    latest = utils.time_until_time(latest, seen)

    return soonest, latest

def get_punch_card(session):
    if get_engine_name(session) in ('sqlite', 'postgresql'):
        bigint = 'BIGINT'
    else:
        bigint = 'UNSIGNED'
    query = session.execute('''
        SELECT
            CAST((expire_timestamp / 300) AS {bigint}) ts_date,
            COUNT(*) how_many
        FROM sightings
        {report_since}
        GROUP BY ts_date
        ORDER BY ts_date
    '''.format(bigint=bigint, report_since=get_since_query_part()))
    results = query.fetchall()
    results_dict = {r[0]: r[1] for r in results}
    filled = []
    for row_no, i in enumerate(range(int(results[0][0]), int(results[-1][0]))):
        item = results_dict.get(i)
        filled.append((row_no, item if item else 0))
    return filled


def get_top_pokemon(session, count=30, order='DESC'):
    query = session.execute('''
        SELECT
            pokemon_id,
            COUNT(*) how_many
        FROM sightings
        {report_since}
        GROUP BY pokemon_id
        ORDER BY how_many {order}
        LIMIT {count}
    '''.format(order=order, count=count, report_since=get_since_query_part()))
    return query.fetchall()


def get_pokemon_ranking(session, order='ASC'):
    ranking = []
    query = session.execute('''
        SELECT
            pokemon_id,
            COUNT(*) how_many
        FROM sightings
        {report_since}
        GROUP BY pokemon_id
        ORDER BY how_many {order}
    '''.format(report_since=get_since_query_part(), order=order))
    db_ids = [r[0] for r in query.fetchall()]
    for pokemon_id in range(1, 152):
        if pokemon_id not in db_ids:
            ranking.append(pokemon_id)
    ranking.extend(db_ids)
    return ranking


def get_sightings_per_pokemon(session):
    query = session.execute('''
        SELECT
            pokemon_id,
            COUNT(*) how_many
        FROM sightings
        GROUP BY pokemon_id
    ''')
    sightings = {}
    for item in query.fetchall():
        sightings[item[0]] = item[1]
    for pokemon_id in range(1, 152):
        if pokemon_id not in sightings:
            sightings[pokemon_id] = 0
    return sightings


def get_rare_pokemon(session):
    result = []

    for pokemon_id in config.RARE_IDS:
        query = session.query(Sighting) \
            .filter(Sighting.pokemon_id == pokemon_id)
        if config.REPORT_SINCE:
            query = query.filter(Sighting.expire_timestamp > get_since())
        count = query.count()
        if count > 0:
            result.append((pokemon_id, count))
    return result


def get_nonexistent_pokemon(session):
    result = []
    query = session.execute('''
        SELECT DISTINCT pokemon_id FROM sightings
        {report_since}
    '''.format(report_since=get_since_query_part()))
    db_ids = [r[0] for r in query.fetchall()]
    for pokemon_id in range(1, 152):
        if pokemon_id not in db_ids:
            result.append(pokemon_id)
    return result


def get_all_sightings(session, pokemon_ids):
    # TODO: rename this and get_sightings
    query = session.query(Sighting) \
        .filter(Sighting.pokemon_id.in_(pokemon_ids))
    if config.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    return query.all()


def get_spawns_per_hour(session, pokemon_id):
    if get_engine_name(session) == 'sqlite':
        ts_hour = 'STRFTIME("%H", expire_timestamp)'
    elif get_engine_name(session) == 'postgresql':
        ts_hour = "TO_CHAR(TO_TIMESTAMP(expire_timestamp), 'HH24')"
    else:
        ts_hour = 'HOUR(FROM_UNIXTIME(expire_timestamp))'
    query = session.execute('''
        SELECT
            {ts_hour} AS ts_hour,
            COUNT(*) AS how_many
        FROM sightings
        WHERE pokemon_id = {pokemon_id}
        {report_since}
        GROUP BY ts_hour
        ORDER BY ts_hour
    '''.format(
        pokemon_id=pokemon_id,
        ts_hour=ts_hour,
        report_since=get_since_query_part(where=False)
    ))
    results = []
    for result in query.fetchall():
        results.append((
            {
                'v': [int(result[0]), 30, 0],
                'f': '{}:00 - {}:00'.format(
                    int(result[0]), int(result[0]) + 1
                ),
            },
            result[1]
        ))
    return results


def get_total_spawns_count(session, pokemon_id):
    query = session.execute('''
        SELECT COUNT(id)
        FROM sightings
        WHERE pokemon_id = {pokemon_id}
        {report_since}
    '''.format(
        pokemon_id=pokemon_id,
        report_since=get_since_query_part(where=False)
    ))
    result = query.first()
    return result[0]


def get_all_spawn_coords(session, pokemon_id=None):
    points = session.query(Sighting.lat, Sighting.lon)
    if pokemon_id:
        points = points.filter(Sighting.pokemon_id == int(pokemon_id))
    if config.REPORT_SINCE:
        points = points.filter(Sighting.expire_timestamp > get_since())
    return points.all()
