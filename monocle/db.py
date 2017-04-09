from datetime import datetime
from collections import OrderedDict
from contextlib import contextmanager
from enum import Enum

import time

from sqlalchemy import Column, Integer, String, Float, SmallInteger, BigInteger, ForeignKey, UniqueConstraint, create_engine, cast, func, desc, asc, and_, exists
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.types import TypeDecorator, Numeric, Text
from sqlalchemy.dialects.mysql import TINYINT, MEDIUMINT, BIGINT, DOUBLE
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.ext.declarative import declarative_base

from . import utils, bounds, spawns, db_proc, sanitized as conf
from .shared import call_at

try:
    assert conf.LAST_MIGRATION < time.time()
except AssertionError:
    raise ValueError('LAST_MIGRATION must be a timestamp from the past.')
except TypeError as e:
    raise TypeError('LAST_MIGRATION must be a numeric timestamp.') from e


class Team(Enum):
    none = 0
    mystic = 1
    valor = 2
    instict = 3


if conf.DB_ENGINE.startswith('mysql'):
    TINY_TYPE = TINYINT(unsigned=True)          # 0 to 255
    MEDIUM_TYPE = MEDIUMINT(unsigned=True)      # 0 to 4294967295
    HUGE_TYPE = BIGINT(unsigned=True)           # 0 to 18446744073709551615
    FLOAT_TYPE = DOUBLE(precision=17, scale=14, asdecimal=False)
elif conf.DB_ENGINE.startswith('postgres'):
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
    FLOAT_TYPE = DOUBLE_PRECISION(asdecimal=False)
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
    FLOAT_TYPE = Float(asdecimal=False)

if conf.SPAWN_ID_INT:
    ID_TYPE = BigInteger
else:
    ID_TYPE = String(11)


def get_engine():
    return create_engine(conf.DB_ENGINE)


def get_engine_name(session):
    return session.connection().engine.name


def combine_key(sighting):
    return sighting['encounter_id'], sighting['spawn_id']

Base = declarative_base()


class SightingCache:
    """Simple cache for storing actual sightings

    It's used in order not to make as many queries to the database.
    It schedules sightings to be removed as soon as they expire.
    """
    def __init__(self):
        self.store = {}

    def add(self, sighting):
        self.store[sighting['spawn_id']] = sighting['expire_timestamp']
        call_at(sighting['expire_timestamp'], self.remove, sighting['spawn_id'])

    def remove(self, spawn_id):
        try:
            del self.store[spawn_id]
        except KeyError:
            pass

    def __contains__(self, raw_sighting):
        try:
            expire_timestamp = self.store[raw_sighting['spawn_id']]
        except KeyError:
            return False
        return (
            expire_timestamp > raw_sighting['expire_timestamp'] - 2 and
            expire_timestamp < raw_sighting['expire_timestamp'] + 2)


class MysteryCache:
    """Simple cache for storing Pokemon with unknown expiration times

    It's used in order not to make as many queries to the database.
    It schedules sightings to be removed an hour after being seen.
    """
    def __init__(self):
        self.store = {}

    def add(self, sighting):
        key = combine_key(sighting)
        self.store[combine_key(sighting)] = [sighting['seen']] * 2
        call_at(sighting['seen'] + 3510, self.remove, key)

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

    def remove(self, key):
        first, last = self.store[key]
        del self.store[key]
        if last != first:
            encounter_id, spawn_id = key
            db_proc.DB_PROC.add({
                'type': 'mystery-update',
                'spawn': spawn_id,
                'encounter': encounter_id,
                'first': first,
                'last': last
            })

    def items(self):
        return self.store.items()


class FortCache:
    """Simple cache for storing fort sightings"""
    def __init__(self):
        self.store = utils.load_pickle('forts') or {}

    def add(self, sighting):
        if sighting['type'] == 'pokestop':
            self.store[sighting['external_id']] = True
        else:
            self.store[sighting['external_id']] = sighting['last_modified']

    def __contains__(self, sighting):
        existing = self.store.get(sighting['external_id'])
        if not existing:
            return False
        if existing is True:
            return True
        return existing == sighting['last_modified']

    def pickle(self):
        utils.dump_pickle('forts', self.store)


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
    lat = Column(FLOAT_TYPE)
    lon = Column(FLOAT_TYPE)
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
    lat = Column(FLOAT_TYPE)
    lon = Column(FLOAT_TYPE)
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
    lat = Column(FLOAT_TYPE)
    lon = Column(FLOAT_TYPE)
    alt = Column(SmallInteger)
    updated = Column(Integer, index=True)
    duration = Column(TINY_TYPE)


class Fort(Base):
    __tablename__ = 'forts'

    id = Column(Integer, primary_key=True)
    external_id = Column(String(35), unique=True)
    lat = Column(FLOAT_TYPE)
    lon = Column(FLOAT_TYPE)

    sightings = relationship(
        'FortSighting',
        backref='fort',
        order_by='FortSighting.last_modified'
    )


class FortSighting(Base):
    __tablename__ = 'fort_sightings'

    id = Column(Integer, primary_key=True)
    fort_id = Column(Integer, ForeignKey('forts.id'))
    last_modified = Column(Integer, index=True)
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
    lat = Column(FLOAT_TYPE, index=True)
    lon = Column(FLOAT_TYPE, index=True)


Session = sessionmaker(bind=get_engine())


@contextmanager
def session_scope(autoflush=False):
    """Provide a transactional scope around a series of operations."""
    session = Session(autoflush=autoflush)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def get_since():
    """Returns 'since' timestamp that should be used for filtering"""
    return time.mktime(conf.REPORT_SINCE.timetuple())


def get_since_query_part(where=True):
    """Returns WHERE part of query filtering records before set date"""
    if conf.REPORT_SINCE:
        return '{noun} expire_timestamp > {since}'.format(
            noun='WHERE' if where else 'AND',
            since=get_since(),
        )
    return ''


def add_sighting(session, pokemon):
    # Check if there isn't the same entry already
    if pokemon in SIGHTING_CACHE:
        return
    existing = session.query(exists().where(and_(
            Sighting.expire_timestamp == pokemon['expire_timestamp'],
            Sighting.encounter_id == pokemon['encounter_id']))
        ).scalar()
    if existing:
        SIGHTING_CACHE.add(pokemon)
        return
    obj = Sighting(
        pokemon_id=pokemon['pokemon_id'],
        spawn_id=pokemon['spawn_id'],
        encounter_id=pokemon['encounter_id'],
        expire_timestamp=pokemon['expire_timestamp'],
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


def add_spawnpoint(session, pokemon):
    # Check if the same entry already exists
    spawn_id = pokemon['spawn_id']
    new_time = pokemon['expire_timestamp'] % 3600
    try:
        if new_time == spawns.despawn_times[spawn_id]:
            return
    except KeyError:
        pass
    existing = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == spawn_id) \
        .first()
    now = round(time.time())
    point = pokemon['lat'], pokemon['lon']
    spawns.add_known(spawn_id, new_time, point)
    if existing:
        existing.updated = now

        if (existing.despawn_time is None or
                existing.updated < conf.LAST_MIGRATION):
            widest = get_widest_range(session, spawn_id)
            if widest and widest > 1710:
                existing.duration = 60
        elif new_time == existing.despawn_time:
            return

        existing.despawn_time = new_time
    else:
        altitude = spawns.get_altitude(point)
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


def add_mystery_spawnpoint(session, pokemon):
    # Check if the same entry already exists
    spawn_id = pokemon['spawn_id']
    point = pokemon['lat'], pokemon['lon']
    if point in spawns.unknown:
        return
    existing = session.query(exists().where(
        Spawnpoint.spawn_id == spawn_id)).scalar()
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

    if point in bounds:
        spawns.add_unknown(point)


def add_mystery(session, pokemon):
    if pokemon in MYSTERY_CACHE:
        return
    add_mystery_spawnpoint(session, pokemon)
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
        existing = session.query(exists().where(and_(
            FortSighting.fort_id == fort.id,
            FortSighting.last_modified == raw_fort['last_modified']
        ))).scalar()
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
    pokestop = session.query(exists().where(
        Pokestop.external_id == raw_pokestop['external_id'])).scalar()
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


def update_mystery(session, mystery):
    encounter = session.query(Mystery) \
                .filter(Mystery.spawn_id == mystery['spawn']) \
                .filter(Mystery.encounter_id == mystery['encounter']) \
                .first()
    if not encounter:
        return
    hour = encounter.first_seen - (encounter.first_seen % 3600)
    encounter.last_seconds = mystery['last'] - hour
    encounter.seen_range = mystery['last'] - mystery['first']


def get_sightings(session, after_id=0):
    q = session.query(Sighting) \
        .filter(Sighting.expire_timestamp > time.time(),
                Sighting.id > after_id)
    if conf.MAP_FILTER_IDS:
        q = q.filter(~Sighting.pokemon_id.in_(conf.MAP_FILTER_IDS))
    return q.all()


def get_spawn_points(session):
    return session.query(Spawnpoint).all()


def get_pokestops(session):
    return session.query(Pokestop).all()


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
    query = session.query(func.min(Sighting.expire_timestamp),
        func.max(Sighting.expire_timestamp))
    if conf.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    min_max_result = query.one()
    length_hours = (min_max_result[1] - min_max_result[0]) // 3600
    if length_hours == 0:
        length_hours = 1
    # Convert to datetime
    return {
        'start': datetime.fromtimestamp(min_max_result[0]),
        'end': datetime.fromtimestamp(min_max_result[1]),
        'length_hours': length_hours
    }


def get_first_last(session, spawn_id):
    result = session.query(func.min(Mystery.first_seconds), func.max(Mystery.last_seconds)) \
        .filter(Mystery.spawn_id == spawn_id) \
        .filter(Mystery.first_seen > conf.LAST_MIGRATION) \
        .first()
    return result


def get_widest_range(session, spawn_id):
    largest = session.query(func.max(Mystery.seen_range)) \
        .filter(Mystery.spawn_id == spawn_id) \
        .filter(Mystery.first_seen > conf.LAST_MIGRATION) \
        .scalar()
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
    query = session.query(cast(Sighting.expire_timestamp / 300, Integer).label('ts_date'), func.count('ts_date')) \
        .group_by('ts_date') \
        .order_by('ts_date')
    if conf.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    results = tuple(query)
    results_dict = {r[0]: r[1] for r in results}
    filled = []
    for row_no, i in enumerate(range(int(results[0][0]), int(results[-1][0]))):
        filled.append((row_no, results_dict.get(i, 0)))
    return filled


def get_top_pokemon(session, count=30, order='DESC'):
    query = session.query(Sighting.pokemon_id, func.count(Sighting.pokemon_id).label('how_many')) \
        .group_by(Sighting.pokemon_id)
    if conf.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    if order == 'DESC':
        query = query.order_by(desc('how_many')).limit(count)
    else:
        query = query.order_by(asc('how_many')).limit(count)
    return query.all()


def get_pokemon_ranking(session):
    ranking = []
    query = session.query(Sighting.pokemon_id, func.count(Sighting.pokemon_id).label('how_many')) \
        .group_by(Sighting.pokemon_id)
    if conf.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    query = query.order_by(asc('how_many'))
    db_ids = [r[0] for r in query]
    for pokemon_id in range(1, 252):
        if pokemon_id not in db_ids:
            ranking.append(pokemon_id)
    ranking.extend(db_ids)
    return ranking


def get_sightings_per_pokemon(session):
    query = session.query(Sighting.pokemon_id, func.count(Sighting.pokemon_id).label('how_many')) \
        .group_by(Sighting.pokemon_id) \
        .order_by('how_many')
    if conf.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    return OrderedDict(query.all())


def sightings_to_csv(since=None, output='sightings.csv'):
    import csv

    if since:
        conf.REPORT_SINCE = since
    with session_scope() as session:
        sightings = get_sightings_per_pokemon(session)
    od = OrderedDict()
    for pokemon_id in range(1, 252):
        if pokemon_id not in sightings:
            od[pokemon_id] = 0
    od.update(sightings)
    with open(output, 'wt') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('pokemon_id', 'count'))
        for item in od.items():
            writer.writerow(item)


def get_rare_pokemon(session):
    result = []

    for pokemon_id in conf.RARE_IDS:
        query = session.query(Sighting) \
            .filter(Sighting.pokemon_id == pokemon_id)
        if conf.REPORT_SINCE:
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
    for pokemon_id in range(1, 252):
        if pokemon_id not in db_ids:
            result.append(pokemon_id)
    return result


def get_all_sightings(session, pokemon_ids):
    # TODO: rename this and get_sightings
    query = session.query(Sighting) \
        .filter(Sighting.pokemon_id.in_(pokemon_ids))
    if conf.REPORT_SINCE:
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
    query = session.query(Sighting) \
        .filter(Sighting.pokemon_id == pokemon_id)
    if conf.REPORT_SINCE:
        query = query.filter(Sighting.expire_timestamp > get_since())
    return query.count()


def get_all_spawn_coords(session, pokemon_id=None):
    points = session.query(Sighting.lat, Sighting.lon)
    if pokemon_id:
        points = points.filter(Sighting.pokemon_id == int(pokemon_id))
    if conf.REPORT_SINCE:
        points = points.filter(Sighting.expire_timestamp > get_since())
    return points.all()
