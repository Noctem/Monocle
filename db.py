from datetime import datetime
from collections import OrderedDict
import enum
import time

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, SmallInteger, BigInteger, Text, ForeignKey, UniqueConstraint, Numeric
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.mysql import TINYINT, MEDIUMINT, BIGINT

import utils

try:
    import config
    DB_ENGINE = config.DB_ENGINE
except (ImportError, AttributeError):
    DB_ENGINE = 'sqlite:///db.sqlite'

OPTIONAL_SETTINGS = {
    'SPAWN_ID_INT': True,
    'RARE_IDS': [],
    'REPORT_SINCE': None,
    'BOUNDARIES': None
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

if config.BOUNDARIES:
    from shapely.geometry import Point

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
    TINY_TYPE = SmallInteger                    # -32768 to 32767
    MEDIUM_TYPE = Integer                       # -2147483648 to 2147483647
    HUGE_TYPE = Numeric(precision=20, scale=0)  # up to 20 digits
else:
    TINY_TYPE = SmallInteger
    MEDIUM_TYPE = Integer
    HUGE_TYPE = Text

if config.SPAWN_ID_INT:
    ID_TYPE = BigInteger
else:
    ID_TYPE = String(11)

def get_engine():
    return create_engine(DB_ENGINE)


def get_engine_name(session):
    return session.connection().engine.name


def combine_key(sighting):
    return(sighting['encounter_id'], sighting['spawn_id'])

Base = declarative_base()


class SightingCache(object):
    """Simple cache for storing actual sightings

    It's used in order not to make as many queries to the database.
    It's also capable of purging old entries.
    """
    def __init__(self):
        self.store = {}
        self.spawn_ids = set()

    def add(self, sighting):
        self.store[combine_key(sighting)] = sighting['expire_timestamp']
        self.spawn_ids.add(sighting['spawn_id'])

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
                    self.spawn_ids.remove(key[1])
                except KeyError:
                    pass
        for key in to_remove:
            del self.store[key]


class LongspawnCache(object):
    """Simple cache for storing longspawns

    It's used in order not to make as many queries to the database.
    It's also capable of purging old entries.
    """
    def __init__(self):
        self.store = {}

    def add(self, sighting):
        self.store[combine_key(sighting)] = sighting['last_modified_timestamp_ms'] / 1000

    def __contains__(self, raw_sighting):
        sighting_time = self.store.get(combine_key(raw_sighting))
        if not sighting_time:
            return False
        raw_sighting_time = raw_sighting['last_modified_timestamp_ms'] / 1000
        timestamp_in_range = (
            sighting_time > raw_sighting_time - 60 and
            sighting_time < raw_sighting_time + 60
        )
        return timestamp_in_range

    def in_store(self, raw_sighting):
        key = combine_key(raw_sighting)
        return key in self.store

    def clean_expired(self):
        to_remove = []
        for key, timestamp in self.store.items():
            if timestamp < time.time() - 3600:
                to_remove.append(key)
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
LONGSPAWN_CACHE = LongspawnCache()
FORT_CACHE = FortCache()


class Sighting(Base):
    __tablename__ = 'sightings'

    id = Column(Integer, primary_key=True)
    pokemon_id = Column(TINY_TYPE, index=True)
    spawn_id = Column(ID_TYPE)
    expire_timestamp = Column(Integer, index=True)
    encounter_id = Column(HUGE_TYPE)
    normalized_timestamp = Column(Integer)
    lat = Column(Float, index=True)
    lon = Column(Float, index=True)
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


class Longspawn(Base):
    __tablename__ = 'longspawns'

    id = Column(Integer, primary_key=True)
    pokemon_id = Column(TINY_TYPE, index=True)
    spawn_id = Column(ID_TYPE)
    encounter_id = Column(HUGE_TYPE)
    lat = Column(Float, index=True)
    lon = Column(Float, index=True)
    time_till_hidden_ms = Column(Integer)
    last_modified_timestamp_ms = Column(BigInteger)

    __table_args__ = (
        UniqueConstraint(
            'encounter_id',
            'last_modified_timestamp_ms',
            name='encounter_time_unique'
        ),
    )


class Spawnpoint(Base):
    __tablename__ = 'spawnpoints'

    id = Column(Integer, primary_key=True)
    spawn_id = Column(ID_TYPE, unique=True)
    despawn_time = Column(SmallInteger, index=True)
    lat = Column(Float)
    lon = Column(Float)
    alt = Column(SmallInteger)
    updated = Column(Integer)


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
    fort_id = Column(SmallInteger, ForeignKey('forts.id'))
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
    spawn_points = []
    for spawn in spawns:
        if config.BOUNDARIES:
            point = Point((spawn.lat, spawn.lon))
            if not config.BOUNDARIES.contains(point):
                continue
        spawn_points.append({
            'id': spawn.spawn_id,
            'point': (spawn.lat, spawn.lon, spawn.alt),
            'despawn_time': spawn.despawn_time,
            'spawn_time': (spawn.despawn_time + 1800) % 3600
        })
    spawn_points = sorted(spawn_points, key=lambda k: k['spawn_time'])
    spawns = OrderedDict()
    for spawn in spawn_points:
        spawns[spawn['id']] = (spawn['point'], spawn['spawn_time'], spawn['despawn_time'])
    return spawns


def get_spawn_locations(session):
    spawns = session.query(Spawnpoint)
    spawn_points = []
    for spawn in spawns:
        spawn_points.append({
            'point': (spawn.lat, spawn.lon, spawn.alt),
            'time': (spawn.despawn_time + 1800) % 3600
        })
    spawn_points = sorted(spawn_points, key=lambda k: k['time'])
    return spawn_points


def get_spawn_ids(session):
    query = session.query(Sighting.spawn_id)
    spawn_ids = [i[0] for i in query]
    return spawn_ids


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
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
    else:
        SIGHTING_CACHE.add(pokemon)


def add_spawnpoint(session, pokemon, spawns=None):
    # Check if there isn't the same entry already
    spawn_id = pokemon['spawn_id']
    new_time = pokemon['expire_timestamp'] % 3600
    if spawns:
        existing_time = spawns.get_despawn_seconds(spawn_id)
        if existing_time and abs(new_time - existing_time) < 2:
            return
    existing = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == spawn_id) \
        .first()
    now = round(time.time())
    if existing:
        existing.updated = now
        if abs(new_time - existing.despawn_time) < 2:
            return
        existing.despawn_time = new_time
    else:
        altitude = utils.get_altitude((pokemon['lat'], pokemon['lon']))
        obj = Spawnpoint(
            spawn_id=spawn_id,
            despawn_time=new_time,
            lat=pokemon['lat'],
            lon=pokemon['lon'],
            alt=altitude,
            updated=now
        )
        session.add(obj)
    session.commit()


def add_longspawn(session, pokemon):
    if pokemon in LONGSPAWN_CACHE:
        return
    obj = Longspawn(
        pokemon_id=pokemon['pokemon_id'],
        spawn_id=pokemon['spawn_id'],
        encounter_id=pokemon['encounter_id'],
        expire_timestamp=pokemon['expire_timestamp'],
        lat=pokemon['lat'],
        lon=pokemon['lon'],
        time_till_hidden_ms=pokemon['time_till_hidden_ms'],
        last_modified_timestamp_ms=pokemon['last_modified_timestamp_ms'],
    )
    session.add(obj)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
    else:
        LONGSPAWN_CACHE.add(pokemon)


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
    try:
        session.commit()
    except IntegrityError:  # skip adding fort this time
        session.rollback()
    else:
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
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
    else:
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
        .first()
    if spawn:
        return spawn.despawn_time
    else:
        return None


def estimate_remaining_time(session, spawn_id):
    query = session.execute('''
        SELECT min((last_modified_timestamp_ms / 1000) % 3600) as min, max((last_modified_timestamp_ms / 1000) % 3600) as max
        FROM longspawns
        WHERE spawn_id = {spawn_id} AND last_modified_timestamp_ms > 1477958400000
    '''.format(spawn_id=spawn_id))

    result = query.first()
    first_sight, last_sight = result

    if not first_sight or not last_sight:
        return 90, 1800

    val_range = last_sight - first_sight
    if val_range > 1710:
        return 90, 3600

    if (last_sight + 89) > (first_sight + 1801):
        return 90, 1800

    earliest_estimate = (last_sight + 89) % 3600
    latest_estimate = (first_sight + 1801) % 3600

    soonest = utils.time_until_time(earliest_estimate)
    latest = utils.time_until_time(latest_estimate)

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
