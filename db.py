from datetime import datetime
import enum
import time

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, SmallInteger, BigInteger, Text, ForeignKey, UniqueConstraint, Numeric
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

import utils

try:
    import config
    DB_ENGINE = config.DB_ENGINE
except (ImportError, AttributeError):
    DB_ENGINE = 'sqlite:///db.sqlite'

OPTIONAL_SETTINGS = {
    'SPAWN_ID_INT': False,
    'STAGE2': [],
    'REPORT_SINCE': None,
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

class Team(enum.Enum):
    none = 0
    mystic = 1
    valor = 2
    instict = 3


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

    def add(self, sighting):
        dictionary = {
            'expire_timestamp': sighting['expire_timestamp'],
            'last_modified_timestamp_ms': sighting['last_modified_timestamp_ms'],
            'time_till_hidden_ms': sighting['time_till_hidden_ms']
        }
        self.store[combine_key(sighting)] = dictionary

    def __contains__(self, raw_sighting):
        expire_timestamp = self.store.get(combine_key(raw_sighting)).get('expire_timestamp')
        if not expire_timestamp:
            return False
        within_range = (
            expire_timestamp > raw_sighting['expire_timestamp'] - 1 and
            expire_timestamp < raw_sighting['expire_timestamp'] + 1
        )
        return within_range

    def clean_expired(self):
        to_remove = []
        for key, dictionary in self.store.items():
            timestamp = dictionary.get('expire_timestamp')
            if timestamp < time.time() - 2700:
                to_remove.append(key)
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
        self.store[combine_key(sighting)] = (sighting['expire_timestamp'], round(sighting['last_modified_timestamp_ms'] / 1000))

    def __contains__(self, raw_sighting):
        timestamps = self.store.get(combine_key(raw_sighting))
        if not timestamps:
            return False
        sighting_time = timestamps[1]
        raw_sighting_time = round(raw_sighting['last_modified_timestamp_ms'] / 1000)
        timestamp_in_range = (
            sighting_time > raw_sighting_time - 5 and
            sighting_time < raw_sighting_time + 5
        )
        return timestamp_in_range

    def clean_expired(self):
        to_remove = []
        for key, timestamps in self.store.items():
            if timestamps[0] < time.time() - 3600:
                to_remove.append(key)
        for key in to_remove:
            del self.store[key]


class FortCache(object):
    """Simple cache for storing fort sightings"""
    def __init__(self):
        self.store = {}

    @staticmethod
    def _make_key(fort_sighting):
        return fort_sighting['external_id']

    def add(self, sighting):
        self.store[self._make_key(sighting)] = (
            sighting['team'],
            sighting['prestige'],
            sighting['guard_pokemon_id'],
        )

    def __contains__(self, sighting):
        params = self.store.get(self._make_key(sighting))
        if not params:
            return False
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
    pokemon_id = Column(SmallInteger, index=True)
    if config.SPAWN_ID_INT:
        spawn_id = Column(BigInteger)
    else:
        spawn_id = Column(String(11))
    expire_timestamp = Column(Integer, index=True)
    if DB_ENGINE.startswith('sqlite'):
        encounter_id = Column(BigInteger)
    else:
        encounter_id = Column(Numeric(precision=20, scale=0))
    normalized_timestamp = Column(Integer)
    lat = Column(Float, index=True)
    lon = Column(Float, index=True)

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
    pokemon_id = Column(SmallInteger, index=True)
    if config.SPAWN_ID_INT:
        spawn_id = Column(BigInteger)
    else:
        spawn_id = Column(String(11))
    expire_timestamp = Column(Integer)
    if DB_ENGINE.startswith('sqlite'):
        encounter_id = Column(BigInteger)
    else:
        encounter_id = Column(Numeric(precision=20, scale=0))
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
    if config.SPAWN_ID_INT:
        spawn_id = Column(BigInteger, unique=True)
    else:
        spawn_id = Column(String(11), unique=True)
    despawn_time = Column(SmallInteger, index=True)
    lat = Column(Float)
    lon = Column(Float)
    alt = Column(SmallInteger)


class Fort(Base):
    __tablename__ = 'forts'

    id = Column(Integer, primary_key=True)
    external_id = Column(Text, unique=True)
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
    team = Column(SmallInteger)
    prestige = Column(Integer)
    guard_pokemon_id = Column(SmallInteger)

    __table_args__ = (
        UniqueConstraint(
            'fort_id',
            'last_modified',
            name='fort_id_last_modified_unique'
        ),
    )


Session = sessionmaker(bind=get_engine())


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
    key = combine_key(pokemon)
    if key in SIGHTING_CACHE.store:
        if pokemon in SIGHTING_CACHE:
            return
        previous_pokemon = pokemon.copy()
        previous_pokemon.update(SIGHTING_CACHE.store.get(key))
        add_longspawn(session, previous_pokemon)
        add_longspawn(session, pokemon)
        return
    if get_engine_name(session) not in ('mysql', 'postgresql'):
        existing = session.query(Sighting) \
            .filter(Sighting.encounter_id == pokemon['encounter_id']) \
            .filter(Sighting.expire_timestamp == pokemon['expire_timestamp']) \
            .first()
        if existing:
            return
    obj = Sighting(
        pokemon_id=pokemon['pokemon_id'],
        spawn_id=pokemon['spawn_id'],
        encounter_id=pokemon['encounter_id'],
        expire_timestamp=pokemon['expire_timestamp'],
        normalized_timestamp=normalize_timestamp(pokemon['expire_timestamp']),
        lat=pokemon['lat'],
        lon=pokemon['lon'],
    )
    session.add(obj)
    SIGHTING_CACHE.add(pokemon)


def add_spawnpoint(session, pokemon):
    # Check if there isn't the same entry already
    spawn_id = pokemon['spawn_id']
    new_time = pokemon['expire_timestamp'] % 3600
    existing = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == pokemon['spawn_id']) \
        .first()
    if existing:
        existing_time = existing.despawn_time
        if abs(new_time - existing_time) < 2:
            return
        existing.despawn_time = new_time
        session.commit()
    else:
        altitude = utils.get_altitude((pokemon['lat'], pokemon['lon']))
        obj = Spawnpoint(
            spawn_id=pokemon['spawn_id'],
            despawn_time=new_time,
            lat=pokemon['lat'],
            lon=pokemon['lon'],
            alt=altitude
        )
        session.add(obj)


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
    LONGSPAWN_CACHE.add(pokemon)


def add_fort_sighting(session, raw_fort):
    if raw_fort in FORT_CACHE:
        return
    # Check if fort exists
    fort = session.query(Fort) \
        .filter(Fort.external_id == raw_fort['external_id']) \
        .filter(Fort.lat == raw_fort['lat']) \
        .filter(Fort.lon == raw_fort['lon']) \
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
            # Why it's not in cache? It should be there!
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
        'per_hour': min_max_result[2] / length_hours,
    }


def get_despawn_time(session, spawn_id):
    spawn = session.query(Spawnpoint) \
        .filter(Spawnpoint.spawn_id == spawn_id) \
        .first()
    if spawn:
        return spawn.despawn_time
    else:
        return None


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
        GROUP BY pokemon_id
        ORDER BY how_many {order}
    '''.format(order=order))
    db_ids = [r[0] for r in query.fetchall()]
    for pokemon_id in range(1, 152):
        if pokemon_id not in db_ids:
            ranking.append(pokemon_id)
    ranking.extend(db_ids)
    return ranking


def get_stage2_pokemon(session):
    result = []
    if not hasattr(config, 'STAGE2'):
        return []
    for pokemon_id in config.STAGE2:
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
