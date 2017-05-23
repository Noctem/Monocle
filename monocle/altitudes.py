from os.path import join

from pogeo.altitude import AltitudeCache

from . import bounds, sanitized as conf
from .shared import get_logger


log = get_logger('altitudes')


ALTITUDES = AltitudeCache(conf.ALT_LEVEL, conf.GOOGLE_MAPS_KEY, conf.ALT_RANGE[0], conf.ALT_RANGE[1])

set_altitude = ALTITUDES.set_alt


def load_alts():
    pickle_path = join(conf.DIRECTORY, 'pickles', 'altcache.pickle')
    try:
        unpickled = ALTITUDES.unpickle(pickle_path, bounds)
    except (FileNotFoundError, EOFError):
        unpickled = False
    except Exception:
        unpickled = False
        log.exception('Error while trying to unpickle altitudes.')

    if not unpickled:
        try:
            ALTITUDES.fetch_all(bounds)
        except Exception:
            log.exception('Error while fetching altitudes.')
        if ALTITUDES:
            log.warning('{} altitudes fetched.', len(ALTITUDES))
            try:
                ALTITUDES.pickle(pickle_path)
            except Exception:
                log.exception('Error while dumping altitude pickle.')
        else:
            log.warning('No altitudes fetched, will use random values within ALT_RANGE.')
            global set_altitude
            set_altitude = ALTITUDES.set_random
