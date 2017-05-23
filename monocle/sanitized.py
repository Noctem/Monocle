import sys

from numbers import Number
from pathlib import Path
from datetime import datetime
from logging import getLogger

try:
    from . import config
except ImportError as e:
    raise ImportError('Please copy config.example.py to config.py and customize it.') from e

sequence = (tuple, list)
path = (str, Path)
set_sequence = (tuple, list, set, frozenset)
set_sequence_range = (tuple, list, range, set, frozenset)

worker_count = config.GRID[0] * config.GRID[1]

_valid_types = {
    'ACCOUNTS': set_sequence,
    'ACCOUNTS_CSV': path,
    'ALT_LEVEL': int,
    'ALT_RANGE': sequence,
    'ALWAYS_NOTIFY': int,
    'ALWAYS_NOTIFY_IDS': set_sequence_range,
    'APP_SIMULATION': bool,
    'AREA_NAME': str,
    'AUTHKEY': bytes,
    'BOOTSTRAP_LEVEL': int,
    'BOUNDARIES': tuple,
    'CACHE_CELLS': bool,
    'CAPTCHAS_ALLOWED': int,
    'CAPTCHA_KEY': str,
    'COMPLETE_TUTORIAL': bool,
    'COROUTINES_LIMIT': int,
    'DB_ENGINE': str,
    'DIRECTORY': path,
    'DISCORD_INVITE_ID': str,
    'ENCOUNTER': str,
    'ENCOUNTER_IDS': set_sequence_range,
    'FAILURES_ALLOWED': int,
    'FAVOR_CAPTCHA': bool,
    'FB_PAGE_ID': str,
    'FIXED_OPACITY': bool,
    'FORCED_KILL': bool,
    'FULL_TIME': Number,
    'GIVE_UP_KNOWN': Number,
    'GIVE_UP_UNKNOWN': Number,
    'GOOD_ENOUGH': Number,
    'GOOGLE_MAPS_KEY': str,
    'GRID': sequence,
    'HASHTAGS': set_sequence,
    'HASH_KEY': (str,) + set_sequence,
    'HEATMAP': bool,
    'HOLES': tuple,
    'IGNORE_IVS': bool,
    'IGNORE_RARITY': bool,
    'IMAGE_STATS': bool,
    'INCUBATE_EGGS': bool,
    'INITIAL_SCORE': Number,
    'ITEM_LIMITS': dict,
    'IV_FONT': str,
    'LANDMARKS': sequence,
    'LANGUAGE': str,
    'LAST_MIGRATION': Number,
    'LOAD_CUSTOM_CSS_FILE': bool,
    'LOAD_CUSTOM_HTML_FILE': bool,
    'LOAD_CUSTOM_JS_FILE': bool,
    'LOGIN_TIMEOUT': Number,
    'MANAGER_ADDRESS': (str, tuple, list),
    'MAP_END': sequence,
    'MAP_FILTER_IDS': sequence,
    'MAP_PROVIDER_ATTRIBUTION': str,
    'MAP_PROVIDER_URL': str,
    'MAP_START': sequence,
    'MAP_WORKERS': bool,
    'MAX_CAPTCHAS': int,
    'MAX_RETRIES': int,
    'MINIMUM_RUNTIME': Number,
    'MINIMUM_SCORE': Number,
    'MOVE_FONT': str,
    'MULTI_BOUNDARIES': tuple,
    'NAME_FONT': str,
    'NEVER_NOTIFY_IDS': set_sequence_range,
    'NOTIFY': bool,
    'NOTIFY_IDS': sequence,
    'NOTIFY_RANKING': int,
    'PASS': str,
    'PB_API_KEY': str,
    'PB_CHANNEL': int,
    'PLAYER_LOCALE': dict,
    'PROVIDER': str,
    'PROXIES': set_sequence,
    'QUERY_SUFFIX': str,
    'RARE_IDS': set_sequence_range,
    'RARITY_OVERRIDE': dict,
    'REFRESH_RATE': Number,
    'REPORT_MAPS': bool,
    'REPORT_SINCE': datetime,
    'RESCAN_UNKNOWN': Number,
    'SCAN_DELAY': Number,
    'SEARCH_SLEEP': Number,
    'SHOW_TIMER': bool,
    'SIMULTANEOUS_LOGINS': int,
    'SIMULTANEOUS_SIMULATION': int,
    'SKIP_SPAWN': Number,
    'SMART_THROTTLE': Number,
    'SPAWN_ID_INT': bool,
    'SPEED_LIMIT': Number,
    'SPEED_UNIT': str,
    'SPIN_COOLDOWN': Number,
    'SPIN_POKESTOPS': bool,
    'STAT_REFRESH': Number,
    'STAY_WITHIN_MAP': bool,
    'SWAP_OLDEST': Number,
    'TELEGRAM_BOT_TOKEN': str,
    'TELEGRAM_CHAT_ID': str,
    'TELEGRAM_USERNAME': str,
    'TIME_REQUIRED': Number,
    'TRASH_IDS': set_sequence_range,
    'TWEET_IMAGES': bool,
    'TWITTER_ACCESS_KEY': str,
    'TWITTER_ACCESS_SECRET': str,
    'TWITTER_CONSUMER_KEY': str,
    'TWITTER_CONSUMER_SECRET': str,
    'TWITTER_SCREEN_NAME': str,
    'TZ_OFFSET': Number,
    'UVLOOP': bool,
    'WEBHOOKS': set_sequence
}

_defaults = {
    'ACCOUNTS': None,
    'ACCOUNTS_CSV': None,
    'ALT_LEVEL': 13,
    'ALT_RANGE': (390.0, 490.0),
    'ALWAYS_NOTIFY': 0,
    'ALWAYS_NOTIFY_IDS': frozenset(),
    'APP_SIMULATION': True,
    'AREA_NAME': 'Area',
    'AUTHKEY': b'm3wtw0',
    'BOOTSTRAP_LEVEL': 16,
    'BOUNDARIES': None,
    'CACHE_CELLS': False,
    'CAPTCHAS_ALLOWED': 3,
    'CAPTCHA_KEY': None,
    'COMPLETE_TUTORIAL': False,
    'CONTROL_SOCKS': None,
    'COROUTINES_LIMIT': worker_count,
    'DIRECTORY': '.',
    'DISCORD_INVITE_ID': None,
    'ENCOUNTER': None,
    'ENCOUNTER_IDS': None,
    'FAVOR_CAPTCHA': True,
    'FAILURES_ALLOWED': 2,
    'FB_PAGE_ID': None,
    'FIXED_OPACITY': False,
    'FORCED_KILL': None,
    'FULL_TIME': 1800.0,
    'GIVE_UP_KNOWN': 75.0,
    'GIVE_UP_UNKNOWN': 60.0,
    'GOOD_ENOUGH': None,
    'GOOGLE_MAPS_KEY': '',
    'HASHTAGS': None,
    'HOLES': None,
    'IGNORE_IVS': False,
    'IGNORE_RARITY': False,
    'IMAGE_STATS': False,
    'INCUBATE_EGGS': True,
    'INITIAL_RANKING': None,
    'ITEM_LIMITS': None,
    'IV_FONT': 'monospace',
    'LANDMARKS': None,
    'LANGUAGE': 'EN',
    'LAST_MIGRATION': 1481932800,
    'LOAD_CUSTOM_CSS_FILE': False,
    'LOAD_CUSTOM_HTML_FILE': False,
    'LOAD_CUSTOM_JS_FILE': False,
    'LOGIN_TIMEOUT': 2.5,
    'MANAGER_ADDRESS': None,
    'MAP_FILTER_IDS': None,
    'MAP_PROVIDER_URL': '//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    'MAP_PROVIDER_ATTRIBUTION': '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    'MAP_WORKERS': True,
    'MAX_CAPTCHAS': 0,
    'MAX_RETRIES': 3,
    'MINIMUM_RUNTIME': 10.0,
    'MOVE_FONT': 'sans-serif',
    'MULTI_BOUNDARIES': None,
    'NAME_FONT': 'sans-serif',
    'NEVER_NOTIFY_IDS': frozenset(),
    'NOTIFY': False,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'PASS': None,
    'PB_API_KEY': None,
    'PB_CHANNEL': None,
    'PLAYER_LOCALE': {'country': 'US', 'language': 'en', 'timezone': 'America/Denver'},
    'PROVIDER': None,
    'PROXIES': None,
    'RARE_IDS': frozenset(),
    'RARITY_OVERRIDE': {},
    'QUERY_SUFFIX': None,
    'REFRESH_RATE': 0.6,
    'REPORT_MAPS': True,
    'REPORT_SINCE': None,
    'RESCAN_UNKNOWN': 90.0,
    'SCAN_DELAY': 10,
    'SEARCH_SLEEP': 2.5,
    'SHOW_TIMER': False,
    'SIMULTANEOUS_LOGINS': 2,
    'SIMULTANEOUS_SIMULATION': 4,
    'SKIP_SPAWN': 90.0,
    'SMART_THROTTLE': False,
    'SPAWN_ID_INT': True,
    'SPEED_LIMIT': None,
    'SPEED_UNIT': 'miles',
    'SPIN_COOLDOWN': 300.0,
    'SPIN_POKESTOPS': True,
    'STAT_REFRESH': 5.0,
    'STAY_WITHIN_MAP': True,
    'SWAP_OLDEST': 21600 / worker_count,
    'TELEGRAM_BOT_TOKEN': None,
    'TELEGRAM_CHAT_ID': None,
    'TELEGRAM_USERNAME': None,
    'TIME_REQUIRED': 600.0,
    'TRASH_IDS': frozenset(),
    'TWEET_IMAGES': False,
    'TWITTER_ACCESS_KEY': None,
    'TWITTER_ACCESS_SECRET': None,
    'TWITTER_CONSUMER_KEY': None,
    'TWITTER_CONSUMER_SECRET': None,
    'TWITTER_SCREEN_NAME': None,
    'TZ_OFFSET': None,
    'UVLOOP': True,
    'WEBHOOKS': None
}

_cast = {
    'ALWAYS_NOTIFY_IDS': set,
    'ENCOUNTER_IDS': set,
    'FULL_TIME': float,
    'GIVE_UP_KNOWN': float,
    'GIVE_UP_UNKNOWN': float,
    'GOOD_ENOUGH': float,
    'INITIAL_SCORE': float,
    'LOGIN_TIMEOUT': float,
    'MAP_FILTER_IDS': tuple,
    'MINIMUM_RUNTIME': float,
    'MINIMUM_SCORE': float,
    'NEVER_NOTIFY_IDS': set,
    'RARE_IDS': set,
    'REFRESH_RATE': float,
    'SCAN_DELAY': float,
    'SEARCH_SLEEP': float,
    'SKIP_SPAWN': float,
    'SMART_THROTTLE': float,
    'SPEED_LIMIT': float,
    'SPIN_COOLDOWN': float,
    'STAT_REFRESH': float,
    'SWAP_OLDEST': float,
    'TIME_REQUIRED': float,
    'TRASH_IDS': set
}


class Config:
    __spec__ = __spec__
    __slots__ = tuple(_valid_types.keys()) + ('log',)

    def __init__(self, valid_types=_valid_types, defaults=_defaults, cast=_cast):
        self.log = getLogger('sanitizer')
        for key, value in (x for x in vars(config).items() if x[0].isupper()):
            try:
                if isinstance(value, valid_types[key]):
                    setattr(self, key, value if key not in cast else cast[key](value))
                    if key in defaults:
                        del defaults[key]
                elif key in defaults and value is defaults[key]:
                    setattr(self, key, defaults.pop(key))
                else:
                    valid = valid_types[key]
                    actual = type(value).__name__
                    if isinstance(valid, type):
                        err = '{} must be {}. Yours is: {}.'.format(
                            key, valid.__name__, actual)
                    else:
                        types = ', '.join((x.__name__ for x in valid))
                        err = '{} must be one of {}. Yours is: {}'.format(
                            key, types, actual)
                    raise TypeError(err)
            except KeyError:
                self.log.warning('{} is not a valid config option'.format(key))

    def __getattr__(self, name):
        try:
            default = _defaults.pop(name)
            setattr(self, name, default)
            return default
        except KeyError:
            if name == '__path__':
                return
            err = '{} not in config, and no default has been set.'.format(name)
            self.log.error(err)
            raise AttributeError(err)

sys.modules[__name__] = Config()

del _cast, _valid_types, config
