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
    'ALT_PRECISION': int,
    'ALT_RANGE': sequence,
    'ALWAYS_NOTIFY': int,
    'ALWAYS_NOTIFY_IDS': set_sequence_range,
    'APP_SIMULATION': bool,
    'AREA_NAME': str,
    'AUTHKEY': bytes,
    'BOOTSTRAP_RADIUS': Number,
    'BOUNDARIES': object,
    'CACHE_CELLS': bool,
    'CAPTCHAS_ALLOWED': int,
    'CAPTCHA_KEY': str,
    'COMPLETE_TUTORIAL': bool,
    'COROUTINES_LIMIT': int,
    'DB': dict,
    'DB_ENGINE': str,
    'DIRECTORY': path,
    'DISCORD_INVITE_ID': str,
    'ENCOUNTER': str,
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
    'IGNORE_IVS': bool,
    'IGNORE_RARITY': bool,
    'INCUBATE_EGGS': bool,
    'INITIAL_SCORE': Number,
    'ITEM_LIMITS': dict,
    'IV_FONT': str,
    'LANDMARKS': object,
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
    'MORE_POINTS': bool,
    'MOVE_FONT': str,
    'NAME_FONT': str,
    'NEVER_NOTIFY_IDS': set_sequence_range,
    'NOTIFY': bool,
    'NOTIFY_IDS': set_sequence_range,
    'NOTIFY_RANKING': int,
    'PASS': str,
    'PB_API_KEY': str,
    'PB_CHANNEL': int,
    'PLAYER_LOCALE': dict,
    'PROVIDER': str,
    'PROXIES': set_sequence,
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
    'ALT_PRECISION': 2,
    'ALT_RANGE': (300, 400),
    'ALWAYS_NOTIFY': 0,
    'ALWAYS_NOTIFY_IDS': set(),
    'APP_SIMULATION': True,
    'AREA_NAME': 'Area',
    'AUTHKEY': b'm3wtw0',
    'BOOTSTRAP_RADIUS': 120,
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
    'FAVOR_CAPTCHA': True,
    'FB_PAGE_ID': None,
    'FIXED_OPACITY': False,
    'FORCED_KILL': None,
    'FULL_TIME': 1800,
    'GIVE_UP_KNOWN': 75,
    'GIVE_UP_UNKNOWN': 60,
    'GOOD_ENOUGH': 0.1,
    'GOOGLE_MAPS_KEY': None,
    'HASHTAGS': None,
    'IGNORE_IVS': False,
    'IGNORE_RARITY': False,
    'INCUBATE_EGGS': False,
    'INITIAL_RANKING': None,
    'ITEM_LIMITS': None,
    'IV_FONT': None,
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
    'MINIMUM_RUNTIME': 10,
    'MORE_POINTS': False,
    'MOVE_FONT': None,
    'NAME_FONT': None,
    'NEVER_NOTIFY_IDS': (),
    'NOTIFY': False,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'PASS': None,
    'PB_API_KEY': None,
    'PB_CHANNEL': None,
    'PLAYER_LOCALE': {'country': 'US', 'language': 'en', 'timezone': 'America/Denver'},
    'PROVIDER': None,
    'PROXIES': None,
    'RARE_IDS': (),
    'RARITY_OVERRIDE': {},
    'REFRESH_RATE': 0.6,
    'REPORT_MAPS': True,
    'REPORT_SINCE': None,
    'RESCAN_UNKNOWN': 90,
    'SCAN_DELAY': 10,
    'SEARCH_SLEEP': 2.5,
    'SHOW_TIMER': False,
    'SIMULTANEOUS_LOGINS': 2,
    'SIMULTANEOUS_SIMULATION': 4,
    'SKIP_SPAWN': 90,
    'SMART_THROTTLE': False,
    'SPAWN_ID_INT': True,
    'SPEED_LIMIT': 19.5,
    'SPEED_UNIT': 'miles',
    'SPIN_COOLDOWN': 300,
    'SPIN_POKESTOPS': True,
    'STAT_REFRESH': 5,
    'STAY_WITHIN_MAP': True,
    'SWAP_OLDEST': 21600 / worker_count,
    'TELEGRAM_BOT_TOKEN': None,
    'TELEGRAM_CHAT_ID': None,
    'TELEGRAM_USERNAME': None,
    'TIME_REQUIRED': 300,
    'TRASH_IDS': (),
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


class Config:
    def __init__(self):
        self.log = getLogger('sanitizer')
        for key, value in (x for x in vars(config).items() if x[0].isupper()):
            try:
                if isinstance(value, _valid_types[key]):
                    setattr(self, key, value)
                    if key in _defaults:
                        del _defaults[key]
                elif key in _defaults and value is _defaults[key]:
                    setattr(self, key, _defaults.pop(key))
                else:
                    valid = _valid_types[key]
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
            err = '{} not in config, and no default has been set.'.format(name)
            self.log.error(err)
            raise AttributeError(err)

sys.modules[__name__] = Config()

del _valid_types, config
