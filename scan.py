#!/usr/bin/env python3

try:
    from monocle import config
except ImportError as e:
    raise ImportError('Please copy config.example.py to config.py and customize it.') from e

import asyncio
try:
    if not hasattr(config, 'UVLOOP') or config.UVLOOP:
        from uvloop import EventLoopPolicy
        asyncio.set_event_loop_policy(EventLoopPolicy())
except ImportError:
    pass

from multiprocessing.managers import BaseManager, DictProxy
from queue import Queue, Full
from argparse import ArgumentParser
from signal import signal, SIGINT, SIG_IGN
from logging import getLogger, basicConfig, WARNING, INFO
from pogo_async import close_sessions
from os.path import exists, join
from threading import Thread
from sys import platform
from sqlalchemy.exc import DBAPIError

import time

# Check whether config has all necessary attributes
_required = (
    'DB_ENGINE',
    'GRID',
    'MAP_START',
    'MAP_END'
)
for setting_name in _required:
    if not hasattr(config, setting_name):
        raise AttributeError('Please set "{}" in config'.format(setting_name))
# Set defaults for missing config options
_optional = {
    'PROXIES': None,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'CONTROL_SOCKS': None,
    'HASH_KEY': None,
    'SMART_THROTTLE': False,
    'MAX_CAPTCHAS': 0,
    'ACCOUNTS': (),
    'ENCOUNTER': None,
    'NOTIFY': False,
    'AUTHKEY': b'm3wtw0',
    'SPIN_POKESTOPS': False,
    'SPIN_COOLDOWN': 300,
    'COMPLETE_TUTORIAL': False,
    'INCUBATE_EGGS': False,
    'MAP_WORKERS': True,
    'APP_SIMULATION': True,
    'ITEM_LIMITS': None,
    'MAX_RETRIES': 3,
    'MORE_POINTS': True,
    'GIVE_UP_KNOWN': 75,
    'GIVE_UP_UNKNOWN': 60,
    'SKIP_SPAWN': 90,
    'LOGIN_TIMEOUT': 2.5,
    'PLAYER_LOCALE': {'country': 'US', 'language': 'en', 'timezone': 'America/Denver'},
    'CAPTCHA_KEY': None,
    'CAPTCHAS_ALLOWED': 3,
    'DIRECTORY': None,
    'FORCED_KILL': None,
    'SWAP_WORST': 600,
    'REFRESH_RATE': 0.6,
    'SPEED_LIMIT': 19.5
}
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)
del (_optional, _required)

# validate PROXIES input and cast to set if needed
if config.PROXIES:
    if isinstance(config.PROXIES, (tuple, list)):
        config.PROXIES = set(config.PROXIES)
    elif isinstance(config.PROXIES, str):
        config.PROXIES = {config.PROXIES}
    elif not isinstance(config.PROXIES, set):
        raise ValueError('PROXIES must be either a list, set, tuple, or str.')

# ensure that user's latitudes and longitudes are different
if (config.MAP_START[0] == config.MAP_END[0]
        or config.MAP_START[1] == config.MAP_END[1]):
    raise ValueError('The latitudes and longitudes of your MAP_START and MAP_END must differ.')

# disable bag cleaning if not spinning PokéStops
if config.ITEM_LIMITS and not config.SPIN_POKESTOPS:
    config.ITEM_LIMITS = None

# ensure that numbers are valid
try:
    if config.SCAN_DELAY < 10:
        raise ValueError('SCAN_DELAY must be at least 10.')
except (TypeError, AttributeError):
    config.SCAN_DELAY = 10
try:
    if config.SIMULTANEOUS_LOGINS < 1:
        raise ValueError('SIMULTANEOUS_LOGINS must be at least 1.')
except (TypeError, AttributeError):
    config.SIMULTANEOUS_LOGINS = 4
try:
    if config.SIMULTANEOUS_SIMULATION < 1:
        raise ValueError('SIMULTANEOUS_SIMULATION must be at least 1.')
except (TypeError, AttributeError):
    config.SIMULTANEOUS_SIMULATION = config.SIMULTANEOUS_LOGINS

if config.ENCOUNTER not in (None, 'notifying', 'all'):
    raise ValueError("Valid ENCOUNTER settings are: None, 'notifying', and 'all'")

if config.DIRECTORY is None:
    if exists(join('..', 'pickles')):
        config.DIRECTORY = '..'
    else:
        config.DIRECTORY = ''

if config.FORCED_KILL is True:
    config.FORCED_KILL = ('0.57.2', '0.55.0', '0.53.0', '0.53.1', '0.53.2')

from monocle.utils import get_address, dump_pickle
from monocle.worker import Worker
from monocle.overseer import Overseer
from monocle import shared

class AccountManager(BaseManager):
    pass


class CustomQueue(Queue):
    def full_wait(self, maxsize=0, timeout=None):
        '''Block until queue size falls below maxsize'''
        starttime = time.monotonic()
        with self.not_full:
            if maxsize > 0:
                if timeout is None:
                    while self._qsize() >= maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time.monotonic() + timeout
                    while self._qsize() >= maxsize:
                        remaining = endtime - time.monotonic()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self.not_empty.notify()
        endtime = time.monotonic()
        return endtime - starttime


_captcha_queue = CustomQueue()
_extra_queue = Queue()
_worker_dict = {}

def get_captchas():
    return _captcha_queue

def get_extras():
    return _extra_queue

def get_workers():
    return _worker_dict

def mgr_init():
    signal(SIGINT, SIG_IGN)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--no-status-bar',
        dest='status_bar',
        help='Log to console instead of displaying status bar',
        action='store_false'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=WARNING
    )
    parser.add_argument(
        '--bootstrap',
        dest='bootstrap',
        help='Bootstrap even if spawns are known.',
        action='store_true'
    )
    parser.add_argument(
        '--no-pickle',
        dest='pickle',
        help='Do not load spawns from pickle',
        action='store_false'
    )
    return parser.parse_args()


def configure_logger(filename='scan.log'):
    basicConfig(
        filename=filename,
        format='[{asctime}][{levelname:>8s}][{name}] {message}',
        datefmt='%Y-%m-%d %X',
        style='{',
        level=INFO
    )


def exception_handler(loop, context):
    try:
        log = getLogger('eventloop')
        log.error('A wild exception appeared!')
        log.error(context)
    except Exception:
        print('Exception in exception handler.')


def main():
    args = parse_args()
    log = shared.get_logger()
    if args.status_bar:
        configure_logger(filename=join(config.DIRECTORY, 'scan.log'))
        log.info('-' * 37)
        log.info('Starting up!')
    else:
        configure_logger(filename=None)
    log.setLevel(args.log_level)

    AccountManager.register('captcha_queue', callable=get_captchas)
    AccountManager.register('extra_queue', callable=get_extras)
    if config.MAP_WORKERS:
        AccountManager.register('worker_dict', callable=get_workers,
                                proxytype=DictProxy)
    address = get_address()
    manager = AccountManager(address=address, authkey=config.AUTHKEY)
    try:
        manager.start(mgr_init)
    except (OSError, EOFError) as e:
        if platform == 'win32' or not isinstance(address, str):
            raise OSError('Another instance is running with the same manager address. Stop that process or change your MANAGER_ADDRESS.') from e
        else:
            raise OSError('Another instance is running with the same socket. Stop that process or: rm {}'.format(address)) from e

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)

    overseer = Overseer(status_bar=args.status_bar, manager=manager)
    overseer.start()
    overseer_thread = Thread(target=overseer.check, name='overseer', daemon=True)
    overseer_thread.start()

    launcher_thread = Thread(target=overseer.launch, name='launcher', daemon=True, args=(args.bootstrap, args.pickle))
    launcher_thread.start()

    try:
        while True:
            try:
                loop.run_forever()
            except Exception:
                log.exception('Caught error on run_forever, restarting loop')
                time.sleep(5)
    except KeyboardInterrupt:
        print('Exiting, please wait until all tasks finish')
        overseer.kill()

        dump_pickle('accounts', Worker.accounts)
        if config.CACHE_CELLS:
            dump_pickle('cells', Worker.cell_ids)

        pending = asyncio.Task.all_tasks(loop=loop)
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception as e:
            log.exception('A wild {} appeared during exit!', e.__class__.__name__)

        shared.DB.stop()

        try:
            shared.spawns.update()
        except Exception:
            pass
    finally:
        manager.shutdown()
        close_sessions()

        try:
            loop.close()
        except RuntimeError:
            pass


if __name__ == '__main__':
    main()
