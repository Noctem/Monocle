#!/usr/bin/env python3

import monocle.sanitized as conf

import asyncio
try:
    if conf.UVLOOP:
        from uvloop import EventLoopPolicy
        asyncio.set_event_loop_policy(EventLoopPolicy())
except ImportError:
    pass

from multiprocessing.managers import BaseManager, DictProxy
from queue import Queue, Full
from argparse import ArgumentParser
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from logging import getLogger, basicConfig, WARNING, INFO
from logging.handlers import RotatingFileHandler
from os.path import exists, join
from sys import platform
from concurrent.futures import TimeoutError

import time

from sqlalchemy.exc import DBAPIError
from aiopogo import close_sessions, activate_hash_server

from monocle.shared import LOOP, get_logger, SessionManager, ACCOUNTS
from monocle.utils import get_address, dump_pickle
from monocle.worker import Worker
from monocle.overseer import Overseer
from monocle.db_proc import DB_PROC
from monocle.db import FORT_CACHE
from monocle import spawns


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
    if filename:
        handlers = (RotatingFileHandler(filename, maxBytes=500000, backupCount=4),)
    else:
        handlers = None
    basicConfig(
        format='[{asctime}][{levelname:>8s}][{name}] {message}',
        datefmt='%Y-%m-%d %X',
        style='{',
        level=INFO,
        handlers=handlers
    )


def exception_handler(loop, context):
    try:
        log = getLogger('eventloop')
        log.error('A wild exception appeared!')
        log.error(context)
    except Exception:
        print('Exception in exception handler.')


def cleanup(overseer, manager):
    try:
        overseer.running = False
        print('Exiting, please wait until all tasks finish')

        log = get_logger('cleanup')
        print('Finishing tasks...')

        LOOP.create_task(overseer.exit_progress())
        pending = asyncio.Task.all_tasks(loop=LOOP)
        gathered = asyncio.gather(*pending, return_exceptions=True)
        try:
            LOOP.run_until_complete(asyncio.wait_for(gathered, 40))
        except TimeoutError as e:
            print('Coroutine completion timed out, moving on.')
        except Exception as e:
            log = get_logger('cleanup')
            log.exception('A wild {} appeared during exit!', e.__class__.__name__)

        overseer.refresh_dict()

        print('Dumping pickles...')
        dump_pickle('accounts', ACCOUNTS)
        FORT_CACHE.pickle()
        if conf.CACHE_CELLS:
            dump_pickle('cells', Worker.cell_ids)

        DB_PROC.stop()
        print("Updating spawns pickle...")
        try:
            spawns.update()
            spawns.pickle()
        except Exception as e:
            log.warning('A wild {} appeared while updating spawns during exit!', e.__class__.__name__)
        while not DB_PROC.queue.empty():
            pending = DB_PROC.queue.qsize()
            # Spaces at the end are important, as they clear previously printed
            # output - \r doesn't clean whole line
            print('{} DB items pending     '.format(pending), end='\r')
            time.sleep(.5)
    finally:
        print('Closing pipes, sessions, and event loop...')
        manager.shutdown()
        SessionManager.close()
        close_sessions()
        LOOP.close()
        print('Done.')


def main():
    args = parse_args()
    log = get_logger()
    if args.status_bar:
        configure_logger(filename=join(conf.DIRECTORY, 'scan.log'))
        log.info('-' * 37)
        log.info('Starting up!')
    else:
        configure_logger(filename=None)
    log.setLevel(args.log_level)

    AccountManager.register('captcha_queue', callable=get_captchas)
    AccountManager.register('extra_queue', callable=get_extras)
    if conf.MAP_WORKERS:
        AccountManager.register('worker_dict', callable=get_workers,
                                proxytype=DictProxy)
    address = get_address()
    manager = AccountManager(address=address, authkey=conf.AUTHKEY)
    try:
        manager.start(mgr_init)
    except (OSError, EOFError) as e:
        if platform == 'win32' or not isinstance(address, str):
            raise OSError('Another instance is running with the same manager address. Stop that process or change your MANAGER_ADDRESS.') from e
        else:
            raise OSError('Another instance is running with the same socket. Stop that process or: rm {}'.format(address)) from e

    LOOP.set_exception_handler(exception_handler)

    overseer = Overseer(manager)
    overseer.start(args.status_bar)
    launcher = LOOP.create_task(overseer.launch(args.bootstrap, args.pickle))
    activate_hash_server(conf.HASH_KEY)
    if platform != 'win32':
        LOOP.add_signal_handler(SIGINT, launcher.cancel)
        LOOP.add_signal_handler(SIGTERM, launcher.cancel)
    try:
        LOOP.run_until_complete(launcher)
    except (KeyboardInterrupt, SystemExit):
        launcher.cancel()
    finally:
        cleanup(overseer, manager)


if __name__ == '__main__':
    main()
