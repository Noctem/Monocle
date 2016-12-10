from queue import Queue
from multiprocessing.managers import BaseManager
from signal import signal, SIGINT, SIG_IGN
from collections import deque
from logging import getLogger, basicConfig, WARNING, INFO
from argparse import ArgumentParser
from threading import Thread
from sqlalchemy.exc import IntegrityError

import time
import pickle

from utils import dump_pickle, load_pickle, get_current_hour, time_until_time, create_accounts_dict
from config import ACCOUNTS

import db


class MalformedResponse(Exception):
    """Raised when server response is malformed"""


class CaptchaException(Exception):
    """Raised when a CAPTCHA is needed."""


class AccountManager(BaseManager):
    pass


class Spawns:

    def __init__(self):
        self.spawns = None
        self.session = db.Session()

    def update_spawns(self, loadpickle=False):
        if loadpickle:
            self.spawns = load_pickle('spawns')
            if self.spawns:
                return
        self.spawns = db.get_spawns(self.session)
        dump_pickle('spawns', self.spawns)

    def have_id(self, spawn_id):
        return spawn_id in self.spawns

    def get_despawn_seconds(self, spawn_id):
        if self.have_id(spawn_id):
            return self.spawns[spawn_id][2]
        else:
            return None

    def get_despawn_time(self, spawn_id):
        if self.have_id(spawn_id):
            current_hour = get_current_hour()
            despawn_time = self.get_despawn_seconds(spawn_id) + current_hour
            if time.time() > despawn_time + 1:
                despawn_time += 3600
            return despawn_time
        else:
            return None

    def get_time_till_hidden(self, spawn_id):
        if not self.have_id(spawn_id):
            return None
        despawn_seconds = self.spawns[spawn_id][2]
        return time_until_time(despawn_seconds)


class DatabaseProcessor(Thread):

    def __init__(self, spawns):
        super().__init__()
        self.spawns = spawns
        self.queue = deque()
        self.logger = getLogger('dbprocessor')
        self.running = True
        self._clean_cache = False
        self.count = 0

    def stop(self):
        self.running = False

    def add(self, obj_list):
        self.queue.extend(obj_list)

    def run(self):
        session = db.Session()

        while self.running or self.queue:
            if self._clean_cache:
                db.SIGHTING_CACHE.clean_expired()
                db.LONGSPAWN_CACHE.clean_expired()
                self._clean_cache = False
            try:
                item = self.queue.popleft()
            except IndexError:
                self.logger.debug('No items - sleeping')
                time.sleep(0.2)
            else:
                try:
                    if item['type'] == 'pokemon':
                        db.add_sighting(session, item)
                        if item['valid'] == True:
                            db.add_spawnpoint(session, item, self.spawns)
                        session.commit()
                        self.count += 1
                    elif item['type'] == 'longspawn':
                        db.add_longspawn(session, item)
                        self.count += 1
                    elif item['type'] == 'fort':
                        db.add_fort_sighting(session, item)
                        # No need to commit here - db takes care of it
                    self.logger.debug('Item saved to db')
                except IntegrityError:
                    session.rollback()
                    self.logger.info(
                        'Tried and failed to add a duplicate to DB.')
                except Exception:
                    session.rollback()
                    self.logger.exception('A wild exception appeared!')
                    self.logger.warning('Tried and failed to add to DB.')
        session.close()

    def clean_cache(self):
        self._clean_cache = True


_captcha_queue = Queue()
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
        action='store_false',
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=WARNING
    )
    return parser.parse_args()


def configure_logger(filename='worker.log'):
    basicConfig(
        filename=filename,
        format=(
            '[%(asctime)s][%(levelname)8s][%(name)s] '
            '%(message)s'
        ),
        style='%',
        level=INFO,
    )


def exception_handler(loop, context):
    logger = getLogger('eventloop')
    logger.exception('A wild exception appeared!')
    logger.error(context)


def load_accounts():
    try:
        with open('pickles/accounts.pickle', 'rb') as f:
            accounts = pickle.load(f)
        if ACCOUNTS and set(accounts) != set(acc[0] for acc in ACCOUNTS):
            accounts = create_accounts_dict(accounts)
            dump_pickle('accounts', accounts)
    except (FileNotFoundError, EOFError):
        if not ACCOUNTS:
            raise ValueError(
                'Must have accounts in config or an accounts pickle.')
        accounts = create_accounts_dict()
        dump_pickle('accounts', accounts)
    return accounts


def check_captcha(responses):
    challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
    if challenge_url != ' ':
        raise CaptchaException
    else:
        return False


DOWNLOAD_HASH = "5296b4d9541938be20b1d1a8e8e3988b7ae2e93b"

BAD_STATUSES = (
    'FAILED LOGIN',
    'EXCEPTION',
    'NOT AUTHENTICATED'
    'BAD LOGIN',
    'RETRYING',
    'THROTTLE',
    'CAPTCHA',
    'BANNED',
    'BENCHING',
    'REMOVING',
    'IP BANNED',
    'MALFORMED RESPONSE'
)
