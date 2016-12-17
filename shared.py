from queue import Queue
from multiprocessing.managers import BaseManager
from signal import signal, SIGINT, SIG_IGN
from collections import deque
from logging import getLogger, basicConfig, WARNING, INFO
from argparse import ArgumentParser
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from geopy.distance import great_circle
from pgoapi.auth_ptc import AuthPtc

from pgoapi import (
    exceptions as pgoapi_exceptions,
    PGoApi,
    utilities as pgoapi_utils,
)

import time
import pickle
import asyncio
import random

from utils import dump_pickle, load_pickle, get_current_hour, time_until_time, create_accounts_dict, load_accounts, random_sleep, get_device_info
from config import NETWORK_THREADS, NOTIFY, ENCRYPT_PATH, HASH_PATH, PROXIES, CONTROL_SOCKS, COMPLETE_TUTORIAL, NOTIFY_IDS, ENCOUNTER

if NOTIFY:
    import notification

import db


class MalformedResponse(Exception):
    """Raised when server response is malformed"""


class CaptchaException(Exception):
    """Raised when a CAPTCHA is needed."""


class AccountManager(BaseManager):
    pass


class Spawns:
    """Manage spawn points and times"""
    session = db.Session()
    spawns = None

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
    spawns = Spawns()

    def __init__(self):
        super().__init__()
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
                        self.count += 1
                    elif item['type'] == 'longspawn':
                        db.add_longspawn(session, item)
                        self.count += 1
                    elif item['type'] == 'fort':
                        db.add_fort_sighting(session, item)
                    elif item['type'] == 'pokestop':
                        db.add_pokestop(session, item)
                    self.logger.debug('Item saved to db')
                except Exception:
                    session.rollback()
                    self.logger.exception('A wild exception appeared!')
        session.close()

    def clean_cache(self):
        self._clean_cache = True


class BaseSlave:
    """Common slave attributes for worker and wander"""

    network_executor = ThreadPoolExecutor(NETWORK_THREADS)
    download_hash = "d3da400db60abf79ea05abc38e2396f0bbd453f9"
    g = {'seen': 0, 'captchas': 0}
    db_processor = DatabaseProcessor()
    spawns = db_processor.spawns
    accounts = load_accounts()
    cell_ids = load_pickle('cells') or {}

    if NOTIFY:
        notifier = notification.Notifier(spawns)
        g['sent'] = 0

    def __init__(
            self,
            worker_no,
            proxy=None
    ):
        self.worker_no = worker_no
        self.logger = getLogger('worker-{}'.format(worker_no))
        # account information
        self.account = self.extra_queue.get()
        self.username = self.account.get('username')
        self.location = self.account.get('location', (0, 0, 0))
        self.inventory_timestamp = self.account.get('inventory_timestamp')
        self.last_visit = self.account.get('time', 0)
        self.items = self.account.get('items', {})
        # API setup
        self.proxy = proxy
        self.initialize_api()
        # State variables
        self.busy = False
        self.killed = False
        # Other variables
        self.after_spawn = None
        self.speed = 0
        self.total_seen = 0
        self.error_code = 'INIT'
        self.item_capacity = 350

    def initialize_api(self):
        device_info = get_device_info(self.account)
        self.logged_in = False
        self.ever_authenticated = False
        self.empty_visits = 0

        self.api = PGoApi(device_info=device_info)
        if ENCRYPT_PATH:
            self.api.set_signature_lib(ENCRYPT_PATH)
        if HASH_PATH:
            self.api.set_hash_lib(HASH_PATH)
        self.api.set_position(*self.location)
        self.set_proxy()
        self.api.set_logger(self.logger)
        if self.account.get('provider') == 'ptc' and self.account.get('refresh'):
            self.api._auth_provider = AuthPtc()
            self.api._auth_provider.set_refresh_token(self.account.get('refresh'))
            self.api._auth_provider._access_token = self.account.get('auth')
            self.api._auth_provider._access_token_expiry = self.account.get('expiry')
            if self.api._auth_provider.check_access_token():
                self.api._auth_provider._login = True
                self.logged_in = True
                self.ever_authenticated = True

    async def call_chain(self, request, stamp=True, buddy=True, dl_hash=True):
        request.check_challenge()
        request.get_hatched_eggs()
        if stamp and self.inventory_timestamp:
            request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        else:
            request.get_inventory()
        request.check_awarded_badges()
        if dl_hash:
            request.download_settings(hash=self.download_hash)
        else:
            request.download_settings()
        if buddy:
            request.get_buddy_walked()

        response = await self.loop.run_in_executor(
            self.network_executor, request.call
        )
        self.last_visit = time.time()
        try:
            if response.get('status_code') == 3:
                logger.warning(self.username + ' is banned.')
                raise pgoapi_exceptions.BannedAccountException
            responses = response.get('responses')
            delta = responses.get('GET_INVENTORY', {}).get('inventory_delta', {})
            timestamp = delta.get('new_timestamp_ms')
            inventory_items = delta.get('inventory_items', [])
            if inventory_items:
                self.update_inventory(inventory_items)
            self.inventory_timestamp = timestamp or self.inventory_timestamp
            d_hash = responses.get('DOWNLOAD_SETTINGS', {}).get('hash')
            self.download_hash = d_hash or self.download_hash
            check_captcha(responses)
        except (TypeError, AttributeError):
            raise MalformedResponse
        return responses

    def set_proxy(self, proxy=None):
        if proxy:
            self.proxy = proxy
        if self.proxy:
            self.api.set_proxy({'http': proxy, 'https': proxy})

    async def new_account(self):
        while self.extra_queue.empty():
            if self.killed:
                return False
            await asyncio.sleep(15)
        self.account = self.extra_queue.get()
        self.username = self.account.get('username')
        self.initialize_api()
        self.error_code = None

    def update_accounts_dict(self, captcha=False, banned=False):
        self.account['captcha'] = captcha
        self.account['banned'] = banned
        self.account['location'] = self.location
        self.account['time'] = self.last_visit
        self.account['inventory_timestamp'] = self.inventory_timestamp
        self.account['items'] = self.items

        if self.api._auth_provider:
            self.account['refresh'] = self.api._auth_provider._refresh_token
            if self.api._auth_provider.check_access_token():
                self.account['auth'] = self.api._auth_provider._access_token
                self.account['expiry'] = self.api._auth_provider._access_token_expiry
            else:
                self.account['auth'], self.account['expiry'] = None, None

        self.accounts[self.username] = self.account

    async def bench_account(self):
        self.error_code = 'BENCHING'
        self.logger.warning('Swapping {} due to CAPTCHA.'.format(self.username))
        self.update_accounts_dict(captcha=True)
        self.captcha_queue.put(self.account)
        await self.new_account()

    async def swap_account(self, reason=''):
        self.error_code = 'SWAPPING'
        self.logger.warning('Swapping out {u} because {r}.'.format(
                            u=self.username, r=reason))
        self.update_accounts_dict()
        while self.extra_queue.empty():
            if self.killed:
                return False
            await asyncio.sleep(15)
        self.extra_queue.put(self.account)
        await self.new_account()

    async def remove_account(self):
        self.error_code = 'REMOVING'
        self.logger.warning('Removing {} due to ban.'.format(self.username))
        self.update_accounts_dict(banned=True)
        await self.new_account()

    def simulate_jitter(self, amount=0.00002):
        self.location = [
            random.uniform(self.location[0] - amount,
                           self.location[0] + amount),
            random.uniform(self.location[1] - amount,
                           self.location[1] + amount),
            random.uniform(self.location[2] - 1.5,
                           self.location[2] + 1.5)
        ]
        self.api.set_position(*self.location)

    async def encounter(self, pokemon):
        pokemon_point = pokemon['latitude'], pokemon['longitude']
        distance_to_pokemon = great_circle(self.location, pokemon_point).meters

        if distance_to_pokemon > 47:
            percent = 1 - (46 / distance_to_pokemon)
            lat_change = (self.location[0] - pokemon['latitude']) * percent
            lon_change = (self.location[1] - pokemon['longitude']) * percent
            self.location = [
                self.location[0] - lat_change,
                self.location[1] - lon_change,
                random.uniform(self.location[2] - 3, self.location[2] + 3)
            ]
            self.api.set_position(*self.location)
            delay_required = (distance_to_pokemon * percent) / 8
            if delay_required < 1.5:
                delay_required = random.triangular(1.5, 4, 2.25)
        else:
            self.simulate_jitter()
            delay_required = random.triangular(1.5, 4, 2.25)

        self.error_code = '~'
        await asyncio.sleep(delay_required)
        self.error_code = 'ENCOUNTERING'

        request = self.api.create_request()
        request = request.encounter(encounter_id=pokemon['encounter_id'],
                                    spawn_point_id=pokemon['spawn_point_id'],
                                    player_latitude=self.location[0],
                                    player_longitude=self.location[1])

        responses = await self.call_chain(request)

        response = responses.get('ENCOUNTER', {})
        pokemon_data = response.get('wild_pokemon', {}).get('pokemon_data', {})
        if 'cp' in pokemon_data:
            for iv in ('individual_attack',
                       'individual_defense',
                       'individual_stamina'):
                if iv not in pokemon_data:
                    pokemon_data[iv] = 0
            pokemon_data['probability'] = response.get(
                'capture_probability', {}).get('capture_probability')
        self.error_code = '!'
        return pokemon_data

    async def spin_pokestop(self, pokestop):
        self.error_code = '$'
        pokestop_location = pokestop['lat'], pokestop['lon']
        distance = great_circle(self.location, pokestop_location).meters
        if distance > 40:
            return False

        await random_sleep(.6, 1.2, .75)

        request = self.api.create_request()
        request.fort_details(fort_id = pokestop['external_id'],
                             latitude = pokestop['lat'],
                             longitude = pokestop['lon'])
        responses = await self.call_chain(request)
        name = responses.get('FORT_DETAILS', {}).get('name')

        await random_sleep(.6, 1.2, .75)

        request = self.api.create_request()
        request.fort_search(fort_id = pokestop['external_id'],
                            player_latitude = self.location[0],
                            player_longitude = self.location[1],
                            fort_latitude = pokestop['lat'],
                            fort_longitude = pokestop['lon'])
        responses = await self.call_chain(request)

        result = responses.get('FORT_SEARCH', {}).get('result')
        if result == 1:
            self.logger.info('Spun {n}: {r}'.format(n=name, r=result))
        else:
            self.logger.warning('Failed spinning {n}: {r}'.format(n=name, r=result))
        self.error_code = '!'
        return responses

    def swap_proxy(self, reason=''):
        self.set_proxy(random.choice(PROXIES))
        self.logger.warning('Swapped out {p} due to {r}.'.format(
                            p=self.proxy, r=reason))

    def swap_circuit(self, reason=''):
        if not CONTROL_SOCKS:
            if PROXIES:
                self.swap_proxy(reason=reason)
            return
        time_passed = time.time() - CIRCUIT_TIME[self.proxy]
        if time_passed > 180:
            socket = CONTROL_SOCKS[self.proxy]
            with Controller.from_socket_file(path=socket) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
            CIRCUIT_TIME[self.proxy] = time.time()
            CIRCUIT_FAILURES[self.proxy] = 0
            self.logger.warning('Changed circuit on {p} due to {r}.'.format(
                                p=self.proxy, r=reason))
        else:
            self.logger.info('Skipped changing circuit on {p} because it was '
                             'changed {s} seconds ago.'.format(
                                 p=self.proxy, s=time_passed))

    async def complete_tutorial(self, tutorial_state):
        self.error_code = 'TUTORIAL'
        if 0 not in tutorial_state:
            await random_sleep(1, 5)
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=0)
            await self.call_chain(request, buddy=False)

        if 1 not in tutorial_state:
            await random_sleep(5, 12)
            request = self.api.create_request()
            request.set_avatar(player_avatar={
                    'hair': random.randint(1,5),
                    'shirt': random.randint(1,3),
                    'pants': random.randint(1,2),
                    'shoes': random.randint(1,6),
                    'gender': random.randint(0,1),
                    'eyes': random.randint(1,4),
                    'backpack': random.randint(1,5)
                })
            await self.call_chain(request, buddy=False)

            await random_sleep(.3, .5)

            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=1)
            await self.call_chain(request, buddy=False)

        await random_sleep(.5, .6)
        request = self.api.create_request()
        request.get_player_profile()
        await self.call_chain(request)

        starter_id = None
        if 3 not in tutorial_state:
            await random_sleep(1, 1.5)
            request = self.api.create_request()
            request.get_download_urls(asset_id=['1a3c2816-65fa-4b97-90eb-0b301c064b7a/1477084786906000',
                                                'aa8f7687-a022-4773-b900-3a8c170e9aea/1477084794890000',
                                                'e89109b0-9a54-40fe-8431-12f7826c8194/1477084802881000'])
            await self.call_chain(request)

            await random_sleep(1, 1.6)
            request = self.api.create_request()
            await self.loop.run_in_executor(self.network_executor, request.call)

            await random_sleep(6, 13)
            request = self.api.create_request()
            starter = random.choice((1, 4, 7))
            request.encounter_tutorial_complete(pokemon_id=starter)
            await self.call_chain(request)

            await random_sleep(.5, .6)
            request = self.api.create_request()
            request.get_player(
                player_locale={
                    'country': 'US',
                    'language': 'en',
                    'timezone': 'America/Denver'})
            responses = await self.call_chain(request)

            inventory = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('inventory_items', [])
            for item in inventory:
                pokemon = item.get('inventory_item_data', {}).get('pokemon_data')
                if pokemon:
                    starter_id = pokemon.get('id')


        if 4 not in tutorial_state:
            await random_sleep(5, 12)
            request = self.api.create_request()
            request.claim_codename(codename=self.username)
            await self.call_chain(request)

            await random_sleep(1, 1.3)
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=4)
            await self.call_chain(request, buddy=False)

            await asyncio.sleep(.1)
            request = self.api.create_request()
            request.get_player(
                player_locale={
                    'country': 'US',
                    'language': 'en',
                    'timezone': 'America/Denver'})
            await self.call_chain(request)

        if 7 not in tutorial_state:
            await random_sleep(4, 10)
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=7)
            await self.call_chain(request)

        if starter_id:
            await random_sleep(3, 5)
            request = self.api.create_request()
            request.set_buddy_pokemon(pokemon_id=starter_id)
            await random_sleep(.8, 1.8)

        await asyncio.sleep(.2)
        return True

    def update_inventory(self, inventory_items):
        for thing in inventory_items:
            item = thing.get('inventory_item_data', {}).get('item')
            if not item:
                continue
            item_id = item.get('item_id')
            self.items[item_id] = item.get('count', 0)

    async def app_simulation_login(self):
        self.error_code = 'APP SIMULATION'
        self.logger.info('Starting RPC login sequence (iOS app simulation)')

        # empty request 1
        request = self.api.create_request()
        await self.loop.run_in_executor(self.network_executor, request.call)
        await random_sleep(1, 1.5, 1.172)

        # empty request 2
        request = self.api.create_request()
        await self.loop.run_in_executor(self.network_executor, request.call)
        await random_sleep(1, 1.5, 1.304)

        # request 1: get_player
        request = self.api.create_request()
        request.get_player(
            player_locale={
                'country': 'US',
                'language': 'en',
                'timezone': 'America/Denver'})

        response = await self.loop.run_in_executor(
            self.network_executor, request.call
        )

        get_player = response.get('responses', {}).get('GET_PLAYER', {})
        tutorial_state = get_player.get('player_data', {}).get('tutorial_state', [])
        self.item_capacity = get_player.get('player_data', {}).get('max_item_storage', 350)

        if get_player.get('banned', False):
            raise pgoapi_exceptions.BannedAccountException
            return False

        await random_sleep(1, 1.5, 1.356)

        version = 4901
        # request 2: download_remote_config_version
        request = self.api.create_request()
        request.download_remote_config_version(platform=1, app_version=version)
        responses = await self.call_chain(request, stamp=False, buddy=False, dl_hash=False)

        inventory_items = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('inventory_items', [])
        player_level = None
        for item in inventory_items:
            player_stats = item.get('inventory_item_data', {}).get('player_stats', {})
            if player_stats:
                player_level = player_stats.get('level')
                break

        await random_sleep(1, 1.2, 1.072)

        # request 3: get_asset_digest
        request = self.api.create_request()
        request.get_asset_digest(platform=1, app_version=version)
        await self.call_chain(request, buddy=False)

        await random_sleep(1, 2, 1.709)

        if (COMPLETE_TUTORIAL and
                tutorial_state is not None and
                not all(x in tutorial_state for x in (0, 1, 3, 4, 7))):
            self.logger.warning('Starting tutorial')
            await self.complete_tutorial(tutorial_state)
        else:
            # request 4: get_player_profile
            request = self.api.create_request()
            request.get_player_profile()
            await self.call_chain(request)
            await random_sleep(1, 1.5, 1.326)

        if player_level:
            # request 5: level_up_rewards
            request = self.api.create_request()
            request.level_up_rewards(level=player_level)
            await self.call_chain(request)
            await random_sleep(1, 1.5, 1.184)
        else:
            self.logger.warning('No player level')

        self.logger.info('Finished RPC login sequence (iOS app simulation)')
        self.error_code = None
        return True

    async def login(self):
        """Logs worker in and prepares for scanning"""
        self.logger.info('Trying to log in')
        self.error_code = 'LOGIN'

        async with self.login_semaphore:
            await random_sleep(minimum=0.5, maximum=1.5)
            await self.loop.run_in_executor(
                self.network_executor,
                partial(
                    self.api.set_authentication,
                    username=self.username,
                    password=self.account.get('password'),
                    provider=self.account.get('provider'),
                )
            )
        if self.killed:
            return False
        if not self.ever_authenticated:
            async with self.simulation_semaphore:
                if not await self.app_simulation_login():
                    return False

        self.ever_authenticated = True
        self.logged_in = True
        self.error_code = None
        return True

    async def notify(self, normalized, pokemon):
        if NOTIFY and normalized['pokemon_id'] in self.notifier.notify_ids:
            if ENCOUNTER in ('all', 'notifying'):
                normalized.update(await self.encounter(pokemon))
            self.error_code = '*'
            notified, explanation = self.notifier.notify(normalized)
            if notified:
                self.logger.info(explanation)
                self.g['sent'] += 1
            else:
                self.error_code = '!'
                self.logger.warning(explanation)
            return normalized, notified
        else:
            return normalized, False

    @property
    def status(self):
        """Returns status message to be displayed in status screen"""
        if self.error_code:
            msg = self.error_code
        else:
            msg = 'P{seen}'.format(
                seen=self.total_seen
            )
        return '[W{worker_no}: {msg}]'.format(
            worker_no=self.worker_no,
            msg=msg
        )

    def kill(self):
        """Marks worker as killed

        Killed worker won't be restarted.
        """
        self.error_code = 'KILLED'
        self.killed = True
        if self.ever_authenticated:
            self.update_accounts_dict()


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


def check_captcha(responses):
    challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
    if challenge_url != ' ':
        raise CaptchaException
    else:
        return False


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
