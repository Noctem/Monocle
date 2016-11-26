# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, CancelledError, Future
from collections import deque
from datetime import datetime
from functools import partial
from sqlalchemy.exc import IntegrityError
from geopy.distance import great_circle
from queue import Queue
from multiprocessing.managers import SyncManager
import argparse
import asyncio
import logging
import os
import random
import sys
import threading
import time
import pickle

from pgoapi import (
    exceptions as pgoapi_exceptions,
    PGoApi,
    utilities as pgoapi_utils,
)

import config
import db
import utils


START_TIME = time.time()
GLOBAL_VISITS = 0
GLOBAL_SEEN = 0
NOTIFICATIONS_SENT = 0
LAST_LOGIN = 0


# Check whether config has all necessary attributes
REQUIRED_SETTINGS = (
    'DB_ENGINE',
    'MAP_START',
    'MAP_END',
    'GRID',
    'ACCOUNTS',
    'COMPUTE_THREADS',
    'NETWORK_THREADS'
)
for setting_name in REQUIRED_SETTINGS:
    if not hasattr(config, setting_name):
        raise RuntimeError('Please set "{}" in config'.format(setting_name))

# Set defaults for missing config options
OPTIONAL_SETTINGS = {
    'LONGSPAWNS': False,
    'PROXIES': None,
    'SCAN_RADIUS': 70,
    'SCAN_DELAY': 15,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'CONTROL_SOCKS': None,
    'ENCRYPT_PATH': None,
    'HASH_PATH': None,
    'MAX_CAPTCHAS': 100
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

if config.CONTROL_SOCKS:
    from stem import Signal
    from stem.control import Controller
    import stem.util.log
    stem.util.log.get_logger().level = 40
    CIRCUIT_TIME = dict()
    CIRCUIT_FAILURES = dict()
    CIRCUIT_LATENCIES = dict()
    for proxy in config.PROXIES:
        CIRCUIT_TIME[proxy] = time.time()
        CIRCUIT_FAILURES[proxy] = 0
        CIRCUIT_LATENCIES[proxy] = deque(maxlen=30)
else:
    CIRCUIT_TIME = None
    CIRCUIT_FAILURES = None
    CIRCUIT_LATENCIES = None

BAD_STATUSES = (
    'LOGIN FAIL',
    'EXCEPTION',
    'BAD LOGIN',
    'RETRYING',
    'THROTTLE',
)


class MalformedResponse(Exception):
    """Raised when server response is malformed"""


def configure_logger(filename='worker.log'):
    logging.basicConfig(
        filename=filename,
        format=(
            '[%(asctime)s][%(levelname)8s][%(name)s] '
            '%(message)s'
        ),
        style='%',
        level=logging.INFO,
    )

try:
    with open('cells.pickle', 'rb') as f:
        CELL_IDS = pickle.load(f)
    if config.COMPUTE_THREADS > 2:
        config.NETWORK_THREADS += config.COMPUTE_THREADS - 2
        config.COMPUTE_THREADS = 2
except Exception:
    CELL_IDS = dict()


class Spawns:
    def __init__(self):
        self.spawns = None
        self.session = db.Session()

    def update_spawns(self):
        self.spawns = db.get_spawns(self.session)

    def have_id(self, spawn_id):
        return spawn_id in self.spawns

    def get_despawn_seconds(self, spawn_id):
        if self.have_id(spawn_id):
            return self.spawns[spawn_id][2]
        else:
            return None

    def get_despawn_time(self, spawn_id):
        if self.have_id(spawn_id):
            current_hour = utils.get_current_hour()
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
        return utils.time_until_time(despawn_seconds)

SPAWNS = Spawns()
DOWNLOAD_HASH = "5296b4d9541938be20b1d1a8e8e3988b7ae2e93b"

if config.NOTIFY_IDS or config.NOTIFY_RANKING:
    import notification
    notifier = notification.Notifier(SPAWNS)

class Slave:
    """Single worker walking on the map"""

    def __init__(
            self,
            worker_no,
            db_processor,
            cell_ids_executor,
            network_executor,
            extra_queue,
            captcha_queue,
            worker_dict,
            device_info=None,
            proxy=None
    ):
        self.worker_no = worker_no
        self.visits = 0
        self.account = config.ACCOUNTS[self.worker_no]
        # asyncio/thread references
        self.future = None  # worker's own future
        self.db_processor = db_processor
        self.cell_ids_executor = cell_ids_executor
        self.network_executor = network_executor
        # Some handy counters
        self.total_seen = 0
        # State variables
        self.busy = False
        self.killed = False  # killed worker will stay killed
        self.logged_in = False
        self.ever_authenticated = False
        # Other variables
        self.last_visit = 0
        self.last_api_latency = 0
        self.error_code = 'INIT'
        self.empty_visits = 0
        self.location = utils.get_start_coords(self.worker_no, altitude=True)
        # And now, configure logger and PGoApi
        self.logger = logging.getLogger('worker-{}'.format(worker_no))
        self.device_info = utils.get_device_info(self.account)
        self.after_spawn = None
        self.speed = 0
        self.api = PGoApi(device_info=self.device_info)
        if config.ENCRYPT_PATH:
            self.api.set_signature_lib(config.ENCRYPT_PATH)
        if config.HASH_PATH:
            self.api.set_hash_lib(config.HASH_PATH)
        self.api.set_position(*self.location)
        self.api.set_logger(self.logger)
        self.extra_queue = extra_queue
        self.captcha_queue = captcha_queue
        self.worker_dict = worker_dict
        if proxy:
            self.set_proxy(proxy)
        else:
            self.proxy = None

    def call_api(self, method, *args, **kwargs):
        """Returns decorated function that measures execution time

        This works exactly like functools.partial does.
        """
        def inner():
            start = time.time()
            result = method(*args, **kwargs)
            self.last_api_latency = time.time() - start
            if CIRCUIT_LATENCIES:
                self.add_circuit_latency()
            return result
        return inner

    def add_circuit_latency(self):
        CIRCUIT_LATENCIES[self.proxy].append(self.last_api_latency)
        samples = len(CIRCUIT_LATENCIES[self.proxy])
        if samples > 10:
            average = sum(CIRCUIT_LATENCIES[self.proxy]) / samples
            if average > 10:
                self.swap_circuit('average latency of ' + str(round(average)) + 's')

    def set_proxy(self, proxy):
        self.proxy = proxy
        self.api.set_proxy({'http': proxy, 'https': proxy})


    async def new_account(self):
        while self.extra_queue.empty():
            await asyncio.sleep(60)
        self.account = self.extra_queue.get()
        self.logged_in = False
        self.ever_authenticated = False
        self.empty_visits = 0
        self.device_info = utils.get_device_info(self.account)
        self.api = PGoApi(device_info=self.device_info)
        self.api.set_position(*self.location)
        self.api.set_logger(self.logger)

    async def bench_account(self):
        self.error_code = 'BENCHING'
        self.logger.warning('Swapping ' + self.account[0] + ' due to CAPTCHA.')
        self.captcha_queue.put(self.account)
        await self.new_account()

    async def swap_account(self, reason=''):
        self.error_code = 'SWAPPING'
        self.logger.warning('Swapping out ' + self.account[0] + ' because ' +
                            reason + '.')
        while self.extra_queue.empty():
            await asyncio.sleep(60)
        self.extra_queue.put(self.account)
        await self.new_account()

    async def remove_account(self):
        self.error_code = 'REMOVING'
        self.logger.warning('Removing ' + self.account[0] + ' due to ban.')
        await self.new_account()

    def swap_proxy(self, reason=''):
        self.set_proxy(random.choice(config.PROXIES))
        self.logger.warning('Swapped out ' + self.proxy +
                            ' due to ' + reason)

    def swap_circuit(self, reason=''):
        if not config.CONTROL_SOCKS:
            if config.PROXIES:
                swap_proxy(self, reason=reason)
            return
        time_passed = time.time() - CIRCUIT_TIME[self.proxy]
        if time_passed > 180:
            socket = config.CONTROL_SOCKS[self.proxy]
            with Controller.from_socket_file(path=socket) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
            CIRCUIT_TIME[self.proxy] = time.time()
            CIRCUIT_FAILURES[self.proxy] = 0
            CIRCUIT_LATENCIES[self.proxy] = deque(maxlen=30)
            self.logger.warning('Changed circuit on ' + self.proxy +
                                ' due to ' + reason)
        else:
            self.logger.info('Skipped changing circuit on ' + self.proxy +
                             ' because it was changed ' + str(time_passed)
                             + ' seconds ago.')

    def get_auth_expiration(self, response):
        auth_expiration = response.get('auth_ticket', {}).get('expire_timestamp_ms')
        if auth_expiration:
            self.auth_expiration = (auth_expiration / 1000) - 5

    def get_inventory_timestamp(self, response):
        timestamp = response.get('responses', {}).get('GET_INVENTORY', {}).get('inventory_delta', {}).get('new_timestamp_ms')
        if timestamp:
            self.inventory_timestamp = timestamp
        elif not self.timestamp:
            self.inventory_timestamp = (time.time() - 2) * 1000

    async def app_simulation_login(self):
        self.error_code = 'APP SIMULATION'
        self.logger.info('Starting RPC login sequence (iOS app simulation)')

        # Send empty initial request
        request = self.api.create_request()
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        self.get_auth_expiration(response)
        await asyncio.sleep(1.172)

        request = self.api.create_request()
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        await asyncio.sleep(1.304)


        # Send GET_PLAYER only
        request = self.api.create_request()
        request.get_player(player_locale = {'country': 'US', 'language': 'en', 'timezone': 'America/Denver'})
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )

        if response.get('responses', {}).get('GET_PLAYER', {}).get('banned', False):
            raise pgoapi_exceptions.BannedAccount

        await asyncio.sleep(1.356)

        request = self.api.create_request()
        request.download_remote_config_version(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory()
        request.check_awarded_badges()
        request.download_settings()
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        responses = response.get('responses', {})
        if await self.check_captcha(responses):
            return False
        await asyncio.sleep(1.072)

        self.get_inventory_timestamp(response)
        responses = response.get('responses', {})
        download_hash = responses.get('DOWNLOAD_SETTINGS', {}).get('hash')
        if download_hash:
            global DOWNLOAD_HASH
            DOWNLOAD_HASH = download_hash
        inventory = responses.get('GET_INVENTORY', {}).get('inventory_delta', {})
        player_level = None
        for item in inventory.get('inventory_items', []):
            player_stats = item.get('inventory_item_data', {}).get('player_stats', {})
            if player_stats:
                player_level = player_stats.get('level')
                break

        request = self.api.create_request()
        request.get_asset_digest(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        await asyncio.sleep(1.709)

        self.get_inventory_timestamp(response)
        request = self.api.create_request()
        request.get_player_profile()
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        request.get_buddy_walked()
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        await asyncio.sleep(1.326)

        self.get_inventory_timestamp(response)
        request = self.api.create_request()
        request.level_up_rewards(level=player_level)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        request.get_buddy_walked()
        response = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        await asyncio.sleep(1.184)

        self.logger.info('Finished RPC login sequence (iOS app simulation)')

        return response

    async def login(self, initial=True):
        """Logs worker in and prepares for scanning"""
        loop = asyncio.get_event_loop()
        self.logger.info('Trying to log in')
        global LAST_LOGIN
        time_required = random.uniform(4, 7)
        while (time.time() - LAST_LOGIN) < time_required:
            self.error_code = 'WAITING'
            await asyncio.sleep(2)
        self.error_code = 'LOGIN'
        LAST_LOGIN = time.time()
        try:
            await loop.run_in_executor(
                self.network_executor,
                self.call_api(
                    self.api.set_authentication,
                    username=self.account[0],
                    password=self.account[1],
                    provider=self.account[2],
                )
            )
            if not self.ever_authenticated:
                if not await self.app_simulation_login():
                    return False
        except pgoapi_exceptions.ServerSideAccessForbiddenException:
            self.logger.error('Banned IP: ' + self.proxy)
            self.error_code = 'IP BANNED'
            self.swap_circuit(reason='ban')
            await self.random_sleep(sleep_min=15, sleep_max=20)
        except pgoapi_exceptions.AuthException:
            self.logger.warning('Login failed: ' + self.account[0])
            self.error_code = 'FAILED LOGIN'
            await self.swap_account(reason='login failed')
            await self.random_sleep()
        except pgoapi_exceptions.NotLoggedInException:
            self.logger.error('Invalid credentials: ' + self.account[0])
            self.error_code = 'BAD LOGIN'
            await self.swap_account(reason='bad login')
            await self.random_sleep()
        except pgoapi_exceptions.ServerBusyOrOfflineException:
            self.logger.info('Server too busy - restarting')
            self.error_code = 'RETRYING'
            await self.random_sleep()
        except pgoapi_exceptions.ServerSideRequestThrottlingException:
            self.logger.info('Server throttling - sleeping for a bit')
            self.error_code = 'THROTTLE'
            await self.random_sleep(sleep_min=10)
        except pgoapi_exceptions.BannedAccount:
            self.error_code = 'BANNED?'
            await self.remove_account()
        except CancelledError:
            self.kill()
        except Exception as err:
            self.logger.exception('A wild exception appeared! ' + str(err))
            self.error_code = 'EXCEPTION'
            await self.random_sleep()
        else:
            self.ever_authenticated = True
            self.logged_in = True
            self.error_code = '@'
            return True
        return False


    async def visit(self, point, i):
        """Wrapper for self.visit_point - runs it a few times before giving up

        Also is capable of restarting in case an error occurs.
        """
        visited = False
        for attempts in range(0,5):
            try:
                if not self.logged_in or time.time() > self.auth_expiration:
                    if not await self.login():
                        await asyncio.sleep(2)
                        continue
                visited = await self.visit_point(point, i)
            except pgoapi_exceptions.ServerSideAccessForbiddenException:
                self.logger.error('Banned IP.')
                self.error_code = 'IP BANNED'
                self.swap_circuit(reason='ban')
                await self.random_sleep(sleep_min=15, sleep_max=20)
            except pgoapi_exceptions.NotLoggedInException:
                self.logger.error('Invalid credentials: ' + self.account[0])
                self.error_code = 'NOT AUTHENTICATED'
                await self.swap_account(reason='not logged in')
                self.logged_in = False
                await self.random_sleep()
            except pgoapi_exceptions.ServerBusyOrOfflineException:
                self.logger.info('Server too busy - restarting')
                self.error_code = 'RETRYING'
                await self.random_sleep()
            except pgoapi_exceptions.ServerSideRequestThrottlingException:
                self.logger.info('Server throttling - sleeping for a bit')
                self.error_code = 'THROTTLE'
                await self.random_sleep(sleep_min=10)
            except MalformedResponse:
                self.logger.warning('Malformed response received!')
                self.error_code = 'RESTART'
                await self.random_sleep()
            except pgoapi_exceptions.BannedAccount:
                self.error_code = 'BANNED?'
                await self.remove_account()
            except Exception as err:
                self.logger.exception('A wild exception appeared!')
                self.error_code = 'EXCEPTION'
                await self.random_sleep()
            else:
                if visited:
                    return True
                else:
                    await self.random_sleep()
        return False

    async def visit_point(self, point, i):
        loop = asyncio.get_event_loop()

        latitude = point[0]
        longitude = point[1]
        try:
            altitude = random.uniform(point[2] - 1, point[2] + 1)
        except KeyError:
            altitude = utils.random_altitude()
        rounded_coords = utils.round_coords(point, precision=5)
        self.error_code = '!'
        self.logger.info(
            'Visiting point %d (%s,%s %sm)', i, rounded_coords[0],
            rounded_coords[1], round(altitude)
        )
        start = time.time()
        self.api.set_position(latitude, longitude, altitude)
        self.location = point
        if rounded_coords not in CELL_IDS or len(CELL_IDS[rounded_coords]) > 25:
            CELL_IDS[rounded_coords] = await loop.run_in_executor(
                self.cell_ids_executor,
                partial(
                    pgoapi_utils.get_cell_ids, latitude, longitude
                )
            )
        cell_ids = CELL_IDS[rounded_coords]
        since_timestamp_ms = [0] * len(cell_ids)
        if self.last_visit:
            last_timestamp = round(self.last_visit * 1000)
        else:
            last_timestamp = round((time.time() - 16) * 1000)
        request = self.api.create_request()
        request.get_map_objects(cell_id=cell_ids,
                                since_timestamp_ms=since_timestamp_ms,
                                latitude=pgoapi_utils.f2i(latitude),
                                longitude=pgoapi_utils.f2i(longitude))
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        request.get_buddy_walked()

        response_dict = await loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        self.last_visit = time.time()
        if not isinstance(response_dict, dict):
            self.logger.warning('Response: %s', response_dict)
            raise MalformedResponse
        if response_dict['status_code'] == 3:
            logger.warning('Account banned')
            raise pgoapi_exceptions.BannedAccount
        responses = response_dict.get('responses')
        if not responses:
            self.logger.warning('Response: %s', response_dict)
            raise MalformedResponse
        if await self.check_captcha(responses):
            return False
        self.get_auth_expiration(response_dict)
        self.get_inventory_timestamp(response_dict)
        map_objects = responses.get('GET_MAP_OBJECTS', {})
        pokemons = []
        ls_seen = []
        forts = []
        pokemon_seen = 0
        sent_notification = False
        global SPAWNS
        if map_objects.get('status') != 1:
            self.error_code = 'UNKNOWNRESPONSE'
            self.logger.warning('Response code : ' + str(map_objects.get('status')))
            self.empty_visits += 1
            if self.empty_visits > 2:
                reason = str(self.empty_visits) + ' empty visits'
                await self.swap_account(reason)
            return False
        for map_cell in map_objects['map_cells']:
            request_time_ms = map_cell['current_timestamp_ms']
            for pokemon in map_cell.get('wild_pokemons', []):
                pokemon_seen += 1
                # Store spawns outside of the 15 minute range in a
                # different table until they fall under 15 minutes,
                # and notify about them differently.
                invalid_tth = (
                    pokemon['time_till_hidden_ms'] < 0 or
                    pokemon['time_till_hidden_ms'] > 90000
                )
                normalized = self.normalize_pokemon(
                    pokemon,
                    request_time_ms
                )
                if invalid_tth:
                    despawn_time = SPAWNS.get_despawn_time(normalized['spawn_id'])
                    if despawn_time:
                        normalized['expire_timestamp'] = despawn_time
                        normalized['time_till_hidden_ms'] = (despawn_time * 1000) - request_time_ms
                        normalized['valid'] = 'fixed'
                    else:
                        normalized['valid'] = False
                else:
                    normalized['valid'] = True

                if normalized['valid']:
                    pokemons.append(normalized)

                if normalized['pokemon_id'] in config.NOTIFY_IDS:
                    self.error_code = '*'
                    notified, explanation = notifier.notify(normalized)
                    if notified:
                        sent_notification = True
                        self.logger.info(explanation)
                        global NOTIFICATIONS_SENT
                        NOTIFICATIONS_SENT += 1
                    else:
                        self.logger.warning(explanation)
                key = db.combine_key(normalized)
                if not normalized['valid'] or key in db.LONGSPAWN_CACHE.store:
                    normalized = normalized.copy()
                    normalized['type'] = 'longspawn'
                    ls_seen.append(normalized)
            for fort in map_cell.get('forts', []):
                if not fort.get('enabled'):
                    continue
                if fort.get('type') == 1:  # probably pokestops
                    continue
                forts.append(self.normalize_fort(fort))

        if pokemons:
            self.db_processor.add(pokemons)
        if forts:
            self.db_processor.add(forts)
        if ls_seen:
            self.db_processor.add(ls_seen)

        if pokemon_seen > 0:
            self.error_code = ':'
            self.total_seen += pokemon_seen
            global GLOBAL_SEEN
            GLOBAL_SEEN += pokemon_seen
            self.empty_visits = 0
            if CIRCUIT_FAILURES:
                CIRCUIT_FAILURES[self.proxy] = 0
        else:
            self.error_code = ','
            self.empty_visits += 1
            if self.empty_visits > 2:
                reason = str(self.empty_visits) + ' empty visits'
                await self.swap_account(reason)
            if CIRCUIT_FAILURES:
                CIRCUIT_FAILURES[self.proxy] += 1
                if CIRCUIT_FAILURES[self.proxy] > 20:
                    reason = str(CIRCUIT_FAILURES[self.proxy]) + ' empty visits'
                    self.swap_circuit(reason)

        global GLOBAL_VISITS
        GLOBAL_VISITS += 1
        self.visits += 1
        self.worker_dict.update([(self.worker_no, ((latitude, longitude), start, self.speed, self.total_seen, self.visits, pokemon_seen, sent_notification))])
        self.logger.info(
            'Point processed, %d Pokemons and %d forts seen!',
            pokemon_seen,
            len(forts),
        )
        return True

    def travel_speed(self, point, spawn_time):
        if self.busy:
            return None
        if self.last_visit == 0:
            return 1
        now = time.time()
        if spawn_time < now:
            spawn_time = now
        time_diff = spawn_time - self.last_visit
        if time_diff < config.SCAN_DELAY:
            return None
        elif time_diff > 60:
            self.error_code = None
        distance = great_circle(self.location, point).miles
        speed = (distance / time_diff) * 3600
        return speed


    async def check_captcha(self, responses):
        challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
        if challenge_url != ' ':
            await self.bench_account()
            return True
        return False


    @staticmethod
    def normalize_pokemon(raw, now):
        """Normalizes data coming from API into something acceptable by db"""
        normalized = {
            'type': 'pokemon',
            'encounter_id': raw['encounter_id'],
            'pokemon_id': raw['pokemon_data']['pokemon_id'],
            'expire_timestamp': round(
                (now + raw['time_till_hidden_ms']) / 1000),
            'lat': raw['latitude'],
            'lon': raw['longitude'],
            'spawn_id': utils.get_spawn_id(raw),
            'time_till_hidden_ms': raw['time_till_hidden_ms'],
            'last_modified_timestamp_ms': raw['last_modified_timestamp_ms']
        }
        return normalized

    @staticmethod
    def normalize_fort(raw):
        return {
            'type': 'fort',
            'external_id': raw['id'],
            'lat': raw['latitude'],
            'lon': raw['longitude'],
            'team': raw.get('owned_by_team', 0),
            'prestige': raw.get('gym_points', 0),
            'guard_pokemon_id': raw.get('guard_pokemon_id', 0),
            'last_modified': round(raw['last_modified_timestamp_ms'] / 1000),
        }


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

    async def random_sleep(self, sleep_min=8, sleep_max=12):
        """Sleeps for a bit, then restarts"""
        await asyncio.sleep(random.uniform(sleep_min, sleep_max))

    def kill(self):
        """Marks worker as killed

        Killed worker won't be restarted.
        """
        self.error_code = 'KILLED'
        self.killed = True


class Overseer:
    def __init__(self, status_bar, loop):
        self.logger = logging.getLogger('overseer')
        self.workers = {}
        self.count = config.GRID[0] * config.GRID[1]
        self.logger.info('Done')
        self.start_date = datetime.now()
        self.status_bar = status_bar
        self.things_count = []
        self.killed = False
        self.last_proxy = 0
        self.loop = loop
        self.db_processor = DatabaseProcessor()
        if config.COMPUTE_THREADS:
            self.cell_ids_executor = ThreadPoolExecutor(config.COMPUTE_THREADS)
        else:
            self.cell_ids_executor = None
        self.network_executor = ThreadPoolExecutor(config.NETWORK_THREADS)
        self.launch_queue_manager()
        self.logger.info('Overseer initialized')

    def kill(self):
        self.killed = True
        self.db_processor.stop()
        for worker in self.workers.values():
            worker.kill()
            if worker.future:
                worker.future.cancel()

    def launch_queue_manager(self):
        captcha = Queue()
        extra = Queue()
        workers = {}
        class QueueManager(SyncManager): pass
        QueueManager.register('captcha_queue', callable=lambda:captcha)
        QueueManager.register('extra_queue', callable=lambda:extra)
        QueueManager.register('worker_dict', callable=lambda:workers)
        manager = QueueManager(address='queue.sock', authkey=b'monkeys')
        manager.start()
        self.captcha_queue = manager.captcha_queue()
        self.extra_queue = manager.extra_queue()
        self.worker_dict = manager.worker_dict()
        for account in config.EXTRA_ACCOUNTS:
            self.extra_queue.put(account)

    def start_worker(self, worker_no, first_run=False):
        if self.killed:
            return

        if isinstance(config.PROXIES, (tuple, list)):
            if self.last_proxy >= len(config.PROXIES) - 1:
                self.last_proxy = 0
            else:
                self.last_proxy += 1
            proxy = config.PROXIES[self.last_proxy]
        elif isinstance(config.PROXIES, str):
            proxy = config.PROXIES
        else:
            proxy = None

        worker = Slave(
            worker_no=worker_no,
            db_processor=self.db_processor,
            cell_ids_executor=self.cell_ids_executor,
            network_executor=self.network_executor,
            extra_queue=self.extra_queue,
            captcha_queue=self.captcha_queue,
            worker_dict=self.worker_dict,
            proxy=proxy
        )
        self.workers[worker_no] = worker

    def start(self):
        for worker_no in range(self.count):
            self.start_worker(worker_no, first_run=True)
        self.db_processor.start()

    def check(self):
        last_cleaned_cache = time.time()
        last_workers_checked = time.time()
        last_things_found_updated = time.time()
        workers_check = [
            (worker, worker.total_seen)
            for worker in self.workers.values()
        ]
        while not self.killed:
            now = time.time()
            # Clean cache
            if now - last_cleaned_cache > 900:  # clean cache after 15min
                self.db_processor.clean_cache()
                last_cleaned_cache = now
            # Check up on workers
            if now - last_workers_checked > (5 * 60):
                last_workers_checked = now
            # Record things found count
            if now - last_things_found_updated > 9:
                self.things_count = self.things_count[-9:]
                self.things_count.append(str(self.db_processor.count))
                last_things_found_updated = now
            if self.status_bar:
                if sys.platform == 'win32':
                    _ = os.system('cls')
                else:
                    _ = os.system('clear')
                print(self.get_status_message())
            time.sleep(1)
        # OK, now we're killed
        while True:
            try:
                tasks = sum(not t.done() for t in asyncio.Task.all_tasks(loop))
            except RuntimeError:
                # Set changed size during iteration
                tasks = '?'
            # Spaces at the end are important, as they clear previously printed
            # output - \r doesn't clean whole line
            print(
                '{} coroutines active   '.format(tasks),
                end='\r'
            )
            if tasks == 0:
                break
            time.sleep(0.5)
        print()


    @staticmethod
    def generate_stats(somelist):
        return {
            'max': max(somelist),
            'min': min(somelist),
            'avg': sum(somelist) / len(somelist)
        }

    def get_api_stats(self):
        api_calls = [w.last_api_latency for w in self.workers.values()]
        return self.generate_stats(api_calls)

    def get_visit_stats(self):
        visits = []
        seconds_since_start = time.time() - START_TIME
        seconds_per_visit = []
        seen_per_worker = []
        after_spawns = []
        speeds = []

        for w in self.workers.values():
            if w.after_spawn:
                after_spawns.append(w.after_spawn)
            seen_per_worker.append(w.total_seen)
            visits.append(w.visits)
            speeds.append(w.speed)
        if after_spawns:
            delay_stats = self.generate_stats(after_spawns)
        else:
            delay_stats = {'min': 0, 'max': 0, 'avg': 0}
        seen_stats = self.generate_stats(seen_per_worker)
        visit_stats = self.generate_stats(visits)
        speed_stats = self.generate_stats(speeds)
        return seen_stats, visit_stats, delay_stats, speed_stats

    def get_dots_and_messages(self):
        """Returns status dots and status messages for workers

        Status dots will be either . or : if everything is OK, or a letter
        if something weird happened (but not dangerous).
        If anything dangerous happened, worker will be displayed as X and
        more detailed message should be displayed below.
        """
        dots = []
        messages = []
        row = []
        for i, worker in enumerate(self.workers.values()):
            if i > 0 and i % config.GRID[1] == 0:
                dots.append(row)
                row = []
            if worker.error_code in BAD_STATUSES:
                row.append('X')
                messages.append(worker.status.ljust(20))
            elif worker.error_code:
                row.append(worker.error_code[0])
            else:
                row.append('.')
        if row:
            dots.append(row)
        return dots, messages


    def get_status_message(self):
        workers_count = len(self.workers)

        api_stats = self.get_api_stats()
        running_for = datetime.now() - self.start_date
        seen_stats, visit_stats, delay_stats, speed_stats = self.get_visit_stats()
        global GLOBAL_SEEN
        global GLOBAL_VISITS
        try:
            coroutines_count = len(asyncio.Task.all_tasks(self.loop))
        except RuntimeError:
            # Set changed size during iteration
            coroutines_count = '?'
        output = [
            'PokeMiner\trunning for {}'.format(running_for),
            '{len} workers'.format(len=workers_count),
            '',
            '{} threads and {} coroutines active'.format(
                threading.active_count(),
                coroutines_count,
            ),
            'API latency: min {min:.2f}, max {max:.2f}, avg {avg:.2f}'.format(
                **api_stats
            ),
            '',
            'Seen per worker: min {min}, max {max}, avg {avg:.1f}'.format(
                **seen_stats
            ),
            'Visits per worker: min {min}, max {max:}, avg {avg:.1f}'.format(
                **visit_stats
            ),
            'Visit delay: min {min:.2f}, max {max:.2f}, avg {avg:.2f}'.format(
                **delay_stats
            ),
            'Speed: min {min:.1f}, max {max:.1f}, avg {avg:.1f}'.format(
                **speed_stats
            ),
            'Extra Accounts: {}, CAPTCHAs needed: {}'.format(
                self.extra_queue.qsize(), self.captcha_queue.qsize()
            ),
            '',
            'Pokemon found count (10s interval):',
            ' '.join(self.things_count),
            '',
            'Notifications sent: ' + str(NOTIFICATIONS_SENT),
        ]
        try:
            output.append('Pokemon seen per visit: ' + str(round(GLOBAL_SEEN / GLOBAL_VISITS, 2)))
        except ZeroDivisionError:
            pass
        seconds_since_start = time.time() - START_TIME
        visits_per_second = GLOBAL_VISITS / seconds_since_start
        output.append('Visits per second: ' + str(round(visits_per_second, 2)))
        output.append('')
        no_sightings = ', '.join(str(w.worker_no)
                                 for w in self.workers.values()
                                 if w.total_seen == 0)
        if no_sightings:
            output += ['Workers without sightings so far:', no_sightings, '']
        dots, messages = self.get_dots_and_messages()
        output += [' '.join(row) for row in dots]
        previous = 0
        for i in range(4, len(messages) + 4, 4):
            output.append('\t'.join(messages[previous:i]))
            previous = i
        return '\n'.join(output)


class DatabaseProcessor(threading.Thread):
    def __init__(self):
        super().__init__()
        self.queue = deque()
        self.logger = logging.getLogger('dbprocessor')
        self.running = True
        self._clean_cache = False
        self.count = 0

    def stop(self):
        self.running = False

    def add(self, obj_list):
        self.queue.extend(obj_list)

    def run(self):
        session = db.Session()
        global SPAWNS
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
                            db.add_spawnpoint(session, item, SPAWNS)
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--no-status-bar',
        dest='status_bar',
        help='Log to console instead of displaying status bar',
        action='store_false',
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=logging.WARNING
    )
    return parser.parse_args()


def exception_handler(loop, context):
    logger = logging.getLogger('eventloop')
    logger.exception('A wild exception appeared!')
    logger.error(context)


class Launcher():
    def __init__(self, overseer, loop):
        self.loop = loop
        self.overseer = overseer
        self.workers = list(self.overseer.workers.values())
        count = len(self.workers)
        self.coroutine_limit = int(count / 2.2) + 1
        self.skipped = 0
        self.visited = 0

    async def best_worker(self, point, spawn_time, give_up=False):
        worker = None
        lowest_speed = float('inf')
        while worker is None or lowest_speed > config.SPEED_LIMIT:
            speed = None
            lowest_speed = float('inf')
            worker = None
            for w in self.workers:
                speed = w.travel_speed(point, spawn_time)
                if speed is not None and speed < lowest_speed:
                    lowest_speed = speed
                    worker = w
                    if speed < 7:
                        break
            if worker is None:
                time_diff = spawn_time - time.time()
                if time_diff < -60:
                    return False, False
                await asyncio.sleep(5)
            elif lowest_speed > config.SPEED_LIMIT:
                await asyncio.sleep(2)
        return worker, lowest_speed

    def launch(self):
        global SPAWNS
        while True:
            if os.path.isfile('cells.pickle'):
                with open('cells.pickle', 'wb') as f:
                    pickle.dump(CELL_IDS, f, pickle.HIGHEST_PROTOCOL)
            SPAWNS.update_spawns()
            current_hour = utils.get_current_hour()
            random.shuffle(self.workers)
            for spawn_id, spawn in SPAWNS.spawns.items():
                while len(self.overseer.captcha_queue > config.MAX_CAPTCHAS):
                    time.sleep(30)
                try:
                    coroutines_count = len(asyncio.Task.all_tasks(self.loop))
                    while coroutines_count > self.coroutine_limit or not isinstance(coroutines_count, int):
                        time.sleep(1)
                        coroutines_count = len(asyncio.Task.all_tasks(self.loop))
                except Exception:
                    pass
                spawn_time = spawn[1] + current_hour
                # negative = already happened
                # positive = hasn't happened yet
                time_diff = spawn_time - time.time()
                if self.visited == 0 and (time_diff < -10 or time_diff > 10):
                    continue
                elif time_diff < -300:
                    self.skipped += 1
                    continue
                elif time_diff > 90:
                    time.sleep(30)
                point = list(spawn[0])
                asyncio.run_coroutine_threadsafe(self.try_point(point, spawn_time, spawn_id), self.loop)

    async def try_point(self, point, spawn_time, spawn_id):
        point[0] = random.uniform(point[0] - 0.0004, point[0] + 0.0004)
        point[1] = random.uniform(point[1] - 0.0004, point[1] + 0.0004)
        time_diff = spawn_time - time.time()
        if time_diff > -2:
            await asyncio.sleep(time_diff + 2)

        worker, speed = await self.best_worker(point, spawn_time)
        if not worker:
            self.skipped += 1
            return False
        worker.busy = True
        worker.after_spawn = time.time() - spawn_time
        worker.speed = speed
        if await worker.visit(point, spawn_id):
            self.visited += 1
        worker.busy = False


if __name__ == '__main__':
    args = parse_args()
    logger = logging.getLogger()
    if args.status_bar:
        configure_logger(filename='worker.log')
        logger.info('-' * 30)
        logger.info('Starting up!')
    else:
        configure_logger(filename=None)
    logger.setLevel(args.log_level)
    loop = asyncio.get_event_loop()
    overseer = Overseer(status_bar=args.status_bar, loop=loop)
    loop.set_default_executor(ThreadPoolExecutor())
    loop.set_exception_handler(exception_handler)
    overseer.start()
    overseer_thread = threading.Thread(target=overseer.check)
    overseer_thread.start()
    launcher = Launcher(overseer, loop=loop)
    launcher_thread = threading.Thread(target=launcher.launch)
    launcher_thread.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Exiting, please wait until all tasks finish')
        overseer.kill()  # also cancels all workers' futures
        time.sleep(1)
        all_futures = [
            w.future for w in overseer.workers.values()
            if w.future and not isinstance(w.future, Future)
        ]
        time.sleep(1)
        loop.run_until_complete(asyncio.gather(*all_futures))
        time.sleep(1)
        loop.stop()
        time.sleep(1)
        loop.close()
