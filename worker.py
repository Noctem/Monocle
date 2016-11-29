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
CAPTCHAS = 0
NOTIFICATIONS_SENT = 0
LAST_LOGIN = 0
DOWNLOAD_HASH = "5296b4d9541938be20b1d1a8e8e3988b7ae2e93b"

# Check whether config has all necessary attributes
REQUIRED_SETTINGS = (
    'DB_ENGINE',
    'MAP_START',
    'MAP_END',
    'GRID',
    'COMPUTE_THREADS',
    'NETWORK_THREADS'
)
for setting_name in REQUIRED_SETTINGS:
    if not hasattr(config, setting_name):
        raise RuntimeError('Please set "{}" in config'.format(setting_name))

# Set defaults for missing config options
OPTIONAL_SETTINGS = {
    'PROXIES': None,
    'SCAN_DELAY': 11,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'CONTROL_SOCKS': None,
    'ENCRYPT_PATH': None,
    'HASH_PATH': None,
    'MAX_CAPTCHAS': 100,
    'ACCOUNTS': ()
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

class CaptchaException(Exception):
    """Raised when a CAPTCHA is needed."""

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


class Spawns:
    def __init__(self):
        self.spawns = None
        self.session = db.Session()

    def update_spawns(self):
        if DEBUG:
            with open('spawns.pickle', 'rb') as f:
                self.spawns = pickle.load(f)
        else:
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
            loop,
            device_info=None,
            proxy=None
    ):
        self.extra_queue = extra_queue
        self.captcha_queue = captcha_queue
        self.worker_dict = worker_dict
        self.worker_no = worker_no
        self.username = self.extra_queue.get()
        self.account = ACCOUNTS[self.username]
        self.location = self.account.get('location', (0,0,0))
        self.logger = logging.getLogger('worker-{}'.format(worker_no))
        self.set_proxy(proxy)
        self.initialize_api()
        # asyncio/thread references
        self.loop = loop
        self.db_processor = db_processor
        self.cell_ids_executor = cell_ids_executor
        self.network_executor = network_executor
        # Some handy counters
        self.total_seen = 0
        self.visits = 0
        # State variables
        self.busy = False
        self.killed = False
        # Other variables
        self.last_visit = self.account.get('time', 0)
        self.last_api_latency = 0
        self.after_spawn = None
        self.speed = 0
        self.error_code = 'INIT'

    def initialize_api(self):
        device_info = utils.get_device_info(self.account)
        self.logged_in = False
        self.ever_authenticated = False
        self.empty_visits = 0
        if not DEBUG:
            self.api = PGoApi(device_info=device_info)
            if config.ENCRYPT_PATH:
                self.api.set_signature_lib(config.ENCRYPT_PATH)
            if config.HASH_PATH:
                self.api.set_hash_lib(config.HASH_PATH)
            self.api.set_position(*self.location)
            self.api.set_logger(self.logger)

    async def call_chain(self, request):
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        request.get_buddy_walked()

        response = await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        try:
            if response.get('status_code') == 3:
                logger.warning(self.username + ' is banned.')
                raise pgoapi_exceptions.BannedAccountException
            responses = response.get('responses')
            self.get_inventory_timestamp(responses)
            self.check_captcha(responses)
        except AttributeError:
            raise MalformedResponse
        return responses


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
        if self.proxy:
            self.api.set_proxy({'http': proxy, 'https': proxy})

    async def new_account(self):
        while self.extra_queue.empty():
            if self.killed:
                return False
            await asyncio.sleep(20)
        if self.killed:
            return False
        self.username = self.extra_queue.get()
        self.account = ACCOUNTS[self.username]
        self.initialize_api()

    def update_accounts_dict(self, captcha=False, banned=False):
        global ACCOUNTS
        ACCOUNTS[self.username]['captcha'] = captcha
        ACCOUNTS[self.username]['banned'] = banned
        ACCOUNTS[self.username]['location'] = self.location
        ACCOUNTS[self.username]['time'] = self.last_visit

    async def bench_account(self):
        self.error_code = 'BENCHING'
        self.logger.warning('Swapping ' + self.username + ' due to CAPTCHA.')
        self.update_accounts_dict(captcha=True)
        self.captcha_queue.put(self.username)
        await self.new_account()

    async def swap_account(self, reason=''):
        self.error_code = 'SWAPPING'
        self.logger.warning('Swapping out ' + self.username + ' because ' +
                            reason + '.')
        self.update_accounts_dict()
        while self.extra_queue.empty():
            if self.killed:
                return False
            await asyncio.sleep(20)
        if self.killed:
            return False
        self.extra_queue.put(self.username)
        await self.new_account()

    async def remove_account(self):
        self.error_code = 'REMOVING'
        self.logger.warning('Removing ' + self.username + ' due to ban.')
        self.update_accounts_dict(banned=True)
        await self.new_account()

    def simulate_jitter(self):
        self.location[0] = random.uniform(self.location[0] - 0.00001,
                                          self.location[0] + 0.00001)
        self.location[1] = random.uniform(self.location[1] - 0.00001,
                                          self.location[1] + 0.00001)
        self.location[2] = random.uniform(self.location[2] - 1,
                                          self.location[2] + 1)
        self.api.set_position(*self.location)

    async def encounter(self, pokemon):
        self.simulate_jitter()

        delay_required = random.triangular(1, 4.5, 2)
        self.error_code = '~'
        while time.time() - self.last_visit < delay_required:
            await asyncio.sleep(1)

        self.error_code = 'ENCOUNTERING'

        request = self.api.create_request()
        request = request.encounter(encounter_id=pokemon['encounter_id'],
                                    spawn_point_id=pokemon['spawn_point_id'],
                                    player_latitude=self.location[0],
                                    player_longitude=self.location[1])

        responses = await self.call_chain(request)
        self.last_visit = time.time()

        response = responses.get('ENCOUNTER', {})
        pokemon_data = response.get('wild_pokemon', {}).get('pokemon_data', {})
        if 'cp' in pokemon_data:
            for iv in ('individual_attack', 'individual_defense', 'individual_stamina'):
                if iv not in pokemon_data:
                    pokemon_data[iv] = 0
            pokemon_data['probability'] = response.get('capture_probability', {}).get('capture_probability')
        self.error_code = '!'
        return pokemon_data


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

    def get_inventory_timestamp(self, responses):
        timestamp = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('new_timestamp_ms')
        if timestamp:
            self.inventory_timestamp = timestamp
        elif not self.timestamp:
            self.inventory_timestamp = (time.time() - 2) * 1000

    async def app_simulation_login(self):
        self.error_code = 'APP SIMULATION'
        self.logger.info('Starting RPC login sequence (iOS app simulation)')

        # empty request 1
        request = self.api.create_request()

        response = await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        await asyncio.sleep(1.172)

        # empty request 2
        request = self.api.create_request()

        response = await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )
        await asyncio.sleep(1.304)


        # request 1: get_player
        request = self.api.create_request()
        request.get_player(player_locale = {'country': 'US', 'language': 'en', 'timezone': 'America/Denver'})

        response = await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )

        if response.get('responses', {}).get('GET_PLAYER', {}).get('banned', False):
            raise pgoapi_exceptions.BannedAccountException

        await asyncio.sleep(1.356)

        # request 2: download_remote_config_version
        request = self.api.create_request()
        request.download_remote_config_version(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory()
        request.check_awarded_badges()
        request.download_settings()

        response = await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )

        responses = response.get('responses', {})
        self.check_captcha(responses)
        self.get_inventory_timestamp(responses)

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

        await asyncio.sleep(1.072)

        # request 3: get_asset_digest
        request = self.api.create_request()
        request.get_asset_digest(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)

        response = await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(request.call)
        )

        self.get_inventory_timestamp(response.get('responses', {}))
        await asyncio.sleep(1.709)

        # request 4: get_player_profile
        request = self.api.create_request()
        request.get_player_profile()

        responses = await self.call_chain(request)
        await asyncio.sleep(1.326)

        # requst 5: level_up_rewards
        request = self.api.create_request()
        request.level_up_rewards(level=player_level)

        responses = await self.call_chain(request)
        await asyncio.sleep(1.184)

        self.logger.info('Finished RPC login sequence (iOS app simulation)')
        return responses

    async def login(self):
        """Logs worker in and prepares for scanning"""
        self.logger.info('Trying to log in')
        self.error_code = 'LOGIN'
        global LAST_LOGIN
        time_required = random.triangular(2, 4, 7)
        while (time.time() - LAST_LOGIN) < time_required:
            self.error_code = 'WAITING'
            if self.killed:
                return False
            await asyncio.sleep(2)
        LAST_LOGIN = time.time()

        await self.loop.run_in_executor(
            self.network_executor,
            self.call_api(
                self.api.set_authentication,
                username=self.username,
                password=self.account.get('password'),
                provider=self.account.get('provider'),
            )
        )
        if self.killed:
            return False
        if not self.ever_authenticated:
            if not await self.app_simulation_login():
                return False

        self.ever_authenticated = True
        self.logged_in = True
        self.error_code = '@'
        return True

    async def visit(self, point, i):
        """Wrapper for self.visit_point - runs it a few times before giving up

        Also is capable of restarting in case an error occurs.
        """
        visited = False
        for attempts in range(0,5):
            try:
                if self.killed:
                    return False
                if not self.logged_in and not DEBUG:
                    self.api.set_position(*point)
                    if not await self.login():
                        await asyncio.sleep(2)
                        continue
                if self.killed:
                    return False
                visited = await self.visit_point(point, i)
            except pgoapi_exceptions.ServerSideAccessForbiddenException:
                err = 'Banned IP.'
                if self.proxy:
                    err += ' ' + self.proxy
                self.logger.error(err)
                self.error_code = 'IP BANNED'
                self.swap_circuit(reason='ban')
                await self.random_sleep(sleep_min=20, sleep_max=30)
            except pgoapi_exceptions.AuthException:
                self.logger.warning('Login failed: ' + self.username)
                self.error_code = 'FAILED LOGIN'
                if self.killed:
                    return False
                await self.swap_account(reason='login failed')
                await self.random_sleep()
            except pgoapi_exceptions.NotLoggedInException:
                self.logger.error(self.username + ' is not logged in.')
                self.error_code = 'NOT AUTHENTICATED'
                if self.killed:
                    return False
                await self.swap_account(reason='not logged in')
                await self.random_sleep()
            except pgoapi_exceptions.ServerBusyOrOfflineException:
                self.logger.info('Server too busy - restarting')
                self.error_code = 'RETRYING'
                await self.random_sleep()
            except pgoapi_exceptions.ServerSideRequestThrottlingException:
                self.logger.info('Server throttling - sleeping for a bit')
                self.error_code = 'THROTTLE'
                await self.random_sleep(sleep_min=10)
            except pgoapi_exceptions.BannedAccountException:
                self.error_code = 'BANNED?'
                if self.killed:
                    return False
                await self.remove_account()
            except CaptchaException:
                CAPTCHAS += 1
                if self.killed:
                    return False
                await self.bench_account()
                self.error_code = 'CAPTCHA'
            except MalformedResponse:
                self.logger.warning('Malformed response received!')
                self.error_code = 'RESTART'
                await self.random_sleep()
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
        global GLOBAL_SEEN
        global GLOBAL_VISITS

        latitude, longitude, altitude = point
        altitude = random.uniform(altitude - 1, altitude + 1)
        rounded_coords = utils.round_coords(point, precision=5)
        self.error_code = '!'
        self.logger.info(
            'Visiting point %d (%s,%s %sm)', i, rounded_coords[0],
            rounded_coords[1], round(altitude)
        )
        start = time.time()
        self.location = point

        if DEBUG:
            self.last_visit = start
            sent_notification = False
            self.error_code = ':'
            pokemon_seen = random.randint(2,9)
            self.total_seen += pokemon_seen
            GLOBAL_SEEN += pokemon_seen
            GLOBAL_VISITS += 1
            self.visits += 1
            if not self.killed:
                self.worker_dict.update([(self.worker_no, ((latitude, longitude), start, self.speed, self.total_seen, self.visits, pokemon_seen, sent_notification))])
            self.db_processor.count += pokemon_seen
            return True

        self.api.set_position(latitude, longitude, altitude)

        if rounded_coords not in CELL_IDS or len(CELL_IDS[rounded_coords]) > 25:
            CELL_IDS[rounded_coords] = await self.loop.run_in_executor(
                self.cell_ids_executor,
                partial(
                    pgoapi_utils.get_cell_ids, latitude, longitude
                )
            )
        cell_ids = CELL_IDS[rounded_coords]
        since_timestamp_ms = [0] * len(cell_ids)

        request = self.api.create_request()
        request.get_map_objects(cell_id=cell_ids,
                                since_timestamp_ms=since_timestamp_ms,
                                latitude=pgoapi_utils.f2i(latitude),
                                longitude=pgoapi_utils.f2i(longitude))

        responses = await self.call_chain(request)
        self.last_visit = time.time()

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
                pokemon_data = None
                pokemon_seen += 1
                # Accurate times only provided in the last 90 seconds
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

                if normalized['pokemon_id'] in config.NOTIFY_IDS:
                    normalized.update(await self.encounter(pokemon))
                    self.error_code = '*'
                    notified, explanation = notifier.notify(normalized)
                    if notified:
                        sent_notification = True
                        self.logger.info(explanation)
                        global NOTIFICATIONS_SENT
                        NOTIFICATIONS_SENT += 1
                    else:
                        self.logger.warning(explanation)

                if normalized['valid'] and normalized not in db.SIGHTING_CACHE:
                    pokemons.append(normalized)
                    if 'cp' not in normalized:
                        normalized.update(await self.encounter(pokemon))

                if not normalized['valid'] or db.LONGSPAWN_CACHE.in_store(normalized):
                    normalized = normalized.copy()
                    normalized['type'] = 'longspawn'
                    ls_seen.append(normalized)
            for fort in map_cell.get('forts', []):
                if not fort.get('enabled'):
                    continue
                if fort.get('type') == 1:  # pokestops
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

        GLOBAL_VISITS += 1
        self.visits += 1
        if not self.killed:
            self.worker_dict.update([(self.worker_no, ((latitude, longitude), start, self.speed, self.total_seen, self.visits, pokemon_seen, sent_notification))])
        self.logger.info(
            'Point processed, %d Pokemons and %d forts seen!',
            pokemon_seen,
            len(forts),
        )
        self.update_accounts_dict()
        return True

    def travel_speed(self, point, spawn_time):
        if self.busy or self.killed:
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


    def check_captcha(self, responses):
        challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
        if challenge_url != ' ':
            raise CaptchaException
        else:
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
        if self.ever_authenticated:
            self.update_accounts_dict()


class Overseer:
    def __init__(self, status_bar, loop):
        self.logger = logging.getLogger('overseer')
        self.workers = {}
        self.count = config.GRID[0] * config.GRID[1]
        self.logger.info('Done')
        self.start_date = datetime.now()
        self.status_bar = status_bar
        self.things_count = []
        self.paused = False
        self.killed = False
        self.last_proxy = 0
        self.loop = loop
        self.db_processor = DatabaseProcessor()
        self.cell_ids_executor = ThreadPoolExecutor(config.COMPUTE_THREADS)
        self.network_executor = ThreadPoolExecutor(config.NETWORK_THREADS)
        self.coroutine_limit = self.count
        self.coroutines_count = 0
        self.logger.info('Overseer initialized')
        self.skipped = 0
        self.visited = 0
        self.searches_without_shuffle = 0

    def kill(self):
        self.killed = True
        try:
            if self.captcha_queue.empty():
                for account in ACCOUNTS.keys():
                    ACCOUNTS[account]['captcha'] = False
            else:
                while not self.extra_queue.empty():
                    username = overseer.extra_queue.get()
                    ACCOUNTS[username]['captcha'] = False
        except Exception as e:
            print(e)

        for worker in self.workers.values():
            try:
                worker.kill()
            except Exception as e:
                print('worker', worker.worker_no, e)

    def launch_queue_manager(self):
        captcha = Queue()
        extra = Queue()
        workers = {}
        class QueueManager(SyncManager): pass
        QueueManager.register('captcha_queue', callable=lambda:captcha)
        QueueManager.register('extra_queue', callable=lambda:extra)
        QueueManager.register('worker_dict', callable=lambda:workers)
        self.manager = QueueManager(address='queue.sock', authkey=b'monkeys')
        self.manager.start()
        self.captcha_queue = self.manager.captcha_queue()
        self.extra_queue = self.manager.extra_queue()
        self.worker_dict = self.manager.worker_dict()
        for username, account in ACCOUNTS.items():
            if account.get('banned'):
                continue
            if account.get('captcha'):
                self.captcha_queue.put(username)
            else:
                self.extra_queue.put(username)

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
            loop=self.loop,
            proxy=proxy
        )
        self.workers[worker_no] = worker

    def start(self):
        self.launch_queue_manager()
        for worker_no in range(self.count):
            self.start_worker(worker_no, first_run=True)
        self.workers_list = list(overseer.workers.values())
        self.db_processor.start()

    def check(self):
        global ACCOUNTS
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
            while self.paused:
                if self.killed:
                    break
                time.sleep(10)
        # OK, now we're killed
        while True:
            try:
                tasks = sum(not t.done() for t in asyncio.Task.all_tasks(self.loop))
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

        try:
            output = [
                'PokeMiner\trunning for {}'.format(running_for),
                '{len} workers'.format(len=workers_count),
                '',
                '{} threads and {} coroutines active'.format(
                    threading.active_count(),
                    self.coroutines_count,
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
        except EOFError:
            pass
        try:
            output.append('Pokemon seen per visit: ' + str(round(GLOBAL_SEEN / GLOBAL_VISITS, 2)))
        except ZeroDivisionError:
            pass
        seconds_since_start = time.time() - START_TIME
        visits_per_second = GLOBAL_VISITS / seconds_since_start
        captchas_per_hour = CAPTCHAS * (3600 / seconds_since_start)
        output.append('Visits per second: ' + str(round(visits_per_second, 2)))
        output.append('Spawns skipped: ' + str(self.skipped))
        output.append('CAPTCHAs per hour: ' + str(round(captchas_per_hour, 2)))
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

    async def best_worker(self, point, spawn_time, give_up=False):
        worker = None
        lowest_speed = float('inf')
        self.searches_without_shuffle += 1
        if self.searches_without_shuffle > 19:
            random.shuffle(self.workers_list)
        workers = self.workers_list.copy()
        while worker is None or lowest_speed > config.SPEED_LIMIT:
            speed = None
            lowest_speed = float('inf')
            worker = None
            for w in workers:
                if self.killed:
                    return False, False
                speed = await self.loop.run_in_executor(
                    self.cell_ids_executor,
                    partial(w.travel_speed, point, spawn_time)
                )
                if speed is not None and speed < lowest_speed:
                    lowest_speed = speed
                    worker = w
                    if speed < 7:
                        break
            if worker.busy:
               worker = None
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
        while not self.killed:
            if self.visited > 0:
                with open('accounts.pickle', 'wb') as f:
                    pickle.dump(ACCOUNTS, f, pickle.HIGHEST_PROTOCOL)
            SPAWNS.update_spawns()
            current_hour = utils.get_current_hour()
            for spawn_id, spawn in SPAWNS.spawns.items():
                try:
                    self.coroutines_count = len(asyncio.Task.all_tasks(self.loop))
                    while self.coroutines_count > self.coroutine_limit or not isinstance(self.coroutines_count, int):
                        time.sleep(1)
                        self.coroutines_count = len(asyncio.Task.all_tasks(self.loop))
                    while self.captcha_queue.qsize() > config.MAX_CAPTCHAS and not self.killed:
                        self.paused = True
                        time.sleep(10)
                except IOError:
                    pass
                except Exception as e:
                    print(e)
                if self.killed:
                    return
                self.paused = False
                spawn_time = spawn[1] + current_hour
                # negative = already happened
                # positive = hasn't happened yet
                time_diff = spawn_time - time.time()
                if self.visited == 0 and (time_diff < -10 or time_diff > 10):
                    continue
                elif time_diff < -180:
                    self.skipped += 1
                    continue
                elif time_diff > 90:
                    time.sleep(30)
                point = list(spawn[0])
                asyncio.run_coroutine_threadsafe(self.try_point(point, spawn_time, spawn_id), loop=self.loop)

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
        if DEBUG:
            return True
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
    parser.add_argument(
        '--debug',
        dest='debug',
        help="For testing, skip actually making requests.",
        action='store_true',
    )
    return parser.parse_args()


def exception_handler(loop, context):
    logger = logging.getLogger('eventloop')
    logger.exception('A wild exception appeared!')
    logger.error(context)



if __name__ == '__main__':
    try:
        with open('cells.pickle', 'rb') as f:
            CELL_IDS = pickle.load(f)
    except FileNotFoundError:
        CELL_IDS = dict()

    try:
        with open('accounts.pickle', 'rb') as f:
            ACCOUNTS = pickle.load(f)
        if (config.ACCOUNTS and
                set(ACCOUNTS) != set(acc[0] for acc in config.ACCOUNTS)):
            ACCOUNTS = utils.generate_accounts_dict(ACCOUNTS)
    except FileNotFoundError:
        if not config.ACCOUNTS:
            raise ValueError('Must have accounts in config or an accounts pickle.')
        ACCOUNTS = utils.generate_accounts_dict()

    SPAWNS = Spawns()

    args = parse_args()
    logger = logging.getLogger()

    if args.status_bar:
        configure_logger(filename='worker.log')
        logger.info('-' * 30)
        logger.info('Starting up!')
    else:
        configure_logger(filename=None)
    global DEBUG
    if args.debug:
        DEBUG = True
    else:
        if config.NOTIFY_IDS or config.NOTIFY_RANKING:
            import notification
            notifier = notification.Notifier(SPAWNS)
        DEBUG = False

    logger.setLevel(args.log_level)
    loop = asyncio.get_event_loop()
    overseer = Overseer(status_bar=args.status_bar, loop=loop)
    loop.set_default_executor(ThreadPoolExecutor())
    loop.set_exception_handler(exception_handler)
    overseer.start()
    overseer_thread = threading.Thread(target=overseer.check)
    overseer_thread.start()
    launcher_thread = threading.Thread(target=overseer.launch)
    launcher_thread.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Exiting, please wait until all tasks finish')
        overseer.kill()  # also cancels all workers' futures

        with open('cells.pickle', 'wb') as f:
            pickle.dump(CELL_IDS, f, pickle.HIGHEST_PROTOCOL)
        with open('accounts.pickle', 'wb') as f:
            pickle.dump(ACCOUNTS, f, pickle.HIGHEST_PROTOCOL)

        try:
            pending = asyncio.Task.all_tasks(loop=loop)
            loop.run_until_complete(asyncio.gather(*pending))
            overseer.cell_ids_executor.shutdown()
            overseer.network_executor.shutdown()
            overseer.db_processor.stop()
            overseer.manager.shutdown()
            notifier.session.close()
            SPAWNS.session.close()
            loop.stop()
            loop.close()
        except Exception as e:
            print(e)
