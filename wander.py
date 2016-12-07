# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, CancelledError, Future
from collections import deque
from datetime import datetime
from functools import partial
from sqlalchemy.exc import IntegrityError
from geopy.distance import great_circle
from queue import Queue
from multiprocessing.managers import BaseManager, DictProxy
from pgoapi.auth_ptc import AuthPtc
from signal import signal, SIGINT, SIG_IGN
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

from worker import CaptchaException, Spawns


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
        level=logging.WARNING,
    )

# Check whether config has all necessary attributes
REQUIRED_SETTINGS = (
    'DB_ENGINE',
    'MAP_START',
    'MAP_END',
    'GRID'
)
for setting_name in REQUIRED_SETTINGS:
    if not hasattr(config, setting_name):
        raise RuntimeError('Please set "{}" in config'.format(setting_name))

_workers_count = config.GRID[0] * config.GRID[1]
# Set defaults for missing config options
OPTIONAL_SETTINGS = {
    'PROXIES': None,
    'CYCLES_PER_WORKER': 3,
    'SCAN_RADIUS': 70,
    'SCAN_DELAY': 11,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'ENCRYPT_PATH': None,
    'HASH_PATH': None,
    'MAX_CAPTCHAS': 200,
    'ACCOUNTS': (),
    'SPEED_LIMIT': 19,
    'ENCOUNTER': None,
    'COMPUTE_THREADS': round(_workers_count / 10) + 1,
    'NETWORK_THREADS': round(_workers_count / 2) + 1
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

BAD_STATUSES = (
    'LOGIN FAIL',
    'EXCEPTION',
    'BAD LOGIN',
    'RETRYING',
    'THROTTLE',
    'NO POKEMON'
)

START_TIME = time.time()
GLOBAL_SEEN = 0
CAPTCHAS = 0
NOTIFICATIONS_SENT = 0
DOWNLOAD_HASH = "5296b4d9541938be20b1d1a8e8e3988b7ae2e93b"


class Spawns:

    def __init__(self):
        self.spawns = None
        self.session = db.Session()

    def update_spawns(self, loadpickle=False):
        if loadpickle:
            try:
                with open('pickles/spawns.pickle', 'rb') as f:
                    self.spawns = pickle.load(f)
                    return
            except (FileNotFoundError, EOFError):
                pass
        self.spawns = db.get_spawns(self.session)
        with open('pickles/spawns.pickle', 'wb') as f:
            pickle.dump(self.spawns, f, pickle.HIGHEST_PROTOCOL)

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
            points,
            db_processor,
            cell_ids_executor,
            network_executor,
            extra_queue,
            captcha_queue,
            worker_dict,
            loop,
            start_step=0,
            device_info=None,
            proxy=None
    ):
        # Set of all points that worker needs to visit
        self.points = points
        self.count_points = len(self.points)
        # set queues and account
        self.extra_queue = extra_queue
        self.captcha_queue = captcha_queue
        self.worker_dict = worker_dict
        self.worker_no = worker_no
        self.username = self.extra_queue.get()
        global ACCOUNTS
        self.account = ACCOUNTS[self.username]
        center = self.points[0]
        self.location = center
        self.inventory_timestamp = self.account.get('inventory_timestamp')
        self.logger = logging.getLogger('worker-{}'.format(worker_no))
        self.proxy = proxy
        self.initialize_api()
        # asyncio/thread references
        self.future = None  # worker's own future
        self.loop = loop
        self.db_processor = db_processor
        self.cell_ids_executor = cell_ids_executor
        self.network_executor = network_executor
        # Some handy counters
        self.start_step = start_step  # allow worker to pick up where it left
        self.step = 0
        self.cycle = 1
        self.seen_per_cycle = 0
        self.total_seen = 0
        # State variables
        self.running = True  # not running worker should be restarted
        self.killed = False  # killed worker will stay killed
        self.restart_me = False  # ask overseer for restarting
        # Other variables
        self.last_visit = self.account.get('time', 0)
        self.error_code = 'INIT'

    def initialize_api(self):
        device_info = utils.get_device_info(self.account)
        self.logged_in = False
        self.ever_authenticated = False
        self.empty_visits = 0

        self.api = PGoApi(device_info=device_info)
        if config.ENCRYPT_PATH:
            self.api.set_signature_lib(config.ENCRYPT_PATH)
        if config.HASH_PATH:
            self.api.set_hash_lib(config.HASH_PATH)
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

    async def call_chain(self, request):
        request.check_challenge()
        request.get_hatched_eggs()
        if self.inventory_timestamp:
            request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        else:
            request.get_inventory()
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        request.get_buddy_walked()

        response = await self.loop.run_in_executor(
            self.network_executor, request.call
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

    def set_proxy(self, proxy=None):
        if proxy:
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
        await self.login()
        self.error_code = None

    def update_accounts_dict(self, captcha=False, banned=False):
        global ACCOUNTS
        account = ACCOUNTS[self.username]
        account['captcha'] = captcha
        account['banned'] = banned
        account['location'] = self.location
        account['time'] = self.last_visit
        account['inventory_timestamp'] = self.inventory_timestamp
        if not self.api._auth_provider:
            return
        account['refresh'] = self.api._auth_provider._refresh_token
        if self.api._auth_provider.check_access_token():
            account['auth'] = self.api._auth_provider._access_token
            account['expiry'] = self.api._auth_provider._access_token_expiry
        else:
            account['auth'], account['expiry'] = None, None

    async def bench_account(self):
        self.error_code = 'BENCHING'
        self.logger.warning('Swapping ' + self.username + ' due to CAPTCHA.')
        self.update_accounts_dict(captcha=True)
        self.captcha_queue.put(self.username)
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
        self.location = [
            random.uniform(self.location[0] - 0.00002,
                           self.location[0] + 0.00002),
            random.uniform(self.location[1] - 0.00002,
                           self.location[1] + 0.00002),
            random.uniform(self.location[2] - 1.5,
                           self.location[2] + 1.5)
        ]
        self.api.set_position(*self.location)

    async def encounter(self, pokemon):
        pokemon_point = (pokemon['latitude'], pokemon['longitude'])
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
        self.last_visit = time.time()

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

    def swap_proxy(self, reason=''):
        self.set_proxy(random.choice(config.PROXIES))
        self.logger.warning('Swapped out {p} due to {r}.'.format(
                            p=self.proxy, r=reason))

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
            self.network_executor, request.call
        )
        await self.random_sleep(1, 1.5, 1.172)

        # empty request 2
        request = self.api.create_request()

        response = await self.loop.run_in_executor(
            self.network_executor, request.call
        )
        await self.random_sleep(1, 1.5, 1.304)

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

        if response.get('responses', {}).get('GET_PLAYER', {}).get('banned', False):
            raise pgoapi_exceptions.BannedAccountException

        await self.random_sleep(1, 1.5, 1.356)

        # request 2: download_remote_config_version
        request = self.api.create_request()
        request.download_remote_config_version(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory()
        request.check_awarded_badges()
        request.download_settings()

        response = await self.loop.run_in_executor(
            self.network_executor, request.call
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

        await self.random_sleep(1, 1.2, 1.072)

        # request 3: get_asset_digest
        request = self.api.create_request()
        request.get_asset_digest(platform=1, app_version=4500)
        request.check_challenge()
        request.get_hatched_eggs()
        request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)

        response = await self.loop.run_in_executor(
            self.network_executor, request.call
        )

        self.get_inventory_timestamp(response.get('responses', {}))
        await self.random_sleep(1, 2, 1.709)

        # request 4: get_player_profile
        request = self.api.create_request()
        request.get_player_profile()

        responses = await self.call_chain(request)
        await self.random_sleep(1, 1.5, 1.326)

        # requst 5: level_up_rewards
        request = self.api.create_request()
        request.level_up_rewards(level=player_level)

        responses = await self.call_chain(request)
        await self.random_sleep(1, 1.5, 1.184)

        self.logger.info('Finished RPC login sequence (iOS app simulation)')
        return responses

    async def first_run(self):
        total_workers = config.GRID[0] * config.GRID[1]
        try:
            await self.sleep(
                self.worker_no / total_workers * config.SCAN_DELAY
            )
            await self.run()
        except CancelledError:
            self.kill()

    async def run(self):
        try:
            await self.run_cycle()
        except CancelledError:
            self.kill()


    async def login(self):
        """Logs worker in and prepares for scanning"""
        self.logger.info('Trying to log in')
        self.error_code = 'LOGIN'
        global LOGIN_SEM
        global SIMULATION_SEM

        async with LOGIN_SEM:
            await self.random_sleep(minimum=0.5, maximum=1.5)
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
            async with SIMULATION_SEM:
                if not await self.app_simulation_login():
                    return False

        self.ever_authenticated = True
        self.logged_in = True
        self.error_code = '@'
        return True

    async def run_cycle(self):
        """Wrapper for self.main - runs it a few times before restarting

        Also is capable of restarting in case an error occurs.
        """
        self.error_code = None
        if self.cycle == 1:
            start_step = self.start_step
        else:
            start_step = 0
        while self.cycle <= config.CYCLES_PER_WORKER:
            try:
                if not self.logged_in:
                    if not await self.login():
                        await asyncio.sleep(2)
                        continue
                if not self.running and not self.killed:
                    await self.restart()
                    return
                await self.main(start_step=start_step)
            except pgoapi_exceptions.ServerSideAccessForbiddenException:
                err = 'Banned IP.'
                if self.proxy:
                    err += ' ' + self.proxy
                self.logger.error(err)
                self.error_code = 'IP BANNED'
                await self.random_sleep(minimum=25, maximum=35)
            except pgoapi_exceptions.AuthException:
                self.logger.warning('Login failed: ' + self.username)
                self.error_code = 'FAILED LOGIN'
                if self.killed:
                    return False
                await self.swap_account(reason='login failed')
            except pgoapi_exceptions.NotLoggedInException:
                self.logger.error(self.username + ' is not logged in.')
                self.error_code = 'NOT AUTHENTICATED'
                if self.killed:
                    return False
                await self.swap_account(reason='not logged in')
            except pgoapi_exceptions.ServerBusyOrOfflineException:
                self.logger.info('Server too busy - restarting')
                self.error_code = 'RETRYING'
                await self.random_sleep()
            except pgoapi_exceptions.ServerSideRequestThrottlingException:
                self.logger.info('Server throttling - sleeping for a bit')
                self.error_code = 'THROTTLE'
                await self.random_sleep(minimum=10)
            except pgoapi_exceptions.BannedAccountException:
                self.error_code = 'BANNED?'
                if self.killed:
                    return False
                await self.remove_account()
            except CaptchaException:
                global CAPTCHAS
                if self.killed:
                    return False
                await self.bench_account()
                self.error_code = 'CAPTCHA'
                CAPTCHAS += 1
            except MalformedResponse:
                self.logger.warning('Malformed response received!')
                self.error_code = 'RESTART'
                await self.random_sleep()
            except CancelledError:
                self.kill()
                return
            except Exception as err:
                self.logger.exception('A wild exception appeared!')
                self.error_code = 'EXCEPTION'
                await self.random_sleep()
            self.cycle += 1
            await self.random_sleep()
        self.error_code = 'RESTART'
        await self.restart()


    async def main(self, start_step=0):
        """Heart of the worker - goes over each point and reports sightings"""
        global GLOBAL_SEEN

        self.seen_per_cycle = 0
        self.step = start_step or 0

        for i, point in enumerate(self.points):
            latitude = random.uniform(point[0] - 0.00001, point[0] + 0.00001)
            longitude = random.uniform(point[1] - 0.00001, point[1] + 0.00001)
            altitude = random.uniform(point[2] - 2, point[2] + 2)
            self.location = (latitude, longitude, altitude)
            if not self.running:
                return
            self.logger.info(
                'Visiting {0[0]:.4f},{0[1]:.4f} {0[2]:.1f}m'.format(point))
            start = time.time()

            self.api.set_position(latitude, longitude, altitude)

            rounded_coords = utils.round_coords(point, precision=5)
            if rounded_coords not in CELL_IDS:
                CELL_IDS[rounded_coords] = await self.loop.run_in_executor(
                    self.cell_ids_executor,
                    partial(
                        pgoapi_utils.get_cell_ids, latitude, longitude, radius=500
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
                self.logger.warning(
                    'Response code: {}'.format(map_objects.get('status')))
                self.empty_visits += 1
                if self.empty_visits > 25:
                    reason = '{} empty visits'.format(self.empty_visits)
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
                        despawn_time = SPAWNS.get_despawn_time(
                            normalized['spawn_id'])
                        if despawn_time:
                            normalized['expire_timestamp'] = despawn_time
                            normalized['time_till_hidden_ms'] = (
                                despawn_time * 1000) - request_time_ms
                            normalized['valid'] = 'fixed'
                        else:
                            normalized['valid'] = False
                    else:
                        normalized['valid'] = True

                    if NOTIFY and normalized['pokemon_id'] in config.NOTIFY_IDS:
                        if config.ENCOUNTER in ('all', 'notifying'):
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
                        if config.ENCOUNTER == 'all':
                            normalized.update(await self.encounter(pokemon))
                        pokemons.append(normalized)

                    if not normalized[
                            'valid'] or db.LONGSPAWN_CACHE.in_store(normalized):
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
                self.total_seen += pokemon_seen
                GLOBAL_SEEN += pokemon_seen
                self.empty_visits = 0
            else:
                self.empty_visits += 1
                if self.empty_visits > 25:
                    reason = '{} empty visits'.format(self.empty_visits)
                    await self.swap_account(reason)

            # Clear error code and let know that there are Pokemon
            if self.error_code and self.seen_per_cycle:
                self.error_code = None
            self.step += 1

            self.worker_dict.update([(self.worker_no,
                ((latitude, longitude), start, None, self.total_seen,
                None, pokemon_seen, sent_notification))])

            await self.sleep(config.SCAN_DELAY)
        if self.seen_per_cycle == 0:
            self.error_code = 'NO POKEMON'

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
            msg = 'C{cycle},P{seen},{progress:.0f}%'.format(
                cycle=self.cycle,
                seen=self.seen_per_cycle,
                progress=(self.step / float(self.count_points) * 100)
            )
        return '[W{worker_no}: {msg}]'.format(
            worker_no=self.worker_no,
            msg=msg
        )

    async def random_sleep(self, minimum=8, maximum=14, mode=10):
        """Sleeps for a bit"""
        if mode:
            await asyncio.sleep(random.triangular(minimum, maximum, mode))
        else:
            await asyncio.sleep(random.uniform(minimum, maximum))

    async def sleep(self, duration):
        """Sleeps and interrupts if detects that worker was killed"""
        try:
            await asyncio.sleep(duration)
        except CancelledError:
            self.kill()

    async def restart(self, sleep_min=5, sleep_max=20):
        """Sleeps for a bit, then restarts"""
        if self.killed:
            return
        self.logger.info('Restarting')
        await self.random_sleep(minimum=sleep_min, maximum=sleep_max)
        self.restart_me = True
        self.running = False

    def slap(self):
        """Slaps worker in face, telling it to improve itself

        It's weaker form of killing - it will be restarted soon.
        """
        self.error_code = 'KILLED'
        self.running = False

    def kill(self):
        """Marks worker as killed

        Killed worker won't be restarted.
        """
        self.error_code = 'KILLED'
        self.running = False
        self.killed = True
        if self.ever_authenticated:
            self.update_accounts_dict()


class Overseer:
    def __init__(self, status_bar, loop):
        self.logger = logging.getLogger('overseer')
        self.workers = {}
        self.count = config.GRID[0] * config.GRID[1]
        self.logger.info('Generating points...')
        self.points = utils.get_points_per_worker(gen_alts=True)
        self.cell_ids = [{} for _ in range(self.count)]
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
        self.logger.info('Overseer initialized')

    def kill(self):
        self.killed = True
        self.db_processor.stop()
        for worker in self.workers.values():
            worker.kill()
            if worker.future:
                worker.future.cancel()

        global ACCOUNTS
        print('Setting CAPTCHA statuses.')

        if self.captcha_queue.empty():
            for account in ACCOUNTS.keys():
                ACCOUNTS[account]['captcha'] = False
        else:
            while not self.extra_queue.empty():
                username = self.extra_queue.get()
                ACCOUNTS[username]['captcha'] = False

    def launch_account_manager(self):
        def mgr_init():
            signal(SIGINT, SIG_IGN)
        captcha_queue = Queue()
        extra_queue = Queue()
        worker_dict = {}

        class AccountManager(BaseManager):
            pass
        AccountManager.register('captcha_queue', callable=lambda:captcha_queue)
        AccountManager.register('extra_queue', callable=lambda:extra_queue)
        AccountManager.register('worker_dict',
            callable=lambda:worker_dict, proxytype=DictProxy)

        self.manager = AccountManager(address='queue.sock', authkey=b'monkeys')
        self.manager.start(mgr_init)
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
        stopped_abruptly = (
            not first_run and
            self.workers[worker_no].step < len(self.points[worker_no]) - 1
        )
        if stopped_abruptly:
            # Restart from NEXT step, because current one may have caused it
            # to restart
            start_step = self.workers[worker_no].step + 1
        else:
            start_step = 0

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
            points=self.points[worker_no],
            db_processor=self.db_processor,
            cell_ids_executor=self.cell_ids_executor,
            network_executor=self.network_executor,
            start_step=start_step,
            extra_queue=self.extra_queue,
            captcha_queue=self.captcha_queue,
            worker_dict=self.worker_dict,
            loop=self.loop,
            proxy=proxy
        )
        self.workers[worker_no] = worker
        # For first time, we need to wait until all workers login before
        # scanning
        if first_run:
            worker.future = asyncio.ensure_future(worker.first_run())
            return
        # WARNING: at this point, we're called by self.check which runs in
        # separate thread than event loop! That's why run_coroutine_threadsafe
        # is used here.
        worker.future = asyncio.run_coroutine_threadsafe(
            worker.run(), self.loop
        )

    def get_point_stats(self):
        lenghts = [len(p) for p in self.points]
        return {
            'max': max(lenghts),
            'min': min(lenghts),
            'avg': int(sum(lenghts) / float(len(lenghts))),
        }

    def start(self):
        self.launch_account_manager()
        for worker_no in range(self.count):
            self.start_worker(worker_no, first_run=True)
        self.db_processor.start()

    def check(self):
        global ACCOUNTS
        last_cleaned_cache = time.time()
        last_workers_checked = time.time()
        last_things_found_updated = time.time()
        workers_check = [
            (worker, worker.total_seen)
            for worker in self.workers.values()
            if worker.running
        ]
        while not self.killed:
            now = time.time()
            # Restart workers that were killed
            for worker_no in self.workers.keys():
                if self.workers[worker_no].restart_me:
                    self.start_worker(worker_no)
            # Clean cache
            if now - last_cleaned_cache > 900:  # clean cache after 15min
                self.db_processor.clean_cache()
                last_cleaned_cache = now
                SPAWNS.update_spawns()
            # Check up on workers
            if now - last_workers_checked > 300:
                # Kill those not doing anything
                for worker, total_seen in workers_check:
                    if not worker.running:
                        continue
                    if worker.total_seen <= total_seen:
                        worker.slap()
                # Prepare new list
                workers_check = [
                    (worker, worker.total_seen)
                    for worker in self.workers.values()
                ]
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
            time.sleep(0.5)
            while self.paused:
                if self.killed:
                    break
                time.sleep(10)
        # OK, now we're killed
        while True:
            try:
                tasks = sum(not t.done()
                            for t in asyncio.Task.all_tasks(self.loop))
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
                print('Done.                ')
                break
        print()

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
                row.append('.' if worker.step % 2 == 0 else ':')
        if row:
            dots.append(row)
        return dots, messages

    def get_status_message(self):
        workers_count = len(self.workers)
        points_stats = self.get_point_stats()
        running_for = datetime.now() - self.start_date
        try:
            coroutines_count = len(asyncio.Task.all_tasks(self.loop))
        except RuntimeError:
            # Set changed size during iteration
            coroutines_count = '?'
        output = [
            'PokeMiner running for {}'.format(running_for),
            '{len} workers, each visiting ~{avg} points per cycle '
            '(min: {min}, max: {max})'.format(
                len=workers_count,
                avg=points_stats['avg'],
                min=points_stats['min'],
                max=points_stats['max'],
            ),
            '',
            '{} threads and {} coroutines active'.format(
                threading.active_count(),
                coroutines_count,
            ),
            'Extra accounts: {a}, CAPTCHAs needed: {c}'.format(
                a=self.extra_queue.qsize(),
                c=self.captcha_queue.qsize()),
            '',
            'Pokemon found count (10s interval):',
            ' '.join(self.things_count),
            '',
        ]
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
        os.makedirs('pickles')
    except OSError:
        pass

    try:
        with open('pickles/cells.pickle', 'rb') as f:
            CELL_IDS = pickle.load(f)
    except (FileNotFoundError, EOFError):
        CELL_IDS = dict()

    try:
        with open('pickles/accounts.pickle', 'rb') as f:
            ACCOUNTS = pickle.load(f)
        if (config.ACCOUNTS and
                set(ACCOUNTS) != set(acc[0] for acc in config.ACCOUNTS)):
            ACCOUNTS = utils.create_accounts_dict(ACCOUNTS)
    except (FileNotFoundError, EOFError):
        if not config.ACCOUNTS:
            raise ValueError(
                'Must have accounts in config or an accounts pickle.')
        ACCOUNTS = utils.create_accounts_dict()

    DEBUG = False
    SPAWNS = Spawns()
    SPAWNS.update_spawns(loadpickle=True)

    args = parse_args()

    logger = logging.getLogger()
    loop = asyncio.get_event_loop()

    if args.status_bar:
        configure_logger(filename='worker.log')
        logger.info('-' * 30)
        logger.info('Starting up!')
    else:
        configure_logger(filename=None)

    if (config.NOTIFY_IDS or config.NOTIFY_RANKING) and (
            config.TWITTER_CONSUMER_KEY or PB_API_KEY):
        NOTIFY = True
        import notification
        notifier = notification.Notifier(SPAWNS)

    LOGIN_SEM = asyncio.BoundedSemaphore(1, loop=loop)
    SIMULATION_SEM = asyncio.BoundedSemaphore(2, loop=loop)
    overseer = Overseer(status_bar=args.status_bar, loop=loop)
    loop.set_default_executor(ThreadPoolExecutor())
    loop.set_exception_handler(exception_handler)
    overseer.start()
    overseer_thread = threading.Thread(target=overseer.check)
    overseer_thread.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Exiting, please wait until all tasks finish')
        overseer.kill()  # also cancels all workers' futures

        print('Dumping pickles.')
        SPAWNS.update_spawns()
        with open('pickles/cells.pickle', 'wb') as f:
            pickle.dump(CELL_IDS, f, pickle.HIGHEST_PROTOCOL)
        with open('pickles/accounts.pickle', 'wb') as f:
            pickle.dump(ACCOUNTS, f, pickle.HIGHEST_PROTOCOL)
        pending = asyncio.Task.all_tasks(loop=loop)
        print('Completing tasks.')
        loop.run_until_complete(asyncio.gather(*pending))
        print('Shutting things down.')
        overseer.cell_ids_executor.shutdown()
        overseer.network_executor.shutdown()
        overseer.db_processor.stop()
        if NOTIFY:
            notifier.session.close()
        SPAWNS.session.close()
        overseer.manager.shutdown()
        print('Stopping and closing loop.')
        loop.stop()
        loop.close()
        print('Exiting.')
