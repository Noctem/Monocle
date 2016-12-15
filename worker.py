#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, CancelledError
from datetime import datetime
from functools import partial
from geopy.distance import great_circle
from multiprocessing.managers import DictProxy
from pgoapi.auth_ptc import AuthPtc
from statistics import median
from logging import getLogger
from threading import Thread, active_count
import threading
from os import system, makedirs
from sys import platform

from pgoapi import (
    exceptions as pgoapi_exceptions,
    PGoApi,
    utilities as pgoapi_utils,
)

import asyncio
import random
import time

import config
import db
import utils

from shared import *


# Check whether config has all necessary attributes
_required = (
    'DB_ENGINE',
    'GRID'
)
for setting_name in _required:
    if not hasattr(config, setting_name):
        raise RuntimeError('Please set "{}" in config'.format(setting_name))

# Set defaults for missing config options
_optional = {
    'PROXIES': None,
    'SCAN_DELAY': 11,
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'CONTROL_SOCKS': None,
    'ENCRYPT_PATH': None,
    'HASH_PATH': None,
    'MAX_CAPTCHAS': 200,
    'ACCOUNTS': (),
    'SPEED_LIMIT': 19,
    'ENCOUNTER': None,
    'NOTIFY': False,
    'AUTHKEY': b'm3wtw0',
    'COMPUTE_THREADS': round((config.GRID[0] * config.GRID[1]) / 4) + 1,
    'NETWORK_THREADS': round((config.GRID[0] * config.GRID[1]) / 10) + 1,
    'SPIN_POKESTOPS': False,
    'COMPLETE_TUTORIAL': False
}
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

if config.CONTROL_SOCKS:
    from stem import Signal
    from stem.control import Controller
    import stem.util.log
    stem.util.log.get_logger().level = 40
    CIRCUIT_TIME = dict()
    CIRCUIT_FAILURES = dict()
    for proxy in config.PROXIES:
        CIRCUIT_TIME[proxy] = time.time()
        CIRCUIT_FAILURES[proxy] = 0
else:
    CIRCUIT_TIME = None
    CIRCUIT_FAILURES = None


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
        self.location = self.account.get('location', (0, 0, 0))
        self.inventory_timestamp = self.account.get('inventory_timestamp')
        self.logger = getLogger('worker-{}'.format(worker_no))
        self.proxy = proxy
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
        self.after_spawn = None
        self.speed = 0
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

    async def call_chain(self, request, buddy=True, incense=False):
        global DOWNLOAD_HASH
        request.check_challenge()
        request.get_hatched_eggs()
        if self.inventory_timestamp:
            request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
        else:
            request.get_inventory()
        request.check_awarded_badges()
        request.download_settings(hash=DOWNLOAD_HASH)
        if incense:
            request.get_incense_pokemon(player_latitude=self.location[0],
                                        player_longitude=self.location[1])
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
            timestamp = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('new_timestamp_ms')
            self.inventory_timestamp = timestamp or self.inventory_timestamp
            download_hash = responses.get('DOWNLOAD_SETTINGS', {}).get('hash')
            DOWNLOAD_HASH = download_hash or DOWNLOAD_HASH
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
            await asyncio.sleep(20)
        if self.killed:
            return False
        self.username = self.extra_queue.get()
        self.account = ACCOUNTS[self.username]
        self.initialize_api()

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
        pokestop_location = pokestop['lat'], pokestop['lon']
        distance = great_circle(self.location, pokestop_location).meters
        if distance > 40:
            return False

        await utils.random_sleep(.6, 1.2, .75)

        request = self.api.create_request()
        request.fort_details(fort_id = pokestop['external_id'],
                             latitude = pokestop['lat'],
                             longitude = pokestop['lon'])
        responses = await self.call_chain(request)
        name = responses.get('FORT_DETAILS', {}).get('name')

        await utils.random_sleep(.6, 1.2, .75)

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
        return responses

    def swap_proxy(self, reason=''):
        self.set_proxy(random.choice(config.PROXIES))
        self.logger.warning('Swapped out {p} due to {r}.'.format(
                            p=self.proxy, r=reason))

    def swap_circuit(self, reason=''):
        if not config.CONTROL_SOCKS:
            if config.PROXIES:
                self.swap_proxy(reason=reason)
            return
        time_passed = time.time() - CIRCUIT_TIME[self.proxy]
        if time_passed > 180:
            socket = config.CONTROL_SOCKS[self.proxy]
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
        self.error_code = '#'
        if 0 not in tutorial_state:
            await utils.random_sleep(1, 5)
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=0)
            await self.call_chain(request, buddy=False)

        if 1 not in tutorial_state:
            await utils.random_sleep(5, 12)
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

            await utils.random_sleep(.3, .5)

            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=1)
            await self.call_chain(request, buddy=False)

        await utils.random_sleep(.5, .6)
        request = self.api.create_request()
        request.get_player_profile()
        await self.call_chain(request)

        starter_id = None
        if 3 not in tutorial_state:
            await utils.random_sleep(1, 1.5)
            request = self.api.create_request()
            request.get_download_urls(asset_id=['1a3c2816-65fa-4b97-90eb-0b301c064b7a/1477084786906000',
                                                'aa8f7687-a022-4773-b900-3a8c170e9aea/1477084794890000',
                                                'e89109b0-9a54-40fe-8431-12f7826c8194/1477084802881000'])
            await self.call_chain(request)

            await utils.random_sleep(1, 1.6)
            request = self.api.create_request()
            await self.loop.run_in_executor(self.network_executor, request.call)

            await utils.random_sleep(6, 13)
            request = self.api.create_request()
            starter = random.choice((1, 4, 7))
            request.encounter_tutorial_complete(pokemon_id=starter)
            await self.call_chain(request)

            await utils.random_sleep(.5, .6)
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
            await utils.random_sleep(5, 12)
            request = self.api.create_request()
            request.claim_codename(codename=self.username)
            await self.call_chain(request)

            await utils.random_sleep(1, 1.3)
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
            await utils.random_sleep(4, 10)
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=7)
            await self.call_chain(request)

        if starter_id:
            await utils.random_sleep(3, 5)
            request = self.api.create_request()
            request.set_buddy_pokemon(pokemon_id=starter_id)
            await utils.random_sleep(.8, 1.8)

        await asyncio.sleep(.2)
        return True

    async def app_simulation_login(self):
        self.error_code = 'APP SIMULATION'
        self.logger.info('Starting RPC login sequence (iOS app simulation)')

        # empty request 1
        request = self.api.create_request()
        await self.loop.run_in_executor(self.network_executor, request.call)
        await utils.random_sleep(1, 1.5, 1.172)

        # empty request 2
        request = self.api.create_request()
        await self.loop.run_in_executor(self.network_executor, request.call)
        await utils.random_sleep(1, 1.5, 1.304)

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

        responses = response.get('responses', {})
        tutorial_state = responses.get('GET_PLAYER', {}).get('player_data', {}).get('tutorial_state')

        if responses.get('GET_PLAYER', {}).get('banned', False):
            raise pgoapi_exceptions.BannedAccountException
            return False

        await utils.random_sleep(1, 1.5, 1.356)

        version = 4901
        # request 2: download_remote_config_version
        request = self.api.create_request()
        request.download_remote_config_version(platform=1, app_version=version)
        responses = await self.call_chain(request, buddy=False)

        inventory = responses.get('GET_INVENTORY', {}).get('inventory_delta', {})
        player_level = None
        for item in inventory.get('inventory_items', []):
            player_stats = item.get('inventory_item_data', {}).get('player_stats', {})
            if player_stats:
                player_level = player_stats.get('level')
                break

        await utils.random_sleep(1, 1.2, 1.072)

        # request 3: get_asset_digest
        request = self.api.create_request()
        request.get_asset_digest(platform=1, app_version=version)
        await self.call_chain(request, buddy=False)

        await utils.random_sleep(1, 2, 1.709)

        if (config.COMPLETE_TUTORIAL and
                tutorial_state is not None and
                not all(x in tutorial_state for x in (0, 1, 3, 4, 7))):
            self.logger.warning('Starting tutorial')
            await self.complete_tutorial(tutorial_state)
        else:
            # request 4: get_player_profile
            request = self.api.create_request()
            request.get_player_profile()
            await self.call_chain(request)
            await utils.random_sleep(1, 1.5, 1.326)

        if player_level:
            # request 5: level_up_rewards
            request = self.api.create_request()
            request.level_up_rewards(level=player_level)
            await self.call_chain(request)
            await utils.random_sleep(1, 1.5, 1.184)
        else:
            self.logger.warning('No player level')

        self.logger.info('Finished RPC login sequence (iOS app simulation)')
        return True

    async def login(self):
        """Logs worker in and prepares for scanning"""
        self.logger.info('Trying to log in')
        self.error_code = 'LOGIN'
        global LOGIN_SEM
        global SIMULATION_SEM

        async with LOGIN_SEM:
            await utils.random_sleep(minimum=0.5, maximum=1.5)
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

    async def visit(self, point):
        """Wrapper for self.visit_point - runs it a few times before giving up

        Also is capable of restarting in case an error occurs.
        """
        visited = False
        for attempts in range(0, 5):
            try:
                if self.killed:
                    return False
                if not self.logged_in:
                    self.api.set_position(*point)
                    if not await self.login():
                        await asyncio.sleep(2)
                        continue
                if self.killed:
                    return False
                visited = await self.visit_point(point)
            except pgoapi_exceptions.ServerSideAccessForbiddenException:
                err = 'Banned IP.'
                if self.proxy:
                    err += ' ' + self.proxy
                self.logger.error(err)
                self.error_code = 'IP BANNED'
                self.swap_circuit(reason='ban')
                await utils.random_sleep(minimum=25, maximum=35)
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
                await utils.random_sleep()
            except pgoapi_exceptions.ServerSideRequestThrottlingException:
                self.logger.info('Server throttling - sleeping for a bit')
                self.error_code = 'THROTTLE'
                await utils.random_sleep(minimum=10)
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
                self.error_code = 'MALFORMED RESPONSE'
                await utils.random_sleep()
            except Exception as err:
                self.logger.exception('A wild exception appeared!')
                self.error_code = 'EXCEPTION'
                await utils.random_sleep()
            else:
                if visited:
                    return True
                else:
                    await utils.random_sleep()
        return False

    async def visit_point(self, point):
        global GLOBAL_SEEN

        latitude, longitude, altitude = point
        altitude = random.uniform(altitude - 1, altitude + 1)
        self.error_code = '!'
        self.logger.info(
            'Visiting {0[0]:.4f},{0[1]:.4f} {0[2]:.1f}m'.format(point))
        start = time.time()
        self.location = point

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

        map_objects = responses.get('GET_MAP_OBJECTS', {})
        pokemons = []
        forts = []
        sent_notification = False

        if map_objects.get('status') != 1:
            self.error_code = 'UNKNOWNRESPONSE'
            self.logger.warning(
                'Response code: {}'.format(map_objects.get('status')))
            self.empty_visits += 1
            if self.empty_visits > 2:
                reason = '{} empty visits'.format(self.empty_visits)
                await self.swap_account(reason)
            return False
        for map_cell in map_objects['map_cells']:
            request_time_ms = map_cell['current_timestamp_ms']
            for pokemon in map_cell.get('wild_pokemons', []):
                pokemon_data = None
                # Accurate times only provided in the last 90 seconds
                invalid_tth = (
                    pokemon['time_till_hidden_ms'] < 0 or
                    pokemon['time_till_hidden_ms'] > 90000
                )
                normalized = utils.normalize_pokemon(
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

                normalized = await self.notify(normalized, pokemon)

                if normalized['valid']:
                    if (config.ENCOUNTER == 'all'
                            and 'individual_attack' not in normalized
                            and normalized not in db.SIGHTING_CACHE):
                        try:
                            normalized.update(await self.encounter(pokemon))
                        except Exception:
                            self.logger.warning('Exception during encounter.')
                    pokemons.append(normalized)

                if not normalized[
                        'valid'] or db.LONGSPAWN_CACHE.in_store(normalized):
                    long_normal = normalized.copy()
                    long_normal['type'] = 'longspawn'
                    pokemons.append(long_normal)
            for fort in map_cell.get('forts', []):
                if not fort.get('enabled'):
                    continue
                if fort.get('type') == 1:  # pokestops
                    if 'lure_info' in fort:
                        norm = utils.normalize_lured(fort, request_time_ms)
                        pokemons.append(norm)
                    pokestop = utils.normalize_pokestop(fort)
                    forts.append(pokestop)
                    if config.SPIN_POKESTOPS:
                        cooldown = fort.get('cooldown_complete_timestamp_ms', 0)
                        if not cooldown or time.time() > cooldown / 1000:
                            await self.spin_pokestop(pokestop)
                else:
                    forts.append(utils.normalize_gym(fort))

        self.db_processor.add(forts)
        pokemon_seen = len(pokemons)

        if pokemon_seen > 0:
            self.db_processor.add(pokemons)
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
                reason = '{} empty visits'.format(self.empty_visits)
                await self.swap_account(reason)
            if CIRCUIT_FAILURES:
                CIRCUIT_FAILURES[self.proxy] += 1
                if CIRCUIT_FAILURES[self.proxy] > 20:
                    reason = '{} empty visits'.format(
                        CIRCUIT_FAILURES[self.proxy])
                    self.swap_circuit(reason)

        self.visits += 1
        if not self.killed:
            self.worker_dict.update([(self.worker_no,
                ((latitude, longitude), start, self.speed, self.total_seen,
                self.visits, pokemon_seen, sent_notification))])
        self.logger.info(
            'Point processed, %d Pokemons and %d forts seen!',
            pokemon_seen,
            len(forts),
        )
        self.update_accounts_dict()
        return True

    async def notify(self, normalized, pokemon):
        if config.NOTIFY and normalized['pokemon_id'] in config.NOTIFY_IDS:
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
                self.error_code = '!'
                self.logger.warning(explanation)
        return normalized

    def travel_speed(self, point, spawn_time):
        if self.busy or self.killed:
            return None
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


class Overseer:

    def __init__(self, status_bar, loop, manager):
        self.logger = getLogger('overseer')
        self.workers = {}
        self.manager = manager
        self.count = config.GRID[0] * config.GRID[1]
        self.start_date = datetime.now()
        self.status_bar = status_bar
        self.things_count = []
        self.paused = False
        self.killed = False
        self.last_proxy = 0
        self.loop = loop
        self.db_processor = DatabaseProcessor(SPAWNS)
        self.cell_ids_executor = ThreadPoolExecutor(config.COMPUTE_THREADS)
        self.network_executor = ThreadPoolExecutor(config.NETWORK_THREADS)
        self.coroutines_count = 0
        self.skipped = 0
        self.visits = 0
        self.searches_without_shuffle = 0
        self.coroutine_limit = self.count * .9
        self.spawn_cache = db.SIGHTING_CACHE.spawn_ids
        self.redundant = 0
        self.spawns_count = 0
        self.all_seen = False
        self.idle_seconds = 0
        self.logger.info('Overseer initialized')

    def kill(self):
        self.killed = True
        print('Killing workers.')
        for worker in self.workers.values():
            worker.kill()

        global ACCOUNTS
        print('Setting CAPTCHA statuses.')

        if self.captcha_queue.empty():
            for account in ACCOUNTS.keys():
                ACCOUNTS[account]['captcha'] = False
        else:
            while not self.extra_queue.empty():
                username = self.extra_queue.get()
                ACCOUNTS[username]['captcha'] = False

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
        self.captcha_queue = self.manager.captcha_queue()
        self.extra_queue = self.manager.extra_queue()
        self.worker_dict = self.manager.worker_dict()
        for username, account in ACCOUNTS.items():
            if account.get('captcha'):
                self.captcha_queue.put(username)
            else:
                self.extra_queue.put(username)

        for worker_no in range(self.count):
            self.start_worker(worker_no, first_run=True)
        self.workers_list = list(self.workers.values())
        self.db_processor.start()

    def check(self):
        last_cleaned_cache = time.time()
        last_things_found_updated = time.time()
        last_stats_updated = 0

        while not self.killed:
            now = time.time()
            # Clean cache
            if now - last_cleaned_cache > 900:  # clean cache after 15min
                self.db_processor.clean_cache()
                last_cleaned_cache = now
            # Record things found count
            if now - last_stats_updated >= 5:
                self.seen_stats, self.visit_stats, self.delay_stats, self.speed_stats = self.get_visit_stats()
                last_stats_updated = now
            if not self.paused and now - last_things_found_updated >= 10:
                self.things_count = self.things_count[-9:]
                self.things_count.append(str(self.db_processor.count))
                last_things_found_updated = now
            if self.status_bar:
                if platform == 'win32':
                    _ = system('cls')
                else:
                    _ = system('clear')
                print(self.get_status_message())
            time.sleep(.5)
            if self.paused:
                time.sleep(15)
        # OK, now we're killed
        while True:
            self.update_coroutines_count()
            # Spaces at the end are important, as they clear previously printed
            # output - \r doesn't clean whole line
            print(
                '{} coroutines active   '.format(self.coroutines_count),
                end='\r'
            )
            if self.coroutines_count == 0 or not isinstance(self.coroutines_count, int):
                print('Done.                ')
                return

    @staticmethod
    def generate_stats(somelist):
        return {
            'max': max(somelist),
            'min': min(somelist),
            'med': median(somelist)
        }

    def get_visit_stats(self):
        visits = []
        seconds_since_start = time.time() - START_TIME - self.idle_seconds
        hours_since_start = seconds_since_start / 3600
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
            delay_stats = {'min': 0, 'max': 0, 'med': 0}
        seen_stats = self.generate_stats(seen_per_worker)
        visit_stats = self.generate_stats(visits)
        speed_stats = self.generate_stats(speeds)
        return seen_stats, visit_stats, delay_stats, speed_stats

    def get_dots_and_messages(self):
        """Returns status dots and status messages for workers

        Dots meaning:
        . = visited more than a minute ago
        , = visited less than a minute ago, nothing seen
        : = visited less than a minute ago, pokemon seen
        ! = currently visiting
        * = sending a notification
        ~ = waiting to encounter
        E = currently encountering
        I = initial, haven't done anything yet
        L = logging in
        A = simulating app startup
        X = something bad happened
        C = CAPTCHA

        Other letters: various errors and procedures
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

    def update_coroutines_count(self):
        try:
            self.coroutines_count = sum(not t.done()
                                    for t in asyncio.Task.all_tasks(self.loop))
        except RuntimeError:
            # Set changed size during iteration
            self.coroutines_count = '?'

    def get_status_message(self):
        workers_count = len(self.workers)

        running_for = datetime.now() - self.start_date

        seconds_since_start = time.time() - START_TIME - self.idle_seconds
        hours_since_start = seconds_since_start / 3600
        visits_per_second = self.visits / seconds_since_start

        output = [
            'PokeMiner running for {}'.format(running_for),
            'Total spawns: {}'.format(self.spawns_count),
            '{w} workers, {t} threads, {c} coroutines'.format(
                w=workers_count,
                t=active_count(),
                c=self.coroutines_count),
            '',
            'Seen per worker: min {min}, max {max}, med {med:.0f}'.format(
                **self.seen_stats),
            'Visits per worker: min {min}, max {max:}, med {med:.0f}'.format(
                **self.visit_stats),
            'Visit delay: min {min:.1f}, max {max:.1f}, med {med:.1f}'.format(
                **self.delay_stats),
            'Speed: min {min:.1f}, max {max:.1f}, med {med:.1f}'.format(
                **self.speed_stats),
            'Extra accounts: {a}, CAPTCHAs needed: {c}'.format(
                a=self.extra_queue.qsize(),
                c=self.captcha_queue.qsize()),
            '',
            'Pokemon found count (10s interval):',
            ' '.join(self.things_count),
            '',
            'Visits: {v}, per second: {ps:.2f}'.format(
                v=self.visits,
                ps=visits_per_second),
            'Skipped: {s}, unnecessary: {u}'.format(
                s=self.skipped,
                u=self.redundant)
        ]

        try:
            output.append('Seen per visit: {v:.2f}, per minute: {m:.0f}'.format(
                v=GLOBAL_SEEN / self.visits,
                m=GLOBAL_SEEN / (seconds_since_start / 60)
            ))

            if CAPTCHAS:
                captchas_per_request = CAPTCHAS / (self.visits / 1000)
                captchas_per_hour = CAPTCHAS / hours_since_start
                output.append(
                    'CAPTCHAs per 1K visits: {r:.1f}, per hour: {h:.1f}'.format(
                    r=captchas_per_request, h=captchas_per_hour))
        except ZeroDivisionError:
            pass

        if NOTIFICATIONS_SENT:
            output.append('Notifications sent: {n}, per hour {p:.1f}'.format(
            n=NOTIFICATIONS_SENT, p=NOTIFICATIONS_SENT / hours_since_start))

        output.append('')
        if not self.all_seen:
            no_sightings = ', '.join(str(w.worker_no)
                                     for w in self.workers.values()
                                     if w.total_seen == 0)
            if no_sightings:
                output += ['Workers without sightings so far:', no_sightings, '']
            else:
                self.all_seen = True

        dots, messages = self.get_dots_and_messages()
        output += [' '.join(row) for row in dots]
        previous = 0
        for i in range(4, len(messages) + 4, 4):
            output.append('\t'.join(messages[previous:i]))
            previous = i
        if self.paused:
            output += ('', 'CAPTCHAs are needed to proceed.')
        return '\n'.join(output)

    async def best_worker(self, point, spawn_time, give_up=False):
        worker = None
        lowest_speed = float('inf')
        self.searches_without_shuffle += 1
        if self.searches_without_shuffle > 30:
            random.shuffle(self.workers_list)
            self.searches_without_shuffle = 0
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
                    if speed < 10:
                        break
            if worker.busy:
                worker = None
            if lowest_speed > config.SPEED_LIMIT or worker is None:
                time_diff = spawn_time - time.time()
                if time_diff < -60:
                    return False, False
                await asyncio.sleep(3)
        return worker, lowest_speed

    def launch(self):
        global SPAWNS
        while not self.killed:
            if self.visits > 0:
                utils.dump_pickle('accounts', ACCOUNTS)
                SPAWNS.update_spawns()
            else:
                SPAWNS.update_spawns(loadpickle=True)

            self.spawns_count = len(SPAWNS.spawns)
            current_hour = utils.get_current_hour()
            for spawn_id, spawn in SPAWNS.spawns.items():
                try:
                    self.update_coroutines_count()
                    while (self.coroutines_count > self.coroutine_limit or not
                            isinstance(self.coroutines_count, int)) and not self.killed:
                        time.sleep(1)
                        self.update_coroutines_count()

                    while self.captcha_queue.qsize() > config.MAX_CAPTCHAS and not self.killed:
                        self.paused = True
                        time.sleep(10)
                        self.idle_seconds += 10
                except IOError:
                    pass
                except Exception as e:
                    self.logger.error(e)

                if self.killed:
                    return

                self.paused = False

                point = list(spawn[0])
                spawn_time = spawn[1] + current_hour

                # negative = already happened
                # positive = hasn't happened yet
                time_diff = spawn_time - time.time()
                if self.visits == 0 and (time_diff < -10 or time_diff > 10):
                    continue
                elif time_diff < -10 and spawn_id in self.spawn_cache:
                    self.redundant += 1
                elif time_diff < -180:
                    self.skipped += 1
                    continue
                elif time_diff > 90:
                    time.sleep(30)

                asyncio.run_coroutine_threadsafe(
                    self.try_point(point, spawn_time), loop=self.loop
                )

    async def try_point(self, point, spawn_time):
        point[0] = random.uniform(point[0] - 0.00033, point[0] + 0.00033)
        point[1] = random.uniform(point[1] - 0.00033, point[1] + 0.00033)
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

        if await worker.visit(point):
            self.visits += 1
        worker.busy = False


if __name__ == '__main__':
    START_TIME = time.time()
    GLOBAL_SEEN = 0
    CAPTCHAS = 0
    NOTIFICATIONS_SENT = 0
    DOWNLOAD_HASH = "d3da400db60abf79ea05abc38e2396f0bbd453f9"

    try:
        makedirs('pickles')
    except OSError:
        pass

    CELL_IDS = utils.load_pickle('cells') or {}
    ACCOUNTS = load_accounts()

    SPAWNS = Spawns()

    args = parse_args()
    logger = getLogger()
    if args.status_bar:
        configure_logger(filename='worker.log')
        logger.info('-' * 30)
        logger.info('Starting up!')
    else:
        configure_logger(filename=None)
    logger.setLevel(args.log_level)

    AccountManager.register('captcha_queue', callable=get_captchas)
    AccountManager.register('extra_queue', callable=get_extras)
    AccountManager.register('worker_dict', callable=get_workers,
                            proxytype=DictProxy)
    manager = AccountManager(address=utils.get_address(), authkey=config.AUTHKEY)
    manager.start(mgr_init)

    if config.NOTIFY:
        import notification
        notifier = notification.Notifier(SPAWNS)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    LOGIN_SEM = asyncio.BoundedSemaphore(1, loop=loop)
    SIMULATION_SEM = asyncio.BoundedSemaphore(2, loop=loop)

    overseer = Overseer(status_bar=args.status_bar, loop=loop, manager=manager)
    overseer.start()
    overseer_thread = Thread(target=overseer.check, name='overseer')
    overseer_thread.start()

    launcher_thread = Thread(target=overseer.launch, name='launcher')
    launcher_thread.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Exiting, please wait until all tasks finish')
        overseer.kill()

        print('Dumping pickles.')
        utils.dump_pickle('accounts', ACCOUNTS)
        utils.dump_pickle('cells', CELL_IDS)

        pending = asyncio.Task.all_tasks(loop=loop)
        print('Completing tasks.    ')
        loop.run_until_complete(asyncio.gather(*pending))
        print('Shutting things down.')
        overseer.cell_ids_executor.shutdown()
        overseer.network_executor.shutdown()
        overseer.db_processor.stop()
        if config.NOTIFY:
            notifier.session.close()
        SPAWNS.session.close()
        manager.shutdown()
        print('Stopping and closing loop.')
        loop.stop()
        loop.close()
