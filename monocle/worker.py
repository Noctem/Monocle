from functools import partial
from pogo_async import PGoApi, exceptions as ex
from pogo_async.auth_ptc import AuthPtc
from pogo_async.utilities import get_cell_ids
from pogo_async.hash_server import HashServer
from asyncio import sleep, Lock, Semaphore, get_event_loop
from random import choice, randint, uniform, triangular
from time import time, monotonic
from array import typecodes
from queue import Empty
from aiohttp import ClientSession

from .db import SIGHTING_CACHE, MYSTERY_CACHE, Bounds
from .utils import random_sleep, round_coords, load_pickle, load_accounts, get_device_info, get_spawn_id, get_distance, get_start_coords
from . import config, shared

try:
    import _thread
except ImportError as e:
    if config.FORCED_KILL:
        raise OSError('Your platform does not support _thread so FORCED_KILL will not work.') from e
    import _dummy_thread as _thread

if config.NOTIFY:
    from .notification import Notifier

if config.CONTROL_SOCKS:
    from stem import Signal
    from stem.control import Controller
    import stem.util.log
    stem.util.log.get_logger().level = 40
    CIRCUIT_TIME = dict()
    CIRCUIT_FAILURES = dict()
    for proxy in config.PROXIES:
        CIRCUIT_TIME[proxy] = monotonic()
        CIRCUIT_FAILURES[proxy] = 0
else:
    CIRCUIT_TIME = None
    CIRCUIT_FAILURES = None


class Worker:
    """Single worker walking on the map"""

    download_hash = "1de302dba2e542b8db8250455fa3e340d78c86f3"
    g = {'seen': 0, 'captchas': 0}

    accounts = load_accounts()
    if config.CACHE_CELLS:
        cell_ids = load_pickle('cells') or {}
        COMPACT = 'Q' in typecodes

    loop = get_event_loop()
    login_semaphore = Semaphore(config.SIMULTANEOUS_LOGINS)
    sim_semaphore = Semaphore(config.SIMULTANEOUS_SIMULATION)

    proxies = None
    proxy = None
    if config.PROXIES:
        if len(config.PROXIES) == 1:
            proxy = config.PROXIES.pop()
        else:
            proxies = config.PROXIES.copy()

    if config.NOTIFY:
        notifier = Notifier()
        g['sent'] = 0

    def __init__(self, worker_no):
        self.worker_no = worker_no
        self.log = shared.get_logger('worker-{}'.format(worker_no))
        # account information
        try:
            self.account = self.extra_queue.get_nowait()
        except Empty as e:
            try:
                self.account = self.captcha_queue.get_nowait()
            except Empty as e:
                raise ValueError("You don't have enough accounts for the number of workers specified in GRID.") from e
        self.username = self.account['username']
        self.location = self.account.get('location', get_start_coords(worker_no))
        self.inventory_timestamp = self.account.get('inventory_timestamp')
        # last time of any request
        self.last_request = self.account.get('time', 0)
        # last time of a request that requires user interaction in the game
        self.last_action = self.last_request
        # last time of a GetMapObjects request
        self.last_gmo = self.last_request
        self.items = self.account.get('items', {})
        self.player_level = self.account.get('player_level')
        self.num_captchas = 0
        self.eggs = {}
        self.unused_incubators = []
        # API setup
        if self.proxies:
            self.new_proxy(set_api=False)
        self.initialize_api()
        # State variables
        self.busy = BusyLock()
        self.killed = False
        # Other variables
        self.after_spawn = None
        self.speed = 0
        self.account_start = None
        self.total_seen = 0
        self.error_code = 'INIT'
        self.item_capacity = 350
        self.visits = 0
        self.pokestops = config.SPIN_POKESTOPS
        self.next_spin = 0

    def initialize_api(self):
        device_info = get_device_info(self.account)
        self.logged_in = False
        self.ever_authenticated = False
        self.empty_visits = 0
        self.account_seen = 0

        self.api = PGoApi(device_info=device_info)
        if config.HASH_KEY:
            self.api.activate_hash_server(config.HASH_KEY)
        self.api.set_position(*self.location)
        if self.proxy:
            self.api.set_proxy(self.proxy)
        try:
            if self.account['provider'] == 'ptc' and 'auth' in self.account:
                self.api._auth_provider = AuthPtc(username=self.username, password=self.account['password'], timeout=config.LOGIN_TIMEOUT)
                self.api._auth_provider._access_token = self.account['auth']
                self.api._auth_provider.set_refresh_token(self.account['refresh'])
                self.api._auth_provider._access_token_expiry = self.account['expiry']
                if self.api._auth_provider.check_access_token():
                    self.api._auth_provider._login = True
                    self.logged_in = True
                    self.ever_authenticated = True
        except KeyError:
            pass

    def new_proxy(self, set_api=True):
        self.proxy = self.proxies.pop()
        if not self.proxies:
            self.proxies.update(config.PROXIES)
        if set_api:
            self.api.set_proxy(self.proxy)

    def swap_circuit(self, reason=''):
        time_passed = monotonic() - CIRCUIT_TIME[self.proxy]
        if time_passed > 180:
            socket = config.CONTROL_SOCKS[self.proxy]
            with Controller.from_socket_file(path=socket) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
            CIRCUIT_TIME[self.proxy] = monotonic()
            CIRCUIT_FAILURES[self.proxy] = 0
            self.log.warning('Changed circuit on {} due to {}.', self.proxy, reason)
        else:
            self.log.info('Skipped changing circuit on {} because it was '
                          'changed {} seconds ago.', self.proxy, time_passed)

    async def login(self):
        """Logs worker in and prepares for scanning"""
        self.log.info('Trying to log in')

        for attempt in range(-1, config.MAX_RETRIES):
            try:
                self.error_code = '»'
                async with self.login_semaphore:
                    self.error_code = 'LOGIN'
                    await self.api.set_authentication(
                        username=self.username,
                        password=self.account['password'],
                        provider=self.account.get('provider', 'ptc'),
                        timeout=config.LOGIN_TIMEOUT
                    )
            except (ex.AuthTimeoutException, ex.AuthConnectionException) as e:
                err = e
                await sleep(2)
            else:
                err = None
                break
        if err:
            raise err

        self.error_code = '°'
        version = 5500
        async with self.sim_semaphore:
            if self.killed:
                return False
            self.error_code = 'APP SIMULATION'
            if config.APP_SIMULATION and not self.ever_authenticated:
                await self.app_simulation_login(version)
            else:
                await self.download_remote_config(version)

        self.ever_authenticated = True
        self.logged_in = True
        self.error_code = None
        self.account_start = time()
        return True

    async def download_remote_config(self, version):
        request = self.api.create_request()
        request.download_remote_config_version(platform=1, app_version=version)
        responses = await self.call(request, stamp=False, buddy=False, settings=True, dl_hash=False)

        inventory_items = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('inventory_items', [])
        for item in inventory_items:
            player_stats = item.get('inventory_item_data', {}).get('player_stats', {})
            if player_stats:
                self.player_level = player_stats.get('level') or self.player_level
                break
        await random_sleep(.78, .95)

    async def set_avatar(self, tutorial=False):
        await random_sleep(7, 14)
        request = self.api.create_request()

        gender = randint(0, 1)
        if gender == 1:
            # female
            shirt = randint(0, 8)
            pants = randint(0, 5)
            backpack = randint(0, 2)
        else:
            # male
            shirt = randint(0, 3)
            pants = randint(0, 2)
            backpack = randint(0, 5)

        request.set_avatar(player_avatar={
                'skin': randint(0, 3),
                'hair': randint(0, 5),
                'shirt': shirt,
                'pants': pants,
                'hat': randint(0, 4),
                'shoes': randint(0, 6),
                'avatar': gender,
                'eyes': randint(0, 4),
                'backpack': backpack
            })
        await self.call(request, buddy=not tutorial, action=1)

        if tutorial:
            await random_sleep(.3, .5)

            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=1)
            await self.call(request, buddy=False)
            await random_sleep(2.5, 2.75)
        else:
            await random_sleep(1, 1.2)

        request = self.api.create_request()
        request.get_player_profile()
        await self.call(request, action=1)

    async def app_simulation_login(self, version):
        self.log.info('Starting RPC login sequence (iOS app simulation)')
        reset_avatar = False

        # empty request
        request = self.api.create_request()
        await self.call(request, chain=False)
        await sleep(.5)

        # request 1: get_player
        request = self.api.create_request()
        request.get_player(player_locale=config.PLAYER_LOCALE)

        responses = await self.call(request, chain=False)

        tutorial_state = None
        try:
            get_player = responses['GET_PLAYER']

            if get_player.get('banned', False):
                raise ex.BannedAccountException

            player_data = get_player['player_data']
            tutorial_state = player_data.get('tutorial_state', [])
            self.item_capacity = player_data['max_item_storage']
            if 'created' not in self.account:
                self.account['created'] = player_data['creation_timestamp_ms'] / 1000
            avatar = player_data['avatar']
            if avatar['avatar'] == 1 and avatar['backpack'] > 2:
                self.log.warning('Invalid backpack for female, resetting avatar.')
                reset_avatar = True
        except (KeyError, TypeError, AttributeError):
            pass

        await random_sleep(.9, 1.2)

        # request 2: download_remote_config_version
        await self.download_remote_config(version)

        # request 3: get_asset_digest
        request = self.api.create_request()
        request.get_asset_digest(platform=1, app_version=version)
        await self.call(request, buddy=False, settings=True)

        await random_sleep(.9, 3.1)

        if (config.COMPLETE_TUTORIAL and
                tutorial_state is not None and
                not all(x in tutorial_state for x in (0, 1, 3, 4, 7))):
            self.log.warning('{} is starting tutorial', self.username)
            await self.complete_tutorial(tutorial_state)
        else:
            # request 4: get_player_profile
            request = self.api.create_request()
            request.get_player_profile()
            await self.call(request, settings=True)
            await random_sleep(.3, .5)

            if self.player_level:
                # request 5: level_up_rewards
                request = self.api.create_request()
                request.level_up_rewards(level=self.player_level)
                await self.call(request, settings=True)
                await random_sleep(.45, .7)
            else:
                self.log.warning('No player level')

            # request 6: register_background_device
            request = self.api.create_request()
            request.register_background_device(device_type='apple_watch')
            await self.call(request, action=0.1)

            self.log.info('Finished RPC login sequence (iOS app simulation)')
            if reset_avatar:
                await self.set_avatar()

            await random_sleep(.2, .462)
        self.error_code = None
        return True

    async def complete_tutorial(self, tutorial_state):
        self.error_code = 'TUTORIAL'
        if 0 not in tutorial_state:
            # legal screen
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=[0])
            await self.call(request, buddy=False)

            await random_sleep(.475, .525)

            request = self.api.create_request()
            request.get_player(player_locale=config.PLAYER_LOCALE)
            await self.call(request, buddy=False)
            await sleep(1)

        if 1 not in tutorial_state:
            # avatar selection
            await self.set_avatar(tutorial=True)

        await random_sleep(.5, .6)
        request = self.api.create_request()
        await self.call(request, chain=False)

        await sleep(.05)

        request = self.api.create_request()
        request.register_background_device(device_type='apple_watch')
        await self.call(request)

        starter_id = None
        if 3 not in tutorial_state:
            # encounter tutorial
            await sleep(1)
            request = self.api.create_request()
            request.get_download_urls(asset_id=['1a3c2816-65fa-4b97-90eb-0b301c064b7a/1477084786906000',
                                                'aa8f7687-a022-4773-b900-3a8c170e9aea/1477084794890000',
                                                'e89109b0-9a54-40fe-8431-12f7826c8194/1477084802881000'])
            await self.call(request)

            await random_sleep(5, 10)
            request = self.api.create_request()
            starter = choice((1, 4, 7))
            request.encounter_tutorial_complete(pokemon_id=starter)
            await self.call(request, action=1)

            await random_sleep(.4, .55)
            request = self.api.create_request()
            request.get_player(player_locale=config.PLAYER_LOCALE)
            responses = await self.call(request)

            inventory = responses.get('GET_INVENTORY', {}).get('inventory_delta', {}).get('inventory_items', [])
            for item in inventory:
                pokemon = item.get('inventory_item_data', {}).get('pokemon_data')
                if pokemon:
                    starter_id = pokemon.get('id')


        if 4 not in tutorial_state:
            # name selection
            await random_sleep(10, 16)
            request = self.api.create_request()
            request.claim_codename(codename=self.username)
            await self.call(request, action=1)

            await random_sleep(1, 1.3)
            request = self.api.create_request()
            request.get_player(player_locale=config.PLAYER_LOCALE)
            await self.call(request)
            await sleep(.1)

            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=4)
            await self.call(request, buddy=False)

        if 7 not in tutorial_state:
            # first time experience
            await random_sleep(3.75, 4.5)
            request = self.api.create_request()
            request.mark_tutorial_complete(tutorials_completed=7)
            await self.call(request)

        if starter_id:
            await random_sleep(3, 5)
            request = self.api.create_request()
            request.set_buddy_pokemon(pokemon_id=starter_id)
            await self.call(request, action=1)
            await random_sleep(.8, 1.2)

        await sleep(.2)
        return True

    def update_inventory(self, inventory_items):
        for thing in inventory_items:
            obj = thing.get('inventory_item_data', {})
            if 'item' in obj:
                item = obj['item']
                item_id = item.get('item_id')
                self.items[item_id] = item.get('count', 0)
            elif config.INCUBATE_EGGS:
                if ('pokemon_data' in obj and
                        obj['pokemon_data'].get('is_egg')):
                    egg = obj['pokemon_data']
                    egg_id = egg.get('id')
                    self.eggs[egg_id] = egg
                elif 'egg_incubators' in obj:
                    self.unused_incubators = []
                    for item in obj['egg_incubators'].get('egg_incubator',[]):
                        if 'pokemon_id' in item:
                            continue
                        if item.get('item_id') == 901:
                            self.unused_incubators.append(item)
                        else:
                            self.unused_incubators.insert(0, item)

    async def call(self, request, chain=True, stamp=True, buddy=True, settings=False, dl_hash=True, action=None):
        if chain:
            request.check_challenge()
            request.get_hatched_eggs()
            if stamp and self.inventory_timestamp:
                request.get_inventory(last_timestamp_ms=self.inventory_timestamp)
            else:
                request.get_inventory()
            request.check_awarded_badges()
            if settings:
                if dl_hash:
                    request.download_settings(hash=self.download_hash)
                else:
                    request.download_settings()
            if buddy:
                request.get_buddy_walked()

        try:
            refresh = HashServer.status.get('period')

            while HashServer.status.get('remaining') < 5 and time() < refresh:
                self.error_code = 'HASH WAITING'
                wait = refresh - time() + 1
                await sleep(wait)
                refresh = HashServer.status.get('period')
        except TypeError:
            pass

        now = time()
        if action:
            # wait for the time required, or at least a half-second
            if self.last_action > now + .5:
                await sleep(self.last_action - now)
            else:
                await sleep(0.5)

        response = None
        err = None
        for attempt in range(-1, config.MAX_RETRIES):
            try:
                response = await request.call()
                if response:
                    err = None
                    break
                else:
                    raise ex.MalformedResponseException('empty response')
            except (ex.NotLoggedInException, ex.AuthException) as e:
                self.log.info('Auth error on {}: {}', self.username, e)
                err = e
                self.logged_in = False
                await self.login()
                await sleep(2)
            except ex.HashingOfflineException as e:
                if err != e:
                    err = e
                    self.log.warning('{}', e)
                self.error_code = 'HASHING OFFLINE'
                await sleep(7.5)
            except ex.NianticOfflineException as e:
                if err != e:
                    err = e
                    self.log.warning('{}', e)
                self.error_code = 'NIANTIC OFFLINE'
                await random_sleep()
            except ex.HashingQuotaExceededException as e:
                if err != e:
                    err = e
                    self.log.warning('Exceeded your hashing quota, sleeping.')
                self.error_code = 'QUOTA EXCEEDED'
                refresh = HashServer.status.get('period')
                now = time()
                if refresh:
                    if refresh > now:
                        await sleep(refresh - now + 1)
                    else:
                        await sleep(5)
                else:
                    await sleep(30)
            except ex.NianticThrottlingException as e:
                if err != e:
                    err = e
                    self.log.warning('{}', e)
                self.error_code = 'THROTTLE'
                await random_sleep(11, 22, 12)
            except ex.ProxyException as e:
                if err != e:
                    err = e
                self.error_code = 'PROXY ERROR'

                if self.proxies:
                    self.log.error('{}, swapping proxy.', e)
                    proxy = self.proxy
                    while proxy == self.proxy:
                        self.new_proxy()
                else:
                    if err != e:
                        self.log.error('{}', e)
                    await sleep(5)
            except (ex.MalformedResponseException, ex.UnexpectedResponseException) as e:
                if err != e:
                    self.log.warning('{}', e)
                self.error_code = 'MALFORMED RESPONSE'
                await random_sleep(10, 14, 11)
        if err is not None:
            raise err

        self.last_request = time()
        if action:
            # pad for time that action would require
            self.last_action = self.last_request + action

        responses = response.get('responses')
        if chain:
            try:
                if (settings and config.FORCED_KILL and
                        responses['DOWNLOAD_SETTINGS']['settings']['minimum_client_version'] not in config.FORCED_KILL):
                    err = 'A new version is being forced, exiting.'
                    self.log.error(err)
                    print(err)
                    _thread.interrupt_main()
                    self.kill()
            except KeyError:
                pass
            delta = responses.get('GET_INVENTORY', {}).get('inventory_delta', {})
            timestamp = delta.get('new_timestamp_ms')
            inventory_items = delta.get('inventory_items', [])
            if inventory_items:
                self.update_inventory(inventory_items)
            self.inventory_timestamp = timestamp or self.inventory_timestamp
            d_hash = responses.get('DOWNLOAD_SETTINGS', {}).get('hash')
            self.download_hash = d_hash or self.download_hash
            if self.check_captcha(responses):
                self.log.warning('{} has encountered a CAPTCHA, trying to solve', self.username)
                self.g['captchas'] += 1
                await self.handle_captcha(responses)
        return responses

    def travel_speed(self, point):
        '''Fast calculation of travel speed to point'''
        if self.busy.locked():
            return None
        time_diff = max(time() - self.last_request, config.SCAN_DELAY)
        if time_diff > 60:
            self.error_code = None
        distance = get_distance(self.location, point)
        # conversion from meters/second to miles/hour
        speed = (distance / time_diff) * 2.236936
        return speed

    async def bootstrap_visit(self, point):
        for _ in range(0,3):
            if await self.visit(point, bootstrap=True):
                return True
            self.error_code = '∞'
            self.simulate_jitter(0.00005)
        return False

    async def visit(self, point, bootstrap=False):
        """Wrapper for self.visit_point - runs it a few times before giving up

        Also is capable of restarting in case an error occurs.
        """
        visited = False
        try:
            altitude = shared.SPAWNS.get_altitude(point)
            altitude = uniform(altitude - 1, altitude + 1)
            self.location = point + [altitude]
            self.api.set_position(*self.location)
            if not self.logged_in:
                if not await self.login():
                    return False
            return await self.visit_point(point, bootstrap=bootstrap)
        except ex.NotLoggedInException:
            self.error_code = 'NOT AUTHENTICATED'
            await sleep(1)
            if not await self.login():
                await self.swap_account(reason='login failed')
            return await self.visit(point, bootstrap)
        except ex.AuthException as e:
            self.log.warning('Auth error on {}: {}', self.username, e)
            self.error_code = 'NOT AUTHENTICATED'
            await sleep(3)
            await self.swap_account(reason='login failed')
        except CaptchaException:
            self.error_code = 'CAPTCHA'
            self.g['captchas'] += 1
            await sleep(1)
            await self.bench_account()
        except CaptchaSolveException:
            self.error_code = 'CAPTCHA'
            await sleep(1)
            await self.swap_account(reason='solving CAPTCHA failed')
        except ex.TempHashingBanException:
            self.error_code = 'HASHING BAN'
            self.log.error('Temporarily banned from hashing server for using invalid keys.')
            await sleep(185)
        except ex.BannedAccountException:
            self.error_code = 'BANNED'
            self.log.warning('{} is banned', self.username)
            await sleep(1)
            await self.remove_account()
        except ex.ProxyException as e:
            self.error_code = 'PROXY ERROR'

            if self.proxies:
                self.log.error('{} Swapping proxy.', e)
                proxy = self.proxy
                while proxy == self.proxy:
                    self.new_proxy()
            else:
                self.log.error('{}', e)
            await sleep(5)
        except ex.NianticIPBannedException:
            self.error_code = 'IP BANNED'

            if config.CONTROL_SOCKS:
                self.swap_circuit('IP ban')
                await random_sleep(minimum=25, maximum=35)
            elif self.proxies:
                self.log.warning('Swapping out {} due to IP ban.', self.proxy)
                proxy = self.proxy
                while proxy == self.proxy:
                    self.new_proxy()
                await random_sleep(minimum=12, maximum=20)
            else:
                self.log.error('IP banned.')
                self.kill()
        except ex.ServerBusyOrOfflineException as e:
            self.log.warning('{}. Giving up.', e)
        except ex.NianticThrottlingException as e:
            self.log.warning('{}. Giving up.', e)
        except ex.ExpiredHashKeyException:
            self.error_code = 'KEY EXPIRED'
            err = 'Hash key has expired: {}'.format(config.HASH_KEY)
            self.log.error(err)
            print(err)
            _thread.interrupt_main()
            self.kill()
        except ex.HashServerException as e:
            self.log.warning('{}', e)
            self.error_code = 'HASHING ERROR'
        except ex.PgoapiError as e:
            self.log.exception(e.__class__.__name__)
            self.error_code = 'PGOAPI ERROR'
        except Exception as e:
            self.log.exception('A wild {} appeared!', e.__class__.__name__)
            self.error_code = 'EXCEPTION'
        await sleep(1)
        return False

    async def visit_point(self, point, bootstrap=False):
        if bootstrap:
            self.error_code = '∞'
        else:
            self.error_code = '!'
        latitude, longitude = point
        self.log.info('Visiting {0[0]:.4f},{0[1]:.4f}', point)
        start = time()

        if config.CACHE_CELLS:
            rounded = round_coords(point, 4)
            try:
                cell_ids = self.cell_ids[rounded]
            except KeyError:
                cell_ids = get_cell_ids(*rounded, compact=self.COMPACT)
                self.cell_ids[rounded] = cell_ids
        else:
            cell_ids = get_cell_ids(latitude, longitude)

        since_timestamp_ms = (0,) * len(cell_ids)

        request = self.api.create_request()
        request.get_map_objects(cell_id=cell_ids,
                                since_timestamp_ms=since_timestamp_ms,
                                latitude=latitude,
                                longitude=longitude)

        diff = self.last_gmo + config.SCAN_DELAY - time()
        if diff > 0:
            await sleep(diff + .25)
        responses = await self.call(request)
        self.last_gmo = time()

        try:
            map_objects = responses['GET_MAP_OBJECTS']

            map_status = map_objects['status']
            if map_status == 3:
                raise ex.BannedAccountException('GMO code 3')
            elif map_status != 1:
                error = 'GetMapObjects code: {}'.format(map_status)
                self.log.warning(error)
                self.empty_visits += 1
                if self.empty_visits > 3:
                    reason = '{} empty visits'.format(self.empty_visits)
                    await self.swap_account(reason)
                raise ex.UnexpectedResponseException(error)
        except KeyError:
            raise ex.UnexpectedResponseException('Bad MapObjects response.')

        sent = False
        pokemon_seen = 0
        forts_seen = 0
        points_seen = 0

        time_of_day = map_objects.get('time_of_day', 0)

        if config.ITEM_LIMITS and self.bag_full():
            await self.clean_bag()

        for map_cell in map_objects['map_cells']:
            request_time_ms = map_cell['current_timestamp_ms']
            for pokemon in map_cell.get('wild_pokemons', []):
                pokemon_seen += 1

                normalized = self.normalize_pokemon(pokemon)

                if config.NOTIFY and self.notifier.eligible(normalized):
                    if config.ENCOUNTER:
                        try:
                            await self.encounter(normalized)
                        except Exception:
                            self.log.exception('Exception during encounter.')
                    sent = self.notify(normalized, time_of_day) or sent

                if (normalized not in SIGHTING_CACHE and
                        normalized not in MYSTERY_CACHE):
                    self.account_seen += 1
                    if (config.ENCOUNTER == 'all' and
                            'individual_attack' not in normalized):
                        try:
                            await self.encounter(normalized)
                        except Exception:
                            self.log.exception('Exception during encounter.')
                shared.DB.add(normalized)

            for fort in map_cell.get('forts', []):
                if not fort.get('enabled'):
                    continue
                forts_seen += 1
                if fort.get('type') == 1:  # pokestops
                    if 'lure_info' in fort:
                        norm = self.normalize_lured(fort, request_time_ms)
                        pokemon_seen += 1
                        if norm not in SIGHTING_CACHE:
                            self.account_seen += 1
                        shared.DB.add(norm)
                    pokestop = self.normalize_pokestop(fort)
                    shared.DB.add(pokestop)
                    if self.pokestops and not self.bag_full() and time() > self.next_spin:
                        cooldown = fort.get('cooldown_complete_timestamp_ms')
                        if not cooldown or time() > cooldown / 1000:
                            await self.spin_pokestop(pokestop)
                else:
                    shared.DB.add(self.normalize_gym(fort))

            if config.MORE_POINTS or bootstrap:
                for point in map_cell.get('spawn_points', []):
                    points_seen += 1
                    try:
                        p = (point['latitude'], point['longitude'])
                        if shared.SPAWNS.have_point(p) or not Bounds.contain(p):
                            continue
                        shared.SPAWNS.add_cell_point(p)
                    except (KeyError, TypeError):
                        self.log.warning('Spawn point exception ignored. {}', point)
                        pass

        if config.INCUBATE_EGGS and len(self.unused_incubators) > 0 and len(self.eggs) > 0:
            await self.incubate_eggs()

        if pokemon_seen > 0:
            self.error_code = ':'
            self.total_seen += pokemon_seen
            self.g['seen'] += pokemon_seen
            self.empty_visits = 0
            if CIRCUIT_FAILURES:
                CIRCUIT_FAILURES[self.proxy] = 0
        else:
            self.empty_visits += 1
            if forts_seen == 0:
                self.error_code = '0 SEEN'
            else:
                self.error_code = ','
            if self.empty_visits > 3:
                reason = '{} empty visits'.format(self.empty_visits)
                await self.swap_account(reason)
            if CIRCUIT_FAILURES:
                CIRCUIT_FAILURES[self.proxy] += 1
                if CIRCUIT_FAILURES[self.proxy] > 20:
                    reason = '{} empty visits'.format(
                        CIRCUIT_FAILURES[self.proxy])
                    self.swap_circuit(reason)

        self.visits += 1
        if config.MAP_WORKERS:
            self.worker_dict.update([(self.worker_no,
                ((latitude, longitude), start, self.speed, self.total_seen,
                self.visits, pokemon_seen, sent))])
        self.log.info(
            'Point processed, {} Pokemon and {} forts seen!',
            pokemon_seen,
            forts_seen,
        )
        self.update_accounts_dict(auth=False)
        return pokemon_seen + forts_seen + points_seen

    async def spin_pokestop(self, pokestop):
        self.error_code = '$'
        pokestop_location = pokestop['lat'], pokestop['lon']
        distance = get_distance(self.location, pokestop_location)
        # permitted interaction distance - 2 (for some jitter leeway)
        # estimation of spinning speed limit
        if distance > 38 or self.speed > 22:
            return False

        # randomize location up to ~1.4 meters
        self.simulate_jitter(amount=0.00001)

        request = self.api.create_request()
        request.fort_details(fort_id = pokestop['external_id'],
                             latitude = pokestop['lat'],
                             longitude = pokestop['lon'])
        responses = await self.call(request, action=1.5)
        name = responses.get('FORT_DETAILS', {}).get('name')

        request = self.api.create_request()
        request.fort_search(fort_id = pokestop['external_id'],
                            player_latitude = self.location[0],
                            player_longitude = self.location[1],
                            fort_latitude = pokestop['lat'],
                            fort_longitude = pokestop['lon'])
        responses = await self.call(request, action=1)

        result = responses.get('FORT_SEARCH', {}).get('result', 0)
        if result == 1:
            self.log.info('Spun {}.', name)
        elif result == 2:
            self.log.info('The server said {} was out of spinning range. {:.1f}m {:.1f}MPH',
                name, distance, self.speed)
        elif result == 3:
            self.log.warning('{} was in the cooldown period.', name)
        elif result == 4:
            self.log.warning('Could not spin {} because inventory was full. {}',
                name, sum(self.items.values()))
        elif result == 5:
            self.log.warning('Could not spin {} because the daily limit was reached.', name)
            self.pokestops = False
        else:
            self.log.warning('Failed spinning {}: {}', name, result)

        self.next_spin = time() + config.SPIN_COOLDOWN
        self.error_code = '!'
        return responses

    async def encounter(self, pokemon):
        distance_to_pokemon = get_distance(self.location, (pokemon['lat'], pokemon['lon']))

        self.error_code = '~'

        if distance_to_pokemon > 47:
            percent = 1 - (46 / distance_to_pokemon)
            lat_change = (self.location[0] - pokemon['lat']) * percent
            lon_change = (self.location[1] - pokemon['lon']) * percent
            self.location = [
                self.location[0] - lat_change,
                self.location[1] - lon_change,
                uniform(self.location[2] - 3, self.location[2] + 3)
            ]
            self.api.set_position(*self.location)
            delay_required = (distance_to_pokemon * percent) / 8
            if delay_required < 1.5:
                delay_required = triangular(1.25, 4, 2)
        else:
            self.simulate_jitter()
            delay_required = triangular(1.25, 4, 2)

        if time() - self.last_request < delay_required:
            await sleep(delay_required)

        try:
            spawn_id = hex(pokemon['spawn_id'])[2:]
        except TypeError:
            spawn_id = pokemon['spawn_id']

        request = self.api.create_request()
        request = request.encounter(encounter_id=pokemon['encounter_id'],
                                    spawn_point_id=spawn_id,
                                    player_latitude=self.location[0],
                                    player_longitude=self.location[1])

        responses = await self.call(request, action=2.25)

        try:
            pdata = responses['ENCOUNTER']['wild_pokemon']['pokemon_data']
            pokemon['move_1'] = pdata['move_1']
            pokemon['move_2'] = pdata['move_2']
            pokemon['individual_attack'] = pdata.get('individual_attack', 0)
            pokemon['individual_defense'] = pdata.get('individual_defense', 0)
            pokemon['individual_stamina'] = pdata.get('individual_stamina', 0)
        except KeyError:
            self.log.error('Missing Pokemon data in encounter response.')
        self.error_code = '!'

    def bag_full(self):
        return sum(self.items.values()) >= self.item_capacity

    async def clean_bag(self):
        self.error_code = '|'
        rec_items = {}
        limits = config.ITEM_LIMITS
        for item, count in self.items.items():
            if item in limits and count > limits[item]:
                discard = count - limits[item]
                if discard > 50:
                    rec_items[item] = randint(50, discard)
                else:
                    rec_items[item] = discard

        removed = 0
        for item, count in rec_items.items():
            request = self.api.create_request()
            request.recycle_inventory_item(item_id=item, count=count)
            responses = await self.call(request, action=2)

            if responses.get('RECYCLE_INVENTORY_ITEM', {}).get('result', 0) != 1:
                self.log.warning("Failed to remove item {}", item)
            else:
                removed += count
        self.log.info("Removed {} items", removed)
        self.error_code = '!'

    async def incubate_eggs(self):
        # copy the list, as self.call could modify it as it updates the inventory
        incubators = self.unused_incubators.copy()
        for egg in sorted(self.eggs.values(), key=lambda x: x.get('egg_km_walked_target')):
            if egg.get('egg_incubator_id'):
                continue

            if not incubators:
                break

            inc = incubators.pop()
            if inc.get('item_id') == 901 or egg.get('egg_km_walked_target', 0) > 9:
                request = self.api.create_request()
                request.use_item_egg_incubator(item_id=inc.get('id'), pokemon_id=egg.get('id'))
                responses = await self.call(request, action=5)

                ret = responses.get('USE_ITEM_EGG_INCUBATOR', {}).get('result', 0)
                if ret == 4:
                    self.log.warning("Failed to use incubator because it was already in use.")
                elif ret != 1:
                    self.log.warning("Failed to apply incubator {} on {}, code: {}",
                        inc.get('id', 0), egg.get('id', 0), ret)

    async def handle_captcha(self, responses):
        if self.num_captchas >= config.CAPTCHAS_ALLOWED:
            self.log.error("{} encountered too many CAPTCHAs, removing.", self.username)
            raise CaptchaException

        self.error_code = 'C'
        self.num_captchas += 1

        self.create_session()
        try:
            params = {
                'key': config.CAPTCHA_KEY,
                'method': 'userrecaptcha',
                'googlekey': '6LeeTScTAAAAADqvhqVMhPpr_vB9D364Ia-1dSgK',
                'pageurl': responses.get('CHECK_CHALLENGE', {}).get('challenge_url'),
                'json': 1
            }
            async with self.session.post('http://2captcha.com/in.php', params=params, timeout=10) as resp:
                response = await resp.json()
        except Exception as e:
            self.log.error('Got an error while trying to solve CAPTCHA. '
                           'Check your API Key and account balance.')
            raise CaptchaSolveException from e

        code = response.get('request')
        if response.get('status') != 1:
            if code in ('ERROR_WRONG_USER_KEY', 'ERROR_KEY_DOES_NOT_EXIST', 'ERROR_ZERO_BALANCE'):
                config.CAPTCHA_KEY = None
                self.log.error('2Captcha reported: {}, disabling CAPTCHA solving', code)
            else:
                self.log.error("Failed to submit CAPTCHA for solving: {}", code)
            raise CaptchaSolveException

        try:
            # Get the response, retry every 5 seconds if it's not ready
            params = {
                'key': config.CAPTCHA_KEY,
                'action': 'get',
                'id': code,
                'json': 1
            }
            while True:
                async with self.session.get("http://2captcha.com/res.php", params=params, timeout=20) as resp:
                    response = await resp.json()
                if response.get('request') != 'CAPCHA_NOT_READY':
                    break
                await sleep(5)
        except Exception as e:
            self.log.error('Got an error while trying to solve CAPTCHA. '
                              'Check your API Key and account balance.')
            raise CaptchaSolveException from e

        token = response.get('request')
        if not response.get('status') == 1:
            self.log.error("Failed to get CAPTCHA response: {}", token)
            raise CaptchaSolveException

        request = self.api.create_request()
        request.verify_challenge(token=token)
        try:
            responses = await self.call(request, action=4)
            self.update_accounts_dict()
            self.log.warning("Successfully solved CAPTCHA")
        except CaptchaException:
            self.log.warning("CAPTCHA #{} for {} was not solved correctly, trying again",
                captcha_id, self.username)
            # try again
            await self.handle_captcha(responses)

    def simulate_jitter(self, amount=0.00002):
        '''Slightly randomize location, by up to ~2.8 meters by default.'''
        self.location = [
            uniform(self.location[0] - amount,
                    self.location[0] + amount),
            uniform(self.location[1] - amount,
                    self.location[1] + amount),
            uniform(self.location[2] - 1,
                    self.location[2] + 1)
        ]
        self.api.set_position(*self.location)

    def notify(self, norm, time_of_day):
        self.error_code = '*'
        notified = self.notifier.notify(norm, time_of_day)
        if notified:
            self.g['sent'] += 1
        self.error_code = '!'
        return notified

    def update_accounts_dict(self, captcha=False, banned=False, auth=True):
        self.account['captcha'] = captcha
        self.account['banned'] = banned
        self.account['location'] = self.location
        self.account['time'] = self.last_request
        self.account['inventory_timestamp'] = self.inventory_timestamp
        self.account['items'] = self.items
        self.account['level'] = self.player_level

        try:
            if auth:
                self.account['refresh'] = self.api._auth_provider._refresh_token
                if self.api._auth_provider.check_access_token():
                    self.account['auth'] = self.api._auth_provider._access_token
                    self.account['expiry'] = self.api._auth_provider._access_token_expiry
                else:
                    self.account['auth'] = self.account['expiry'] = None
        except AttributeError:
            pass

        self.accounts[self.username] = self.account

    async def remove_account(self):
        self.error_code = 'REMOVING'
        self.log.warning('Removing {} due to ban.', self.username)
        self.update_accounts_dict(banned=True)
        await self.new_account()

    async def bench_account(self):
        self.error_code = 'BENCHING'
        self.log.warning('Swapping {} due to CAPTCHA.', self.username)
        self.update_accounts_dict(captcha=True)
        self.captcha_queue.put(self.account)
        await self.new_account()

    async def swap_account(self, reason='', lock=False):
        self.error_code = 'SWAPPING'
        self.log.warning('Swapping out {} because {}.', self.username, reason)
        if lock:
            await self.busy.acquire()
        self.update_accounts_dict()
        while self.extra_queue.empty():
            if self.killed:
                return False
            await sleep(15)
        self.extra_queue.put(self.account)
        await self.new_account(lock)

    async def new_account(self, lock=False):
        captcha = False
        while self.extra_queue.empty():
            if config.CAPTCHA_KEY and not self.captcha_queue.empty():
                captcha = True
                break
            if self.killed:
                return False
            await sleep(15)
        if captcha:
            self.account = self.captcha_queue.get()
        else:
            self.account = self.extra_queue.get()
        self.username = self.account.get('username')
        self.location = self.account.get('location', get_start_coords(self.worker_no))
        self.inventory_timestamp = self.account.get('inventory_timestamp')
        self.player_level = self.account.get('player_level')
        self.last_request = self.account.get('time', 0)
        self.last_action = self.last_request
        self.last_gmo = self.last_request
        self.items = self.account.get('items', {})
        self.num_captchas = 0
        self.eggs = {}
        self.unused_incubators = []
        self.initialize_api()
        self.error_code = None
        if lock:
            self.busy.release()

    def seen_per_second(self, now):
        try:
            seconds_active = now - self.account_start
            if seconds_active < 120:
                return None
            return self.account_seen / seconds_active
        except TypeError:
            return None

    def kill(self):
        """Marks worker as killed

        Killed worker won't be restarted.
        """
        self.error_code = 'KILLED'
        self.killed = True
        if self.ever_authenticated:
            self.update_accounts_dict()

    @classmethod
    def create_session(cls):
        try:
            return cls.session
        except AttributeError:
            cls.session = ClientSession(loop=cls.loop)

    @classmethod
    def close_session(cls):
        try:
            cls.session.close()
        except Exception:
            pass

    @staticmethod
    def normalize_pokemon(raw):
        """Normalizes data coming from API into something acceptable by db"""
        tsm = raw['last_modified_timestamp_ms']
        tss = round(tsm / 1000)
        tth = raw['time_till_hidden_ms']
        norm = {
            'type': 'pokemon',
            'encounter_id': raw['encounter_id'],
            'pokemon_id': raw['pokemon_data']['pokemon_id'],
            'lat': raw['latitude'],
            'lon': raw['longitude'],
            'spawn_id': get_spawn_id(raw),
            'seen': tss
        }
        if tth > 0 and tth <= 90000:
            norm['expire_timestamp'] = round((tsm + tth) / 1000)
            norm['time_till_hidden'] = tth / 1000
            norm['inferred'] = False
        else:
            despawn = shared.SPAWNS.get_despawn_time(norm['spawn_id'], tss)
            if despawn:
                norm['expire_timestamp'] = despawn
                norm['time_till_hidden'] = despawn - tss
                norm['inferred'] = True
            else:
                norm['type'] = 'mystery'
        return norm

    @staticmethod
    def normalize_lured(raw, now):
        if config.SPAWN_ID_INT:
            spawn_id = -1
        else:
            spawn_id = 'LURED'
        return {
            'type': 'pokemon',
            'encounter_id': raw['lure_info']['encounter_id'],
            'pokemon_id': raw['lure_info']['active_pokemon_id'],
            'expire_timestamp': raw['lure_info']['lure_expires_timestamp_ms'] // 1000,
            'lat': raw['latitude'],
            'lon': raw['longitude'],
            'spawn_id': spawn_id,
            'time_till_hidden': (raw['lure_info']['lure_expires_timestamp_ms'] - now) // 1000,
            'inferred': 'pokestop'
        }

    @staticmethod
    def normalize_gym(raw):
        return {
            'type': 'fort',
            'external_id': raw['id'],
            'lat': raw['latitude'],
            'lon': raw['longitude'],
            'team': raw.get('owned_by_team', 0),
            'prestige': raw.get('gym_points', 0),
            'guard_pokemon_id': raw.get('guard_pokemon_id', 0),
            'last_modified': raw['last_modified_timestamp_ms'] // 1000,
        }

    @staticmethod
    def normalize_pokestop(raw):
        return {
            'type': 'pokestop',
            'external_id': raw['id'],
            'lat': raw['latitude'],
            'lon': raw['longitude']
        }

    @staticmethod
    def check_captcha(responses):
        challenge_url = responses.get('CHECK_CHALLENGE', {}).get('challenge_url', ' ')
        verify = responses.get('VERIFY_CHALLENGE', {})
        success = verify.get('success')
        if challenge_url != ' ' and not success:
            if config.CAPTCHA_KEY and not verify:
                return True
            else:
                raise CaptchaException
        else:
            return False

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


class BusyLock(Lock):
    def acquire_now(self):
        if not self._locked and all(w.cancelled() for w in self._waiters):
            self._locked = True
            return True
        else:
            return False

class CaptchaException(Exception):
    """Raised when a CAPTCHA is needed."""

class CaptchaSolveException(Exception):
    """Raised when solving a CAPTCHA has failed."""
