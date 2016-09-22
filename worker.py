# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, CancelledError, Future
from collections import deque
from datetime import datetime
from functools import partial
from sqlalchemy.exc import IntegrityError
from stem import Signal
from stem.control import Controller
from geopy.distance import great_circle
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


# Check whether config has all necessary attributes
REQUIRED_SETTINGS = (
    'DB_ENGINE',
    'ENCRYPT_PATH',
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
    'CYCLES_PER_WORKER': 3,
    'SCAN_RADIUS': 70,
    'SCAN_DELAY': (10, 12, 11),
    'NOTIFY_IDS': None,
    'NOTIFY_RANKING': None,
    'CONTROL_SOCKS': None
}
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

if config.CONTROL_SOCKS:
    import stem.util.log
    stem.util.log.get_logger().level = 40
    CIRCUIT_TIME = dict()
    for proxy in config.PROXIES:
        address = proxy.get('https')
        CIRCUIT_TIME[address] = time.time()

BAD_STATUSES = (
    'LOGIN FAIL',
    'EXCEPTION',
    'BAD LOGIN',
    'RETRYING',
    'THROTTLE',
    'NO POKEMON',
)

if config.NOTIFY_IDS or config.NOTIFY_RANKING:
    import notification
    notifier = notification.Notifier()


class MalformedResponse(Exception):
    """Raised when server response is malformed"""


class BannedAccount(Exception):
    """Raised when account is banned"""


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


class Slave:
    """Single worker walking on the map"""

    def __init__(
            self,
            worker_no,
            points,
            cell_ids,
            db_processor,
            start_step=0,
            device_info=None,
            proxies=None
    ):
        self.worker_no = worker_no
        # Set of all points that worker needs to visit
        self.points = points
        self.count_points = len(self.points)
        # Cache for cell_ids for all points
        self.cell_ids = cell_ids
        # asyncio/thread references
        self.future = None  # worker's own future
        self.db_processor = db_processor
        # Some handy counters
        self.start_step = start_step  # allow worker to pick up where it left
        self.step = 0
        self.cycle = 0
        self.seen_per_cycle = 0
        self.loop = asyncio.get_event_loop()
        self.total_seen = 0
        # State variables
        self.running = True  # not running worker should be restarted
        self.killed = False  # killed worker will stay killed
        self.restart_me = False  # ask overseer for restarting
        self.logged_in = False
        # Other variables
        self.last_step_run_time = 0
        self.last_visit = 0
        self.last_api_latency = 0
        self.error_code = 'INIT'
        self.location = self.points[0]
        # And now, configure logger and PGoApi
        self.logger = logging.getLogger('worker-{}'.format(worker_no))
        self.device_info = device_info
        self.api = PGoApi(device_info=device_info)
        self.api.activate_signature(config.ENCRYPT_PATH)
        self.api.set_position(self.location[0], self.location[1], self.location[2])  # lat, lon, alt
        self.api.set_logger(self.logger)
        self.proxies = proxies
        self.api.set_proxy(self.proxies)

    async def first_run(self):
        total_workers = config.GRID[0] * config.GRID[1]
        try:
            await self.sleep(
                self.worker_no / total_workers * config.SCAN_DELAY[0]
            )
            await self.run()
        except CancelledError:
            self.kill()

    async def run(self):
        try:
            if not self.logged_in:
                await self.login()
        except CancelledError:
            self.kill()

    def call_api(self, method, *args, **kwargs):
        """Returns decorated function that measures execution time

        This works exactly like functools.partial does.
        """
        def inner():
            start = time.time()
            result = method(*args, **kwargs)
            self.last_api_latency = time.time() - start
            if self.last_api_latency > 60:
                self.swap_proxy(reason='excessive latency')
            return result
        return inner

    def swap_account(self, reason=''):
        self.logger.warning('Swapping out ' +
                            config.ACCOUNTS[self.worker_no][0] +
                            ' for ' + config.EXTRA_ACCOUNTS[0][0])
        t = datetime.now().strftime('%Y-%m-%d %H:%M')
        with open('account_failures.txt', 'at') as f:
            print(config.ACCOUNTS[self.worker_no][0], reason, t, file=f)
        config.EXTRA_ACCOUNTS.append(config.ACCOUNTS[self.worker_no])
        config.ACCOUNTS[self.worker_no] = config.EXTRA_ACCOUNTS.pop(0)
        return

    def swap_proxy(self, reason=''):
        if not config.CONTROL_SOCKS:
            return
        address = self.proxies.get('https')
        time_passed = time.time() - CIRCUIT_TIME[address]
        if time_passed > 60:
            socket = config.CONTROL_SOCKS[address]
            with Controller.from_socket_file(path=socket) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
            CIRCUIT_TIME[address] = time.time()
            self.logger.warning('Changed circuit on ' + address +
                                ' due to ' + reason)
        else:
            self.logger.info('Skipped changing circuit on ' + address +
                             ' because it was changed ' + str(time_passed)
                             + ' seconds ago.')

    def login(self):
        """Logs worker in and prepares for scanning"""
        self.cycle = 1
        self.error_code = None
        self.error_code = 'LOGIN'
        self.logger.info('Trying to log in')
        try:
            self.api.set_authentication(provider=config.ACCOUNTS[self.worker_no][2], username=config.ACCOUNTS[self.worker_no][0], password=config.ACCOUNTS[self.worker_no][1])
        except pgoapi_exceptions.ServerSideAccessForbiddenException:
            import requests
            ip_address = requests.get(
                'https://icanhazip.com/',
                proxies=self.proxies).text
            self.logger.error('Banned IP: ' + ip_address)
            self.error_code = 'IP BANNED'
            with open('banned_ips.txt', 'at') as f:
                f.write(ip_address + '\n')
            self.swap_proxy(reason='ban')
            return
        except pgoapi_exceptions.AuthException:
            self.logger.warning('Login failed!')
            self.error_code = 'LOGIN FAIL'
            time.sleep(10)
            self.login()
            return
        except pgoapi_exceptions.NotLoggedInException:
            self.logger.error('Invalid credentials')
            self.error_code = 'BAD LOGIN'
            time.sleep(10)
            self.login()
            return
        except pgoapi_exceptions.ServerBusyOrOfflineException:
            self.logger.info('Server too busy - restarting')
            self.error_code = 'RETRYING'
            time.sleep(10)
            self.login()
            return
        except pgoapi_exceptions.ServerSideRequestThrottlingException:
            self.logger.info('Server throttling - sleeping for a bit')
            self.error_code = 'THROTTLE'
            time.sleep(10)
            self.login()
            return
        except Exception as err:
            print(err)
            return
        else:
            self.logged_in = True
            self.error_code = 'READY'


    def visit(self, point, i):
        """Wrapper for self.main - runs it a few times before restarting

        Also is capable of restarting in case an error occurs.
        """
        visited = False
        self.error_code = None
        try:
            self.visit_point(point, i)
        except pgoapi_exceptions.ServerSideAccessForbiddenException:
            import requests
            ip_address = requests.get(
                'https://icanhazip.com/',
                proxies=self.proxies).text
            self.logger.error('Banned IP: ' + ip_address)
            self.error_code = 'IP BANNED'
            with open('banned_ips.txt', 'at') as f:
                f.write(ip_address + '\n')
            self.swap_proxy(reason='ban')
            #print('Trying', i, 'again.')
            #self.future.cancel()
            #parent.try_point(point, spawn_seconds, i)
            return
        except pgoapi_exceptions.AuthException:
            self.logger.warning('Login failed!')
            self.error_code = 'LOGIN FAIL'
            self.logged_in = False
            time.sleep(10)
            self.login()
            #print('Trying', i, 'again.')
            #self.future.cancel()
            #parent.try_point(point, spawn_seconds, i)
            return
        except pgoapi_exceptions.NotLoggedInException:
            self.logger.error('Invalid credentials')
            self.error_code = 'BAD LOGIN'
            self.logged_in = False
            time.sleep(10)
            self.login()
            #print('Trying', i, 'again.')
            #self.future.cancel()
            #parent.try_point(point, spawn_seconds, i)
            return
        except pgoapi_exceptions.ServerSideRequestThrottlingException:
            self.logger.info('Server throttling - sleeping for a bit')
            self.error_code = 'THROTTLE'
            time.sleep(10)
            return
        except MalformedResponse:
            self.logger.warning('Malformed response received!')
            self.error_code = 'RESTART'
            #print('Trying', i, 'again.')
            #self.future.cancel()
            #parent.try_point(point, spawn_seconds, i)
            return
        except BannedAccount:
            self.error_code = 'BANNED?'
            self.swap_account(reason='code 3')
            #print('Trying', i, 'again.')
            #self.future.cancel()
            #parent.try_point(point, spawn_seconds, i)
            return
        except Exception as err:
            print(err)
            return

    def visit_point(self, point, i):
        print('Worker', self.worker_no, 'visiting', i)
        latitude = random.uniform(point[0] - 0.00001, point[0] + 0.00001)
        longitude = random.uniform(point[1] - 0.00001, point[1] + 0.00001)
        altitude = random.uniform(*config.ALT_RANGE)
        self.error_code = 'v'
        self.logger.info(
            'Visiting point %d (%s,%s %sm)', i, round(latitude, 4),
            round(longitude, 4), round(altitude)
        )
        start = time.time()
        self.api.set_position(latitude, longitude, altitude)
        if not self.logged_in:
            self.login()
        self.last_visit = start
        self.location = point
        if i not in self.cell_ids:
            self.cell_ids[i] = pgoapi_utils.get_cell_ids(latitude, longitude, radius=1500)
        cell_ids = self.cell_ids[i]
        response_dict = self.api.get_map_objects(
                latitude=pgoapi_utils.f2i(latitude),
                longitude=pgoapi_utils.f2i(longitude),
                cell_id=cell_ids
            )
        if not isinstance(response_dict, dict):
            self.logger.warning('Response: %s', response_dict)
            raise MalformedResponse
        if response_dict['status_code'] == 3:
            logger.warning('Account banned')
            raise BannedAccount
        responses = response_dict.get('responses')
        if not responses:
            self.logger.warning('Response: %s', response_dict)
            raise MalformedResponse
        map_objects = responses.get('GET_MAP_OBJECTS', {})
        pokemons = []
        ls_seen = []
        forts = []
        if map_objects.get('status') == 1:
            for map_cell in map_objects['map_cells']:
                for pokemon in map_cell.get('wild_pokemons', []):
                    # Store spawns outside of the 15 minute range in a
                    # different table until they fall under 15 minutes,
                    # and notify about them differently.
                    long_spawn = (
                        pokemon['time_till_hidden_ms'] < 0 or
                        pokemon['time_till_hidden_ms'] > 3600000
                    )
                    if long_spawn and not config.LONGSPAWNS:
                        continue
                    normalized = self.normalize_pokemon(
                        pokemon,
                        map_cell['current_timestamp_ms']
                    )
                    pokemons.append(normalized)

                    if normalized['pokemon_id'] in config.NOTIFY_IDS:
                        notified, explanation = notifier.notify(pokemon)
                        if notified:
                            self.logger.info(explanation)
                        else:
                            self.logger.warning(explanation)
                    key = db.combine_key(normalized)
                    if config.LONGSPAWNS and (long_spawn
                            or key in db.LONGSPAWN_CACHE.store):
                        normalized = normalized.copy()
                        normalized['type'] = 'longspawn'
                        ls_seen.append(normalized)
                for fort in map_cell.get('forts', []):
                    if not fort.get('enabled'):
                        continue
                    if fort.get('type') == 1:  # probably pokestops
                        continue
                    forts.append(self.normalize_fort(fort))
            self.db_processor.add(pokemons)
            self.db_processor.add(forts)
            if ls_seen:
                self.db_processor.add(ls_seen)
            self.total_seen += len(pokemons)
            #print(len(pokemons), 'Pokemon seen at', i)
            self.seen_per_cycle += len(pokemons)
            self.logger.info(
                'Point processed, %d Pokemons and %d forts seen!',
                len(pokemons),
                len(forts),
            )
            # Clear error code and let know that there are Pokemon
            if self.error_code and self.seen_per_cycle:
                self.error_code = None
            return True

    def travel_speed(self, point, spawn_time):
        now = time.time()
        if spawn_time < now:
            spawn_time = now
        time_diff = spawn_time - self.last_visit
        if time_diff < 10:
            return None
        distance = great_circle(self.location, point).meters
        speed = distance / time_diff
        #print('Worker', self.worker_no, 'speed:', speed)
        return speed

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
        }
        if config.SPAWN_ID_INT:
            normalized['spawn_id'] = int(raw['spawn_point_id'], 16)
        else:
            normalized['spawn_id'] = raw['spawn_point_id']
        if config.LONGSPAWNS:
            normalized['time_till_hidden_ms'] = raw['time_till_hidden_ms']
            normalized['last_modified_timestamp_ms'] = raw['last_modified_timestamp_ms']
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
        await self.sleep(random.randint(sleep_min, sleep_max))


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
        self.killed = False
        self.loop = loop
        self.db_processor = DatabaseProcessor()
        self.logger.info('Overseer initialized')

    def kill(self):
        self.killed = True
        self.db_processor.stop()
        for worker in self.workers.values():
            worker.kill()
            if worker.future:
                worker.future.cancel()

    def start_worker(self, worker_no, first_run=False):
        if self.killed:
            return

        if isinstance(config.PROXIES, (tuple, list)):
            proxies = random.choice(config.PROXIES)
        elif isinstance(config.PROXIES, dict):
            proxies = config.PROXIES
        else:
            proxies = None

        worker = Slave(
            worker_no=worker_no,
            points=self.points[worker_no],
            cell_ids=self.cell_ids[worker_no],
            db_processor=self.db_processor,
            device_info=utils.get_worker_device(worker_no),
            proxies=proxies
        )
        self.workers[worker_no] = worker
        # For first time, we need to wait until all workers login before
        # scanning
        # WARNING: at this point, we're called by self.check which runs in
        # separate thread than event loop! That's why run_coroutine_threadsafe
        # is used here.

    def get_point_stats(self):
        lenghts = [len(p) for p in self.points]
        return {
            'max': max(lenghts),
            'min': min(lenghts),
            'avg': int(sum(lenghts) / float(len(lenghts))),
        }

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
            if worker.running
        ]
        while not self.killed:
            now = time.time()
            # Restart workers that were killed
            for worker_no in self.workers.keys():
                if self.workers[worker_no].restart_me:
                    self.start_worker(worker_no)
            # Clean cache
            if now - last_cleaned_cache > 900:  # clean cache
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


    def get_time_stats(self):
        steps = [w.last_step_run_time for w in self.workers.values()]
        api_calls = [w.last_api_latency for w in self.workers.values()]
        return {
            'api_calls': {
                'max': max(api_calls),
                'min': min(api_calls),
                'avg': sum(api_calls) / len(api_calls),
            },
            'steps': {
                'max': max(steps),
                'min': min(steps),
                'avg': sum(steps) / len(steps),
            }
        }

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
        time_stats = self.get_time_stats()
        running_for = datetime.now() - self.start_date
        try:
            coroutines_count = len(asyncio.Task.all_tasks(self.loop))
        except RuntimeError:
            # Set changed size during iteration
            coroutines_count = '?'
        output = [
            'PokeMiner\trunning for {}'.format(running_for),
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
            'API latency: min {min:.3f}, max {max:.3f}, avg {avg:.3f}'.format(
                **time_stats['api_calls']
            ),
            'step time: min {min:.3f}, max {max:.3f}, avg {avg:.3f}'.format(
                **time_stats['steps']
            ),
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
                        session.commit()
                        self.count += 1
                    elif item['type'] == 'longspawn':
                        db.add_longspawn(session, item)
                    elif item['type'] == 'fort':
                        db.add_fort_sighting(session, item)
                        # No need to commit here - db takes care of it
                    self.logger.debug('Item saved to db')
                except IntegrityError:
                    session.rollback()
                    self.logger.info(
                        'Tried and failed to add a duplicate to DB.')
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
    def __init__(self, overseer):
        #self.loop = loop
        self.overseer = overseer
        self.assignments = dict()
        self.workers = self.overseer.workers.values()
        self.spawns = db.get_spawn_locations(db.Session())
        with open('spawns.pickle', 'rb') as f:
            self.spawns = pickle.load(f)
        self.set_time()
        #self.visitor = ThreadPoolExecutor(max_workers=10)
    
    def set_time(self):
        self.now = time.time()
        self.current_seconds = self.now % 3600
        self.current_hour = self.now - (self.now % 3600)

    def best_worker(self, point, spawn_time):
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
            lowest_speed = round(lowest_speed)
            if worker is None:
                print('No eligible workers')
                time.sleep(10)
            elif lowest_speed > config.SPEED_LIMIT:
                print('Over speed limit:', lowest_speed)
                sleep_time = 5 * (lowest_speed / config.SPEED_LIMIT)
                if sleep_time > 20:
                    sleep_time = 20
                time.sleep(sleep_time)
            else:
                print('Speed:', lowest_speed)
        return worker

    def launch(self):
        #loop = asyncio.new_event_loop()
        #sem = asyncio.Semaphore(7)
        while True:
            self.set_time()
            for spawn in self.spawns:
                spawn_seconds = spawn['time']
                spawn['spawn_time'] = spawn_seconds + self.current_hour
            for x, spawn in enumerate(self.spawns):
                spawn_time = spawn['spawn_time']
                if self.now > spawn_time + 60:
                    print('Skipping', x)
                    continue

                point = spawn['point']
                self.try_point(point, spawn_time, x)
    
    async def wrapper(self, worker, point, x, spawn_seconds):
        await self.loop.run_in_executor(
            self.visitor,
            partial(
                worker.visit, point, x, spawn_seconds, parent=self
            )
        )

    def try_point(self, point, spawn_time, x):
        if x in self.assignments:
            worker_number = self.assignments[x]
            worker = self.overseer.workers[worker_number]
            speed = worker.travel_speed(point, spawn_time)
            if speed is None or speed > config.SPEED_LIMIT:
                worker = self.best_worker(point, spawn_time)
                self.assignments[x] = worker.worker_no
        else:
            worker = self.best_worker(point, spawn_time)
            self.assignments[x] = worker.worker_no
        self.set_time()
        time_diff = spawn_time + 2 - self.now
        if time_diff > 0:
            print('Waiting', round(time_diff, 1), 'seconds for', x)
            time.sleep(time_diff)
        else:
            print(round(abs(time_diff), 1), 'seconds after spawn')
        #asyncio.run_coroutine_threadsafe(worker.visit(point, x))
        #loop.call_soon(bound_visit(worker, point, x, sem))
        
        #asyncio.ensure_future(self.wrapper(worker, point, x, spawn_seconds))
        #worker.last_visit = time.time()
        worker.visit(point, x)
        #self.loop.run_in_executor(self.visitor, partial(worker.visit, point, x, spawn_seconds, parent=self))
        #tasks.append(task)
        #asyncio.ensure_future(bound_visit(worker, point, x, sem))
        #await worker.visit(point, x)
        #asyncio.ensure_future(bound_visit(worker, point, x, sem))
        #asyncio.ensure_future(bound_fetch(sem, url.format(i), session))
        # await bound_visit(worker, point, x, sem)


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
    loop.set_exception_handler(exception_handler)
    overseer.start()
    #overseer_thread = threading.Thread(target=overseer.check)
    #overseer_thread.start()
    #time.sleep(10)
    #print(loop.is_running())
    launcher = Launcher(overseer)
    launcher.launch()
    #loop.run_forever()
    #another_thread = threading.Thread(target=launcher.launch)
    #another_thread.start()

    '''
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
    '''
