#!/usr/bin/env python3

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from functools import partial
from geopy.distance import great_circle
from multiprocessing.managers import DictProxy
from statistics import median
from logging import getLogger
from threading import Thread, active_count, Semaphore
from os import system, makedirs
from sys import platform

from pgoapi import (
    exceptions as pgoapi_exceptions,
    utilities as pgoapi_utils,
)

import asyncio
import random
import time

import config
import db
import utils

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
    'NETWORK_THREADS': round((config.GRID[0] * config.GRID[1]) / 10) + 1,
    'SPIN_POKESTOPS': False,
    'COMPLETE_TUTORIAL': False
}
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

from shared import *

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


class Slave(BaseSlave):
    """Single worker walking on the map"""

    process_executor = ProcessPoolExecutor()

    def __init__(self, worker_no, proxy=None):
        super().__init__(worker_no, proxy=proxy)
        self.visits = 0

    async def travel_speed(self, point):
        if self.busy or self.killed:
            return None
        now = time.time()
        time_diff = now - self.last_visit
        if time_diff < config.SCAN_DELAY:
            return None
        elif time_diff > 60:
            self.error_code = None
        distance = await self.loop.run_in_executor(
            self.process_executor,
            partial(great_circle, self.location, point)
        )
        speed = (distance.miles / time_diff) * 3600
        return speed

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
                self.error_code = 'CAPTCHA'
                if self.killed:
                    return False
                await self.bench_account()
                self.g['captchas'] += 1
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
        latitude, longitude, altitude = point
        altitude = random.uniform(altitude - 1, altitude + 1)
        self.error_code = '!'
        self.logger.info(
            'Visiting {0[0]:.4f},{0[1]:.4f} {0[2]:.1f}m'.format(point))
        start = time.time()
        self.location = point

        self.api.set_position(latitude, longitude, altitude)

        rounded = utils.round_coords(point, precision=5)
        if rounded not in self.cell_ids:
            self.cell_ids[rounded] = await self.loop.run_in_executor(
                self.process_executor,
                partial(pgoapi_utils.get_cell_ids, *rounded, radius=500)
            )
        cell_ids = self.cell_ids[rounded]
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

        sent = False

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
                    despawn_time = self.spawns.get_despawn_time(
                        normalized['spawn_id'], normalized['seen'])
                    if despawn_time:
                        normalized['expire_timestamp'] = despawn_time
                        normalized['time_till_hidden_ms'] = (
                            despawn_time * 1000) - request_time_ms
                        normalized['valid'] = 'fixed'
                    else:
                        normalized['valid'] = False
                else:
                    normalized['valid'] = True

                normalized, sent = await self.notify(normalized, pokemon)

                if (config.ENCOUNTER == 'all'
                        and 'individual_attack' not in normalized
                        and normalized not in db.SIGHTING_CACHE
                        and normalized not in db.MYSTERY_CACHE):
                    try:
                        normalized.update(await self.encounter(pokemon))
                    except Exception:
                        self.logger.warning('Exception during encounter.')
                self.db_processor.add(normalized)
            for fort in map_cell.get('forts', []):
                if not fort.get('enabled'):
                    continue
                if fort.get('type') == 1:  # pokestops
                    if 'lure_info' in fort:
                        norm = utils.normalize_lured(fort, request_time_ms)
                        pokemons.append(norm)
                    pokestop = utils.normalize_pokestop(fort)
                    forts.append(pokestop)
                    if config.SPIN_POKESTOPS and sum(self.items.values()) < self.item_capacity:
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
            self.g['seen'] += pokemon_seen
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
                self.visits, pokemon_seen, sent))])
        self.logger.info(
            'Point processed, %d Pokemons and %d forts seen!',
            pokemon_seen,
            len(forts),
        )
        self.update_accounts_dict()
        return True


class Overseer:
    db_processor = Slave.db_processor
    spawns = Slave.spawns
    accounts = Slave.accounts

    def __init__(self, status_bar, loop, manager):
        self.logger = getLogger('overseer')
        self.workers = {}
        self.loop = loop
        self.manager = manager
        self.count = config.GRID[0] * config.GRID[1]
        self.start_date = datetime.now()
        self.status_bar = status_bar
        self.things_count = []
        self.paused = False
        self.killed = False
        self.last_proxy = 0
        self.coroutines_count = 0
        self.skipped = 0
        self.visits = 0
        self.searches_without_shuffle = 0
        self.coroutine_semaphore = Semaphore(self.count - 1)
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

        print('Setting CAPTCHA statuses.')

        if self.captcha_queue.empty():
            for account in self.accounts.keys():
                self.accounts[account]['captcha'] = False
        else:
            while not self.extra_queue.empty():
                account = self.extra_queue.get()
                username = account.get('username')
                self.accounts[username]['captcha'] = False

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

        worker = Slave(worker_no=worker_no, proxy=proxy)
        self.workers[worker_no] = worker

    def start(self):
        self.captcha_queue = self.manager.captcha_queue()
        Slave.captcha_queue = self.manager.captcha_queue()
        self.extra_queue = self.manager.extra_queue()
        Slave.extra_queue = self.manager.extra_queue()
        Slave.worker_dict = self.manager.worker_dict()

        for username, account in self.accounts.items():
            account['username'] = username
            if account.get('captcha'):
                self.captcha_queue.put(account)
            else:
                self.extra_queue.put(account)

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
                self.update_coroutines_count()
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
            if self.coroutines_count == 0:
                print('Done.                ')
                return
            time.sleep(.25)

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
        self.spawns_count = len(self.spawns.despawn_times)

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
            seen = Slave.g['seen']
            captchas = Slave.g['captchas']
            sent = Slave.g['sent']
            output.append('Seen per visit: {v:.2f}, per minute: {m:.0f}'.format(
                v=seen / self.visits, m=seen / (seconds_since_start / 60)))

            if captchas:
                captchas_per_request = captchas / (self.visits / 1000)
                captchas_per_hour = captchas / hours_since_start
                output.append(
                    'CAPTCHAs per 1K visits: {r:.1f}, per hour: {h:.1f}'.format(
                    r=captchas_per_request, h=captchas_per_hour))
        except ZeroDivisionError:
            pass

        if sent:
            output.append('Notifications sent: {n}, per hour {p:.1f}'.format(
                n=sent, p=sent / hours_since_start))

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

    async def best_worker(self, point, spawn_time=None):
        if spawn_time:
            visit_time = spawn_time
            skip_time = -600
        else:
            visit_time = time.time()
            skip_time = -6

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
                speed = await w.travel_speed(point)
                if speed is not None and speed < lowest_speed:
                    lowest_speed = speed
                    worker = w
                    if speed < 10:
                        break
            if self.killed:
                return None, None
            if worker and worker.busy:
                worker = None
            if lowest_speed > config.SPEED_LIMIT or worker is None:
                time_diff = visit_time - time.time()
                if time_diff < skip_time:
                    return False, False
                await asyncio.sleep(2)
        return worker, lowest_speed

    def launch(self):
        while not self.killed:
            current_hour = utils.get_current_hour()
            if self.visits > 0:
                utils.dump_pickle('accounts', self.accounts)
                self.spawns.update_spawns()
            else:
                self.spawns.update_spawns(loadpickle=True)
                mysteries = deque(self.spawns.mysteries)
                random.shuffle(mysteries)
                self.spawns_count = len(self.spawns.despawn_times)
                if self.spawns_count == 0:
                    raise ValueError('No spawnpoints.')
                smallest_diff = None
                start = None
                now = time.time()
                for spawn_id, spawn in self.spawns.spawns.items():
                    spawn_time = spawn[1] + current_hour
                    time_diff = abs(spawn_time - now)
                    if not smallest_diff or time_diff < smallest_diff:
                        smallest_diff = time_diff
                        closest = spawn_id
                    if smallest_diff < 1:
                        break
                first = True

            for spawn_id, spawn in self.spawns.spawns.items():
                try:
                    if first:
                        if spawn_id == closest:
                            first = False
                        else:
                            continue

                    while (self.captcha_queue.qsize() > config.MAX_CAPTCHAS
                            and not self.killed):
                        self.paused = True
                        time.sleep(10)
                        self.idle_seconds += 10

                    if self.killed:
                        return

                    self.paused = False

                    point = list(spawn[0])
                    spawn_time = spawn[1] + current_hour

                    # negative = already happened
                    # positive = hasn't happened yet
                    time_diff = spawn_time - time.time()

                    while time_diff > 0 and not self.killed:
                        try:
                            mystery_point = list(mysteries.pop())

                            self.coroutine_semaphore.acquire()
                            asyncio.run_coroutine_threadsafe(
                                self.try_point(mystery_point), loop=self.loop
                            )
                        except IndexError:
                            mysteries = deque(self.spawns.mysteries)
                            random.shuffle(mysteries)
                        time_diff = spawn_time - time.time()

                    if time_diff < -10 and spawn_id in self.spawn_cache:
                        self.redundant += 1
                    elif time_diff < -270:
                        self.skipped += 1
                        continue

                    if self.killed:
                        return
                    self.coroutine_semaphore.acquire()
                    asyncio.run_coroutine_threadsafe(
                        self.try_point(point, spawn_time), loop=self.loop
                    )
                except Exception as e:
                    self.logger.exception(e)

    async def try_point(self, point, spawn_time=None):
        try:
            point[0] = random.uniform(point[0] - 0.00033, point[0] + 0.00033)
            point[1] = random.uniform(point[1] - 0.00033, point[1] + 0.00033)

            if spawn_time:
                time_diff = spawn_time - time.time()
                if time_diff > -2:
                    await asyncio.sleep(time_diff + 2)

            worker, speed = await self.best_worker(point, spawn_time)
            if not worker:
                if spawn_time:
                    self.skipped += 1
                return False

            worker.busy = True
            if spawn_time:
                worker.after_spawn = time.time() - spawn_time
            worker.speed = speed

            if await worker.visit(point):
                self.visits += 1
            worker.busy = False
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.coroutine_semaphore.release()


if __name__ == '__main__':
    START_TIME = time.time()

    try:
        makedirs('pickles')
    except OSError:
        pass

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


    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    Slave.loop = loop
    Slave.login_semaphore = asyncio.Semaphore(1, loop=loop)
    Slave.simulation_semaphore = asyncio.Semaphore(2, loop=loop)

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
        utils.dump_pickle('accounts', Slave.accounts)
        utils.dump_pickle('cells', Slave.cell_ids)

        pending = asyncio.Task.all_tasks(loop=loop)
        print('Completing tasks.    ')
        loop.run_until_complete(asyncio.gather(*pending))
        print('Shutting things down.')
        Slave.network_executor.shutdown()
        Slave.db_processor.stop()
        if config.NOTIFY:
            Slave.notifier.session.close()
        Slave.spawns.session.close()
        Slave.process_executor.shutdown()
        manager.shutdown()
        print('Stopping and closing loop.')
        loop.stop()
        loop.close()
