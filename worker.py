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
from collections import deque

from pgoapi import (
    exceptions as pgoapi_exceptions,
    utilities as pgoapi_utils,
)

import asyncio
import uvloop
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
    'MAX_CAPTCHAS': 0,
    'ACCOUNTS': (),
    'SPEED_LIMIT': 19,
    'ENCOUNTER': None,
    'NOTIFY': False,
    'AUTHKEY': b'm3wtw0',
    'NETWORK_THREADS': round((config.GRID[0] * config.GRID[1]) / 10) + 1,
    'SPIN_POKESTOPS': False,
    'COMPLETE_TUTORIAL': False,
    'MAP_WORKERS': True
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
        CIRCUIT_TIME[proxy] = time.monotonic()
        CIRCUIT_FAILURES[proxy] = 0
else:
    CIRCUIT_TIME = None
    CIRCUIT_FAILURES = None


class Slave(BaseSlave):
    """Single worker walking on the map"""

    process_executor = ProcessPoolExecutor(max_workers=3)

    def __init__(self, worker_no, proxy=None):
        super().__init__(worker_no, proxy=proxy)
        self.visits = 0

    def seen_per_second(self, now):
        try:
            seconds_active = now - self.account_start
            if seconds_active < 120:
                return None
            return self.account_seen / seconds_active
        except TypeError:
            return None

    async def travel_speed(self, point):
        if self.busy.locked() or self.killed:
            return None
        now = time.time()
        if now - self.last_gmo < config.SCAN_DELAY:
            return None
        time_diff = now - self.last_visit
        if time_diff > 60:
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
        for attempts in range(0, 4):
            try:
                if not self.logged_in:
                    self.api.set_position(*point)
                    if not await self.login():
                        await asyncio.sleep(2)
                        continue
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
                self.logger.warning('Login failed: {}'.format(self.username))
                self.error_code = 'FAILED LOGIN'
                await self.swap_account(reason='login failed')
                return False
            except pgoapi_exceptions.NotLoggedInException:
                self.logger.error('{} is not logged in.'.format(self.username))
                self.error_code = 'NOT AUTHENTICATED'
                await self.swap_account(reason='not logged in')
                return False
            except pgoapi_exceptions.ServerBusyOrOfflineException:
                self.logger.info('Server too busy - restarting')
                self.error_code = 'RETRYING'
                await utils.random_sleep()
            except pgoapi_exceptions.ServerSideRequestThrottlingException:
                self.logger.warning('Server throttling - sleeping for a bit')
                self.error_code = 'THROTTLE'
                await utils.random_sleep(11, 30)
            except pgoapi_exceptions.BannedAccountException:
                self.error_code = 'BANNED?'
                self.logger.warning('Account appears to be banned')
                await self.remove_account()
                return False
            except CaptchaException:
                self.error_code = 'CAPTCHA'
                await self.bench_account()
                self.g['captchas'] += 1
                return False
            except MalformedResponse:
                self.logger.warning('Malformed response received!')
                self.error_code = 'MALFORMED RESPONSE'
                await utils.random_sleep()
            except Exception as err:
                self.logger.exception('A wild exception appeared! {}'.format(err))
                self.error_code = 'EXCEPTION'
                await utils.random_sleep(15, 20)
            else:
                return visited
        return False

    async def visit_point(self, point):
        latitude, longitude = point
        altitude = self.spawns.get_altitude(point)
        altitude = random.uniform(altitude - 1, altitude + 1)
        self.error_code = '!'
        self.logger.info(
            'Visiting {0[0]:.4f},{0[1]:.4f}'.format(point))
        start = time.time()
        self.location = point + [altitude]

        self.api.set_position(latitude, longitude, altitude)

        rounded = utils.round_coords(point, precision=4)
        if rounded not in self.cell_ids:
            self.cell_ids[rounded] = tuple(await self.loop.run_in_executor(
                self.process_executor,
                partial(pgoapi_utils.get_cell_ids, *rounded, radius=500)
            ))
        cell_ids = list(self.cell_ids[rounded])
        since_timestamp_ms = [0] * len(cell_ids)

        request = self.api.create_request()
        request.get_map_objects(cell_id=cell_ids,
                                since_timestamp_ms=since_timestamp_ms,
                                latitude=pgoapi_utils.f2i(latitude),
                                longitude=pgoapi_utils.f2i(longitude))

        responses = await self.call_chain(request)
        self.last_gmo = time.time()

        map_objects = responses.get('GET_MAP_OBJECTS', {})

        sent = False
        pokemon_seen = 0
        forts_seen = 0

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
                pokemon_seen += 1
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

                if (normalized not in db.SIGHTING_CACHE and
                        normalized not in db.MYSTERY_CACHE):
                    self.account_seen += 1
                    self.db_processor.add(normalized)
                    if (config.ENCOUNTER == 'all' and
                            'individual_attack' not in normalized):
                        try:
                            normalized.update(await self.encounter(pokemon))
                        except Exception:
                            self.logger.warning('Exception during encounter.')
            for fort in map_cell.get('forts', []):
                if not fort.get('enabled'):
                    continue
                forts_seen += 1
                if fort.get('type') == 1:  # pokestops
                    if 'lure_info' in fort:
                        norm = utils.normalize_lured(fort, request_time_ms)
                        pokemon_seen += 1
                        if norm not in db.SIGHTING_CACHE:
                            self.account_seen += 1
                            self.db_processor.add(norm)
                    pokestop = utils.normalize_pokestop(fort)
                    self.db_processor.add(pokestop)
                    if (config.SPIN_POKESTOPS and
                            sum(self.items.values()) < self.item_capacity):
                        cooldown = fort.get('cooldown_complete_timestamp_ms')
                        if not cooldown or time.time() > cooldown / 1000:
                            await self.spin_pokestop(pokestop)
                else:
                    self.db_processor.add(utils.normalize_gym(fort))

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
                self.error_code = 'NOTHING SEEN'
                await self.swap_account('no Pokemon or forts seen')
            else:
                self.error_code = ','
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
        if config.MAP_WORKERS:
            self.worker_dict.update([(self.worker_no,
                ((latitude, longitude), start, self.speed, self.total_seen,
                self.visits, pokemon_seen, sent))])
        self.logger.info(
            'Point processed, %d Pokemon and %d forts seen!',
            pokemon_seen,
            forts_seen,
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
        self.coroutines_count = 0
        self.skipped = 0
        self.visits = 0
        self.searches_without_shuffle = 0
        self.coroutine_semaphore = Semaphore(self.count)
        self.redundant = 0
        self.spawns_count = 0
        self.all_seen = False
        self.idle_seconds = 0
        self.logger.info('Overseer initialized')
        if config.PROXIES:
            self.last_proxy = 0

    def kill(self):
        self.killed = True
        print('Killing workers.')
        for worker in self.workers.values():
            worker.kill()

        print('Setting CAPTCHA statuses.')

        while not self.extra_queue.empty():
            account = self.extra_queue.get()
            username = account.get('username')
            self.accounts[username] = account

    def start_worker(self, worker_no, first_run=False):
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
        if config.MAP_WORKERS:
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
        now = time.monotonic()
        last_commit = now
        last_cleaned_cache = now
        last_things_found_updated = now
        last_swap = now
        last_stats_updated = 0

        while not self.killed:
            try:
                now = time.monotonic()
                # Clean cache
                if now - last_cleaned_cache > 900:  # clean cache after 15min
                    self.db_processor.clean_cache()
                    last_cleaned_cache = now
                if now - last_commit > 8:
                    self.db_processor.commit()
                    last_commit = now
                if not self.paused and now - last_swap > 600:
                    if not self.extra_queue.empty():
                        worst, per_minute = self.least_productive()
                        if worst:
                            asyncio.run_coroutine_threadsafe(
                                worst.swap_account(
                                    reason='only {:.1f} seen per minute.'.format(per_minute),
                                    lock=True),
                                loop=self.loop
                            )
                    last_swap = now
                # Record things found count
                if not self.paused and now - last_stats_updated > 5:
                    self.seen_stats, self.visit_stats, self.delay_stats, self.speed_stats = self.get_visit_stats()
                    self.update_coroutines_count()
                    last_stats_updated = now
                if not self.paused and now - last_things_found_updated > 10:
                    self.things_count = self.things_count[-9:]
                    self.things_count.append(str(self.db_processor.count))
                    last_things_found_updated = now
                if self.status_bar:
                    if platform == 'win32':
                        _ = system('cls')
                    else:
                        _ = system('clear')
                    print(self.get_status_message())

                if self.paused:
                    time.sleep(15)
                else:
                    time.sleep(.5)
            except Exception as e:
                self.logger.exception(e)
        # OK, now we're killed
        try:
            while (self.coroutines_count > 0 or
                       self.coroutines_count == '?' or
                       not self.db_processor.queue.empty()):
                try:
                    self.coroutines_count = sum(not t.done()
                                            for t in asyncio.Task.all_tasks(self.loop))
                except RuntimeError:
                    self.coroutines_count = 0
                pending = self.db_processor.queue.qsize()
                # Spaces at the end are important, as they clear previously printed
                # output - \r doesn't clean whole line
                print(
                    '{c} coroutines active, {d} DB items pending   '.format(
                        c=self.coroutines_count, d=pending),
                    end='\r'
                )
                time.sleep(.5)
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.db_processor.queue.put({'type': 'kill'})
            print('Done.                                          ')

    @staticmethod
    def generate_stats(somelist):
        return {
            'max': max(somelist),
            'min': min(somelist),
            'med': median(somelist)
        }

    def get_visit_stats(self):
        visits = []
        seconds_since_start = time.monotonic() - START_TIME - self.idle_seconds
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
            self.coroutines_count = len(asyncio.Task.all_tasks(self.loop))
        except RuntimeError:
            # Set changed size during iteration
            self.coroutines_count = '?'

    def get_status_message(self):
        workers_count = len(self.workers)
        self.spawns_count = len(self.spawns)

        running_for = datetime.now() - self.start_date

        seconds_since_start = time.monotonic() - START_TIME - self.idle_seconds
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

    def least_productive(self):
        worker = None
        lowest = None
        workers = self.workers_list.copy()
        now = time.time()
        for account in workers:
            per_second = account.seen_per_second(now)
            if not lowest or (per_second and per_second < lowest):
                lowest = per_second
                worker = account
        try:
            per_minute = lowest * 60
            return worker, per_minute
        except TypeError:
            return None, None

    async def best_worker(self, point, spawn_time=None):
        skip_time = random.uniform(60, 480)

        worker = None
        lowest_speed = float('inf')
        self.searches_without_shuffle += 1
        if self.searches_without_shuffle > 100:
            random.shuffle(self.workers_list)
            self.searches_without_shuffle = 0
        workers = self.workers_list.copy()
        while worker is None:
            speed = None
            lowest_speed = float('inf')
            worker = None
            for w in workers:
                try:
                    speed = await w.travel_speed(point)
                except Exception as e:
                    self.logger.exception(e)
                    continue
                if (speed and speed < lowest_speed and
                        speed < config.SPEED_LIMIT):
                    if not w.busy.acquire_now():
                        continue
                    try:
                        worker.busy.release()
                    except (AttributeError, RuntimeError):
                        pass
                    lowest_speed = speed
                    worker = w
                    if speed < 10:
                        break
            if self.killed:
                return None
            if worker is None:
                if not spawn_time:
                    return None
                time_diff = time.time() - spawn_time
                if time_diff > skip_time:
                    return None
                await asyncio.sleep(1)
            else:
                worker.speed = lowest_speed
        return worker

    def start_point(self):
        smallest_diff = None
        start = None
        now = time.time() % 3600

        for spawn_id, spawn in self.spawns.items():
            time_diff = abs(spawn[1] - now)
            if not smallest_diff or time_diff < smallest_diff:
                smallest_diff = time_diff
                closest = spawn_id
            if smallest_diff < 1:
                break
        return closest

    def launch(self):
        initial = True
        while not self.killed:
            current_hour = utils.get_current_hour()
            if initial:
                self.spawns.update(loadpickle=True)
                self.mysteries = self.spawns.get_mysteries()
                self.spawns_count = len(self.spawns)
                if self.spawns_count == 0 and len(self.mysteries) == 0:
                    raise ValueError('No spawnpoints.')
                closest = self.start_point()
            else:
                utils.dump_pickle('accounts', self.accounts)
                self.spawns.update()

            for spawn_id, spawn in self.spawns.items():
                if initial:
                    if spawn_id == closest:
                        initial = False
                    else:
                        continue

                if self.captcha_queue.qsize() > config.MAX_CAPTCHAS:
                    self.paused = True
                    try:
                        self.idle_seconds += self.captcha_queue.full_wait(
                            maxsize=config.MAX_CAPTCHAS)
                    except EOFError:
                        pass
                    self.paused = False

                point = list(spawn[0])
                spawn_time = spawn[1] + current_hour

                # negative = hasn't happened yet
                # positive = already happened
                time_diff = time.time() - spawn_time

                while time_diff < 0 and not self.killed:
                    try:
                        mystery_point = list(self.mysteries.popleft())

                        self.coroutine_semaphore.acquire()
                        asyncio.run_coroutine_threadsafe(
                            self.try_point(mystery_point), loop=self.loop
                        )
                    except IndexError:
                        self.mysteries = self.spawns.get_mysteries()
                    time_diff = time.time() - spawn_time

                if time_diff > 5 and spawn_id in db.SIGHTING_CACHE.spawns:
                    self.redundant += 1
                    continue
                elif time_diff > 20:
                    self.skipped += 1
                    continue

                if self.killed:
                    return
                self.coroutine_semaphore.acquire()
                asyncio.run_coroutine_threadsafe(
                    self.try_point(point, spawn_time), loop=self.loop
                )

    async def try_point(self, point, spawn_time=None):
        try:
            point[0] = random.uniform(point[0] - 0.00033, point[0] + 0.00033)
            point[1] = random.uniform(point[1] - 0.00033, point[1] + 0.00033)

            worker = await self.best_worker(point, spawn_time)

            if not worker:
                if spawn_time:
                    self.skipped += 1
                else:
                    self.mysteries.append(point)
                return
            try:
                if spawn_time:
                    if time.time() - spawn_time < 1:
                        asyncio.sleep(1)
                    worker.after_spawn = time.time() - spawn_time

                if await worker.visit(point):
                    self.visits += 1
            finally:
                worker.busy.release()
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.coroutine_semaphore.release()


if __name__ == '__main__':
    START_TIME = time.monotonic()

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
    if config.MAP_WORKERS:
        AccountManager.register('worker_dict', callable=get_workers,
                                proxytype=DictProxy)
    manager = AccountManager(address=utils.get_address(), authkey=config.AUTHKEY)
    manager.start(mgr_init)

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    Slave.loop = loop
    Slave.login_semaphore = asyncio.Semaphore(1, loop=loop)
    Slave.simulation_semaphore = asyncio.Semaphore(2, loop=loop)

    overseer = Overseer(status_bar=args.status_bar, loop=loop, manager=manager)
    overseer.start()
    overseer_thread = Thread(target=overseer.check, name='overseer', daemon=True)
    overseer_thread.start()

    launcher_thread = Thread(target=overseer.launch, name='launcher', daemon=True)
    launcher_thread.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('Exiting, please wait until all tasks finish')
        overseer.kill()

        utils.dump_pickle('accounts', Slave.accounts)
        utils.dump_pickle('cells', Slave.cell_ids)

        pending = asyncio.Task.all_tasks(loop=loop)
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception as e:
            print('Exception: {}'.format(e))
        Slave.db_processor.stop()
        if config.NOTIFY:
            Slave.notifier.session.close()
        Slave.spawns.session.close()
        manager.shutdown()
        try:
            loop.close()
        except RuntimeError:
            pass
