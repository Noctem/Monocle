import asyncio

from datetime import datetime
from statistics import median
from threading import active_count
from os import system
from sys import platform
from random import uniform
from collections import deque
from pogo_async.hash_server import HashServer
from sqlalchemy.exc import OperationalError

import time

try:
    import _thread
except ImportError:
    import _dummy_thread as _thread

from .db import SIGHTING_CACHE, MYSTERY_CACHE, FORT_CACHE
from .utils import get_current_hour, dump_pickle, get_start_coords, get_bootstrap_points, randomize_point
from .shared import get_logger, LOOP
from .db_proc import DB_PROC
from .spawns import SPAWNS
from . import config
from .worker import Worker

BAD_STATUSES = (
    'FAILED LOGIN',
    'EXCEPTION',
    'NOT AUTHENTICATED',
    'KEY EXPIRED',
    'HASHING OFFLINE',
    'NIANTIC OFFLINE',
    'THROTTLE',
    'CAPTCHA',
    'BANNED',
    'BENCHING',
    'REMOVING',
    'IP BANNED',
    'MALFORMED RESPONSE',
    'PGOAPI ERROR',
    'MAX RETRIES',
    'HASHING ERROR',
    'PROXY ERROR',
    'TIMEOUT'
)

START_TIME = time.monotonic()


class Overseer:
    accounts = Worker.accounts

    def __init__(self, status_bar, manager):
        self.log = get_logger('overseer')
        self.workers = []
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
        self.mysteries = deque()
        self.coroutine_semaphore = asyncio.Semaphore(config.COROUTINES_LIMIT)
        self.redundant = 0
        self.all_seen = False
        self.idle_seconds = 0
        self.log.info('Overseer initialized')

    def start(self):
        self.captcha_queue = self.manager.captcha_queue()
        Worker.captcha_queue = self.manager.captcha_queue()
        self.extra_queue = self.manager.extra_queue()
        Worker.extra_queue = self.manager.extra_queue()
        if config.MAP_WORKERS:
            Worker.worker_dict = self.manager.worker_dict()

        for username, account in self.accounts.items():
            account['username'] = username
            if account.get('banned'):
                continue
            if account.get('captcha'):
                self.captcha_queue.put(account)
            else:
                self.extra_queue.put(account)

        self.workers = tuple(Worker(worker_no=x) for x in range(self.count))
        DB_PROC.start()

    async def check(self):
        now = time.monotonic()
        last_commit = now
        last_things_found_updated = now
        last_swap = now
        last_stats_updated = 0

        while not self.killed:
            try:
                now = time.monotonic()
                if now - last_commit > 5:
                    DB_PROC.commit()
                    last_commit = now
                if not self.paused and now - last_swap > config.SWAP_WORST:
                    if not self.extra_queue.empty():
                        worst, per_minute = self.least_productive()
                        if worst:
                            asyncio.ensure_future(
                                worst.swap_account(
                                    reason='only {:.1f} seen per minute'.format(per_minute),
                                    lock=True),
                                loop=LOOP
                            )
                    last_swap = now
                # Record things found count
                if not self.paused and now - last_stats_updated > config.STAT_REFRESH:
                    self.seen_stats, self.visit_stats, self.delay_stats, self.speed_stats = self.get_visit_stats()
                    self.update_coroutines_count()
                    last_stats_updated = now
                if not self.paused and now - last_things_found_updated > 10:
                    self.things_count = self.things_count[-9:]
                    self.things_count.append(str(DB_PROC.count))
                    last_things_found_updated = now
                if self.status_bar:
                    if platform == 'win32':
                        system('cls')
                    else:
                        system('clear')
                    print(self.get_status_message())

                if self.paused:
                    await asyncio.sleep(max(15, config.REFRESH_RATE))
                else:
                    await asyncio.sleep(config.REFRESH_RATE)
            except Exception as e:
                self.log.exception('A wild {} appeared in check!', e.__class__.__name__)
        # OK, now we're killed
        try:
            while (self.coroutines_count > 0 or
                       self.coroutines_count == '?' or
                       not DB_PROC.queue.empty()):
                try:
                    self.coroutines_count = sum(not t.done()
                                            for t in asyncio.Task.all_tasks(LOOP))
                except RuntimeError:
                    self.coroutines_count = 0
                pending = DB_PROC.queue.qsize()
                # Spaces at the end are important, as they clear previously printed
                # output - \r doesn't clean whole line
                print(
                    '{c} coroutines active, {d} DB items pending   '.format(
                        c=self.coroutines_count, d=pending),
                    end='\r'
                )
                await asyncio.sleep(.5)
        except Exception as e:
            self.log.exception('A wild {} appeared during exit!', e.__class__.__name__)
        finally:
            DB_PROC.queue.put({'type': 'kill'})
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

        for w in self.workers:
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
        , = visited less than a minute ago, no pokemon seen
        0 = visited less than a minute ago, no pokemon or forts seen
        : = visited less than a minute ago, pokemon seen
        ! = currently visiting
        | = cleaning bag
        $ = spinning a PokéStop
        * = sending a notification
        ~ = encountering a Pokémon
        I = initial, haven't done anything yet
        » = waiting to log in (limited by SIMULTANEOUS_LOGINS)
        ° = waiting to start app simulation (limited by SIMULTANEOUS_SIMULATION)
        ∞ = bootstrapping
        L = logging in
        A = simulating app startup
        T = completing the tutorial
        X = something bad happened
        H = waiting for the next period on the hashing server
        C = CAPTCHA

        Other letters: various errors and procedures
        """
        dots = []
        messages = []
        row = []
        for i, worker in enumerate(self.workers):
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
            self.coroutines_count = len(asyncio.Task.all_tasks(LOOP))
        except RuntimeError:
            # Set changed size during iteration
            self.coroutines_count = '?'

    def get_status_message(self):
        running_for = datetime.now() - self.start_date

        seconds_since_start = time.monotonic() - START_TIME - self.idle_seconds
        hours_since_start = seconds_since_start / 3600
        visits_per_second = self.visits / seconds_since_start

        output = [
            'Monocle running for {}'.format(running_for),
            'Known spawns: {}, unknown: {}, more: {}'.format(
                len(SPAWNS),
                SPAWNS.mysteries_count,
                SPAWNS.cells_count),
            '{} workers, {} threads, {} coroutines'.format(
                self.count,
                active_count(),
                self.coroutines_count),
            'DB queue: {}, sightings cache: {}, mystery cache: {}'.format(
                DB_PROC.queue.qsize(),
                len(SIGHTING_CACHE.store),
                len(MYSTERY_CACHE.store)),
            '',
            'Seen per worker: min {min}, max {max}, med {med:.0f}'.format(
                **self.seen_stats),
            'Visits per worker: min {min}, max {max:}, med {med:.0f}'.format(
                **self.visit_stats),
            'Visit delay: min {min:.1f}, max {max:.1f}, med {med:.1f}'.format(
                **self.delay_stats),
            'Speed: min {min:.1f}, max {max:.1f}, med {med:.1f}'.format(
                **self.speed_stats),
            'Extra accounts: {}, CAPTCHAs needed: {}'.format(
                self.extra_queue.qsize(),
                self.captcha_queue.qsize()),
            '',
            'Pokemon found count (10s interval):',
            ' '.join(self.things_count),
            '',
            'Visits: {}, per second: {:.2f}'.format(
                self.visits,
                visits_per_second),
            'Skipped: {}, unnecessary: {}'.format(
                self.skipped,
                self.redundant)
        ]

        try:
            seen = Worker.g['seen']
            captchas = Worker.g['captchas']
            output.append('Seen per visit: {v:.2f}, per minute: {m:.0f}'.format(
                v=seen / self.visits, m=seen / (seconds_since_start / 60)))

            if captchas:
                captchas_per_request = captchas / (self.visits / 1000)
                captchas_per_hour = captchas / hours_since_start
                output.append(
                    'CAPTCHAs per 1K visits: {r:.1f}, per hour: {h:.1f}, total: {t:d}'.format(
                    r=captchas_per_request, h=captchas_per_hour, t=captchas))
        except ZeroDivisionError:
            pass

        if config.HASH_KEY:
            try:
                refresh = HashServer.status.get('period') - time.time()
                output.append('Hashes: {r}/{m}, refresh in {t:.0f}'.format(
                    r=HashServer.status.get('remaining'),
                    m=HashServer.status.get('maximum'),
                    t=refresh
                ))
            except TypeError:
                pass

        try:
            output.append('Notifications sent: {}, per hour {:.1f}'.format(
                Worker.notifier.sent, Worker.notifier.sent / hours_since_start))
        except AttributeError:
            pass

        output.append('')
        if not self.all_seen:
            no_sightings = ', '.join(str(w.worker_no)
                                     for w in self.workers
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
        now = time.time()
        for account in self.workers:
            per_second = account.seen_per_second(now)
            if not lowest or (per_second and per_second < lowest):
                lowest = per_second
                worker = account
        try:
            per_minute = lowest * 60
            return worker, per_minute
        except TypeError:
            return None, None

    def get_start_point(self):
        smallest_diff = float('inf')
        now = time.time() % 3600
        closest = None

        for spawn_id, spawn in SPAWNS.items():
            time_diff = now - spawn[1]
            if 0 < time_diff < smallest_diff:
                smallest_diff = time_diff
                closest = spawn_id
            if smallest_diff < 3:
                break
        return closest

    async def launch(self, bootstrap, pickle):
        initial = True
        exceptions = 0
        while not self.killed:
            if not initial:
                pickle = False
                bootstrap = False

            while True:
                try:
                    await LOOP.run_in_executor(None, SPAWNS.update, pickle)
                except OperationalError as e:
                    self.log.exception('Operational error while trying to update spawns.')
                    if initial:
                        raise OperationalError('Could not update spawns, ensure your DB is set up.') from e
                    await asyncio.sleep(20)
                except Exception as e:
                    self.log.exception('A wild {} appeared while updating spawns!', e.__class__.__name__)
                    await asyncio.sleep(20)
                else:
                    break

            if not SPAWNS or bootstrap:
                self.bootstrap()

            current_hour = get_current_hour()
            if SPAWNS.after_last():
                current_hour += 3600
                initial = False

            if initial:
                start_point = self.get_start_point()
                if not start_point:
                    initial = False
            else:
                asyncio.ensure_future(
                    LOOP.run_in_executor(None, dump_pickle, 'accounts', self.accounts)
                )

            for spawn_id, spawn in SPAWNS.items():
                try:
                    if initial:
                        if spawn_id == start_point:
                            initial = False
                        else:
                            continue

                    try:
                        if self.captcha_queue.qsize() > config.MAX_CAPTCHAS:
                            self.paused = True
                            self.idle_seconds += LOOP.run_in_executor(None, self.captcha_queue.full_wait, config.MAX_CAPTCHAS)
                            self.paused = False
                    except (EOFError, BrokenPipeError, FileNotFoundError):
                        continue

                    point = spawn[0]
                    spawn_time = spawn[1] + current_hour

                    # negative = hasn't happened yet
                    # positive = already happened
                    time_diff = time.time() - spawn_time

                    while time_diff < 0:
                        try:
                            mystery_point = self.mysteries.popleft()

                            await self.coroutine_semaphore.acquire()
                            asyncio.ensure_future(
                                self.try_point(mystery_point), loop=LOOP
                            )
                        except IndexError:
                            self.mysteries = SPAWNS.get_mysteries()
                            if not self.mysteries:
                                time_diff = time.time() - spawn_time
                                break
                        time_diff = time.time() - spawn_time

                    if time_diff > 5 and spawn_id in SIGHTING_CACHE.store:
                        self.redundant += 1
                        continue
                    elif time_diff > config.SKIP_SPAWN:
                        self.skipped += 1
                        continue

                    await self.coroutine_semaphore.acquire()
                    asyncio.ensure_future(
                        self.try_point(point, spawn_time), loop=LOOP
                    )
                except Exception:
                    exceptions += 1
                    if exceptions > 100:
                        self.log.exception('Over 100 errors occured in launcher loop, exiting.')
                        return False
                    else:
                        self.log.exception('Error occured in launcher loop.')

    async def bootstrap(self):
        try:
            await self.bootstrap_one()
            await asyncio.sleep(10)
        except Exception:
            self.log.exception('An exception occurred during bootstrap phase 1.')

        try:
            self.log.warning('Starting bootstrap phase 2.')
            await self.bootstrap_two()
            await asyncio.sleep(1)
            self.log.warning('Finished bootstrapping.')
        except Exception:
            self.log.exception('An exception occurred during bootstrap phase 2.')

    async def bootstrap_one(self):
        async def visit_release(worker, point):
            async with self.coroutine_semaphore:
                async with worker.busy:
                    if await worker.bootstrap_visit(point):
                        self.visits += 1

        for worker in self.workers:
            number = worker.worker_no
            worker.bootstrap = True
            point = get_start_coords(number)
            await asyncio.sleep(.25)
            asyncio.ensure_future(visit_release(worker, point), loop=LOOP)

    def bootstrap_two(self):
        async def bootstrap_try(point):
            async with self.coroutine_semaphore:
                worker = await self.best_worker(point, must_visit=True)
                try:
                    if await worker.bootstrap_visit(point):
                        self.visits += 1
                finally:
                    worker.busy.release()

        for point in get_bootstrap_points():
            asyncio.ensure_future(bootstrap_try(point), loop=LOOP)

    async def try_point(self, point, spawn_time=None):
        try:
            point = randomize_point(point)
            worker = await self.best_worker(point, spawn_time)

            if not worker:
                if spawn_time:
                    self.skipped += 1
                else:
                    self.mysteries.append(point)
                return
            try:
                if spawn_time:
                    time_diff = spawn_time - time.time() + 1
                    if time_diff > 0:
                        await asyncio.sleep(time_diff)
                    worker.after_spawn = time.time() - spawn_time

                if await worker.visit(point):
                    self.visits += 1
            finally:
                try:
                    worker.busy.release()
                except RuntimeError:
                    pass
        except Exception:
            self.log.exception('An exception occurred in try_point')
        finally:
            self.coroutine_semaphore.release()

    async def best_worker(self, point, spawn_time=None, must_visit=False):
        if spawn_time:
            skip_time = max(time.monotonic() + config.GIVE_UP_KNOWN, spawn_time)
        elif must_visit:
            skip_time = None
        else:
            skip_time = time.monotonic() + config.GIVE_UP_UNKNOWN

        while True:
            speed = None
            lowest_speed = float('inf')
            for w in self.workers:
                speed = w.travel_speed(point)
                if speed and speed < lowest_speed and speed < config.SPEED_LIMIT:
                    if not w.busy.acquire_now():
                        continue
                    try:
                        worker.busy.release()
                    except (NameError, AttributeError, RuntimeError):
                        pass
                    lowest_speed = speed
                    worker = w
                    if config.GOOD_ENOUGH and speed < config.GOOD_ENOUGH:
                        break
            try:
                worker.speed = lowest_speed
                return worker
            except (NameError, AttributeError):
                try:
                    if self.killed or time.monotonic() > skip_time:
                        return None
                except TypeError:
                    pass
                await asyncio.sleep(config.SEARCH_SLEEP)

    def kill(self):
        self.killed = True
        print('Killing workers.')
        for worker in self.workers:
            worker.kill()

        FORT_CACHE.pickle()

        while not self.extra_queue.empty():
            account = self.extra_queue.get()
            username = account.get('username')
            self.accounts[username] = account
