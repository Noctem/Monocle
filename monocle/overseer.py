import asyncio

from datetime import datetime
from statistics import median
from os import system
from sys import platform
from random import shuffle, uniform
from collections import deque
from concurrent.futures import CancelledError
from itertools import dropwhile
from time import time, monotonic

from aiopogo.hash_server import HashServer
from sqlalchemy.exc import OperationalError

from .db import SIGHTING_CACHE, MYSTERY_CACHE
from .utils import get_current_hour, dump_pickle, get_start_coords, get_bootstrap_points, randomize_point, best_factors, percentage_split
from .shared import get_logger, LOOP, run_threaded, ACCOUNTS
from .db_proc import DB_PROC
from . import bounds, spawns, sanitized as conf
from .worker import Worker

BAD_STATUSES = (
    'FAILED LOGIN',
    'EXCEPTION',
    'NOT AUTHENTICATED',
    'KEY EXPIRED',
    'HASHING OFFLINE',
    'NIANTIC OFFLINE',
    'BAD REQUEST',
    'INVALID REQUEST',
    'CAPTCHA',
    'BANNED',
    'BENCHING',
    'REMOVING',
    'IP BANNED',
    'MALFORMED RESPONSE',
    'AIOPOGO ERROR',
    'MAX RETRIES',
    'HASHING ERROR',
    'PROXY ERROR',
    'TIMEOUT'
)

START_TIME = monotonic()


class Overseer:
    def __init__(self, manager):
        self.log = get_logger('overseer')
        self.workers = []
        self.manager = manager
        self.count = conf.GRID[0] * conf.GRID[1]
        self.start_date = datetime.now()
        self.things_count = deque(maxlen=9)
        self.paused = False
        self.coroutines_count = 0
        self.skipped = 0
        self.visits = 0
        self.coroutine_semaphore = asyncio.Semaphore(conf.COROUTINES_LIMIT, loop=LOOP)
        self.redundant = 0
        self.running = True
        self.all_seen = False
        self.idle_seconds = 0
        if platform == 'win32':
            self.clear = 'cls'
        else:
            self.clear = 'clear'
        self.log.info('Overseer initialized')

    def start(self, status_bar):
        self.captcha_queue = self.manager.captcha_queue()
        Worker.captcha_queue = self.manager.captcha_queue()
        self.extra_queue = self.manager.extra_queue()
        Worker.extra_queue = self.manager.extra_queue()
        if conf.MAP_WORKERS:
            Worker.worker_dict = self.manager.worker_dict()

        for username, account in ACCOUNTS.items():
            account['username'] = username
            if account.get('banned'):
                continue
            if account.get('captcha'):
                self.captcha_queue.put(account)
            else:
                self.extra_queue.put(account)

        self.workers = tuple(Worker(worker_no=x) for x in range(self.count))
        DB_PROC.start()
        LOOP.call_later(10, self.update_count)
        LOOP.call_later(max(conf.SWAP_OLDEST, conf.MINIMUM_RUNTIME), self.swap_oldest)
        LOOP.call_soon(self.update_stats)
        if status_bar:
            LOOP.call_soon(self.print_status)

    def update_count(self):
        self.things_count.append(str(DB_PROC.count))
        LOOP.call_later(10, self.update_count)

    def swap_oldest(self):
        if not self.paused and not self.extra_queue.empty():
            oldest, minutes = self.longest_running()
            if minutes > conf.MINIMUM_RUNTIME:
                LOOP.create_task(oldest.lock_and_swap(minutes))
        LOOP.call_later(conf.SWAP_OLDEST, self.swap_oldest)

    def update_stats(self):
        self.seen_stats, self.visit_stats, self.delay_stats, self.speed_stats = self.get_visit_stats()
        self.update_coroutines_count()
        LOOP.call_later(conf.STAT_REFRESH, self.update_stats)

    def print_status(self):
        try:
            system(self.clear)
            print(self.get_status_message())
            if self.running:
                LOOP.call_later(conf.REFRESH_RATE, self.print_status)
        except CancelledError:
            return
        except Exception as e:
            self.log.exception('{} occurred while printing status.', e.__class__.__name__)

    async def exit_progress(self):
        while self.coroutines_count > 2:
            try:
                self.update_coroutines_count()
                pending = DB_PROC.queue.qsize()
                # Spaces at the end are important, as they clear previously printed
                # output - \r doesn't clean whole line
                print(
                    '{} coroutines active, {} DB items pending   '.format(
                        self.coroutines_count, pending),
                    end='\r'
                )
                await asyncio.sleep(.5)
            except CancelledError:
                return
            except Exception as e:
                self.log.exception('A wild {} appeared in exit_progress!', e.__class__.__name__)

    @staticmethod
    def generate_stats(somelist):
        return {
            'max': max(somelist),
            'min': min(somelist),
            'med': median(somelist)
        }

    def get_visit_stats(self):
        visits = []
        seconds_since_start = monotonic() - START_TIME - self.idle_seconds
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
        C = CAPTCHA

        Other letters: various errors and procedures
        """
        dots = []
        messages = []
        row = []
        for i, worker in enumerate(self.workers):
            if i > 0 and i % conf.GRID[1] == 0:
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
            tasks = asyncio.Task.all_tasks(LOOP)
            if self.running:
                self.coroutines_count = len(tasks)
            else:
                self.coroutines_count = sum(not t.done() for t in tasks)
        except RuntimeError:
            # Set changed size during iteration
            self.coroutines_count = '-1'

    def get_status_message(self):
        running_for = datetime.now() - self.start_date

        seconds_since_start = monotonic() - START_TIME - self.idle_seconds
        hours_since_start = seconds_since_start / 3600
        visits_per_second = self.visits / seconds_since_start

        output = [
            'Monocle running for {}'.format(running_for),
            'Known spawns: {}, unknown: {}, more: {}'.format(
                len(spawns),
                len(spawns.unknown),
                spawns.cells_count),
            '{} workers, {} coroutines'.format(
                self.count,
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

        if conf.HASH_KEY:
            try:
                refresh = HashServer.status['period'] - time()
                output.append('Hashes: {r}/{m}, refresh in {t:.0f}'.format(
                    r=HashServer.status['remaining'],
                    m=HashServer.status['maximum'],
                    t=refresh
                ))
            except (KeyError, TypeError):
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

    def longest_running(self):
        workers = (x for x in self.workers if x.start_time)
        worker = next(workers)
        earliest = worker.start_time
        for w in workers:
            if w.start_time < earliest:
                worker = w
                earliest = w.start_time
        minutes = ((time() * 1000) - earliest) / 60000
        return worker, minutes

    def get_start_point(self):
        smallest_diff = float('inf')
        now = time() % 3600
        closest = None

        for spawn_id, spawn_time in spawns.known.values():
            time_diff = now - spawn_time
            if 0 < time_diff < smallest_diff:
                smallest_diff = time_diff
                closest = spawn_id
            if smallest_diff < 3:
                break
        return closest

    async def update_spawns(self, initial=False):
        while True:
            try:
                await run_threaded(spawns.update)
                LOOP.create_task(run_threaded(spawns.pickle))
            except OperationalError as e:
                self.log.exception('Operational error while trying to update spawns.')
                if initial:
                    raise OperationalError('Could not update spawns, ensure your DB is set up.') from e
                await asyncio.sleep(15, loop=LOOP)
            except CancelledError:
                raise
            except Exception as e:
                self.log.exception('A wild {} appeared while updating spawns!', e.__class__.__name__)
                await asyncio.sleep(15, loop=LOOP)
            else:
                break

    async def launch(self, bootstrap, pickle):
        exceptions = 0
        self.next_mystery_reload = 0

        if not pickle or not spawns.unpickle():
            await self.update_spawns(initial=True)

        if not spawns or bootstrap:
            try:
                await self.bootstrap()
                await self.update_spawns()
            except CancelledError:
                return

        update_spawns = False
        self.mysteries = spawns.mystery_gen()
        while True:
            try:
                await self._launch(update_spawns)
                update_spawns = True
            except CancelledError:
                return
            except Exception:
                exceptions += 1
                if exceptions > 25:
                    self.log.exception('Over 25 errors occured in launcher loop, exiting.')
                    return False
                else:
                    self.log.exception('Error occured in launcher loop.')
                    update_spawns = False

    async def _launch(self, update_spawns):
        if update_spawns:
            await self.update_spawns()
            LOOP.create_task(run_threaded(dump_pickle, 'accounts', ACCOUNTS))
            spawns_iter = iter(spawns.items())
        else:
            start_point = self.get_start_point()
            if start_point and not spawns.after_last():
                spawns_iter = dropwhile(
                    lambda s: s[1][0] != start_point, spawns.items())
            else:
                spawns_iter = iter(spawns.items())

        current_hour = get_current_hour()
        if spawns.after_last():
            current_hour += 3600

        captcha_limit = conf.MAX_CAPTCHAS
        skip_spawn = conf.SKIP_SPAWN
        for point, (spawn_id, spawn_seconds) in spawns_iter:
            try:
                if self.captcha_queue.qsize() > captcha_limit:
                    self.paused = True
                    self.idle_seconds += await run_threaded(self.captcha_queue.full_wait, conf.MAX_CAPTCHAS)
                    self.paused = False
            except (EOFError, BrokenPipeError, FileNotFoundError):
                pass

            spawn_time = spawn_seconds + current_hour

            # negative = hasn't happened yet
            # positive = already happened
            time_diff = time() - spawn_time

            while time_diff < 0.5:
                try:
                    mystery_point = next(self.mysteries)

                    await self.coroutine_semaphore.acquire()
                    LOOP.create_task(self.try_point(mystery_point))
                except StopIteration:
                    if self.next_mystery_reload < monotonic():
                        self.mysteries = spawns.mystery_gen()
                        self.next_mystery_reload = monotonic() + conf.RESCAN_UNKNOWN
                    else:
                        await asyncio.sleep(min(spawn_time - time() + .5, self.next_mystery_reload - monotonic()), loop=LOOP)
                time_diff = time() - spawn_time

            if time_diff > 5 and spawn_id in SIGHTING_CACHE.store:
                self.redundant += 1
                continue
            elif time_diff > skip_spawn:
                self.skipped += 1
                continue

            await self.coroutine_semaphore.acquire()
            LOOP.create_task(self.try_point(point, spawn_time))

    async def try_again(self, point):
        async with self.coroutine_semaphore:
            worker = await self.best_worker(point, False)
            async with worker.busy:
                if await worker.visit(point):
                    self.visits += 1

    async def bootstrap(self):
        try:
            self.log.warning('Starting bootstrap phase 1.')
            await self.bootstrap_one()
        except CancelledError:
            raise
        except Exception:
            self.log.exception('An exception occurred during bootstrap phase 1.')

        try:
            self.log.warning('Starting bootstrap phase 2.')
            await self.bootstrap_two()
        except CancelledError:
            raise
        except Exception:
            self.log.exception('An exception occurred during bootstrap phase 2.')

        self.log.warning('Starting bootstrap phase 3.')
        unknowns = list(spawns.unknown)
        shuffle(unknowns)
        tasks = (self.try_again(point) for point in unknowns)
        await asyncio.gather(*tasks, loop=LOOP)
        self.log.warning('Finished bootstrapping.')

    async def bootstrap_one(self):
        async def visit_release(worker, num, *args):
            async with self.coroutine_semaphore:
                async with worker.busy:
                    point = get_start_coords(num, *args)
                    self.log.warning('start_coords: {}', point)
                    self.visits += await worker.bootstrap_visit(point)

        if bounds.multi:
            areas = [poly.boundaries.area for poly in bounds.polygons]
            area_sum = sum(areas)
            percentages = [area / area_sum for area in areas]
            tasks = []
            for i, workers in enumerate(percentage_split(
                    self.workers, percentages)):
                grid = best_factors(len(workers))
                tasks.extend(visit_release(w, n, grid, bounds.polygons[i])
                             for n, w in enumerate(workers))
        else:
            tasks = (visit_release(w, n) for n, w in enumerate(self.workers))
        await asyncio.gather(*tasks, loop=LOOP)

    async def bootstrap_two(self):
        async def bootstrap_try(point):
            async with self.coroutine_semaphore:
                randomized = randomize_point(point, randomization)
                LOOP.call_later(1790, LOOP.create_task, self.try_again(randomized))
                worker = await self.best_worker(point, False)
                async with worker.busy:
                    self.visits += await worker.bootstrap_visit(point)

        # randomize to within ~140m of the nearest neighbor on the second visit
        randomization = conf.BOOTSTRAP_RADIUS / 155555 - 0.00045
        tasks = (bootstrap_try(x) for x in get_bootstrap_points(bounds))
        await asyncio.gather(*tasks, loop=LOOP)

    async def try_point(self, point, spawn_time=None):
        try:
            point = randomize_point(point)
            skip_time = monotonic() + (conf.GIVE_UP_KNOWN if spawn_time else conf.GIVE_UP_UNKNOWN)
            worker = await self.best_worker(point, skip_time)
            if not worker:
                if spawn_time:
                    self.skipped += 1
                return
            async with worker.busy:
                if spawn_time:
                    worker.after_spawn = time() - spawn_time

                if await worker.visit(point):
                    self.visits += 1
        except CancelledError:
            raise
        except Exception:
            self.log.exception('An exception occurred in try_point')
        finally:
            self.coroutine_semaphore.release()

    async def best_worker(self, point, skip_time):
        good_enough = conf.GOOD_ENOUGH
        while self.running:
            gen = (w for w in self.workers if not w.busy.locked())
            try:
                worker = next(gen)
                lowest_speed = worker.travel_speed(point)
            except StopIteration:
                lowest_speed = float('inf')
            for w in gen:
                speed = w.travel_speed(point)
                if speed < lowest_speed:
                    lowest_speed = speed
                    worker = w
                    if speed < good_enough:
                        break
            if lowest_speed < conf.SPEED_LIMIT:
                worker.speed = lowest_speed
                return worker
            if skip_time and monotonic() > skip_time:
                return None
            await asyncio.sleep(conf.SEARCH_SLEEP, loop=LOOP)

    def refresh_dict(self):
        while not self.extra_queue.empty():
            account = self.extra_queue.get()
            username = account['username']
            ACCOUNTS[username] = account
