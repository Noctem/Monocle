from asyncio import gather, Semaphore, sleep, Task, CancelledError
from datetime import datetime
from statistics import median
from sys import platform
from cyrandom import shuffle
from collections import deque
from itertools import dropwhile
from time import time, monotonic

from aiopogo.hash_server import HashServer
from sqlalchemy.exc import OperationalError

from .db import SIGHTING_CACHE, MYSTERY_CACHE
from .utils import get_current_hour, dump_pickle, get_start_coords, get_bootstrap_points, randomize_point, best_factors, percentage_split
from .shared import get_logger, LOOP, run_threaded, ACCOUNTS
from . import bounds, db_proc, spawns, sanitized as conf
from .worker import Worker

ANSI = '\x1b[2J\x1b[H'
if platform == 'win32':
    try:
        from platform import win32_ver
        from distutils.version import LooseVersion
        if LooseVersion(win32_ver()[1]) >= LooseVersion('10.0.10586'):
            import ctypes
            ctypes.windll.kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        else:
            from os import system
            ANSI = ''
    except Exception:
        from os import system
        ANSI = ''

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


class Overseer:
    def __init__(self, manager):
        self.log = get_logger('overseer')
        self.workers = []
        self.manager = manager
        self.things_count = deque(maxlen=9)
        self.paused = False
        self.coroutines_count = 0
        self.skipped = 0
        self.visits = 0
        self.coroutine_semaphore = Semaphore(conf.COROUTINES_LIMIT, loop=LOOP)
        self.redundant = 0
        self.running = True
        self.all_seen = False
        self.idle_seconds = 0
        self.log.info('Overseer initialized')
        self.pokemon_found = ''

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

        self.workers = tuple(Worker(worker_no=x) for x in range(conf.GRID[0] * conf.GRID[1]))
        db_proc.start()
        LOOP.call_later(10, self.update_count)
        LOOP.call_later(max(conf.SWAP_OLDEST, conf.MINIMUM_RUNTIME), self.swap_oldest)
        LOOP.call_soon(self.update_stats)
        if status_bar:
            LOOP.call_soon(self.print_status)

    def update_count(self):
        self.things_count.append(str(db_proc.count))
        self.pokemon_found = (
            'Pokemon found count (10s interval):\n'
            + ' '.join(self.things_count)
            + '\n')
        LOOP.call_later(10, self.update_count)

    def swap_oldest(self, interval=conf.SWAP_OLDEST, minimum=conf.MINIMUM_RUNTIME):
        if not self.paused and not self.extra_queue.empty():
            oldest, minutes = self.longest_running()
            if minutes > minimum:
                LOOP.create_task(oldest.lock_and_swap(minutes))
        LOOP.call_later(interval, self.swap_oldest)

    def print_status(self, refresh=conf.REFRESH_RATE):
        try:
            self._print_status()
        except CancelledError:
            return
        except Exception as e:
            self.log.exception('{} occurred while printing status.', e.__class__.__name__)
        self.print_handle = LOOP.call_later(refresh, self.print_status)

    async def exit_progress(self):
        while self.coroutines_count > 2:
            try:
                self.update_coroutines_count(simple=False)
                pending = len(db_proc)
                # Spaces at the end are important, as they clear previously printed
                # output - \r doesn't clean whole line
                print(
                    '{} coroutines active, {} DB items pending   '.format(
                        self.coroutines_count, pending),
                    end='\r'
                )
                await sleep(.5)
            except CancelledError:
                return
            except Exception as e:
                self.log.exception('A wild {} appeared in exit_progress!', e.__class__.__name__)

    def update_stats(self, refresh=conf.STAT_REFRESH, med=median, count=conf.GRID[0] * conf.GRID[1]):
        visits = []
        seen_per_worker = []
        after_spawns = []
        speeds = []

        for w in self.workers:
            after_spawns.append(w.after_spawn)
            seen_per_worker.append(w.total_seen)
            visits.append(w.visits)
            speeds.append(w.speed)

        self.stats = (
            'Seen per worker: min {}, max {}, med {:.0f}\n'
            'Visits per worker: min {}, max {}, med {:.0f}\n'
            'Visit delay: min {:.1f}, max {:.1f}, med {:.1f}\n'
            'Speed: min {:.1f}, max {:.1f}, med {:.1f}\n'
            'Extra accounts: {}, CAPTCHAs needed: {}\n'
        ).format(
            min(seen_per_worker), max(seen_per_worker), med(seen_per_worker),
            min(visits), max(visits), med(visits),
            min(after_spawns), max(after_spawns), med(after_spawns),
            min(speeds), max(speeds), med(speeds),
            self.extra_queue.qsize(), self.captcha_queue.qsize()
        )

        self.sighting_cache_size = len(SIGHTING_CACHE.store)
        self.mystery_cache_size = len(MYSTERY_CACHE.store)

        self.update_coroutines_count()
        self.counts = (
            'Known spawns: {}, unknown: {}, more: {}\n'
            '{} workers, {} coroutines\n'
            'sightings cache: {}, mystery cache: {}, DB queue: {}\n'
        ).format(
            len(spawns), len(spawns.unknown), spawns.cells_count,
            count, self.coroutines_count,
            len(SIGHTING_CACHE), len(MYSTERY_CACHE), len(db_proc)
        )
        LOOP.call_later(refresh, self.update_stats)

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

    def update_coroutines_count(self, simple=True, loop=LOOP):
        try:
            tasks = Task.all_tasks(loop)
            self.coroutines_count = len(tasks) if simple else sum(not t.done() for t in tasks)
        except RuntimeError:
            # Set changed size during iteration
            self.coroutines_count = '-1'

    def _print_status(self, _ansi=ANSI, _start=datetime.now(), _notify=conf.NOTIFY):
        running_for = datetime.now() - _start

        seconds_since_start = running_for.seconds - self.idle_seconds or 0.1
        hours_since_start = seconds_since_start / 3600

        output = [
            '{}Monocle running for {}'.format(_ansi, running_for),
            self.counts,
            self.stats,
            self.pokemon_found,
            ('Visits: {}, per second: {:.2f}\n'
             'Skipped: {}, unnecessary: {}').format(
                self.visits, self.visits / seconds_since_start,
                self.skipped, self.redundant)
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

        try:
            hash_status = HashServer.status
            output.append('Hashes: {}/{}, refresh in {:.0f}'.format(
                hash_status['remaining'],
                hash_status['maximum'],
                hash_status['period'] - time()
            ))
        except (KeyError, TypeError):
            pass

        if _notify:
            sent = Worker.notifier.sent
            output.append('Notifications sent: {}, per hour {:.1f}'.format(
                sent, sent / hours_since_start))

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
            output.append('\nCAPTCHAs are needed to proceed.')
        if not _ansi:
            system('cls')
        print('\n'.join(output))

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
                await sleep(15, loop=LOOP)
            except CancelledError:
                raise
            except Exception as e:
                self.log.exception('A wild {} appeared while updating spawns!', e.__class__.__name__)
                await sleep(15, loop=LOOP)
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
                        await sleep(min(spawn_time - time() + .5, self.next_mystery_reload - monotonic()), loop=LOOP)
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
        await gather(*tasks, loop=LOOP)
        self.log.warning('Finished bootstrapping.')

    async def bootstrap_one(self):
        async def visit_release(worker, num, *args):
            async with self.coroutine_semaphore:
                async with worker.busy:
                    point = get_start_coords(num, *args)
                    self.log.warning('start_coords: {}', point)
                    self.visits += await worker.bootstrap_visit(point)

        if bounds.multi:
            areas = [poly.polygon.area for poly in bounds.polygons]
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
        await gather(*tasks, loop=LOOP)

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
        await gather(*tasks, loop=LOOP)

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
            await sleep(conf.SEARCH_SLEEP, loop=LOOP)

    def refresh_dict(self):
        while not self.extra_queue.empty():
            account = self.extra_queue.get()
            username = account['username']
            ACCOUNTS[username] = account
