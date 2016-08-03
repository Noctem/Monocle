# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
import argparse
import asyncio
import logging
import os
import random
import sys
import threading
import time

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
    'CYCLES_PER_WORKER',
    'MAP_START',
    'MAP_END',
    'GRID',
    'ACCOUNTS',
    'SCAN_RADIUS',
    'SCAN_DELAY',
)
for setting_name in REQUIRED_SETTINGS:
    if not hasattr(config, setting_name):
        raise RuntimeError('Please set "{}" in config'.format(setting_name))


class CannotProcessStep(Exception):
    """Raised when servers are too busy"""


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

logger = logging.getLogger()


class Slave:
    """Single worker walking on the map"""
    def __init__(self, worker_no, points):
        self.worker_no = worker_no
        self.points = points
        self.count_points = len(self.points)
        self.step = 0
        self.cycle = 0
        self.seen_per_cycle = 0
        self.total_seen = 0
        self.error_code = None
        self.running = True
        self.restart_me = False
        self.logged_in = False
        center = self.points[0]
        self.logger = logging.getLogger('worker-{}'.format(worker_no))
        self.api = PGoApi()
        self.api.set_position(center[0], center[1], 100)  # lat, lon, alt
        self.api.set_logger(self.logger)

    async def first_run(self):
        await asyncio.sleep(self.worker_no * 2)
        await self.login()

    async def run(self):
        if not self.logged_in:
            await self.login()
        await self.run_cycle()

    async def login(self):
        """Logs worker in and prepares for scanning"""
        self.cycle = 1
        self.error_code = None
        loop = asyncio.get_event_loop()
        service = config.ACCOUNTS[self.worker_no][2]
        self.error_code = 'LOGIN'
        while True:
            try:
                await loop.run_in_executor(None, partial(
                    self.api.login,
                    provider=service,
                    username=config.ACCOUNTS[self.worker_no][0],
                    password=config.ACCOUNTS[self.worker_no][1],
                ))
            except pgoapi_exceptions.AuthException:
                self.error_code = 'LOGIN FAIL'
                await self.restart()
                return
            except pgoapi_exceptions.NotLoggedInException:
                self.error_code = 'BAD LOGIN'
                await self.restart()
                return
            except pgoapi_exceptions.ServerBusyOrOfflineException:
                self.error_code = 'RETRYING'
                await self.restart()
                return
            except pgoapi_exceptions.ServerSideRequestThrottlingException:
                await self.sleep(random.uniform(5, 10))
                continue
            except Exception:
                self.logger.exception('A wild exception appeared!')
                self.error_code = 'EXCEPTION'
                await self.restart()
                return
            break
        self.logged_in = True
        self.error_code = 'READY'

    async def run_cycle(self):
        """Wrapper for self.main - runs it a few times before restarting

        Also is capable of restarting in case an error occurs.
        """
        self.error_code = None
        while self.cycle <= config.CYCLES_PER_WORKER:
            if not self.running:
                await self.restart()
                return
            try:
                await self.main()
            except CannotProcessStep:
                self.error_code = 'RESTART'
                await self.restart()
            except Exception:
                self.logger.exception('A wild exception appeared!')
                self.error_code = 'EXCEPTION'
                await self.restart()
                return
            if not self.running:
                await self.restart()
                return
            self.cycle += 1
            if self.cycle <= config.CYCLES_PER_WORKER:
                self.error_code = 'SLEEP'
                self.running = False
                await self.sleep(random.randint(30, 60))
                self.running = True
                self.error_code = None
        self.error_code = 'RESTART'
        await self.restart()

    async def main(self):
        """Heart of the worker - goes over each point and reports sightings"""
        session = db.Session()
        self.seen_per_cycle = 0
        self.step = 0
        loop = asyncio.get_event_loop()
        for i, point in enumerate(self.points):
            if not self.running:
                return
            self.logger.info(
                'Visiting point %d (%s %s)', i, point[0], point[1]
            )
            cell_ids = pgoapi_utils.get_cell_ids(point[0], point[1])
            self.api.set_position(point[0], point[1], 100)
            response_dict = await loop.run_in_executor(None, partial(
                self.api.get_map_objects,
                latitude=pgoapi_utils.f2i(point[0]),
                longitude=pgoapi_utils.f2i(point[1]),
                cell_id=cell_ids
            ))
            if response_dict is False:
                raise CannotProcessStep
            now = time.time()
            map_objects = response_dict['responses'].get('GET_MAP_OBJECTS', {})
            pokemons = []
            if map_objects.get('status') == 1:
                for map_cell in map_objects['map_cells']:
                    for pokemon in map_cell.get('wild_pokemons', []):
                        # Care only about 15 min spawns
                        # 30 and 45 min ones will be just put after
                        # time_till_hidden is below 15 min
                        if pokemon['time_till_hidden_ms'] < 0:
                            continue
                        pokemons.append(self.normalize_pokemon(pokemon, now))
            for raw_pokemon in pokemons:
                db.add_sighting(session, raw_pokemon)
                self.seen_per_cycle += 1
                self.total_seen += 1
            self.logger.info(
                'Point processed, %d Pokemons seen!', len(pokemons)
            )
            session.commit()
            # Clear error code and let know that there are Pokemon
            if self.error_code and self.seen_per_cycle:
                self.error_code = None
            self.step += 1
            await self.sleep(
                random.uniform(config.SCAN_DELAY, config.SCAN_DELAY + 2)
            )
        session.close()
        if self.seen_per_cycle == 0:
            self.error_code = 'NO POKEMON'

    @staticmethod
    def normalize_pokemon(raw, now):
        """Normalizes data coming from API into something acceptable by db"""
        return {
            'encounter_id': raw['encounter_id'],
            'spawn_id': raw['spawn_point_id'],
            'pokemon_id': raw['pokemon_data']['pokemon_id'],
            'expire_timestamp': now + raw['time_till_hidden_ms'] / 1000.0,
            'lat': raw['latitude'],
            'lon': raw['longitude'],
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
        while duration > 0:
            if not self.running:
                return
            await asyncio.sleep(0.5)
            duration -= 0.5

    async def restart(self, sleep_min=5, sleep_max=20):
        """Sleeps for a bit, then restarts"""
        await self.sleep(random.randint(sleep_min, sleep_max))
        self.restart_me = True

    def kill(self):
        """Marks worker as not running

        It should stop any operation as soon as possible and restart itself.
        """
        self.error_code = 'KILLED'
        self.running = False


class Overseer:
    def __init__(self, status_bar):
        self.workers = {}
        self.count = config.GRID[0] * config.GRID[1]
        self.points = utils.get_points_per_worker()
        self.start_date = datetime.now()
        self.status_bar = status_bar
        self.killed = False

    def kill(self):
        self.killed = True
        for worker in self.workers.values():
            worker.kill()

    def start_worker(self, worker_no, first_run=False):
        if self.killed:
            return
        loop = asyncio.get_event_loop()
        worker = Slave(worker_no, self.points[worker_no])
        self.workers[worker_no] = worker
        future = loop.create_task(worker.first_run())
        # For first time, we need to wait until all workers login before
        # scanning
        if first_run:
            return future
        loop.create_task(worker.run())

    def get_point_stats(self):
        lenghts = [len(p) for p in self.points]
        return {
            'max': max(lenghts),
            'min': min(lenghts),
            'avg': sum(lenghts) / float(len(lenghts)),
        }

    def start(self):
        futures = [
            self.start_worker(worker_no, first_run=True)
            for worker_no in range(self.count)
        ]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(futures))
        # After all logged in, run them
        for worker in self.workers.values():
            loop.create_task(worker.run())

    def check(self):
        last_cleaned_cache = time.time()
        last_workers_checked = time.time()
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
            if now - last_cleaned_cache > (15 * 60):  # clean cache
                db.CACHE.clean_expired()
                last_cleaned_cache = now
            # Check up on workers
            if now - last_workers_checked > (5 * 60):
                # Kill those not doing anything
                for worker, total_seen in workers_check:
                    if not worker.running:
                        continue
                    if worker.total_seen <= total_seen:
                        worker.kill()
                # Prepare new list
                workers_check = [
                    (worker, worker.total_seen)
                    for worker in self.workers.values()
                ]
                last_workers_checked = now
            if self.status_bar:
                if sys.platform == 'win32':
                    _ = os.system('cls')
                else:
                    _ = os.system('clear')
                print(self.get_status_message())
            time.sleep(0.5)

    def get_status_message(self):
        workers_count = len(self.workers)
        points_stats = self.get_point_stats()
        messages = [
            self.workers[i].status.ljust(20) for i in range(workers_count)
        ]
        running_for = datetime.now() - self.start_date
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
            '{} threads active'.format(threading.active_count()),
            '',
        ]
        previous = 0
        for i in range(4, workers_count + 4, 4):
            output.append('\t'.join(messages[previous:i]))
            previous = i
        return '\n'.join(output)


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
        default=logging.INFO
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.status_bar:
        configure_logger(filename='worker.log')
        logger.info('-' * 30)
        logger.info('Starting up!')
    else:
        configure_logger(filename=None)
    logger.setLevel(args.log_level)
    overseer = Overseer(status_bar=args.status_bar)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor())
    loop.run_in_executor(None, overseer.check)
    overseer.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        overseer.kill()
        loop.run_until_complete(asyncio.gather(*asyncio.Task.all_tasks()))
        loop.close()
