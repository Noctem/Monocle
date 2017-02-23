from queue import Queue
from threading import Thread

from . import db
from .shared import get_logger

class DatabaseProcessor(Thread):

    def __init__(self):
        super().__init__()
        self.queue = Queue()
        self.log = get_logger('dbprocessor')
        self.running = True
        self.count = 0
        self._commit = False

    def stop(self):
        self.update_mysteries()
        self.running = False

    def add(self, obj):
        self.queue.put(obj)

    def run(self):
        session = db.Session()

        while self.running or not self.queue.empty():
            try:
                item = self.queue.get()

                if item['type'] == 'pokemon':
                    db.add_sighting(session, item)
                    self.count += 1
                    if not item['inferred']:
                        db.add_spawnpoint(session, item)
                elif item['type'] == 'mystery':
                    db.add_mystery(session, item)
                    self.count += 1
                elif item['type'] == 'fort':
                    db.add_fort_sighting(session, item)
                elif item['type'] == 'pokestop':
                    db.add_pokestop(session, item)
                elif item['type'] == 'mystery-update':
                    db.update_mystery(session, item)
                self.log.debug('Item saved to db')
                if self._commit:
                    session.commit()
                    self._commit = False
            except Exception as e:
                session.rollback()
                self.log.exception('A wild {} appeared in the DB processor!', e.__class__.__name__)
        try:
            session.commit()
        except Exception:
            pass
        session.close()

    def commit(self):
        self._commit = True

    def update_mysteries(self):
       for key, times in db.MYSTERY_CACHE.items():
           first, last = times
           if last != first:
               encounter_id, spawn_id = key
               mystery = {
                   'type': 'mystery-update',
                   'spawn': spawn_id,
                   'encounter': encounter_id,
                   'first': first,
                   'last': last
               }
               self.add(mystery)

DB_PROC = DatabaseProcessor()
