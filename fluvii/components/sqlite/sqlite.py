import os

from sqlitedict import SqliteDict
from sqlite3 import DatabaseError
from pathlib import Path
from os import makedirs
import json
from copy import deepcopy
import logging
from time import sleep

LOGGER = logging.getLogger(__name__)


# TODO: make a non-fluvii version for just reading tables like a typical client
class SqliteFluvii:
    def __init__(self, table_name, config, auto_start=True, allow_commits=False):
        self._allow_commits = allow_commits
        self._max_pending_writes_count = config.max_pending_writes_count
        self._min_cache_count = config.min_cache_count
        self._max_cache_count = config.max_cache_count
        self.db = None
        self.table_name = table_name
        self.db_cache = {}
        self.offset = None
        self.write_cache = {}
        self._current_read = (None, '')
        makedirs(config.table_directory, exist_ok=True)
        self.db_folder = Path(f"{config.table_directory}").absolute()
        self.full_db_path = Path(f"{config.table_directory}/{table_name}.sqlite").absolute()

        if auto_start:
            self._init_db()
            self.offset = self._get_offset()

    def _get_offset(self):
        try:
            offset = int(self._read_db('offset'))
            LOGGER.debug(f'Retrieved table {self.table_name} offset {offset}')
            return offset
        except:
            LOGGER.debug(f'No offset for table {self.table_name}, setting to 0')
            return 0

    def _init_db(self):
        LOGGER.debug(f'initing table {self.table_name}')
        db = SqliteDict(self.full_db_path.as_posix(), tablename='fluvii', autocommit=False, journal_mode="WAL")
        self.db = db
        sleep(.2)
        if self._allow_commits:
            self.db['init'] = 'init'
            self.db.commit()  # confirms db is actually fully init-ed by doing this superfluous commit
        LOGGER.info(f'table {self.table_name} initialized')

    def write(self, key, value):
        self.write_cache[key] = value

    def write_batch(self, batch):
        self.write_cache.update(batch)

    def delete(self, key):
        self.write(key, '-DELETED-')

    def set_offset(self, value):
        self.offset = value

    def _commit_and_cleanup_check(self, recovery_multiplier=1):
        LOGGER.debug(f'Current table write cache count for {self.table_name}: {len(self.write_cache)}')
        if len(self.write_cache) >= (self._max_pending_writes_count * recovery_multiplier):
            self.commit()

    def _cleanup_db_cache_check(self):
        if len(self.db_cache) >= self._max_cache_count:
            self.prune_db_cache()

    def commit_and_cleanup_if_ready(self, recovery_multiplier=None):
        LOGGER.debug(f'Checking if table {self.table_name} is ready to commit or prune cache...')
        if not recovery_multiplier:
            recovery_multiplier = 1
        self._commit_and_cleanup_check(recovery_multiplier=recovery_multiplier)
        self._cleanup_db_cache_check()

    def prune_db_cache(self):
        remove_count = min(int(.25 * len(self.db_cache)), len(self.db_cache) - self._min_cache_count)
        LOGGER.info(f'Pruning {remove_count} from {len(self.db_cache)} table {self.table_name} db cached records')
        self.db_cache = {d: self.db_cache[d] for idx, d in enumerate(self.db_cache) if idx > remove_count}

    def commit(self):
        if not self._allow_commits:
            raise Exception('This client instance is read-only. Re-init with "allow_commits=True" to enable commits')
        try:
            if self.write_cache:
                LOGGER.info(f'committing {len(self.write_cache)} records in table {self.table_name} write cache')
                for key, value in self.write_cache.items():
                    if key != '-DELETED-':
                        self.db[key] = json.dumps(value)
                    else:
                        del self.db[key]
            self.db['offset'] = str(self.offset)
            self.db.commit()
            self.db_cache.update(self.write_cache)
            self.write_cache = {}
        except DatabaseError as e:
            if [arg for arg in e.args if 'malformed' in arg]:
                LOGGER.error(f'Database {self.table_name} is corrupt; deleting all related files and terminating application '
                             f'so the db will rebuild via restart')
                delete_files = [self.db_folder / f for f in os.listdir(self.db_folder.as_posix()) if f.startswith(f'{self.table_name}.sqlite')]
                LOGGER.info(f'Deleting the following files: {delete_files}')
                for f in delete_files:
                    os.remove(f.as_posix())
                    sleep(.2)
                LOGGER.info('File deletion complete; re-raising error')
            raise

    def _confirm_previous_read_in_cache(self):
        """
        Adds a key to the db cache if it was read but wasn't followed with a write operation
        """
        old_key = self._current_read[0]
        if old_key and (old_key not in self.write_cache) and (old_key not in self.db_cache):
            self.db_cache[old_key] = self._current_read[1]

    def _read_db(self, key):
        LOGGER.debug(f'{key} not in table {self.table_name} cache; querying the DB')
        try:
            return self.db[key]
        except KeyError:
            return None

    def _read_all(self, key):
        """
        Pops the key off the cache so that the keys are always in last-accessed order. This makes for easy slicing later
        to remove stale entries.
        """
        try:
            return key, self.write_cache[key]
        except KeyError:
            pass
        try:
            return key, self.db_cache.pop(key)
        except KeyError:
            pass
        val = self._read_db(key)
        try:
            return key, json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return key, val

    def read(self, key):
        self._confirm_previous_read_in_cache()
        self._current_read = self._read_all(key)
        return deepcopy(self._current_read[1])

    def close(self):
        self.db.close()
