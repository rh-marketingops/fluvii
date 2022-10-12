from sqlitedict import SqliteDict
from pathlib import Path
from os import makedirs
import json
from copy import deepcopy
import logging
from time import sleep

LOGGER = logging.getLogger(__name__)


# TODO: make a non-fluvii version for just reading tables like a typical client
class SqliteFluvii:
    def __init__(self, table_name, fluvii_config, table_path=None, auto_init=True, max_pending_writes_count=None, min_cache_count=None, max_cache_count=None):
        if not max_pending_writes_count:
            max_pending_writes_count = fluvii_config.consumer_config.batch_consume_max_count * 5
        if not min_cache_count:
            min_cache_count = fluvii_config.consumer_config.batch_consume_max_count * 20
        if not max_cache_count:
            max_cache_count = fluvii_config.consumer_config.batch_consume_max_count * 50
        self._max_pending_writes_count = max_pending_writes_count
        self._min_cache_count = min_cache_count
        self._max_cache_count = max_cache_count
        self.db = None
        self.table_name = table_name
        self.db_cache = {}
        self.offset = None
        self.write_cache = {}
        self._current_read = (None, '')
        if not table_path:
            table_path = fluvii_config.table_folder_path
        makedirs(table_path, exist_ok=True)
        self.full_db_path = Path(f"{table_path}/{table_name}.sqlite").absolute()

        if auto_init:
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
        sleep(.1)
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
