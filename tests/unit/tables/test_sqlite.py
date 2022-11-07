from unittest.mock import patch
import os

import pytest

from fluvii.fluvii_app import FluviiConfig
from fluvii.sqlite.sqlite import SqliteFluvii


def test_disabled_auto_init_does_not_initialize():
    table = SqliteFluvii(
        table_name="environment_override",
        fluvii_config=FluviiConfig(
            client_urls=["mock"],
            schema_registry_url="mock",
        ),
        auto_init=False,
    )
    assert table.offset is None
    assert table.db is None


def test_fluvii_config_can_provide_table_path(tmp_path):
    with patch.dict("os.environ", {"FLUVII_TABLE_FOLDER_PATH": tmp_path.as_posix()}):
        table = SqliteFluvii(
            table_name="environment_override",
            fluvii_config=FluviiConfig(
                client_urls=["mock"],
                schema_registry_url="mock",
            ),
        )
    assert table.full_db_path == tmp_path / "environment_override.sqlite"


class TestSqliteFluvii:
    def _initialize_table(self):
        self.table = SqliteFluvii(
            table_name="test_sqlite_fluvii",
            table_path=self.table_path,
            fluvii_config=FluviiConfig(
                client_urls=["mock"],
                schema_registry_url="mock",
            ),
        )

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.table_path = tmp_path
        self._initialize_table()

    def test_empty_table_sets_initial_offset_to_0(self):
        assert self.table.offset == 0

    def test_offset_can_be_overridden(self):
        self.table.set_offset(5)
        assert self.table.offset == 5

    def test_db_file_exists(self):
        assert os.path.exists(self.table.full_db_path)

    @pytest.mark.parametrize("key,value", [("hello", "goodbye"), ("numbers", 42), ("custom", ["tuple", 800])])
    def test_read_write_works_and_persists_after_reopen(self, key, value):
        assert self.table.read(key) is None
        self.table.write(key, value)
        assert self.table.read(key) == value

        self.table.commit()
        self.table.close()
        self._initialize_table()
        assert self.table.read(key) == value
