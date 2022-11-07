import pytest

from fluvii.fluvii_app import FluviiConfig
from fluvii.sqlite.sqlite import SqliteFluvii


class TestSqliteFluvii:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.table = SqliteFluvii(
            table_name="test_sqlite_fluvii",
            table_path=tmp_path,
            fluvii_config=FluviiConfig(
                client_urls=["mock"],
                schema_registry_url="mock",
            ),
        )

    def test_empty_table_sets_initial_offset_to_0(self):
        assert self.table.offset == 0

    def test_offset_can_be_overridden(self):
        self.table.set_offset(5)
        assert self.table.offset == 5
