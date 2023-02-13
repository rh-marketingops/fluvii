from fluvii.config_bases import FluviiConfigBase


class SqliteConfig(FluviiConfigBase):
    table_directory: str = '/tmp/fluvii_tables'
    max_pending_writes_count: int = 1000
    min_cache_count: int = 20000
    max_cache_count: int = 50000

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_SQLITE_"
