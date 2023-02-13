from datetime import datetime
from pydantic import validator
from fluvii.config_bases import FluviiConfigBase
from typing import Optional
from fluvii.components.sqlite import SqliteConfig


class FluviiAppConfig(FluviiConfigBase):
    # - values also passed to other components/configs
    name: str
    hostname: Optional[str] = None

    # related configs
    sqlite_config: Optional[SqliteConfig] = SqliteConfig()

    # - Tabling
    table_changelog_topic: Optional[str] = None
    table_recovery_multiplier: int = 10

    @validator('hostname')
    def set_hostname(cls, value, values):
        if not value:
            return f'{values["name"]}_{datetime.timestamp(datetime.now())}'
        return value

    @validator('table_changelog_topic')
    def set_changelog_topic(cls, value, values):
        if not value:
            return f'{values["name"]}__changelog'
        return value

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_APP_"
