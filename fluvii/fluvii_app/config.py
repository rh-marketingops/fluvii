from datetime import datetime
from pydantic import BaseSettings


class FluviiConfig(BaseSettings):
    # - values also passed to other components/configs
    name: str = 'FluviiApp'
    hostname: str = f'{name}_{datetime.timestamp(datetime.now())}'

    # - Tabling
    # --- Recommended to change
    table_folder_path: str = '/tmp'
    # --- Sufficient Defaults
    table_changelog_topic: str = f'{name}__changelog'
    table_recovery_multiplier: int = 10

    class Config:
        env_prefix = "FLUVII_APP_"

    def get_auth(self, client_type):
        creds = {cred: self.__getattribute__(f'auth_{client_type}_{cred}') for cred in ['username', 'password', 'oauth_scope']}
        return {k: v for k, v in creds.items() if v}
