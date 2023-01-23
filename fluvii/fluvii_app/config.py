from datetime import datetime
from typing import Optional
from pydantic import BaseSettings

# client_auth_config=None, schema_registry_auth_config=None, producer_config=None, consumer_config=None, metrics_manager_config=None, metrics_pusher_config=None


class FluviiConfig(BaseSettings):

    # Convenience
    kafka_urls: Optional[str] = None
    registry_url: Optional[str] = None

    # shared configs
    name: str = 'FluviiApp'
    hostname: str = f'{name}_{datetime.timestamp(datetime.now())}'

    # auth
    # --- client
    auth_kafka_username: Optional[str] = None
    auth_kafka_password: Optional[str] = None
    auth_kafka_oauth_scope: Optional[str] = None

    # --- schema registry
    auth_registry_username: Optional[str] = None
    auth_registry_password: Optional[str] = None
    auth_registry_oauth_scope: Optional[str] = None

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
