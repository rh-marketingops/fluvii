from confluent_kafka.admin import AdminClient


class Admin:
    def __init__(self, config):
        self._config = config
        self.admin_client = self._init_admin_client()

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self._admin_client.__getattribute__(attr)

    def _init_admin_client(self):
        return AdminClient(self._config.as_client_dict())
