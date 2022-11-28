from time import sleep
from confluent_kafka.admin import AdminClient


class Admin:
    def __init__(self, urls, auth=None):
        if not auth:
            auth = {}
        self._auth = auth
        self._urls = urls
        self._admin_client = None
        self._init_admin_client()

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self._admin_client.__getattribute__(attr)

    def _init_admin_client(self):
        auth = self._auth.as_client_dict() if self._auth else self._auth
        self._admin_client = AdminClient({
            "bootstrap.servers": self._urls,
            **auth})


def parse_headers(msg_header):
    """
    Converts headers to a dict
    :param msg_header: A message .headers() (confluent)
    :return: a decoded dict version of the headers
    """
    if msg_header:
        if not isinstance(msg_header, dict):
            msg_header = dict(msg_header)
            return {key: value.decode() for key, value in msg_header.items()}
        return msg_header
    return {}


def get_guid_from_message(message):
    guid = None
    headers = (i for i in message.headers())
    while not guid:
        h = next(headers)
        guid = h[1].decode() if h[0] == 'guid' else None
    return guid


def log_and_raise_error(metrics_manager, error):
    """
    Since the metric manager pushing is a separate thread, ensure an exception gets sent to prometheus
    """
    metrics_manager.inc_metric('message_errors', label_dict={'exception': error})
    sleep(5)
    raise
