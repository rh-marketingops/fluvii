from os import environ


# TODO: Not happy with how this is organized/works, but needed to release something for now.
class MetricsManagerConfig:
    def __init__(self, hostname=None, app_name=None):

        if not hostname:
            hostname = environ.get('FLUVII_HOSTNAME')
        if not app_name:
            app_name = environ.get('FLUVII_APP_NAME')

        self.hostname = hostname
        self.app_name = app_name
