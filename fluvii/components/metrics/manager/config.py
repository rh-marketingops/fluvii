from fluvii.config_bases import FluviiConfigBase


class MetricsManagerConfig(FluviiConfigBase):
    hostname: str
    app_name: str
    enable_metrics: bool = False
    enable_pushing: bool = False

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_METRICS_MANAGER_"
