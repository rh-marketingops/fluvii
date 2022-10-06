from confluent_kafka.schema_registry import SchemaRegistryClient


# fixes a bug in confluent-kafka TODO: keep a look out for this in >1.8.2, should be fixed since I took it from a MR.
def patched_schema_loads(schema_str):
    from confluent_kafka.schema_registry.avro import Schema
    schema_str = schema_str.strip()
    if schema_str[0] != "{" and schema_str[0] != "[":
        schema_str = '{"type":' + schema_str + '}'
    return Schema(schema_str, schema_type='AVRO')


import confluent_kafka
confluent_kafka.schema_registry.avro._schema_loads = patched_schema_loads


class SchemaRegistry:
    def __init__(self, url, auth_config=None):
        self.registry = None
        self.url = url
        self._auth = auth_config
        self._init_registry()

    def _init_registry(self):
        if self._auth:
            full_url = f"{self._auth.username}:{self._auth.password}@{self.url}"
        else:
            full_url = self.url
        # TODO: allow the original URL to have http/s in it, and put it in the right spot
        self.registry = SchemaRegistryClient({'url': f"https://{full_url}"})
