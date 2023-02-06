# Components

Fluvii comprises "components" which `FluviiApp`'s coordinate and orchestrate. These include:
- SchemaRegistry
- MetricsManager
- MetricsPusher (generally auto-handled by the MetricsManager)
- [Transactional]Producer
- [Transactional]Consumer
- [Table]Transaction

These components each require a respective config class (aptly named `{component_name}Config`) to...well, configure some settings on them!

# Rules of thumb about `*Configs`
Here's some important rules of thumb for these config classes:
- They generally contain settings that often don't need to be adjusted.
- However, if you needed to tweak performance, it provides options for more control over processing speed, data storage, and timeouts.
- All the configs can be set via environment variables, and most have working defaults.
- "configs" and "environment" are analagous within the context of fluvii.

For example, it's not super likely that you'll need to change the timeout of a consumer...
so it lives in the `ConsumerConfig`. Similarly, you probably aren't managing a ton of clusters, 
so just set your URLS via env variable, and never think about it again!

Conversely, each set of your apps is likely going to need its own consumer group...so 
that is required as its own additional argument to initialize the Consumer component.


## So how do you configure fluvii?

## Configuring a component
By default, components each auto-generate their own config objects. If there are required config values (urls, for example),
then you'll either need to set that environment variable, or make the respective `*Config` object and pass it as an argument
to the component instead of allowing it to generate one.

Examples (skipping other args):

If missing env _FLUVII_PRODUCER_URLS_:
    
- do `Producer(settings_config=ProducerConfig(urls='my_url')`

or, if your env of _FLUVII_PRODUCER_URLS_=**my_url**
- do `Producer()`

NOTE: some components themselves require other components, with their own respective configs.

## Configuration Examples

### FluviiApp + Auth + optional config change; no environment variables
Here, we set up a FluviiApp with required values only, and where authentication is required. 

The environment has no additional configuration (aka no values set).

We also change the `consumer_timeout` from the default to 7 minutes.
```python

from fluvii.fluvii_app import FluviiAppFactory, FluviiConfig
from fluvii.producer import ProducerConfig
from fluvii.consumer import ConsumerConfig
from fluvii.auth import AuthKafkaConfig
from fluvii.schema_registry import SchemaRegistryConfig

my_schema = 'your schema here'

def my_business_logic(transaction):
    pass # do stuff to messages


def build_my_app():
    auth_kafka_kws = {
        'urls': 'my.broker.url0,my.broker.url1', 
        'auth_kafka_config': AuthKafkaConfig(username='my_kafka_un', password='my_kafka_pw')
    }
    return FluviiAppFactory(
        app_function=my_business_logic,
        consume_topics_list=['my_input_topic'],
        produce_topic_schema_dict={'my_output_topic': my_schema},
        fluvii_config=FluviiConfig(app_name='my_fluvii_app'),
        schema_registry_config=SchemaRegistryConfig(url='my.sr.url', username='my_sr_un', password='my_sr_pw'),
        consumer_config=ConsumerConfig(**auth_kafka_kws, timeout_minutes=7),
        producer_config=ProducerConfig(**auth_kafka_kws),
    )


if __name__ == '__main__':
    app = build_my_app()
    app.run()
```


### FluviiApp + Auth + optional config change; all via environment variables
Here, we set up a FluviiApp with required values only, and where authentication is required. 

The environment is fully configured via sourcing a dotenv file.

We also change optional consumer setting `consumer_timeout` from the default to 7 minutes.

dotenv:
```.dotenv
### Connection/authentication
# Kafka Consumer/Producer
KAFKA_BROKER_URLS=my.broker.url0,my.broker.url1
FLUVII_PRODUCER_URLS=$KAFKA_BROKER_URLS
FLUVII_CONSUMER_URLS=$KAFKA_BROKER_URLS
FLUVII_KAFKA_AUTH_USERNAME=my_kafka_un
FLUVII_KAFKA_AUTH_PASSWORD=my_kafka_pw
# Schema Registry
FLUVII_SCHEMA_REGISTRY_URL=my.sr.url
FLUVII_SCHEMA_REGISTRY_USERNAME=my_sr_un
FLUVII_SCHEMA_REGISTRY_PASSWORD=my_sr_pw

### fluvii app settings
FLUVII_APP_NAME=my_fluvii_app

### Consumer settings
FLUVII_CONSUMER_TIMEOUT_MINUTES=7
```

python:
```python

from fluvii.fluvii_app import FluviiAppFactory

my_schema = 'your schema here'

def my_business_logic(transaction):
    pass # do stuff to messages


def build_my_app():
    return FluviiAppFactory(
        app_function=my_business_logic,
        consume_topics_list=['my_input_topic'],
        produce_topic_schema_dict={'my_output_topic': my_schema},
    )


if __name__ == '__main__':
    app = build_my_app()
    app.run()
```