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


## Configuring a FluviiApp
FluviiApp works the same way; it has its own config, `FluviiConfig`. 

In addition, `FluviiApp` will generate any underlying necessary component configs using this `FluviiConfig`,
where some values in the config will "override" certain values in each respective component config, ensuring they are 
"shared" across all components correctly.

You can still augment any configs via environment, or manually and hand those augments to `FluviiApp` via the `component_configs` argument,
and all will be applied except for any of the settings that are shared from `FluviiApp`, which take precidence if defined.


## Examples of FluviiConfig Inheritence
If a `FluviiApp` is init'ed and both `ConsumerConfig`'s _FLUVII_CONSUMER_URLS_ and `FluviiConfig`'s _FLUVII_APP_KAFKA_URLS_ 
are set, _FLUVII_APP_KAFKA_URLS_ would take precidence since `FluviiApp` passes this value from `FluviiConfig` to `ConsumerConfig` 
when it generates a `TransactionalConsumer`. However, if _FLUVII_APP_KAFKA_URLS_ is not set, 
yet both _FLUVII_CONSUMER_URLS_ and _FLUVII_PRODUCER_URLS_ are, then your app would instead use those.

All "replacement" configurations will be dictated in the FluviiConfig.


## How to extend `FluviiApp` to help enforce specific config settings
If you are extending the `FluviiApp` class and wish to enforce certain config values despite user input, 
the best course of action is to just extend the `set_config_dict` method and override the configs with your desired values.


## Full Configuration Examples not using environment, non-default config values


## FluviiApp
```python

from fluvii.fluvii_app import FluviiApp, FluviiConfig
from fluvii.consumer import ConsumerConfig

my_schema = 'your schema here'

def my_business_logic(transaction):
    pass # do stuff to messages

fluvii_config = FluviiConfig(
    kafka_urls='my.broker.url0,my.broker.url1',
    auth_kafka_username='my_username',
    auth_kafka_password='my_password',
    registry_url='my.schema.registry.url',
    auth_registry_username='my_schema_registry_username',
    auth_registry_password='my_schema_registry_password'
)

# optional config changes, shown a couple ways
### Variation 1
### url is a required setting, but gets overwritten with fluvii_config's "kafka_urls"
consumer_config = ConsumerConfig(url='will_get_overridden', timeout_minutes=7)
configs = [consumer_config]

### Variation 2
### can ignore required fields like "url" this way, but lose out on validation and auto-complete
configs = {'consumer': {'timeout_minutes': 7}}

def my_app():
    return FluviiApp(
        app_function=my_business_logic,
        consume_topics_list=['my_input_topic'],
        produce_topic_schema_dict={'my_output_topic': my_schema},
        fluvii_config=fluvii_config,
        component_configs=configs
    )

```