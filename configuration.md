## Components

Fluvii comprises "components" which `FluviiApp`'s coordinate and orchestrate. These include:
- SchemaRegistry
- MetricsManager
- MetricsPusher (generally auto-handled by the MetricsManager)
- [Transactional]Producer
- [Transactional]Consumer
- [Table]Transaction

These components each require a respective config class (aptly named `{component_name}Config`) to...well, configure some settings on them!

## Rules of thumb about `*Configs`
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

### Configuring a component
By default, components each auto-generate their own config objects. If there are required config values (urls, for example),
then you'll either need to set that environment variable, or make the respective `*Config` object and pass it as an argument
to the component instead of allowing it to generate one.

Examples (skipping other args):

If missing env _FLUVII_PRODUCER_URLS_:
    
- do `Producer(settings_config=ProducerConfig(urls='my_url')`

or, if your env of _FLUVII_PRODUCER_URLS_=**my_url**
- do `Producer()`

NOTE: some components themselves require other components, with their own respective configs.


### Configuring a FluviiApp
FluviiApp works the same way; it has its own config, `FluviiConfig`. 

Ideally, you handle everything via environment variables, as inheritence is the cleanest in this manner. 


### Configuration Inheritence/Precidence
Specifically, using `FluviiApp` as an example since it will be the most common use case, 
the order of configuration precidence is dictated by the following, with the top being the highest priority 
(replacing any "overlapped" values below it):

1. A (pre-configured) component handed directly to `FluviiApp` (which by nature skips steps 2-3)
2. Configuration set via `FluviiConfig`, which hands some settings down to other component configs
3. Configuration per individual component configs.


### Examples of Precidence
For example, if you were testing some consumer settings on the fly via `FluviiApp(consumer=TransactionalConsumer(settings_config=my_test_config))`, 
this would essentially stop `FluviiApp` from auth-generating a `TransactionalConsumer` (and thus a respective config).

In another example, if you init a `FluviiApp` and both `ConsumerConfig`'s _FLUVII_CONSUMER_URLS_ and `FluviiConfig`'s _FLUVII_APP_KAFKA_URLS_ 
are set, _FLUVII_APP_KAFKA_URLS_ would take precidence since `FluviiApp` passes this value from `FluviiConfig` to `ConsumerConfig` 
when it generates a `TransactionalConsumer`. However, if _FLUVII_APP_KAFKA_URLS_ is not set, 
yet both _FLUVII_CONSUMER_URLS_ and _FLUVII_PRODUCER_URLS_ are, then your app would instead use those.

All "replacement" configurations will be dictated in the FluviiConfig.


## Full Configuration Examples not using environment


### FluviiApp
```python

from fluvii.consumer import ConsumerConfig, TransactionalConsumer



```
# my environment

my_app = 