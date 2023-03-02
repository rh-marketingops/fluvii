## Join us on Slack!

Feel free to join our [Fluvii Slack Community!](https://join.slack.com/t/fluvii/shared_invite/zt-1j9hjm7lh-iowP9Z9vE94HAzNmUBfbbQ)

This will be the easiest way to get assistance from us should you need it =)

# State of Fluvii as of v1.0.0

We feel _Fluvii_ is now stable enough that you can reliably use it. Our team at Red Hat runs our production
systems on it.

Most of the major bugs have been stamped out, but like with anything, there are always hard-to-catch
edge cases. We try to fix these issues as we find them, and they are becoming increasingly rare!

## Things in the Pipeline

Here's what to expect over the next 3-6 months (~H1 of 2023):

- Proper Unit/Integration Testing
- Improved general documentation
- Contribution Guide and tooling (including how to setup pre-commit)
- Continued stability improvements around rebalancing (specifically `FluviiTableApp`)

# Installation

`pip install fluvii`

# What is _Fluvii_?

_Fluvii_ is a Kafka client library built on top of [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python).

_Fluvii_ was originally written as a simpler alternative for the other popular Python client library [Faust](https://github.com/robinhood/faust).

Much like Faust, _Fluvii_ offers _similar_ functionality to the Kafka-Streams java client;
it's likely no surpise that "Fluvii" translates to "streams" in Latin!

We like to think of _Fluvii_ as a simplified kafka-streams; it was designed to make Exactly Once Semantics (aka transactions)
and basic state storage (aka "tabling") require as little code as possible.

**We also take full advantage of transactions by handling multiple consumed/produced messages at once per transaction,
significantly cutting down on I/O, which can tremendously improve throughput!**

See below for further elaborations on _Fluvii_'s use-cases.

## _Fluvii_ or Faust?

We designed _Fluvii_ after using Faust/Kafka extensively; we found that in most cases, we didn't
need most of the functionality provided by Faust to accomplish 95% of our work. We imagine this will be true for you, too!

As such, we wanted an interface that required minimal interaction/repetition,
yet offered the ability to easily extend it (or manually handle anything) when needed.

The main reason we wrote _Fluvii_ was to replace Faust, for a few reasons:
1. Faust is more monolithic in design:
   1. It's very much its own orchestrator via agents, which if you use your own orchestrator (like Kubernetes), it creates unneccesary redundancy and complexity.
   2. To help enable this orchestration, Faust is `async`, making it difficult to debug and extend.
2. Faust is fairly bloated; it was likely designed to easily integrate with other existing event systems at Robinhood.
3. The original project (and supporting libraries) have been dead for a long time. There are [other forks](https://github.com/faust-streaming/faust) being maintained, but when you combine the above points, we felt it prudent to design something new!

All that being said, you might still choose Faust if:

1. You prefer how Faust operates as an orchestrator, managing kafka clients via workers and agents.
2. You have a beefy server to run it on...it may perform faster overall than a bunch of `Fluvii` apps.
3. You're happy with the current feature set Faust offers, which is arguably a little larger than _Fluvii_ since it has rolling table windows.
4. You want a fairly flexible interface and configuration options, _Fluvii_ is still maturing in this regard.
5. You want a testing framework; Faust has a built-in testing framework...it's not fool-proof, but it's useful.

## Operational Assumptions of _Fluvii_

_Fluvii_ assumes/defaults to:

- Using a schema-registry
- Using avro schemas (see examples for format)
- 1 schema per topic
- If tabling, each app that is part of the same consumer group has a shared storage point (like a mounted drive in Kubernetes).

_Fluvii_ also assumes/defaults to these, but these are likely to extend/grow:

- Using Prometheus metrics (and a push-gateway for app metrics); **the metrics are turned off by default.**
- Using Sqlite as your table database (which also changes the table storage point above)
- Using SASL, Oauth, or nothing as your authentication scheme


# Getting started with _Fluvii_


## Understanding `*Factory` classes
In general, you'll be using "Factory" classes, which will generate the objects you will interface with.

They basically encapsulate the configuration step of their respective non-factory object;
i.e. `FluviiAppFactory` helps generate a fully-configured `FluviiApp` instance.

The main ones to focus on would be:
- `FluviiAppFactory`
- `FluviiTableAppFactory`
- `ProducerFactory`

(Note: though `ConsumerFactory` exists, we generally recommend using a `FluviiAppFactory` instead, which uses transactions)

If you are only producing messages, the producer factory is probably the cleanest way to go, else you'll likely want
to use one of the `FluviiApp*` factories.

TL;DR `FluviiAppFactory` will likely be your one-stop shop, which makes a `FluviiApp` for you!

## Understanding "Components"

_Fluvii_ comprises several classes that work in tandem. Some classes are referred to as "components",
which `FluviiApp`'s coordinate and orchestrate (and some work as stand-alones, like the `Producer`). These include
- SchemaRegistry
- MetricsManager/Pusher
- Producer
- Consumer

One good rule of thumb for components is that they have a corresponding `config.py` to...well, configure them!

For the most part, you won't have to mess with any of these directly if you use the `*Factory` classes, which will generate all
the necessary components and their respective config objects for you! But, it's good to be aware of their existence.

## `Transaction` objects

**`Transaction`s will be the object your application logic interfaces with for producing and handling messages while using any `FluviiApp` variant**.

It is aptly named because the object is very much the equivalent of a Kafka transaction, and it gets recreated each time
a new transaction is required.

You will see how this applies in the **Creating a FluviiApp** section below.

Note that the state of said transaction is managed behind the scenes by the `FluviiApp`, you don't have to manage it yourself.


# Creating a FluviiApp (via a Factory)

A `FluviiApp` is any of the `*_app.py` file names located in the library folder `/fluvii/apps`.

As previously mentioned, `FluviiApp`s have a corresponding `Factory` associated with them; we will outline their usage below.

## `*Factory` args
Here are the basic arguments when it comes to creating a `FluviiApp` (through a `*Factory`)

### Args without defaults (aka direct setup)

- `app_function` (arg0)
  - This is where you hand in your business logic function. See the **Setting up your app_function** section below
- `consume_topics_list` (arg1)
  - The topics you want to consume from
- `produce_topic_schema_dict` (optional kwarg)
  - Optionally, the topics (and respective schema) you may produce to in the app.

### Args with defaults - `*_config` args (aka indirect setup)
Any arguments that end in `_config` are optional in that they generally have working defaults.

Additionally, they are considered "indirect" because they can be set via the environment rather than using the factory argument.

This somewhat artificial deliniation was made because these settings are far less likely to change between
different application instances (among other design reasons).

Note: any configs that are manually handed here supercede any environment configurations.

For more details, see the **Configuring _Fluvii_** section.

## Setting up your app_function
The `app_function` you pass to Fluvii is the heart of your application.

Typically, you consume a message and do "logic n' stuff", which could include producing new messages. This function encapsulates all of that.

Some notes:
- By default, the function MUST take at minimum 1 argument, of which the first will be an injection of a `Transaction` object instance at runtime. For example,
   ```python
   def my_app_function(transaction, my_arg, my_arg2):
      pass # do stuff; using the transaction object throughout
   ```
- You can access the currently consumed message via `transaction.message`, or shorthand via `transaction.key()`
- You can produce messages via `transaction.produce(ARGS_HERE)`
- once the app runs through the entirety of your app_function, it will commit the current message and apply the function to the next message...and so on.

# Configuring _Fluvii_

_Fluvii_ allows you to configure its various components via environment (or directly via the config
object itself).

`FluviiApp`s also have an associated config object.

These component configs (i.e. any `config.py`) typically have working defaults for most of their settings, and most
of them will not need any additional tweaking.

## Configuration options
1. Component config handed to a `*Factory` argument.
   - An (incomplete) example of this might look like:
       ```python
       app = FluviiAppFactory(consumer_config=ConsumerConfig(timeout_minutes=10))
       ```
2. With environment variables
    - In general, this is the reccommended approach.
    - You can look at each config object, with defines a prefix. For any given config value, just prepend the prefix to said value.
    - For example, with `ConsumerConfig`:
      - its prefix is `FLUVII_CONSUMER_`.
      - It has a setting called `timeout_minutes`.
      - Then the respective environment variable for this setting is `FLUVII_CONSUMER_TIMEOUT_MINUTES`

3. With a `.env` file, where the environment variable `FLUVII_CONFIG_DOTENV` is the filepath to it
    - This approach uses the same setting naming scheme as in (2.)
    - An (incomplete) example might look like:
        ```dotenv
        FLUVII_CONSUMER_TIMEOUT_MINUTES=10
        FLUVII_AUTH_KAFKA_USERNAME=my_cool_username
        ```

## Configuration precedence

Under the hood, Fluvii uses `Pydantic`, and as such, follows its rules for configuration precedence for each config value.

Do note that the configs only populate with actual defined values at each step, so non-defined values won't overwrite
previously defined ones unless you specifically define them to be empty.

Here's the order from highest precedence to lowest (higher supercedes anything below it):

1. config object with direct arguments handed to a Factory
2. environment variable
3. dotenv file

## Minimum configuration requirements

No matter what, you will need to populate these configuration settings:

### FluviiApp
```dotenv
### Connection
# Kafka Consumer/Producer
FLUVII_PRODUCER_URLS=
FLUVII_CONSUMER_URLS=
# Schema Registry
FLUVII_SCHEMA_REGISTRY_URL=

### This sets the consumer group name in FluviiApps
FLUVII_APP_NAME=

### ONLY IF AUTHENTICATING, else ignore...see auth config for OAUTH
# Kafka Consumer/Producer
FLUVII_AUTH_KAFKA_USERNAME=
FLUVII_AUTH_KAFKA_PASSWORD=
# Schema Registry
FLUVII_SCHEMA_REGISTRY_USERNAME=
FLUVII_SCHEMA_REGISTRY_PASSWORD=
```

## Other configuration suggestions

In general, we recommend using the environment variable configuration approach.

There is also a `FLUVII_APP_HOSTNAME` which we suggest setting to your kubernetes pod hostname, but it's not required.


# Simple FluviiApp Examples

## Initializing/running a bare-bones `FluviiApp`:

Note: This example assumes you have set the few required config settings via environment variables.
You can find a manual configuration example further below.

There are two basic components you need to initialize an app at all:

- `app_function` (first arg): the logic of your application, of which the first argument (which MUST exist) is assumed to be a transaction object that will be handed to it at runtime. Additional arguments, if needed, can be handed to `app_function_arglist`.

- `consume_topics_list` (second arg): the topics you are consuming. Can be comma-separated string or python list.

That's it! That being said, this is if your app only consumes. To produce, you will additionally need, at minimum:

- `produce_topic_schema_dict`: a dict that maps {'topic_name': _schema_obj_}, where the _schema_obj_ is a valid avro
schema.

Then, to run the app, do:

`FluviiApp.run()`

Altogether, that might look something like this:

```python
from fluvii import FluviiAppFactory

# assume the message consumed also had this schema to make it easy
a_cool_schema = {
    "name": "CoolSchema",
    "type": "record",
    "fields": [
        {
            "name": "cool_field",
            "type": "string"
        }
    ]
}

# can hand off objects you'd like to init separately, like an api session object, or a heavy object built at runtime
heavy_thing_inited_at_runtime = 'heavy thing'


def my_app_logic(transaction, heavy_init_thing):
    # All we're gonna do is set our 'cool_field' to a new value...very exciting, right?
    msg = transaction.message # can also do transaction.value() to skip a step
    cool_message_out = msg.value()
    cool_message_out['cool_field'] = heavy_init_thing #  'heavy thing'
    transaction.produce(
        {'value': cool_message_out, 'topic': 'cool_topic_out', 'key': msg.key(), 'headers': msg.headers()}
    )

fluvii_app = FluviiAppFactory(
    my_app_logic,
    ['test_topic_a', 'test_topic_b'],
    produce_topic_schema_dict={'cool_topic_out': a_cool_schema},
    app_function_arglist = [heavy_thing_inited_at_runtime])  # optional! Here to show functionality.
fluvii_app.run()
```
If you're feeling really lazy, you can actually cut some the `transaction.produce()` dict arguments, as anything you
don't pass (except `partition`) will be inhereted from the consumed message, like so:

```python
## will produce with the consumed message's key and headers
transaction.produce({'value': cool_message_out, 'topic': 'cool_topic_out'}
)
```

For maximum laziness,you could also cut the dict out entirely if there was only 1 topic in the `produce_topic_schema_dict`, as it will assume that's the topic
you want to produce to, like so:

```python
## will produce with the consumed message's key and headers
transaction.produce(cool_message_out)
```

Just make sure your message value isn't itself a dictionary with a "value" key in this case =)

## Using a table with a `FluviiTableApp`

Using tabling via `FLuviiTableApp` is very simple.

Here is an example where we are consuming messages around purchases made by various account holders,
storing the new balance for later comparisons, and producing that remaining balance downstream (maybe to notify the account holder?).

`consumed message key` = account holder number

`consumed message value` = purchase amount

```python
from fluvii import FluviiTableAppFactory

# -- Not used here, just so you know what the consumed messages look like
# purchase_schema = {
#     "name": "AccountPurchase",
#     "type": "record",
#     "fields": [
#         {"name": "Account Number", "type": "string"},
#         {"name": "Purchase Timestamp", "type": "string"},
#         {"name": "Purchase Amount", "type": "string"},
#     ]
# }

account_balance_schema = {
    "name": "AccountBalance",
    "type": "record",
    "fields": [
        {"name": "Account Number", "type": "string"},
        {"name": "Account Balance", "type": "string"},
    ]
}

def my_app_logic(transaction):
    record = transaction.value()
    current_account_balance = float(transaction.read_table_entry()['balance'])  # looks up record via the message.key()
    purchase_amount = float(record['Purchase Amount'])
    new_balance = str(current_account_balance - purchase_amount)
    message = {"Account Number": record["Account Number"], "Account Balance": new_balance}

    transaction.update_table_entry({'balance': new_balance})  # store the updated balance for later...must be a valid json object (a dict works)
    transaction.produce(message)


def fluvii_table_app():
    return FluviiTableAppFactory(
        my_app_logic,
        ['purchases_made'],
        produce_topic_schema_dict={'accounts_to_notify': account_balance_schema},
    )


fluvii_table_app().run()
```

## Configuring a FluviiApp more explicitly (no environment set):

Here, we set up a FluviiApp with required values only, and where authentication is required.

The environment has no additional configuration (aka no values set).

We also change the `consumer_timeout` from the default to 10 minutes.
```python

from fluvii import (
    FluviiAppFactory,
    FluviiAppConfig,
    ProducerConfig,
    ConsumerConfig,
    AuthKafkaConfig,
    SchemaRegistryConfig
)

my_schema = 'your schema here'

def my_business_logic(transaction):
    pass # do stuff to messages


def my_fluvii_app():
    auth_kafka_kws = {
        'urls': 'my.broker.url0,my.broker.url1',
        'auth_kafka_config': AuthKafkaConfig(username='my_kafka_un', password='my_kafka_pw')
    }
    return FluviiAppFactory(
        my_business_logic,
        ['my_input_topic'],
        produce_topic_schema_dict={'my_output_topic': my_schema},
        fluvii_config=FluviiAppConfig(app_name='my_fluvii_app'),
        schema_registry_config=SchemaRegistryConfig(url='my.sr.url', username='my_sr_un', password='my_sr_pw'),
        consumer_config=ConsumerConfig(**auth_kafka_kws, timeout_minutes=10),
        producer_config=ProducerConfig(**auth_kafka_kws),
    )


if __name__ == '__main__':
    app = my_fluvii_app()
    app.run()
```

Note that if we had set any of those values up via the environment, they would have been overwritten by what I handed here.

# FluviiToolbox

Want some help managing topics, or need an easy way to produce or consume on the fly? `FluviiToolbox` has you covered!

`FluviiToolbox` has multiple features, including
- listing all topics on the broker, including the configs for them
- producing a set of messages from a dict
- consuming all messages from a topic, including starting from a specific offset
- creating/altering/deleting topics

### Configuring `FluviiToolbox`

`FluviiToolbox` uses the same configs as the producer and consumer. Just make sure those are set!

# CLI

Right now, the CLI is fairly basic, but does allow you to perform similar actions to the `FluviiToolbox` (in fact,
it's basically an API for the CLI.)

As such, you can do all the same things the `FluviiToolbox` can do!

### Configuring the CLI

Same as `FluviiToolbox`.

## CLI examples

### Producing to a topic from a json

First, you must always provide a `topic-schema-dict` as an argument, but the allowed values depend on what you have configured.

Assume you have a schema, saved two different ways, at the following filepaths:
- `/home/my_schemas/cool_schema.py`, where it's just a python dict (named `my_cool_schema`)
- `/home/my_schemas/cool_schema.avro` (.json is also valid)

The schema is:
```json
{
    "name": "CoolSchema",
    "type": "record",
    "fields": [
        {
            "name": "cool_field",
            "type": "string"
        }
    ]
}
```

There is an optional producer config value `schema_library_root` (so `FLUVII_PRODUCER_SCHEMA_LIBRARY_ROOT`) that can
be set to a folder path where your schemas are contained.

If _not_ set, you can only produce messages by setting `topic-schema-dict` to the following:

- `'{"my_topic": "/home/my_schemas/schema.avro"}'`
- `'{"my_topic": {"name": "CoolSchema", "type": "record", "fields": [{"name": "cool_field","type": "string"}]}'`

However, assume you have set `FLUVII_PRODUCER_SCHEMA_LIBRARY_ROOT=/home/my_schemas`. This additionally enables you
to both import it as a python object using the full pythonpath the object from that directory, and use relative filepaths:

- `'{"my_topic": "cool_schema.avro"}'`
- `'{"my_topic": "my_schemas.cool_schema.my_cool_schema"}'`

If you have your own schema library/package, this can be a nice option to utilize!

Anyway, let's say you wish to produce the following 2 messages in `/home/msgs/inputs.json` using said schema:

```json
[
  {
    "key": "my_key",
    "value": {"cool_field": "value0"},
    "headers": {"my_header": "header_value0"},
    "topic": "my_topic"
  },
  {
    "key": "other_key",
    "value": {"cool_field": "other_value0"},
    "headers": {"my_header": "header_value1"},
    "topic": "my_topic"
  }
]
```
then the full command is:

`fluvii topics produce --topic-schema-dict '{"my_topic": "cool_schema.avro"}' --input-filepath /home/msgs/inputs.json`

If you don't want to produce the given topic in the message body, you can also override it with a new topic via the following argument:

`--topic-override "a_different_topic"`


### Consuming topics and dumping to a json

Consuming is relatively simple, but it does have some additional flexibility should you need it.

Basically, there is an argument named `topic-offset-dict`, which takes a dict of offsets should you need to specify a
starting point for any given partition. Otherwise, it defaults to `earliest`.

Here is an example where we consume two topics, and we want to start from offset 100 for partition 1 and 2 for "topic_2",
otherwise we want everything else.

`fluvii topics consume --topic-offset-dict '{"topic_1": {}, "topic_2": {"1": 100, "2": 100}' --output-filepath /home/msgs/data_out.json`

### Creating topics

When creating topics, it is recommended to set, at minimum, your `partitions` and `replication.factor`.

You can look up all potential settings via [kafka's documentation](https://kafka.apache.org/documentation/#topicconfigs).

Here is an example where we create two topics, where one is compacted. You can look up other topic configurations via Kafka's documentation.

`fluvii topics create --topic-config-dict '{"topic_1": {"partitions": 2, "replication.factor": 2}, "topic_2": {"partitions": 3, "replication.factor": 2, "cleanup.policy": "compact"}}'`

### Deleting topics

As easy as it gets!

`fluvii topics delete --topic-list 'topic_1,topic_2'`


# Important Feature and Usage Elaborations

Wanna learn more? You've come to the right section!

Up until now, we've only touched on the very basics to show how easy it is to get an app up and running.
However, you should also be aware of some of the magic happening under the hood.

## Processing Guarantees

ALl _FluviiApp_ iterations use transactions under the hood, which translates to Exactly Once Semantics.

This is also true for the underlying tabling framework in _FluviiTableApp_, where the table is only written to
after the changelog message is committed. If the app were to somehow crash before it's able to write to the
table, it will recover the missing writes from the changelog topic.


## Batching

All _FluviiApp_ iterations batch under the hood through transactions. _Fluvii_ will consume multiple messages at once, and feed them
individuallyy to the `app_function`. Resulting produces will be queued via the transaction itself, and the consumed offsets will get committed
along with them once the configured maximum batch count threshold is hit.

While this could mean you have to reprocess a batch of messages again if there was an exception, the speed tradeoff
is well worth this rare inconvenience.


## _FluviiTableApp_ Caching

There are two different caching mechanisms happpening in `FluviiTableApp` with respect to tables.

The first layer is caching table writes in the `TableTransaction` object until the transaction is committed (this coincides
with the batching that's happening).

The second layer is stored on the `FluviiSqlite` object, which is what `TableTransaction` hands its writes off to. Table writes are stored
here until the configured write cache count exceeds the maximum, in which they are truly committed to the `sqlite` database.


This was implemented for two related reasons, both around utilizing networked storage situations:
1. to keep the most recent read/writes in memory for faster access
2. to reduce network I/O


## _FluviiTableApp_ limitations

To keep implementation around tabling simple, there were some design choices made that incurred
some limitations, most of which have fairly straightforward options for overcoming. Most of these are because of the complexities
around topic partition assignments.

Those caveats include (and you'll notice a consistent theme):

(For clarity, an "app" in this context refers to any collection of _FluviiTableApp_ instances that
share the same consumer group.)

- The app can only consume from one topic (you can make an upstream "funnel" app as needed).
- The app only has 1 table.
- The table is keyed on the consumed message keys; you can only compare your record to the one with the same key on the table.
- The changelog topic (which defaults to the name `{FLUVII_APP_NAME}__changelog`), must have same partition count as the consumed topic.
- Each app needs to share a storage volume.
- Table data needs to be stored as a json/dict (it is stored via `json.dumps` and `json.load`-ed when read)

In general, most of these can be overcomed by just linking different apps together since they will generally
be pretty light.

## Class Usage Information

Here is some more specific information around how you should use the `Transaction` objects, which
is the main application interface.

### Transaction objects

Assume we have a `Transaction` object, called "transaction".

`Transaction` objects will have the general workflow of:

1. `transaction.consume()`
2. do stuff to the resulting consumed message, stored as `transaction.message` (or `.value()`)
3. Send changes downstream via `transaction.produce(my_changes)` as needed (or skip this)
4. Commit or abort changes, via `transaction.commit()` or `transaction.abort_transaction()`, which enables the consumption of the next (or same if aborted) message.

Now, you might notice that in our example, we only had to call `.produce()`. That's because the `FluviiApp` handles
everything else! It knows that when your `app_function` logic is finished, it should commit. Similarly, when it's about
to run it again, it knows it should consume a new message to operate on. It will similarly handle errors by
aborting and retrying as needed.

If you need more manual access to things, the `TransactionalConsumer`, `TransactionalProducer`, and the `FluviiApp` context is
passed to every transaction object, so you'll always have access to whatever objects you need should the need arise.

You can also refresh the transaction object to re-use should it prove difficult to manage your logic
by remaking the transaction object, but this is certainly a more advanced use-case.

### TableTransaction

`TableTransaction` objects operate very similarly, except you'll need to add two extra commands before committing.

To read an entry, call `transaction.read_table_entry()`, which will return a dict.

To update an entry for the given message key, call `transaction.update_table_entry(my_update_dict)`. Of course, skip this
if you don't need to update it.

One thing to keep in mind is the object stored in the table just needs to be a valid json (which includes dicts).

Also, `FluviiTableApp` will ensure everything gets committed correctly for you just like the `FluviiApp`!
