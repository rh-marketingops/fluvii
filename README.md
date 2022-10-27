### Disclaimer

_Fluvii_ is approaching a stable enough state that you can reliably test it out...maybe even run it in production
with some low-stakes use cases!

That being said, it's still in a transitional state, so don't be surprised if there are still some more organizational
efforts underway!

That being said, we don't envision the object methods and interface changing much, other than of course to
add new features, which is more down the road.

On that note...

### Things in the Pipeline

- Unit Tests
- Better general documentation
- More configuration options
- Improved configuration structure
- Contribution Guide
- Fixing some more rare/minor rebalance bugs in the `FluviiTableApp`
- More usage examples in README

We are dedicated to having these things by the end of 2022.

# Installation

`pip intall fluvii`

# What is _Fluvii_?

_Fluvii_ is a Kafka client library built on top of [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python).

_Fluvii_ was originally written as a simpler alternative for the other popular Python client library [Faust](https://github.com/robinhood/faust). 

Much like Faust, _Fluvii_ offers _similar_ functionality to the Kafka-Streams java client; 
it's likely no surpise that "Fluvii" translates to "streams" in Latin!

We designed _Fluvii_ after using Faust/Kafka extensively; we found that in most cases, we didn't
need anything fancy to accomplish 95% of our work, which included Exactly Once Semantics (aka transactions) and simple tabling. 

As such, we wanted an interface that required minimal interaction/repetition, 
yet offered the ability to easily extend it (or manually handle anything) when needed.

**We also take full advantage of transactions by handling multiple consumed/produced messages at once per transaction,
significantly cutting down on I/O, which tremendously improves throughput!**

See below for further elaborations on _Fluvii_'s use-cases.

## _Fluvii_ or Faust?

The main reason we wrote _Fluvii_ was to replace Faust, for a few reasons:
1. Faust is more monolithic in design; it's not fully intended to be run in a containerized (i.e. Kubernetes) environment. It supports it, but it's not ideal...we ran into plenty of operational issues.
2. Faust is fairly complex/heavy; it's difficult to extend (especially since it's `async`) and was clearly designed to additionally integrate with other systems/libraries at Robinhood.
3. The underlying confluent libraries [librdkafka](https://github.com/edenhill/librdkafka) and [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) are active, maintained, and easy to iterate on.
4. Most importantly, the original project is dead, and there were (at the time) many bugs. There are [other forks](https://github.com/faust-streaming/faust) being maintained, but when you combine the above points, we felt it prudent to design something new!

All that being said, you might still choose Faust if:

1. You prefer how Faust manages its "clients" via workers and agents.
2. You have a beefy server to run it on...it may perform faster overall than a bunch of `Fluvii` apps.
3. You're happy with the current feature set Faust offers, which is arguably a little larger than _Fluvii_ since it has rolling table windows.
4. You want a fairly mature/flexible interface and configuration, Faust is far superior in this regard compared to _Fluvii_.
5. You want a testing framework; Faust has a built-in testing framework...it's not fool-proof, but it's useful.

## Operational Assumptions of _Fluvii_

_Fluvii_ is still in its infancy, thus there are several aspects of it that lack extensibility.

Right now, _Fluvii_ assumes/defaults to: 

- Using a schema-registry
- Using avro schemas (see examples for format)
- Using Prometheus metrics (and a push-gateway for app metrics)
- Using Sqlite as your table database
- Using SASL (or nothing) as your auth scheme
- 1 schema per topic
- If tabling, each app that is part of the same consumer group has a shared storage point (like a mounted drive in Kubernetes).

Most of these will eventually be generalized to handle alternatives.


# How to Use _Fluvii_

There are two primary ways to you'll probably use _Fluvii_:

1. (most common) `FluviiApp` or `FluviiTableApp` (which might include extending).
2. Using the `Producer` and `Consumer` classes for more basic use cases.

Here's a breakdown of _Fluvii_, it's components, and how to use it.  

## Understanding the Components

_Fluvii_ comprises several classes that work in tandem. Here is a brief summary of each of
the major ones:

### Producer and Consumer

The `Producer` and `Consumer` classes are the baseline classes. Their functionally differs little from
the simplest implementation within the _confluent-kafka-python_ library.

`Consumer` is very unlikely to be used because you
can simply use `FluviiApp` instead and get Exactly-Once guarantees with it.

`Producer` can be used in the use case where you aren't consuming.

### TransactionalProducer and TransactionalConsumer

The `TransactionalProducer` and `TransactionalConsumer` classes are intended to help manage transactional
state for their respective functions. 

They _can_ be employed manually, but they were intended to be managed by the 
`Transaction` class. As such, their usage is for far more advanced use-cases and not recommended.

### Transaction

**`Transaction` is the object your application will interface with for producing and consuming when using a `FluviiApp`**.

The `Transaction` class is meant to handle the interdependent state changes between the 
`TransactionalProducer` and `TransactionalConsumer` classes. 

It is aptly named because the object is very much the equivalent of a Kafka transaction, and it gets recreated each time
a new transaction is required.

Keep in mind: our transactions can include multiple consumed/produced messages!

### FluviiApp

`FluviiApp` is basically your entrypoint. It handles configuration and all the context/state management. 

You will configure it with desired settings (or just use the defaults!), hand it an application function to loop on, and call `run()`... and that will be the last of your interaction!

Once running, `FluviiApp` manages `Transaction` objects; it creates, commits and aborts them as needed.

### TableTransaction and FluviiTableApp

The `TableTransaction` and `FluviiTableApp` share a similar partnership as `Transaction` and `FluviiApp`.

`TableTransaction` generally handles the writing to the table and its Kafka changelog topic.

`FluviiTableApp` helps manage the Kafka and table assignments that occur from rebalancing, and initializes
table recovery as needed.

You'll typically use this if you need to keep track of certain records and compare them against incoming messages.

**NOTE: You can only consume from 1 topic per `FluviiTableApp`. 
This was to keep the implementation simplier and more intuitive.**

Usage does not differ much from the non-table variants.

Please see the **"Important Usage Information"** section for more insight around functionality and proper usage.

## Simple Examples

### Initializing/running a bare-bones `FluviiApp`:

Note: This example assumes you have set the few required config settings via environment variables. 
You can find a manual configuration example further below.

There are two basic components you need to initialize an app at all:

- `app_function`: the logic of your application, of which the first argument (which MUST exist) is assumed to be a transaction object that will be handed to it at runtime. Additional arguments, if needed, can be handed to `app_function_arglist`.


- `consume_topics_list`: the topics you are consuming. Can be comma-separated string or python list.

That's it! That being said, this is if your app only consumes. To produce, you will additionally need, at minimum:

- `produce_topic_schema_dict`: a dict that maps {'topic_name': _schema_obj_}, where the _schema_obj_ is a valid avro
schema.

Then, to run the app, do:

`FluviiApp.run()`

Altogether, that might look something like this:

```python
from fluvii import FluviiApp
from fluvii.transaction import Transaction

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

init_at_runtime_thing = 'cool value' # can hand off objects you'd like to init separately, like an api session object


def my_app_logic(transaction: Transaction, thing_inited_at_runtime):
    # All we're gonna do is set our field to a new value...very exciting, right?
    msg = transaction.message # can also do transaction.value() to skip a step
    cool_message_out = msg.value()
    cool_message_out['cool_field'] = thing_inited_at_runtime #  'cool value'
    transaction.produce(
        {'value': cool_message_out, 'topic': 'cool_topic_out', 'key': msg.key(), 'headers': msg.headers()}
    )
    
fluvii_app = FluviiApp(
    app_function=my_app_logic, 
    consume_topics_list=['test_topic_1', 'test_topic_2'],
    produce_topic_schema_dict={'cool_topic_out': a_cool_schema},
    app_function_arglist = [init_at_runtime_thing])  # optional! Here to show functionality.
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

### Using a table with a `FluviiTableApp`

Using tabling via `FLuviiTableApp` is very simple. 

Here is an example where we are consuming messages around purchases made by various account holders, 
storing the new balance for later comparisons, and producing that remaining balance downstream (maybe to notify the account holder?). 

`consumed message key` = account holder number

`consumed message value` = purchase amount

```python
from fluvii import FluviiTableApp
from fluvii.transaction import TableTransaction

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

def my_app_logic(transaction: TableTransaction):
    record = transaction.value()
    current_account_balance = float(transaction.read_table_entry()['balance'])  # looks up record via the message.key()
    purchase_amount = float(record['Purchase Amount'])
    new_balance = str(current_account_balance - purchase_amount)
    message = {"Account Number": record["Account Number"], "Account Balance": new_balance}

    transaction.update_table_entry({'balance': new_balance})  # store the updated balance for later...must be a valid json object (a dict works)
    transaction.produce(message)


def fluvii_table_app():
    return FluviiTableApp(
        app_function=my_app_logic,
        consume_topic='purchases_made',
        produce_topic_schema_dict={'accounts_to_notify': account_balance_schema},
    )


fluvii_table_app().run()
```

# Important Feature, Configuration and Usage Elaborations

Up until now, we've only touched on the very basics to show how easy it is to get an app up and running.
However, you should also be aware of some of the magic happening under the hood, along with configuration
options to ensure proper use.

## Important Feature Insights

### Processing Guarantees

ALl _FluviiApp_ iterations use transactions under the hood, which translates to Exactly Once Semantics. 

This is also true for the underlying tabling framework in _FluviiTableApp_, where the table is only written to
after the changelog message is committed. If the app were to somehow crash before it's able to write to the 
table, it will recover the missing writes from the changelog topic.


### Batching

All _FluviiApp_ iterations do batching under the hood through transactions. _Fluvii_ will consume multiple messages at once, and feed them
individuallyy to the `app_function`. Resulting produces will be queued via the transaction itself, and the consumed offsets will get committed
along with them once the configured maximum batch count threshold is hit.

While this could mean you have to reprocess a batch of messages again if there was an exception, the speed tradeoff
is well worth this rare inconvenience.


### _FluviiTableApp_ Caching

There are two different caching mechanisms happpening in `FluviiTableApp` with respect to tables.

The first layer is caching table writes in the `TableTransaction` object until the transaction is committed (this coincides
with the batching that's happening).

The second layer is stored on the `FluviiSqlite` object, which is what `TableTransaction` hands its writes off to. Table writes are stored
here until the configured write cache count exceeds the maximum, in which they are truly committed to the `sqlite` database. 


This was implemented for two related reasons, both around utilizing networked storage situations:
1. to keep the most recent read/writes in memory for faster access
2. to reduce network I/O


### _FluviiTableApp_ limitations

To keep implementation around tabling simple, there were some design choices made that incurred 
some limitations, most of which have fairly straightforward options for overcoming. Most of these are because of the complexities
around topic partition assignments.

Those caveats include (and you'll notice a consistent theme):

(For clarity, an "app" in this context refers to any collection of _FluviiTableApp_ instances that
share the same consumer group.)

- The app can only consume from one topic (you can make an upstream "funnel" app as needed).
- The app only has 1 table.
- The table is keyed on the consumed message keys; you can only compare your record to the one with the same key on the table.
- The changelog topic (which defaults to the name `{consumer_group_id}__changelog`), must have same partition count as the consumed topic.
- Each app needs to share a storage volume.
- Table data needs to be stored as a json/dict (it is stored via `json.dumps` and `json.load`-ed when read)

In general, most of these can be overcomed by just linking different apps together since they will generally
be pretty light.

## Configuration

Configuration can be handled in two different ways: via environment variables and/or specialized
config objects. You can override the config objects at runtime if you wish.

Let's take a look at the various config objects you can interact with.

### ProducerConfig and ConsumerConfig

The `ProducerConfig` and `ConsumerConfig` objects will contain all configs around client settings,
and any _Fluvii_-specific functionality surrounding them. 

They require a url list as an argument. They also accept an optional Auth config; see below.

These are either used themselves, or handed to a `FluviiApp` instance.

### Various *Auth configs

There are numerous ways to authenticate with Kafka, and so you can choose the auth object that
suits you.

These are generally handed to other objects.

### FluviiAppConfig

The `FluviiAppConfig` can comprise any number of other various configs, and also has a few
of its own settings.

In general, you'll likely only need to adjust/hand it a few settings/config objects, as it has defaults for most
of the other various config objects you'd normally use (or of course, just use environment variables).

## Class Usage Information

Here is some more specific information around how you should use the `Transaction` objects, which
is the main application interface.

### All Transaction objects

Assume we have a `Transaction` object, called "transaction".

ALl `Transaction` objects will have the general workflow of:

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
by remaking the transaction object.

### TableTransaction

`TableTransaction` objects operate very similarly, except you'll need to add two extra commands before committing.

To read an entry, call `transaction.read_table_entry()`, which will return a dict.

To update an entry for the given message key, call `transaction.update_table_entry(my_update_dict)`. Of course, skip this
if you don't need to update it.

One thing to keep in mind is the object stored in the table just needs to be a valid json (which includes dicts).

Also, `FluviiTableApp` will ensure everything gets committed correctly for you just like the `FluviiApp`!

# Further Examples

Here's a few more examples.

## Configuration example
