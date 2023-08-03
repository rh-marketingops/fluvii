import sys
sys.path.insert(0, "/Users/clementhurel/Sites/fluvii")
from fluvii import FluviiApp
from fluvii.transaction import Transaction
from fluvii.producer import Producer
from fluvii.consumer import Consumer
from fluvii.auth import SaslPlainClientConfig, AWSRegistryConfig, SaslOauthClientConfig
from os import environ
from fluvii.schema_registry import GlueSchemaRegistryClient
import uuid





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
schema_registry_auth_config = AWSRegistryConfig(
                environ["FLUVII_SCHEMA_REGISTRY_ACCESS_KEY_ID"],
                environ["FLUVII_SCHEMA_REGISTRY_SECRET_ACCESS_KEY"],
                environ["FLUVII_SCHEMA_REGISTRY_REGION_NAME"],
                environ["FLUVII_SCHEMA_REGISTRY_REGISTRY_NAME"]
            )
schema_registry = GlueSchemaRegistryClient(auth_config=schema_registry_auth_config)
client_auth_config = SaslPlainClientConfig(
    environ["FLUVII_CLIENT_USERNAME"],
    environ["FLUVII_CLIENT_PASSWORD"],
    environ["FLUVII_CLIENT_MECHANISMS"]
)

init_at_runtime_thing = 'cool value' # can hand off objects you'd like to init separately, like an api session object
# producer = Producer(
#     urls= 'b-3.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096,b-2.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096,b-1.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096',
#     topic_schema_dict={'TEST__PeopleStream_Canon_Input': a_cool_schema},
#     client_auth_config=client_auth_config,
#     schema_registry=schema_registry
# )
# producer.produce(value= {'cool_field': 'test'}, topic= 'TEST__PeopleStream_Canon_Input', key= 'hclementisfun', headers ={'test' : 'test'})
# producer.flush()


# consumer = Consumer(
#     urls= 'b-3.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096,b-2.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096,b-1.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096',
#     #topic_schema_dict={'TEST__PeopleStream_Canon_Input': a_cool_schema},
#     client_auth_config=client_auth_config,
#     schema_registry=schema_registry,
#     group_id='amazon.msk.canary.group.broker-2',
#     consume_topics_list='TEST__PeopleStream_Canon_Input'
# )

# try:
#     while True:
#         msg = consumer.poll(1.0)
#         if msg is None:
#             print('no message')
#             continue
#         if msg.error():
#             print("Consumer error: {}".format(msg.error()))
#             continue
#         print('Received message: {}'.format(msg.value()))
# except KeyboardInterrupt:
#     pass
# finally:
#     # Close down consumer to commit final offsets.
#     c.close()

def my_app_logic(transaction: Transaction, thing_inited_at_runtime):
    # All we're gonna do is set our field to a new value...very exciting, right?
    print('test')
    msg = transaction.message # can also do transaction.value() to skip a step
    cool_message_out = msg.value()
    cool_message_out['cool_field'] = thing_inited_at_runtime #  'cool value'
    transaction.produce(
        {'value': cool_message_out, 'topic': 'TEST__PeopleStream_Canon_Input', 'key': msg.key(), 'headers': msg.headers()}
    )
    
fluvii_app = FluviiApp(
    app_function=my_app_logic, 
    consume_topics_list=['TEST__PeopleStream_Canon_Input'],
    produce_topic_schema_dict={'TEST__PeopleStream_Canon_Input': a_cool_schema},
    app_function_arglist = [init_at_runtime_thing])  # optional! Here to show functionality.
fluvii_app.run()