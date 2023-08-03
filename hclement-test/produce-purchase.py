import sys
sys.path.insert(0, "/Users/clementhurel/Sites/fluvii")
from fluvii import FluviiApp
from fluvii.transaction import Transaction
from fluvii.producer import Producer
from fluvii.consumer import Consumer
from fluvii.auth import SaslScramClientConfig, GlueRegistryClientConfig
from os import environ
from fluvii.schema_registry import GlueSchemaRegistryClient
import inspect
from confluent_kafka import SerializingProducer

print(inspect.isgeneratorfunction(SerializingProducer.list_topics))
test_schema = {'type': 'record', 'name': 'PersonSchema', 'fields': [{'name': 'personal_facts', 'type': {'type': 'record', 'name': 'PersonalFacts', 'fields': [{'eloqua_hash': True, 'name': 'email_address', 'type': {'type': 'record', 'name': 'RichField', 'fields': [{'name': 'field_value', 'type': 'string', 'default': ''}, {'name': 'last_sourced_by', 'type': 'string', 'default': ''}, {'name': 'last_modified_date', 'type': 'string', 'default': ''}, {'name': 'blank_on_purpose', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}}, {'eloqua_hash': True, 'name': 'is_bounceback', 'type': 'nubium.RichField'}, {'name': 'is_a_test_contact', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'salutation', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'first_name', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'middle_name', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'last_name', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'mobile_phone', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'language_preference', 'type': 'nubium.RichField'}, {'name': 'address', 'type': {'type': 'record', 'name': 'Address', 'fields': [{'eloqua_hash': True, 'name': 'country_name', 'type': 'nubium.RichField'}, {'name': 'country_code', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'address_street_1', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'address_street_2', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'address_street_3', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'address_city', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'address_state_province', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'address_postal_code', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'core_based_statistical_area', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'combined_statistical_area', 'type': 'nubium.RichField'}], 'namespace': 'nubium'}}, {'name': 'job', 'type': {'type': 'record', 'name': 'Job', 'fields': [{'eloqua_hash': True, 'name': 'company', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'business_phone', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'fax_number', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'job_title', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'department', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'job_role', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'job_level', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'job_function', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'annual_revenue', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'company_size', 'type': 'nubium.RichField'}], 'namespace': 'nubium'}}], 'namespace': 'nubium'}}, {'name': 'marketing_descriptors', 'type': {'type': 'record', 'name': 'MarketingDescriptors', 'fields': [{'eloqua_hash': True, 'name': 'persona', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'super_region', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'sub_region', 'type': 'nubium.RichField'}, {'eloqua_hash': True, 'name': 'provisional_account_match', 'type': 'nubium.RichField'}, {'name': 'lead_scores', 'type': {'type': 'map', 'values': {'type': 'map', 'values': 'nubium.RichField', 'name': 'lead_score'}, 'name': 'lead_score'}, 'default': {'domain': {'metric': {'field_value': '', 'last_sourced_by': '', 'last_modified_date': '', 'blank_on_purpose': ''}}}}], 'namespace': 'nubium'}}, {'name': 'privacy', 'type': {'type': 'record', 'name': 'Privacy', 'fields': [{'eloqua_hash': True, 'name': 'consent_email_marketing', 'type': 'string', 'default': ''}, {'name': 'consent_email_marketing_timestamp', 'type': 'string', 'default': ''}, {'name': 'consent_email_marketing_source', 'type': 'string', 'default': ''}, {'name': 'consent_share_to_partner', 'type': 'string', 'default': ''}, {'name': 'consent_share_to_partner_timestamp', 'type': 'string', 'default': ''}, {'name': 'consent_share_to_partner_source', 'type': 'string', 'default': ''}, {'name': 'consent_phone_marketing', 'type': 'string', 'default': ''}, {'name': 'consent_phone_marketing_timestamp', 'type': 'string', 'default': ''}, {'name': 'consent_phone_marketing_source', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}}, {'name': 'last_submission', 'type': {'type': 'record', 'name': 'LastSubmission', 'fields': [{'name': 'submission_date', 'type': 'string', 'default': ''}, {'name': 'submission_source', 'type': 'string', 'default': ''}, {'name': 'opt_in', 'type': {'type': 'record', 'name': 'OptIn', 'fields': [{'name': 'f_formdata_optin', 'type': 'string', 'default': ''}, {'name': 'f_formdata_optin_phone', 'type': 'string', 'default': ''}, {'name': 'f_formdata_sharetopartner', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}}, {'name': 'location', 'type': {'type': 'record', 'name': 'Location', 'fields': [{'name': 'city_from_ip', 'type': 'string', 'default': ''}, {'name': 'state_province_from_ip', 'type': 'string', 'default': ''}, {'name': 'postal_code_from_ip', 'type': 'string', 'default': ''}, {'eloqua_hash': True, 'name': 'country_from_ip', 'type': 'string', 'default': ''}, {'eloqua_hash': True, 'name': 'country_from_dns', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}}, {'name': 'campaign_response_metadata', 'type': {'type': 'record', 'name': 'CampaignResponseMetadata', 'fields': [{'name': 'ext_tactic_id', 'type': 'string', 'default': ''}, {'name': 'int_tactic_id', 'type': 'string', 'default': ''}, {'name': 'offer_id', 'type': 'string', 'default': ''}, {'name': 'offer_consumption_timestamp', 'type': 'string', 'default': ''}, {'name': 'is_lead_activity', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}}], 'namespace': 'nubium'}}, {'name': 'tracking_ids', 'type': {'type': 'map', 'values': {'type': 'map', 'values': {'type': 'record', 'name': 'TrackingIds', 'fields': [{'name': 'record_status', 'type': 'string', 'default': ''}, {'name': 'record_made_inactive_date', 'type': 'string', 'default': ''}, {'name': 'created_date', 'type': 'string', 'default': ''}, {'name': 'attributes', 'type': {'type': 'map', 'values': 'string', 'name': 'attribute'}, 'default': {}}], 'namespace': 'nubium'}, 'name': 'tracking_id'}, 'name': 'tracking_id'}, 'default': {'domain': {'unique_id': {'record_status': '', 'record_made_inactive_date': '', 'created_date': '', 'attributes': {}}}}}, {'name': 'tombstone', 'type': {'type': 'record', 'name': 'Tombstone', 'fields': [{'name': 'is_tombstoned', 'type': 'string', 'default': ''}, {'name': 'tombstone_timestamp', 'type': 'string', 'default': ''}, {'name': 'tombstone_source', 'type': 'string', 'default': ''}, {'name': 'delete_all_data', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}}, {'eloqua_hash': True, 'name': 'last_evaluated_by_dwm', 'type': 'string', 'default': ''}], 'namespace': 'nubium'}
purchase_schema = {
    "name": "AccountPurchase",
    "type": "record",
    "fields": [
        {"name": "AccountNumber", "type": "string"},
        {"name": "PurchaseTimestamp", "type": "string"},
        {"name": "PurchaseAmount", "type": "string"},
    ]
}
schema_registry_auth_config = GlueRegistryClientConfig(
                environ["FLUVII_SCHEMA_REGISTRY_ACCESS_KEY_ID"],
                environ["FLUVII_SCHEMA_REGISTRY_SECRET_ACCESS_KEY"],
                environ["FLUVII_SCHEMA_REGISTRY_REGION_NAME"],
                environ["FLUVII_SCHEMA_REGISTRY_REGISTRY_NAME"]
            )
schema_registry = GlueSchemaRegistryClient(auth_config=schema_registry_auth_config)
client_auth_config = SaslScramClientConfig(
    environ["FLUVII_CLIENT_USERNAME"],
    environ["FLUVII_CLIENT_PASSWORD"],
    environ["FLUVII_CLIENT_MECHANISMS"]
)

init_at_runtime_thing = 'cool value' # can hand off objects you'd like to init separately, like an api session object
producer = Producer(
    urls= 'b-3.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096,b-2.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096,b-1.itpreprod.sui7dp.c7.kafka.us-east-1.amazonaws.com:9096',
    topic_schema_dict={'PeopleStream_Canon_Input': purchase_schema},
    client_auth_config=client_auth_config,
    schema_registry=schema_registry
)
producer.add_topic('TEST__PeopleStream_Canon_Input', test_schema)
producer.produce(value= {'PurchaseAmount': '12', 'PurchaseTimestamp': 'test', 'AccountNumber': '122121'}, topic= 'PeopleStream_Canon_Input', key= 'hclementisfun', headers ={'test' : 'test'})
producer.flush()

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
#         print('Received message: {}'.format(msg.value().decode('utf-8')))
# except KeyboardInterrupt:
#     pass
# finally:
#     # Close down consumer to commit final offsets.
#     c.close()

# def my_app_logic(transaction: Transaction, thing_inited_at_runtime):
#     # All we're gonna do is set our field to a new value...very exciting, right?
#     print('test')
#     msg = transaction.message # can also do transaction.value() to skip a step
#     cool_message_out = msg.value()
#     cool_message_out['cool_field'] = thing_inited_at_runtime #  'cool value'
#     transaction.produce(
#         {'value': cool_message_out, 'topic': 'TEST__PeopleStream_Canon_1687908848', 'key': msg.key(), 'headers': msg.headers()}
#     )
    
# fluvii_app = FluviiApp(
#     app_function=my_app_logic, 
#     consume_topics_list=['TEST__PeopleStream_Canon_1687908848'],
#     produce_topic_schema_dict={'TEST__PeopleStream_Canon_1687908848': a_cool_schema},
#     app_function_arglist = [init_at_runtime_thing])  # optional! Here to show functionality.
# fluvii_app.run()