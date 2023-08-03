import sys
sys.path.insert(0, "/Users/clementhurel/Sites/fluvii")
from fluvii import FluviiTableApp
from os import environ
from fluvii.transaction import TableTransaction
from fluvii.auth import SaslPlainClientConfig, AWSRegistryConfig, SaslOauthClientConfig
from fluvii.schema_registry import GlueSchemaRegistryClient
from fluvii.producer import Producer
# -- Not used here, just so you know what the consumed messages look like
# purchase_schema = {
#     "name": "AccountPurchase",
#     "type": "record",
#     "fields": [
#         {"name": "AccountNumber", "type": "string"},
#         {"name": "PurchaseTimestamp", "type": "string"},
#         {"name": "PurchaseAmount", "type": "string"},
#     ]
# }
account_balance_schema = {
    "name": "AccountBalance",
    "type": "record",
    "fields": [
        {"name": "AccountNumber", "type": "string"},
        {"name": "AccountBalance", "type": "string"},
    ]
}
client_auth_config = SaslPlainClientConfig(
    environ["FLUVII_CLIENT_USERNAME"],
    environ["FLUVII_CLIENT_PASSWORD"],
    environ["FLUVII_CLIENT_MECHANISMS"]
)
schema_registry_auth_config = AWSRegistryConfig(
                environ["FLUVII_SCHEMA_REGISTRY_ACCESS_KEY_ID"],
                environ["FLUVII_SCHEMA_REGISTRY_SECRET_ACCESS_KEY"],
                environ["FLUVII_SCHEMA_REGISTRY_REGION_NAME"],
                environ["FLUVII_SCHEMA_REGISTRY_REGISTRY_NAME"]
            )
schema_registry = GlueSchemaRegistryClient(auth_config=schema_registry_auth_config)

def my_app_logic(transaction: TableTransaction):
    print('CLEMENT CLEMENT')
    record = transaction.value()
    print(record)
    current_account_balance = float(11)  # looks up record via the message.key()
    print('CLEMENT CLEMENT2')
    purchase_amount = float(record['PurchaseAmount'])
    print('CLEMENT CLEMENT3')
    new_balance = str(current_account_balance - purchase_amount)
    print('CLEMENT CLEMENT4')
    message = {"AccountNumber": record["AccountNumber"], "AccountBalance": new_balance}
    print('CLEMENT CLEMENT5')
    transaction.update_table_entry({'balance': new_balance})  # store the updated balance for later...must be a valid json object (a dict works)
    print('CLEMENT CLEMENT6')
    transaction.produce({'value': message, 'key': transaction.key(), 'headers': transaction.headers()})
    print('CLEMENT CLEMENT7')


def fluvii_table_app():
    return FluviiTableApp(
        app_function=my_app_logic,
        consume_topic='PeopleStream_Canon_Input',
        produce_topic_schema_dict={'PeopleStream_Canon': account_balance_schema},
    )


fluvii_table_app().run()