from fluvii.components.producer import ProducerFactory
from time import sleep, time
from random import randint, random

purchase_schema = {
    "name": "AccountBalanceChangeRequest",
    "type": "record",
    "fields": [
        {"name": "account_id", "type": "string", "default": ""},
        {"name": "timestamp", "type": "float", "default": 0},
        {"name": "amount", "type": "int", "default": 0},
    ]
}

produce_topic = "account_update_requests"
producer = ProducerFactory(topic_schema_dict={produce_topic: purchase_schema})
producer.produce(
    {"account_id": "TEST", "timestamp": time(), "amount": 0},
    key="TEST",
    topic=produce_topic
)
count = 0
try:
    while count < 100000:
        account = f"A{randint(0, 10)}"
        producer.produce(
            {
                "account_id": account,
                "timestamp": time(),
                "amount": randint(-2500, 500)
            },
            key=account,
            topic=produce_topic
        )
        sleep(random()*2)
        count += 1
finally:
    producer.close()