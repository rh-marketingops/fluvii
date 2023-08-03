from confluent_kafka import Producer
from confluent_kafka import Consumer



def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


p = Producer({
    'bootstrap.servers': "<>",
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': '<>',
    'sasl.password': '<>'
})

for data in range(100):
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)
    data_str = {'personal_facts': {'email_address': f'test_email_{data}@test.com', 'salutation': '', 'first_name': '', 'last_name': '', 'mobile_phone': '', 'language_preference': '', 'address': {'country_name': '', 'country_code': '', 'address_street_1': '', 'address_street_2': '', 'address_street_3': '', 'address_city': '', 'address_state_province': '', 'address_postal_code': '', 'core_based_statistical_area': '', 'combined_statistical_area': ''}, 'job': {'company': '', 'business_phone': '', 'job_title': '', 'department': '', 'job_role': '', 'job_level': '', 'job_function': '', 'industry': '', 'annual_revenue': '', 'company_size': ''}}, 'marketing_descriptors': {'persona': '', 'super_region': '', 'sub_region': '', 'penalty_box_reason': '', 'penalty_box_expiration': '', 'lead_score': {'mlsm': {'lead_ranking': '', 'lead_rating': '', 'interest_level': '', 'qualification_level': '', 'all_scores': ''}}}, 'privacy': {'consent_email_marketing': '', 'consent_email_marketing_timestamp': '', 'consent_email_marketing_source': '', 'consent_share_to_partner': '', 'consent_share_to_partner_timestamp': '', 'consent_share_to_partner_source': '', 'consent_phone_marketing': '', 'consent_phone_marketing_timestamp': '', 'consent_phone_marketing_source': ''}, 'last_submission': {'submission_date': '', 'submission_source': '', 'opt_in': {'f_formdata_optin': '', 'f_formdata_optin_phone': '', 'f_formdata_sharetopartner': ''}, 'location': {'city_from_ip': '', 'state_province_from_ip': '', 'postal_code_from_ip': '', 'country_from_ip': '', 'country_from_dns': ''}}, 'tracking_ids': {'eloqua_contact_id': '', 'sfdc_lead_ids': [{'lead_id': '', 'record_status': ''}], 'sfdc_contact_ids': [{'contact_id': '', 'account_id': '', 'record_status': ''}]}, 'tombstone': {'is_tombstoned': '', 'tombstone_timestamp': '', 'tombstone_source': '', 'delete_all_data': ''}, 'last_evaluated_by_dwm': ''}
    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('TEST__PeopleStream_Canon_Input', value=str(data_str), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports
# to be triggered.
p.flush()

# --------------------------------------

c = Consumer({
    'bootstrap.servers': "<>",
    'group.id': 'amazon.msk.canary.group.broker-3',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': '<>',
    'sasl.password': '<>'
})

c.subscribe(['TEST__PeopleStream_Canon_Input'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: {}'.format(msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    c.close()