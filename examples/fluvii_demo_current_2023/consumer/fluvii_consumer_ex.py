from fluvii import FluviiTableAppFactory


def load_my_heavy_ml_model():
    def heavy_ml_model(balance_change):
        if balance_change < -2000:
            return 'FRAUD!'
    return heavy_ml_model


balance_update_schema = {
    "name": "AccountBalanceUpdate",
    "type": "record",
    "fields": [
        {"name": "account_id", "type": "string", "default": ""},
        {"name": "update_message", "type": "string", "default": ""},
    ]
}


def my_app_logic(transaction, fraud_detection_model):
    record = transaction.value()
    change_amount = float(record['amount'])
    print(f"update of {change_amount} for AID {record['account_id']}")

    if change_amount == 0:
        print('System test; ignore')
        return  # No more processing, will still commit message

    account_balance = float((transaction.read_table_entry() or {'balance': 10000})['balance'])  # looks up record via the message.key()
    if fraud_detection_model(change_amount):
        new_balance = account_balance
        msg = f"FRAUDULENT ACTIVITY DETECTED: purchase attempt of {change_amount} at has been rejected and balance remains {new_balance}"
    else:
        if (new_balance := account_balance + change_amount) >= 0:
            transaction.update_table_entry({'balance': new_balance})  # store the updated balance for later...must be a valid json object (a dict works)
            msg = f"{'Purchase' if change_amount < 0 else 'Deposit'} has succeeded and new balance is {new_balance}"
        else:
            new_balance = account_balance
            msg = f"WARNING: Not enough funds; purchase has been rejected and balance remains {new_balance}"
    transaction.produce({"value": {"account_id": record["account_id"], "update_message": msg}})
    print(f"AcctID {record['account_id']}: {msg}")


def fluvii_table_app():
    return FluviiTableAppFactory(
        my_app_logic,
        ['account_update_requests'],
        produce_topic_schema_dict={'account_notifications': balance_update_schema},
        app_function_arglist=[load_my_heavy_ml_model()]
    )


fluvii_table_app().run()