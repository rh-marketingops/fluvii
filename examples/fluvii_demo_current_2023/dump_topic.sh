source venv/bin/activate

set -a
source ./kafka/creds.env
set +a

fluvii topics consume --topic-offset-dict '{"account_notifications": {}}' --output-filepath ./consumer/msgs.json \

echo "dumped to ./consumer/msgs.json"