source venv/bin/activate

set -a
source ./kafka/creds.env
set +a

cat /home/tsawicki/Documents/fluvii_demo/kafka/topics.json | fluvii topics create \
&& sleep 2 \
&& fluvii topics list | grep account
