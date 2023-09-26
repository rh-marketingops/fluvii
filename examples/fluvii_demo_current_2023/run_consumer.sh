source venv/bin/activate

set -a
source consumer/creds.env
set +a

python consumer/fluvii_consumer_ex.py