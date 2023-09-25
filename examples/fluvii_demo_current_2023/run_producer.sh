source venv/bin/activate

set -a
source producer/creds.env
set +a

python producer/fluvii_producer_ex.py
