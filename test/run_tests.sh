#!/bin/bash -xe

# This file contains only test tokens. You need to set yours.
export P1_API_TOKEN='e44e7c6b04ef3ea1cfb7a8a67db74751c177259e'
export P1_EDGAR_API_TOKEN='8c9c9458b145202c7a6b6cceaabd82023e957a46d6cf7061ed8e1c94a168f2fd'

echo "P1_API_TOKEN=$P1_API_TOKEN"
echo "P1_EDGAR_API_TOKEN=$P1_EDGAR_API_TOKEN"
echo "done"

VENV_DIR="./venv"

if [[ -d "$VENV_DIR" ]]; then
    rm -rf $VENV_DIR
fi;

python3 -m venv $VENV_DIR
source venv/bin/activate
pip install -r requirements.txt

python -c "import p1_data_client_python.version as version; print(version.VERSION)"

pytest -vv
