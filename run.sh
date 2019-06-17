#!/bin/bash -u

if [ ! -d 'venv' ]; then
  python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt

moto_server s3 &
MOTO_PID=$!

python test.py

kill -- $MOTO_PID
