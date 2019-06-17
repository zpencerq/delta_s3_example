#!/bin/bash -u

if [ ! -d 'venv' ]; then
  python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt

moto_server s3 &
MOTO_PID=$!

AWS_ACCESS_KEY_ID=fake AWS_SECRET_ACCESS_KEY=fake python test.py

kill -- $MOTO_PID
