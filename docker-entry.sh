#!/bin/bash

set -e

exec python /code/web.py &

exec python /code/reporter.py &

exec python /code/main.py
