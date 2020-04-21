#!/bin/bash

set -e

hostname worker

exec python /code/web.py &

exec python /code/main.py
