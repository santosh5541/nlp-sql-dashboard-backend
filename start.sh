#!/usr/bin/env bash
python3 -m gunicorn --bind 0.0.0.0:$PORT app:app