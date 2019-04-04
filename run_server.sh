#!/bin/sh

PATH_TAR='s3://mo-ml-dev/ann/dresses.tar.gz'

gunicorn \
    -k gevent \
    -b 0.0.0.0:8000 \
    "api.app_falcon:build_app('$PATH_TAR')"
