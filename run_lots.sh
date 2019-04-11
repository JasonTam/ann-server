#!/bin/sh

PATH_ANN_DIR='s3://mo-ml-dev/ann/'

gunicorn \
    -k gevent \
    -b 0.0.0.0:8000 \
    "api.app_falcon_mono:build_app('$PATH_ANN_DIR')"
