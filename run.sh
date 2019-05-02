#!/bin/sh

BUCKET=my-bucket

PATH_ANN=${1:-"s3://${BUCKET}/ann/"}
OOI_TABLE=${2:-""}
PATH_FALLBACK=${3:-""}
CHECK_INTERVAL=${4:-3600}


APP_FN="app_builder:build_many_app("\
"'$PATH_ANN',"\
"'$OOI_TABLE',"\
"'$PATH_FALLBACK',"\
"$CHECK_INTERVAL,"\
")"


gunicorn \
    --timeout 90 \
    -k gevent \
    -b 0.0.0.0:8000 \
    ${APP_FN}
