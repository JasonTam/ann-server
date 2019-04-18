#!/bin/sh

PATH_ANN=${1:-"s3://mo-ml-dev/ann/"}
OOI_TABLE=${2:-"img-reprs"}
PATH_FALLBACK=${3:-"s3://mo-ml-dev/subcat_parent_map.json"}
CHECK_INTERVAL=${4:-3600}


APP_FN="api.app_falcon:build_many_app("\
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
