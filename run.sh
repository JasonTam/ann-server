#!/bin/sh

MODE=${1:-"many"}  # { "many" | "single" }
PATH_ANN=${2:-"s3://mo-ml-dev/ann/"}


if [ ${MODE} = "many" ]
then
    APP_FN="api.app_falcon:build_many_app('$PATH_ANN')"
else
    APP_FN="api.app_falcon:build_single_app('$PATH_ANN')"
fi

gunicorn \
    -k gevent \
    -b 0.0.0.0:8000 \
    ${APP_FN}
