#!/bin/sh

MODE=${1:-"many"}  # { "many" | "single" }
PATH_ANN=${2:-"s3://mo-ml-dev/ann/"}
OOI_TABLE=${3:-"img-reprs"}
PATH_FALLBACK=${4:-"s3://mo-ml-dev/subcat_parent_map.json"}


if [ ${MODE} = "many" ]
then
    APP_FN="api.app_falcon:build_many_app('$PATH_ANN','$OOI_TABLE','$PATH_FALLBACK')"
else
    APP_FN="api.app_falcon:build_single_app('$PATH_ANN')"
fi

gunicorn \
    --timeout 90 \
    -k gevent \
    -b 0.0.0.0:8000 \
    ${APP_FN}
