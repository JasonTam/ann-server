#!/bin/sh

#PATH_INDEX='s3://jason-garbage/ann/glove100/glove100.ann'
#PATH_IDS='s3://jason-garbage/ann/glove100/glove100_ids'
#N_DIM=100

PATH_INDEX='s3://jason-garbage/ann/glove1m/glove1m.ann'
PATH_IDS='s3://jason-garbage/ann/glove1m/glove1m_ids'
N_DIM=25

gunicorn \
    -k gevent \
    -b 0.0.0.0:8000 \
    "api.app_falcon:build_app('$PATH_INDEX', '$PATH_IDS', $N_DIM)"
