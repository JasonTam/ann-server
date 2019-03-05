#!/usr/bin/env bash

PATH_INDEX='s3://jason-garbage/ann/glove100/glove100.ann'
PATH_IDS='s3://jason-garbage/ann/glove100/glove100_ids'
N_DIM=100

gunicorn "api.app_falcon:build_app('$PATH_INDEX', '$PATH_IDS', $N_DIM)"
