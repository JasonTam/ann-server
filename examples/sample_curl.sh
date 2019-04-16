#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

curl \
   localhost:8000/ann \
    -XPOST \
    -H "Content-type: application/json" \
    -d@$BASEDIR/sample_payload2.json

#    -d \
#'{
#    "k": 5,
#    "search_k": -1,
#    "id": "word_1"
#}'

echo
