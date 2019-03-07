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
#    "ids": [ "word_1"
#            ,"word_12"
#            ,"word_123"
#            ,"word_1234"
#            ,"word_12345"
#            ,"word_123456"
#    ]
#}'

echo
