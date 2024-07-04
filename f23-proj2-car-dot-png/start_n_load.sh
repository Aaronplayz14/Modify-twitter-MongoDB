#!/bin/bash

bash start_mongo.sh $1
python3 load-json.py farmers-protest-tweets-2021-03-5.json $1
