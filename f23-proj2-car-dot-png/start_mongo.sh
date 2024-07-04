#!/bin/bash

mongod --port $1 --dbpath ~/mongodb_data_folder --quiet --logpath /dev/null &
