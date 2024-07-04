#!/bin/bash

CURR_USER=$(whoami)
PIDS=$(pgrep -u "$CURR_USER" mongod)

kill -9 "$PIDS"
echo "Killed: $PIDS"
