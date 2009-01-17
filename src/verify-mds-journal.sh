#!/bin/bash

while [ 1 ]
do
  ./cmds -f --debug_mds 20 --debug_ms 1 --standby_replay_for 0 || exit 1
  echo replay ok, sleeping
  sleep 30
done