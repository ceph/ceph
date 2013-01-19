#!/bin/bash

set -e

# basic tests of O_SYNC, O_DSYNC, O_RSYNC
# test O_SYNC
iozone -c -e -s 512M -r 1M -t 1 -F osync1 -i 0 -i 1 -o
# test O_DSYNC
iozone -c -e -s 512M -r 1M -t 1 -F odsync1 -i 0 -i 1 -+D
# test O_RSYNC
iozone -c -e -s 512M -r 1M -t 1 -F orsync1 -i 0 -i 1 -+r

# test same file with O_SYNC in one process, buffered in the other
# the sync test starts first, so the buffered test should blow
# past it and 
iozone -c -e -s 512M -r 1M -t 1 -F osync2 -i 0 -i 1 -o &
sleep 1
iozone -c -e -s 512M -r 256K -t 1 -F osync2 -i 0
wait $!

# test same file with O_SYNC from different threads
iozone -c -e -s 512M -r 1M -t 2 -F osync3 -i 2 -o
