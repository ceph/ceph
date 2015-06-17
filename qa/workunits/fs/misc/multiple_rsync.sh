#!/bin/sh -ex


# Populate with some arbitrary files from the local system.  Take
# a copy to protect against false fails from system updates during test.
export PAYLOAD=/tmp/multiple_rsync_payload.$$
sudo cp -r /usr/lib/ $PAYLOAD

set -e

sudo rsync -av $PAYLOAD payload.1
sudo rsync -av $PAYLOAD payload.2

# this shouldn't transfer any additional files
echo we should get 4 here if no additional files are transfered
sudo rsync -auv $PAYLOAD payload.1 | tee /tmp/$$
hexdump -C /tmp/$$
wc -l /tmp/$$ | grep 4
sudo rsync -auv $PAYLOAD payload.2 | tee /tmp/$$
hexdump -C /tmp/$$
wc -l /tmp/$$ | grep 4
echo OK

rm /tmp/$$
sudo rm -rf $PAYLOAD
