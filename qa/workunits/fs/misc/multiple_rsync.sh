#!/bin/sh -ex

rsync -av --exclude local/ /usr/ usr.1
rsync -av --exclude local/ /usr/ usr.2

# this shouldn't transfer any additional files
echo we should get 4 here if no additional files are transfered
rsync -auv --exclude local/ /usr/ usr.1 | tee /tmp/$$
hexdump -C /tmp/$$
wc -l /tmp/$$ | grep 4
rsync -auv --exclude local/ /usr/ usr.2 | tee /tmp/$$
hexdump -C /tmp/$$
wc -l /tmp/$$ | grep 4
rm /tmp/$$

echo OK