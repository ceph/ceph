#!/bin/sh -ex

rsync -av --exclude local/ /usr/ usr.1
rsync -av --exclude local/ /usr/ usr.2

# this shouldn't transfer any additional files
echo we should get 4 here if no additional files are transfered
rsync -auv --exclude local/ /usr/ usr.1 | tee a
hexdump -C a
wc -l a | grep 4
rsync -auv --exclude local/ /usr/ usr.2 | tee a
hexdump -C a
wc -l a | grep 4

echo OK