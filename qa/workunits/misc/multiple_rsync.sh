#!/bin/sh -ex

rsync -av /usr/ usr.1
rsync -av /usr/ usr.2

# this shouldn't transfer any additional files
echo we should get 4 here if no additional files are transfered
rsync -auv /usr/ usr.1 | tee a
wc -l a | grep 4
rsync -auv /usr/ usr.2 | tee a
wc -l a | grep 4

echo OK