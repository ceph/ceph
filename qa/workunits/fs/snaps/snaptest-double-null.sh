#!/bin/sh -x

set -e

# multiple intervening snapshots with no modifications, and thus no
# snapflush client_caps messages.  make sure the mds can handle this.

for f in `seq 1 20` ; do

mkdir a
cat > a/foo &
mkdir a/.snap/one
mkdir a/.snap/two
wait
chmod 777 a/foo
sync   # this might crash the mds
ps
rmdir a/.snap/*
rm a/foo
rmdir a

done

echo OK
