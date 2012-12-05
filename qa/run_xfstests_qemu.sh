#!/bin/bash

mkdir /tmp/cephtest
wget https://raw.github.com/ceph/ceph/master/qa/run_xfstests.sh
chmod +x run_xfstests.sh
# tests excluded require extra packages for advanced acl and quota support
./run_xfstests.sh -c 1 -f xfs -t /dev/vdb -s /dev/vdc 1-26 28-49 51-63 65-83 85-233 235-291
