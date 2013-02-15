#!/bin/bash

mkdir /tmp/cephtest
#wget https://raw.github.com/ceph/ceph/master/qa/run_xfstests.sh
wget https://ceph.com/git/?p=ceph.git;a=blob_plain;f=qa/run_xfstests.sh
chmod +x run_xfstests.sh
# tests excluded fail in the current testing vm regardless of whether
# rbd is used

./run_xfstests.sh -c 1 -f xfs -t /dev/vdb -s /dev/vdc 1-17 19-26 28-49 51-61 63 66-67 69-79 83 85-105 108-110 112-135 137-170 174-204 206-217 220-227 230-231 233 235-241 243-249 251-262 264-278 281-286 288-289
