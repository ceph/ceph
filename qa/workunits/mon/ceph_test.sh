#!/bin/bash

set -ex

ls /etc/ceph
export CEPH_CONF=/etc/ceph/ceph.conf
export CEPH_KEYRING=/etc/ceph/ceph.keyring
export CEPH_ARGS='--id admin'

# Ideally we would run every ceph_test_mon_* but many of these require special
# arguments, acccess to audit the mon logs (to check rss/memory), or require
# standing up fake OSDs with real keyrings. This is awkward to do in general
# QA.
#for t in $(compgen -c ceph_test_mon_ | sort -u); do
#    "$t"
#done

ceph_test_mon_msg
