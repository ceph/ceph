#!/bin/bash

if git $current_pr stats | grep ceph-backport.sh ; then
    git clone https://lab.fedeproxy.eu/ceph/ceph-backport
    cd ceph-backport
    tests/setup.sh # launches a redmine and a gitea in docker containers
    tests/test-ceph-backport.sh
fi
