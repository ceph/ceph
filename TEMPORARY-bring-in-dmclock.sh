#!/bin/sh

git subtree pull --prefix src/dmclock \
    git@github.com:ceph/dmclock.git master --squash

git subtree pull --prefix src/dmclock \
    git@github.com:ivancich/dmclock-fork.git wip-librados-adjustments --squash
