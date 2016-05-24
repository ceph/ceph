#!/bin/sh

# dmclock
git subtree pull \
    --prefix src/dmclock \
    git@github.com:ceph/dmclock.git master --squash

# add other subtree pull commands here...
