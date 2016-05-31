#!/bin/sh

# dmclock
git subtree push \
    --prefix src/dmclock \
    git@github.com:ceph/dmclock.git master

# add other subtree pull commands here...
