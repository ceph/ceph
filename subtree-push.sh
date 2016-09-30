#!/bin/sh

# dmclock
git subtree push \
    --prefix src/dmclock \
    git@github.com:ceph/dmclock.git ceph

# add other subtree pull commands here...
