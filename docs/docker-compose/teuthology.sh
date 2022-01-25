#!/usr/bin/bash
set -e
# We don't want -x yet, in case the private key is sensitive
echo "$SSH_PRIVKEY" > $HOME/.ssh/id_ed25519
source /teuthology/virtualenv/bin/activate
set -x
teuthology-suite -v \
    --ceph-repo https://github.com/ceph/ceph.git \
    --suite-repo https://github.com/ceph/ceph.git \
    -c master \
    -m testnode \
    --limit 1 \
    -n 100 \
    --suite dummy \
    --suite-branch master \
    --subset 9000/100000 \
    -p 75 \
    --force-priority \
    /teuthology/custom_conf.yaml
teuthology-dispatcher --log-dir /teuthology/log --tube testnode
tail -f /dev/null