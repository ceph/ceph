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
    --suite teuthology:no-ceph \
    --filter-out "libcephfs,kclient,stream,centos,rhel" \
    -d ubuntu -D 20.04 \
    --suite-branch master \
    --subset 9000/100000 \
    -p 75 \
    --seed 349 \
    --force-priority \
    /teuthology/custom_conf.yaml
teuthology-dispatcher -v \
    --log-dir /teuthology/log \
    --tube testnode \
    --exit-on-empty-queue