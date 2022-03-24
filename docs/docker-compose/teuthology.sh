#!/usr/bin/bash
# We don't want -x yet, in case the private key is sensitive
if [ -n "$SSH_PRIVKEY_FILE" ]; then
    echo "$SSH_PRIVKEY" > $HOME/.ssh/$SSH_PRIVKEY_FILE
fi
source /teuthology/virtualenv/bin/activate
set -x
if [ -n "$TESTNODES" ]; then
    for node in $(echo $TESTNODES | tr , ' '); do
        teuthology-update-inventory $node
    done
fi
export MACHINE_TYPE=${MACHINE_TYPE:-testnode}
teuthology-suite -v \
    --ceph-repo https://github.com/ceph/ceph.git \
    --suite-repo https://github.com/ceph/ceph.git \
    -c master \
    -m $MACHINE_TYPE \
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
    --tube $MACHINE_TYPE \
    --exit-on-empty-queue