#!/bin/bash
set -e
if [ -z "$TEUTHOLOGY_BRANCH" -a -n "$GITHUB_HEAD_REF" ]; then
    TEUTHOLOGY_BRANCH=${GITHUB_HEAD_REF}
fi
if [ ! -d ./teuthology ]; then
    git clone \
    --depth 1 \
    -b ${TEUTHOLOGY_BRANCH:-$(git branch --show-current)} \
    https://github.com/ceph/teuthology.git
fi

cp .teuthology.yaml teuthology/
cp Dockerfile teuthology/
cp teuthology.sh teuthology/
cp custom_conf.yaml teuthology/



# Generate an SSH keypair to use if necessary
if [ -z "$SSH_PRIVKEY_PATH" ]; then
    SSH_PRIVKEY_PATH=$(mktemp -u /tmp/teuthology-ssh-key-XXXXXX)
    ssh-keygen -t ed25519 -N '' -f $SSH_PRIVKEY_PATH
    export SSH_PRIVKEY=$(cat $SSH_PRIVKEY_PATH)
    export SSH_PUBKEY=$(cat $SSH_PRIVKEY_PATH.pub)
    export SSH_PRIVKEY_FILE=id_ed25519
else
    export SSH_PRIVKEY=$(cat $SSH_PRIVKEY_PATH)
    export SSH_PRIVKEY_FILE=$(basename $SSH_PRIVKEY_PATH | cut -d. -f1)
fi

trap "docker-compose down" SIGINT
docker-compose up \
    --build \
    --abort-on-container-exit \
    --exit-code-from teuthology
docker-compose down