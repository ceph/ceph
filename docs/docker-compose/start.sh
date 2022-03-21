#!/bin/bash
set -e
if [ -z "$TEUTHOLOGY_BRANCH" -a -n "$GITHUB_HEAD_REF" ]; then
    TEUTHOLOGY_BRANCH=${GITHUB_HEAD_REF}
fi
git clone \
  --depth 1 \
  -b ${TEUTHOLOGY_BRANCH:-$(git branch --show-current)} \
  https://github.com/ceph/teuthology.git

# Check for .teuthology.yaml file and copy it to teuthology
if [ -f ".teuthology.yaml" ]; 
then
    cp .teuthology.yaml teuthology/.
else
    echo ".teuthology.yaml doesn't exists"
    exit 1
fi

# Copy Docker file into teuthology
cp ./Dockerfile teuthology/.

cp ./teuthology.sh teuthology/

cp ./custom_conf.yaml teuthology/

# Generate an SSH keypair to use
SSH_PRIVKEY_PATH=$(mktemp -u /tmp/teuthology-ssh-key-XXXXXX)
ssh-keygen -t ed25519 -N '' -f $SSH_PRIVKEY_PATH
export SSH_PRIVKEY=$(cat $SSH_PRIVKEY_PATH)
export SSH_PUBKEY=$(cat $SSH_PRIVKEY_PATH.pub)

trap "docker-compose down" SIGINT
docker-compose up \
    --build \
    --abort-on-container-exit \
    --exit-code-from teuthology
docker-compose down