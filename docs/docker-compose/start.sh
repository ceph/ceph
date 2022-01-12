#!/bin/bash
# Clone teuthology
git clone https://github.com/ceph/teuthology.git

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

# Generate an SSH keypair to use
SSH_PRIVKEY_PATH=$(mktemp -u /tmp/teuthology-ssh-key-XXXXXX)
ssh-keygen -t ed25519 -N '' -f $SSH_PRIVKEY_PATH
export SSH_PRIVKEY=$(cat $SSH_PRIVKEY_PATH)
export SSH_PUBKEY=$(cat $SSH_PRIVKEY_PATH.pub)

docker-compose up --build
