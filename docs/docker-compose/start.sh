#!/bin/bash
git clone https://github.com/ceph/paddles.git
cd paddles
git fetch origin pull/94/head:wip-amathuria-removing-beanstalkd
git checkout wip-amathuria-removing-beanstalkd
cd ../
git clone https://github.com/ceph/teuthology.git
cd teuthology
git fetch origin pull/1650/head:wip-amathuria-replace-beanstalkd-paddles
git fetch origin pull/94/head:wip-amathuria-removing-beanstalkd
git checkout wip-amathuria-replace-beanstalkd-paddles
cd ..
if [ -f ".teuthology.yaml" ]; 
then
    cp .teuthology.yaml teuthology/.
else
    echo ".teuthology.yaml doesn't exists"
    exit 1
fi

# until the branch we check out above has a Dockerfile of its own
cp ../../Dockerfile teuthology/.
docker-compose up --build
