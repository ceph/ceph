#!/bin/bash
git clone https://github.com/ceph/paddles.git
cd paddles
git fetch origin pull/94/head:wip-amathuria-removing-beanstalkd
git checkout wip-amathuria-removing-beanstalkd
cd ../
git clone https://github.com/ceph/pulpito.git
git clone https://github.com/ceph/teuthology.git
cd teuthology
git fetch origin pull/1650/head:wip-amathuria-replace-beanstalkd-paddles
git checkout wip-amathuria-replace-beanstalkd-paddles
docker-compose up