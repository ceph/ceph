#!/bin/bash

trap "rm -f teuthology-integration.pem ; openstack keypair delete teuthology-integration ; openstack server delete teuthology-integration" EXIT

openstack keypair create teuthology-integration > teuthology-integration.pem
chmod 600 teuthology-integration.pem
teuthology-openstack --name teuthology-integration --key-filename teuthology-integration.pem --key-name teuthology-integration --suite teuthology/integration --wait --teardown --upload
