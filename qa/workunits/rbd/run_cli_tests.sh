#!/bin/bash -ex

wget -q http://download.ceph.com/qa/rbd_cli_tests.pls
wget -q http://download.ceph.com/qa/RbdLib.pm
perl rbd_cli_tests.pls --pool test
