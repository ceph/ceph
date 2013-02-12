#!/bin/bash 

wget -q http://ceph.com/qa/rbd_cli_tests.pl
wget -q http://ceph.com/qa/RbdLib.pm
sudo perl rbd_cli_tests.pl --pool test
exit 0

