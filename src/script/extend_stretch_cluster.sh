#!/usr/bin/env bash

set -ex

../src/script/add_osd.sh 4 'host=host1-1 datacenter=site1 root=default'
../src/script/add_osd.sh 5 'host=host1-2 datacenter=site1 root=default'
../src/script/add_osd.sh 6 'host=host2-1 datacenter=site2 root=default'
../src/script/add_osd.sh 7 'host=host2-2 datacenter=site2 root=default'
