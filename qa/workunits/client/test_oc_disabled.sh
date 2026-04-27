#!/bin/sh

set -ex

ceph_test_client --client_oc=false
ceph_test_client_fscrypt --client_oc=false
