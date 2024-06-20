#!/bin/sh -ex

ceph_test_objectcacher_misc --flush-test > /dev/null 2>&1

echo OK
