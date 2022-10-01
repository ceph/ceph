#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/librbd
python3 $relpath/test_notify.py slave
exit 0
