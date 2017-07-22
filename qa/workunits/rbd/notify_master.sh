#!/bin/sh -ex

relpath=$(dirname $0)/../../../src/test/librbd
python $relpath/test_notify.py master
exit 0
