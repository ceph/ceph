#!/bin/sh -ex

mydir=`dirname $0`
$mydir/test_o_trunc trunc.foo 600

echo OK

