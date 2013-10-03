#!/bin/sh -ex

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

pool="pool-$$"
rados mkpool $pool

rados -p $pool tmap set foo key1 value1
rados -p $pool tmap set foo key2 value2
rados -p $pool tmap set foo key2 value2
rados -p $pool tmap dump foo | grep key1
rados -p $pool tmap dump foo | grep key2
rados -p $pool tmap-to-omap foo
expect_false rados -p $pool tmap dump foo
expect_false rados -p $pool tmap dump foo

rados -p $pool listomapkeys foo | grep key1
rados -p $pool listomapkeys foo | grep key2
rados -p $pool getomapval foo key1 | grep value1
rados -p $pool getomapval foo key2 | grep value2

rados rmpool $pool $pool --yes-i-really-really-mean-it

echo OK
