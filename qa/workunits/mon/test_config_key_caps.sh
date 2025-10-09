#!/usr/bin/env bash

set -x
set -e

tmp=$(mktemp -d -p /tmp test_mon_config_key_caps.XXXXX)
entities=()

function cleanup()
{
	set +e
	set +x
	if [[ -e $tmp/keyring ]] && [[ -e $tmp/keyring.orig ]]; then
		grep '\[.*\..*\]' $tmp/keyring.orig > $tmp/entities.orig
		for e in $(grep '\[.*\..*\]' $tmp/keyring | \
			diff $tmp/entities.orig - | \
			sed -n 's/^.*\[\(.*\..*\)\]/\1/p');
		do
			ceph auth rm $e 2>&1 >& /dev/null
		done
	fi
	#rm -fr $tmp
}

trap cleanup 0 # cleanup on exit

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

# for cleanup purposes
ceph auth export -o $tmp/keyring.orig

k=$tmp/keyring

# setup a few keys
ceph config-key ls
ceph config-key set daemon-private/osd.123/test-foo
ceph config-key set mgr/test-foo
ceph config-key set device/test-foo
ceph config-key set test/foo

allow_aa=client.allow_aa
allow_bb=client.allow_bb
allow_cc=client.allow_cc

mgr_a=mgr.a
mgr_b=mgr.b
osd_a=osd.100
osd_b=osd.200

prefix_aa=client.prefix_aa
prefix_bb=client.prefix_bb
prefix_cc=client.prefix_cc
match_aa=client.match_aa
match_bb=client.match_bb

fail_aa=client.fail_aa
fail_bb=client.fail_bb
fail_cc=client.fail_cc
fail_dd=client.fail_dd
fail_ee=client.fail_ee
fail_ff=client.fail_ff
fail_gg=client.fail_gg
fail_writes=client.fail_writes

ceph auth get-or-create $allow_aa mon 'allow *'
ceph auth get-or-create $allow_bb mon 'allow service config-key rwx'
ceph auth get-or-create $allow_cc mon 'allow command "config-key get"'

ceph auth get-or-create $mgr_a mon 'allow profile mgr'
ceph auth get-or-create $mgr_b mon 'allow profile mgr'
ceph auth get-or-create $osd_a mon 'allow profile osd'
ceph auth get-or-create $osd_b mon 'allow profile osd'

ceph auth get-or-create $prefix_aa mon \
	"allow command \"config-key get\" with key prefix client/$prefix_aa"

cap="allow command \"config-key set\" with key prefix client/"
cap="$cap,allow command \"config-key get\" with key prefix client/$prefix_bb"
ceph auth get-or-create $prefix_bb mon "$cap"

cap="allow command \"config-key get\" with key prefix client/"
cap="$cap, allow command \"config-key set\" with key prefix client/"
cap="$cap, allow command \"config-key ls\""
ceph auth get-or-create $prefix_cc mon "$cap"

cap="allow command \"config-key get\" with key=client/$match_aa/foo"
ceph auth get-or-create $match_aa mon "$cap"
cap="allow command \"config-key get\" with key=client/$match_bb/foo"
cap="$cap,allow command \"config-key set\" with key=client/$match_bb/foo"
ceph auth get-or-create $match_bb mon "$cap"

ceph auth get-or-create $fail_aa mon 'allow rx'
ceph auth get-or-create $fail_bb mon 'allow r,allow w'
ceph auth get-or-create $fail_cc mon 'allow rw'
ceph auth get-or-create $fail_dd mon 'allow rwx'
ceph auth get-or-create $fail_ee mon 'allow profile bootstrap-rgw'
ceph auth get-or-create $fail_ff mon 'allow profile bootstrap-rbd'
# write commands will require rw; wx is not enough
ceph auth get-or-create $fail_gg mon 'allow service config-key wx'
# read commands will only require 'r'; 'rx' should be enough.
ceph auth get-or-create $fail_writes mon 'allow service config-key rx'

# grab keyring
ceph auth export -o $k

# keys will all the caps can do whatever
for c in $allow_aa $allow_bb $allow_cc $mgr_a $mgr_b; do
	ceph -k $k --name $c config-key get daemon-private/osd.123/test-foo
	ceph -k $k --name $c config-key get mgr/test-foo
	ceph -k $k --name $c config-key get device/test-foo
	ceph -k $k --name $c config-key get test/foo
done

for c in $osd_a $osd_b; do
	ceph -k $k --name $c config-key put daemon-private/$c/test-foo
	ceph -k $k --name $c config-key get daemon-private/$c/test-foo
	expect_false ceph -k $k --name $c config-key ls
	expect_false ceph -k $k --name $c config-key get mgr/test-foo
	expect_false ceph -k $k --name $c config-key get device/test-foo
	expect_false ceph -k $k --name $c config-key get test/foo
done

expect_false ceph -k $k --name $osd_a get daemon-private/$osd_b/test-foo
expect_false ceph -k $k --name $osd_b get daemon-private/$osd_a/test-foo

expect_false ceph -k $k --name $prefix_aa \
	config-key ls
expect_false ceph -k $k --name $prefix_aa \
	config-key get daemon-private/osd.123/test-foo
expect_false ceph -k $k --name $prefix_aa \
	config-key set test/bar
expect_false ceph -k $k --name $prefix_aa \
	config-key set client/$prefix_aa/foo

# write something so we can read, use a custom entity
ceph -k $k --name $allow_bb config-key set client/$prefix_aa/foo
ceph -k $k --name $prefix_aa config-key get client/$prefix_aa/foo
# check one writes to the other's prefix, the other is able to read
ceph -k $k --name $prefix_bb config-key set client/$prefix_aa/bar
ceph -k $k --name $prefix_aa config-key get client/$prefix_aa/bar

ceph -k $k --name $prefix_bb config-key set client/$prefix_bb/foo
ceph -k $k --name $prefix_bb config-key get client/$prefix_bb/foo

expect_false ceph -k $k --name $prefix_bb config-key get client/$prefix_aa/bar
expect_false ceph -k $k --name $prefix_bb config-key ls
expect_false ceph -k $k --name $prefix_bb \
	config-key get daemon-private/osd.123/test-foo
expect_false ceph -k $k --name $prefix_bb config-key get mgr/test-foo
expect_false ceph -k $k --name $prefix_bb config-key get device/test-foo
expect_false ceph -k $k --name $prefix_bb config-key get test/bar
expect_false ceph -k $k --name $prefix_bb config-key set test/bar

ceph -k $k --name $prefix_cc config-key set client/$match_aa/foo
ceph -k $k --name $prefix_cc config-key set client/$match_bb/foo
ceph -k $k --name $prefix_cc config-key get client/$match_aa/foo
ceph -k $k --name $prefix_cc config-key get client/$match_bb/foo
expect_false ceph -k $k --name $prefix_cc config-key set other/prefix
expect_false ceph -k $k --name $prefix_cc config-key get mgr/test-foo
ceph -k $k --name $prefix_cc config-key ls >& /dev/null

ceph -k $k --name $match_aa config-key get client/$match_aa/foo
expect_false ceph -k $k --name $match_aa config-key get client/$match_bb/foo
expect_false ceph -k $k --name $match_aa config-key set client/$match_aa/foo
ceph -k $k --name $match_bb config-key get client/$match_bb/foo
ceph -k $k --name $match_bb config-key set client/$match_bb/foo
expect_false ceph -k $k --name $match_bb config-key get client/$match_aa/foo
expect_false ceph -k $k --name $match_bb config-key set client/$match_aa/foo

keys=(daemon-private/osd.123/test-foo
	  mgr/test-foo
	  device/test-foo
	  test/foo
	  client/$prefix_aa/foo
	  client/$prefix_bb/foo
	  client/$match_aa/foo
	  client/$match_bb/foo
)
# expect these all to fail accessing config-key
for c in $fail_aa $fail_bb $fail_cc \
		 $fail_dd $fail_ee $fail_ff \
		 $fail_gg; do
	for m in get set; do
		for key in ${keys[*]} client/$prefix_aa/foo client/$prefix_bb/foo; do
			expect_false ceph -k $k --name $c config-key $m $key
		done
	done
done

# fail writes but succeed on reads
expect_false ceph -k $k --name $fail_writes config-key set client/$match_aa/foo
expect_false ceph -k $k --name $fail_writes config-key set test/foo
ceph -k $k --name $fail_writes config-key ls
ceph -k $k --name $fail_writes config-key get client/$match_aa/foo 
ceph -k $k --name $fail_writes config-key get daemon-private/osd.123/test-foo

echo "OK"
