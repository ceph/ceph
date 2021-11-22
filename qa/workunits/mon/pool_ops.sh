#!/usr/bin/env bash

set -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

function get_config_value_or_die()
{
  local pool_name config_opt raw val

  pool_name=$1
  config_opt=$2

  raw="`$SUDO ceph osd pool get $pool_name $config_opt 2>/dev/null`"
  if [[ $? -ne 0 ]]; then
    echo "error obtaining config opt '$config_opt' from '$pool_name': $raw"
    exit 1
  fi

  raw=`echo $raw | sed -e 's/[{} "]//g'`
  val=`echo $raw | cut -f2 -d:`

  echo "$val"
  return 0
}

function expect_config_value()
{
  local pool_name config_opt expected_val val
  pool_name=$1
  config_opt=$2
  expected_val=$3

  val=$(get_config_value_or_die $pool_name $config_opt)

  if [[ "$val" != "$expected_val" ]]; then
    echo "expected '$expected_val', got '$val'"
    exit 1
  fi
}

# pg_num min/max
TEST_POOL=testpool1234
ceph osd pool create testpool1234 8 --autoscale-mode off
ceph osd pool set $TEST_POOL pg_num_min 2
ceph osd pool get $TEST_POOL pg_num_min | grep 2
ceph osd pool set $TEST_POOL pg_num_max 33
ceph osd pool get $TEST_POOL pg_num_max | grep 33
expect_false ceph osd pool set $TEST_POOL pg_num_min 9
expect_false ceph osd pool set $TEST_POOL pg_num_max 7
expect_false ceph osd pool set $TEST_POOL pg_num 1
expect_false ceph osd pool set $TEST_POOL pg_num 44
ceph osd pool set $TEST_POOL pg_num_min 0
expect_false ceph osd pool get $TEST_POOL pg_num_min
ceph osd pool set $TEST_POOL pg_num_max 0
expect_false ceph osd pool get $TEST_POOL pg_num_max
ceph osd pool delete $TEST_POOL $TEST_POOL --yes-i-really-really-mean-it

# note: we need to pass the other args or ceph_argparse.py will take
# 'invalid' that is not replicated|erasure and assume it is the next
# argument, which is a string.
expect_false ceph osd pool create foo 123 123 invalid foo-profile foo-ruleset

ceph osd pool create foo 123 123 replicated
ceph osd pool create fooo 123 123 erasure default
ceph osd pool create foooo 123

ceph osd pool create foo 123 # idempotent

ceph osd pool set foo size 1 --yes-i-really-mean-it
expect_config_value "foo" "min_size" 1
ceph osd pool set foo size 4
expect_config_value "foo" "min_size" 2
ceph osd pool set foo size 10
expect_config_value "foo" "min_size" 5
expect_false ceph osd pool set foo size 0
expect_false ceph osd pool set foo size 20

ceph osd pool set foo size 3
ceph osd getcrushmap -o crush
crushtool -d crush -o crush.txt
sed -i 's/max_size 10/max_size 3/' crush.txt
crushtool -c crush.txt -o crush.new
ceph osd setcrushmap -i crush.new
expect_false ceph osd pool set foo size 4
ceph osd setcrushmap -i crush
rm -f crush crush.txt crush.new

# should fail due to safety interlock
expect_false ceph osd pool delete foo
expect_false ceph osd pool delete foo foo
expect_false ceph osd pool delete foo foo --force
expect_false ceph osd pool delete foo fooo --yes-i-really-mean-it
expect_false ceph osd pool delete foo --yes-i-really-mean-it foo

ceph osd pool delete foooo foooo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete foo foo --yes-i-really-really-mean-it

# idempotent
ceph osd pool delete foo foo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it

# non-existent pool
ceph osd pool delete fuggg fuggg --yes-i-really-really-mean-it

echo OK


