#!/bin/bash

tmp=/tmp/cephtest-mon-caps-madness

exit_on_error=1

[[ ! -z $TEST_EXIT_ON_ERROR ]] && exit_on_error=$TEST_EXIT_ON_ERROR

expect()
{
  cmd=$1
  expected_ret=$2

  echo $cmd
  eval $cmd >&/dev/null
  ret=$?

  if [[ $ret -ne $expected_ret ]]; then
    echo "Error: Expected return $expected_ret, got $ret"
    [[ $exit_on_error -eq 1 ]] && exit 1
    return 1
  fi

  return 0
}

expect "ceph auth get-or-create client.bazar > $tmp.bazar.keyring" 0
expect "ceph -k $tmp.bazar.keyring --user bazar mon_status" 13
ceph auth del client.bazar

c="'allow command \"auth list\", allow command mon_status'"
expect "ceph auth get-or-create client.foo mon $c > $tmp.foo.keyring" 0
expect "ceph -k $tmp.foo.keyring --user foo mon_status" 0
expect "ceph -k $tmp.foo.keyring --user foo auth list" 0
expect "ceph -k $tmp.foo.keyring --user foo auth export" 13
expect "ceph -k $tmp.foo.keyring --user foo auth del client.bazar" 13
expect "ceph -k $tmp.foo.keyring --user foo osd dump" 13
expect "ceph -k $tmp.foo.keyring --user foo pg dump" 13
expect "ceph -k $tmp.foo.keyring --user foo quorum_status" 13
ceph auth del client.foo

c="'allow command service with prefix=list, allow command mon_status'"
expect "ceph auth get-or-create client.bar mon $c > $tmp.bar.keyring" 0
expect "ceph -k $tmp.bar.keyring --user bar mon_status" 0
expect "ceph -k $tmp.bar.keyring --user bar auth list" 13
expect "ceph -k $tmp.bar.keyring --user bar auth export" 13
expect "ceph -k $tmp.bar.keyring --user bar auth del client.foo" 13
expect "ceph -k $tmp.bar.keyring --user bar osd dump" 13
expect "ceph -k $tmp.bar.keyring --user bar pg dump" 13
expect "ceph -k $tmp.bar.keyring --user bar quorum_status" 13
ceph auth del client.bar

rm $tmp.bazar.keyring $tmp.foo.keyring $tmp.bar.keyring

echo OK