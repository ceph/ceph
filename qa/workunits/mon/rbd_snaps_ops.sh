#!/bin/bash

# attempt to trigger #6047


cmd_no=0
expect()
{
  cmd_no=$(($cmd_no+1))
  cmd="$1"
  expected=$2
  echo "[$cmd_no] $cmd"
  eval  $cmd
  ret=$?
  if [[ $ret -ne $expected ]]; then
    echo "[$cmd_no] unexpected return '$ret', expected '$expected'"
    exit 1
  fi
}

expect 'ceph osd pool create test 256 256' 0
expect 'ceph osd pool mksnap test snapshot' 0
expect 'ceph osd pool rmsnap test snapshot' 0

expect 'rbd --pool=test create --size=102400 image' 0
expect 'rbd --pool=test snap create image@snapshot' 22

expect 'ceph osd pool delete test test --yes-i-really-really-mean-it' 0
expect 'ceph osd pool create test 256 256' 0
expect 'rbd --pool=test create --size=102400 image' 0
expect 'rbd --pool=test snap create image@snapshot' 0
expect 'rbd --pool=test snap ls image' 0
expect 'rbd --pool=test snap rm image@snapshot' 0

expect 'ceph osd pool mksnap test snapshot' 22

expect 'ceph osd pool delete test test --yes-i-really-really-mean-it' 0

# reproduce 7210 and expect it to be fixed
# basically create such a scenario where we end up deleting what used to
# be an unmanaged snapshot from a not-unmanaged pool

expect 'rados mkpool test-foo' 0
expect 'rbd --pool test-foo create --size 1024 image' 0
expect 'rbd --pool test-foo snap create image@snapshot' 0
expect 'rados mkpool test-bar' 0
expect 'rados cppool test-foo test-bar' 0
expect 'rbd --pool test-bar snap rm image@snapshot' 95
expect 'ceph osd pool delete test-foo test-foo --yes-i-really-really-mean-it' 0
expect 'ceph osd pool delete test-bar test-bar --yes-i-really-really-mean-it' 0


echo OK
