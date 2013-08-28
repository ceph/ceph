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

echo OK
