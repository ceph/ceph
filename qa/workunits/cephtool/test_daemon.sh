#!/bin/bash -x

set -e

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

echo note: assuming mon.a is on the currenet host

sudo ceph daemon mon.a version | grep version

# get debug_ms setting and strip it, painfully for reuse
old_ms=$(sudo ceph daemon mon.a config get debug_ms | grep debug_ms | \
	sed -e 's/.*: //' -e 's/["\}\\]//g')
sudo ceph daemon mon.a config set debug_ms 13
new_ms=$(sudo ceph daemon mon.a config get debug_ms | grep debug_ms | \
	sed -e 's/.*: //' -e 's/["\}\\]//g')
[ "$new_ms" = "13/13" ]
sudo ceph daemon mon.a config set debug_ms $old_ms
new_ms=$(sudo ceph daemon mon.a config get debug_ms | grep debug_ms | \
	sed -e 's/.*: //' -e 's/["\}\\]//g')
[ "$new_ms" = "$old_ms" ]

# unregistered/non-existent command
expect_false sudo ceph daemon mon.a bogus_command_blah foo

echo OK
