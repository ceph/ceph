#!/bin/bash -x

set -e

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

echo note: assuming mon.a is on the current host

# can set to 'sudo ./ceph' to execute tests from current dir for development
CEPH=${CEPH:-'sudo ceph'}

${CEPH} daemon mon.a version | grep version

# get debug_ms setting and strip it, painfully for reuse
old_ms=$(${CEPH} daemon mon.a config get debug_ms | \
	grep debug_ms | sed -e 's/.*: //' -e 's/["\}\\]//g')
${CEPH} daemon mon.a config set debug_ms 13
new_ms=$(${CEPH} daemon mon.a config get debug_ms | \
	grep debug_ms | sed -e 's/.*: //' -e 's/["\}\\]//g')
[ "$new_ms" = "13/13" ]
${CEPH} daemon mon.a config set debug_ms $old_ms
new_ms=$(${CEPH} daemon mon.a config get debug_ms | \
	grep debug_ms | sed -e 's/.*: //' -e 's/["\}\\]//g')
[ "$new_ms" = "$old_ms" ]

# unregistered/non-existent command
expect_false ${CEPH} daemon mon.a bogus_command_blah foo

set +e
OUTPUT=$(${CEPH} -c /not/a/ceph.conf daemon mon.a help 2>&1)
# look for EINVAL
if [ $? != 22 ] ; then exit 1; fi
if ! echo "$OUTPUT" | grep -q '.*open.*/not/a/ceph.conf'; then 
	echo "didn't find expected error in bad conf search"
	exit 1
fi
set -e

echo OK
