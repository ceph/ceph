#!/bin/bash -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

ceph config dump

# value validation
ceph config set mon.a debug_asok 22
ceph config set mon.a debug_asok 22/33
ceph config get mon.a debug_asok | grep 22
ceph config set mon.a debug_asok 1/2
expect_false ceph config set mon.a debug_asok foo
expect_false ceph config set mon.a debug_asok -10
ceph config rm mon.a debug_asok

ceph config set global log_graylog_port 123
expect_false ceph config set global log_graylog_port asdf
ceph config rm global log_graylog_port

ceph config set mon mon_cluster_log_to_stderr true
ceph config get mon.a mon_cluster_log_to_stderr | grep true
ceph config set mon mon_cluster_log_to_stderr 2
ceph config get mon.a mon_cluster_log_to_stderr | grep true
ceph config set mon mon_cluster_log_to_stderr 1
ceph config get mon.a mon_cluster_log_to_stderr | grep true
ceph config set mon mon_cluster_log_to_stderr false
ceph config get mon.a mon_cluster_log_to_stderr | grep false
ceph config set mon mon_cluster_log_to_stderr 0
ceph config get mon.a mon_cluster_log_to_stderr | grep false
expect_false ceph config set mon mon_cluster_log_to_stderr fiddle
expect_false ceph config set mon mon_cluster_log_to_stderr ''
ceph config rm mon mon_cluster_log_to_stderr

expect_false ceph config set mon.a osd_pool_default_type foo
ceph config set mon.a osd_pool_default_type replicated
ceph config rm mon.a osd_pool_default_type

# scoping
ceph config set global debug_asok 33
ceph config get mon.a debug_asok | grep 33
ceph config set mon debug_asok 11
ceph config get mon.a debug_asok | grep 11
ceph config set mon.a debug_asok 22
ceph config get mon.a debug_asok | grep 22
ceph config rm mon.a debug_asok
ceph config get mon.a debug_asok | grep 11
ceph config rm mon debug_asok
ceph config get mon.a debug_asok | grep 33
ceph config rm global debug_asok

# help
ceph config help debug_asok | grep debug_asok

# show
ceph config set osd.0 debug_asok 33
while ! ceph config show osd.0 | grep debug_asok | grep 33 | grep mon
do
    sleep 1
done
ceph config set osd.0 debug_asok 22
while ! ceph config show osd.0 | grep debug_asok | grep 22 | grep mon
do
    sleep 1
done

ceph tell osd.0 config set debug_asok 99
while ! ceph config show osd.0 | grep debug_asok | grep 99
do
    sleep 1
done
ceph config show osd.0 | grep debug_asok | grep 'override mon'
ceph tell osd.0 config unset debug_asok
ceph tell osd.0 config unset debug_asok

ceph config rm osd.0 debug_asok
while ceph config show osd.0 | grep debug_asok | grep mon
do
    sleep 1
done
ceph config show osd.0 | grep -c debug_asok | grep 0

ceph config set osd.0 osd_data testorama
while ! ceph config show osd.0 | grep osd_data | grep mon
do
    sleep 1
done
ceph config rm osd.0 osd_data

# show-with-defaults
ceph config show-with-defaults osd.0 | grep debug_asok

# assimilate
t1=`mktemp`
t2=`mktemp`
cat <<EOF > $t1
[osd.0]
keyring = foo
debug_asok = 66
EOF
ceph config assimilate-conf -i $t1 | tee $t2

grep keyring $t2
expect_false grep debug_asok $t2
rm -f $t1 $t2

expect_false ceph config reset
expect_false ceph config reset -1
# we are at end of testing, so it's okay to revert everything
ceph config reset 0

echo OK
