#!/bin/bash -ex

# this is a smoke test, meant to be run against vstart.sh.

host="$(hostname)"

bin/init-ceph stop || true
MON=1 OSD=1 MDS=0 MGR=1 ../src/vstart.sh -d -n -x -l --cephadm

export CEPH_DEV=1

bin/ceph orch ls
bin/ceph orch apply mds foo 1
bin/ceph orch ls | grep foo
while ! bin/ceph orch ps | grep mds.foo ; do sleep 1 ; done
bin/ceph orch ps

bin/ceph orch host ls

bin/ceph orch rm crash
! bin/ceph orch ls | grep crash
bin/ceph orch apply crash '*'
bin/ceph orch ls | grep crash

while ! bin/ceph orch ps | grep crash ; do sleep 1 ; done
bin/ceph orch ps | grep crash.$host | grep running
bin/ceph orch ls | grep crash | grep 1/1
bin/ceph orch daemon rm crash.$host
while ! bin/ceph orch ps | grep crash ; do sleep 1 ; done

bin/ceph orch daemon stop crash.$host
bin/ceph orch daemon start crash.$host
bin/ceph orch daemon restart crash.$host
bin/ceph orch daemon reconfig crash.$host
bin/ceph orch daemon redeploy crash.$host

bin/ceph orch host ls | grep $host
bin/ceph orch host label add $host fooxyz
bin/ceph orch host ls | grep $host | grep fooxyz
bin/ceph orch host label rm $host fooxyz
! bin/ceph orch host ls | grep $host | grep fooxyz
bin/ceph orch host set-addr $host $host

bin/ceph cephadm check-host $host
#! bin/ceph cephadm check-host $host 1.2.3.4
#bin/ceph orch host set-addr $host 1.2.3.4
#! bin/ceph cephadm check-host $host
bin/ceph orch host set-addr $host $host
bin/ceph cephadm check-host $host

bin/ceph orch apply mgr 1
bin/ceph orch rm mgr --force     # we don't want a mgr to take over for ours

bin/ceph orch daemon add mon $host:127.0.0.1

while ! bin/ceph mon dump | grep 'epoch 2' ; do sleep 1 ; done

bin/ceph orch apply rbd-mirror 1

bin/ceph orch apply node-exporter '*'
bin/ceph orch apply prometheus 1
bin/ceph orch apply alertmanager 1
bin/ceph orch apply grafana 1

while ! bin/ceph dashboard get-grafana-api-url | grep $host ; do sleep 1 ; done

bin/ceph orch apply rgw myrealm myzone 1

bin/ceph orch ps
bin/ceph orch ls

# clean up
bin/ceph orch rm mds.foo
bin/ceph orch rm rgw.myrealm.myzone
bin/ceph orch rm rbd-mirror
bin/ceph orch rm node-exporter
bin/ceph orch rm alertmanager
bin/ceph orch rm grafana
bin/ceph orch rm prometheus
bin/ceph orch rm crash

bin/ceph mon rm $host
! bin/ceph orch daemon rm mon.$host
bin/ceph orch daemon rm mon.$host --force

echo OK
