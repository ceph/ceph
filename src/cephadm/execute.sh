#!/bin/bash

ceph orch apply mon --unmanaged
ceph orch apply mgr --unmanaged
ceph orch host add ceph-node-1 171.254.95.193 --labels=_admin,mgr,mon,osd
ceph orch host add ceph-node-2 171.254.95.156 --labels=_admin,mgr,mon,osd
ceph orch host add ceph-node-3 171.254.93.217 --labels=_admin,mgr,mon,osd
ceph orch apply mon --placement="label:mon count-per-host:1"
ceph orch apply mgr --placement="label:mgr count-per-host:1"
ceph orch apply rgw public '--placement=label:rgw count-per-host:1' --port=8888
ceph orch apply rgw private '--placement=label:rgw count-per-host:1' --port=8889
ceph orch apply -i osd_spec.yml --dry-run
