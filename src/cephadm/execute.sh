#!/bin/bash

ceph orch apply mon --unmanaged
ceph orch apply mgr --unmanaged
ceph orch host add LAPTOP-EJ8ISJEC 172.26.224.220 --labels=_admin,mgr,mon,osd,rgw
ceph orch apply mon --placement="label:mon count-per-host:1"
ceph orch apply mgr --placement="label:mgr count-per-host:1"
ceph orch apply rgw public '--placement=label:rgw count-per-host:1' --port=8888
ceph orch apply rgw private '--placement=label:rgw count-per-host:1' --port=8889
