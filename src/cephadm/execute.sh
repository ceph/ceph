#!/bin/bash

ceph orch apply mon --unmanaged
ceph orch apply mgr --unmanaged
ceph orch host add ceph-node-1 171.254.95.193 --labels=osd
ceph orch host add ceph-node-2 171.254.95.156 --labels=_admin,mgr,mon,osd
ceph orch host add ceph-node-3 171.254.93.217 --labels=osd
ceph orch apply mon --placement="label:mon count-per-host:1"
