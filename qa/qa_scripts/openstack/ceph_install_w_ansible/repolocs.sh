#! /usr/bin/env bash
SPECIFIC_VERSION=latest-Ceph-2-RHEL-7 
#SPECIFIC_VERSION=Ceph-2-RHEL-7-20160630.t.0
#SPECIFIC_VERSION=Ceph-2.0-RHEL-7-20160718.t.0
export CEPH_REPO_TOOLS=http://download.eng.bos.redhat.com/rcm-guest/ceph-drops/auto/ceph-2-rhel-7-compose/${SPECIFIC_VERSION}/compose/Tools/x86_64/os/
export CEPH_REPO_MON=http://download.eng.bos.redhat.com/rcm-guest/ceph-drops/auto/ceph-2-rhel-7-compose/${SPECIFIC_VERSION}/compose/MON/x86_64/os/
export CEPH_REPO_OSD=http://download.eng.bos.redhat.com/rcm-guest/ceph-drops/auto/ceph-2-rhel-7-compose/${SPECIFIC_VERSION}/compose/OSD/x86_64/os/
export INSTALLER_REPO_LOC=http://download.eng.bos.redhat.com/rcm-guest/ceph-drops/auto/rhscon-2-rhel-7-compose/latest-RHSCON-2-RHEL-7/compose/Installer/x86_64/os/
