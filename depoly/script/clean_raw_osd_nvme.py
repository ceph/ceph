import os
from depoly_tools import remote_exec_cmd
from depoly_tools import local_exec_cmd

target=remote_exec_cmd("hostname")
nvmes=('nvme0n1', 'nvme1n1')
devices={'nvmes':nvmes}

for i in range( len(devices['nvmes']) ):
    remote_exec_cmd("ceph osd down osd.{id}".format(osd_id=i) )
    remote_exec_cmd("ceph osd out osd.{id}".format(osd_id=i) )
    remote_exec_cmd("ceph osd crush remove osd.{id}".format(osd_id=i) )
    remote_exec_cmd("ceph auth del osd.{id}".format(osd_id=i) )
    remote_exec_cmd("ceph osd rm osd.{id}".format(osd_id=i) )
    remote_exec_cmd("systemctl stop ceph-osd@{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 1")
    remote_exec_cmd("systemctl disable ceph-osd@{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 2")

for i in range( len(devices['nvmes']) ):
    remote_exec_cmd("rm -rf /var/lib/ceph/osd/ceph-{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 10")

for i in range( len(devices['nvmes']) ):
    remote_exec_cmd( "/usr/bin/dd if=/dev/zero of=/dev/{disk_name} bs=1M count=10 conv=fsync".format(
        disk_name=devices['nvmes'][i]) )
    local_exec_cmd("sleep 10")

