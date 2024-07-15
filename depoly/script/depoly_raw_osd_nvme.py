import os
from depoly_tools import remote_exec_cmd
from depoly_tools import local_exec_cmd

target=remote_exec_cmd("hostname")
nvmes=('nvme0n1', 'nvme1n1')

devices={'nvmes':nvmes}
cluster="ceph"

# 清理磁盘 osd磁盘
for i in range( len(devices['nvmes']) ):
    remote_exec_cmd( "/usr/bin/dd if=/dev/zero of=/dev/{disk_name} bs=1M count=10 conv=fsync".format(
        disk_name=devices['nvmes'][i]) )
'''
# 进行分区
for i in range( len(devices['nvmes']) ):
    remote_exec_cmd( "parted /dev/{disk_name} --script mktable gpt".format(disk_name=devices['nvmes'][i]) )
    local_exec_cmd( "sleep 1")

disk_id = 0
ready_disks=[]
for i in range( len(devices['nvmes']) ):
    # 进行db分区
    db_internal = 200
    wal_internal = 10
    offt_start = 0
    offt_end = 0
    disk = {}
    lv = 0
    lv = lv + 1
    offt_start = offt_end;
    offt_end = offt_start + wal_internal;
     # 进行 wal 分区
    print (lv)
    remote_exec_cmd( "parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
        disk_name=devices['nvmes'][i], start=offt_start, end=offt_end))
    disk['wal'] = "/dev/{disk_name}p{lv}".format(disk_name=devices['nvmes'][i], lv=lv)

    # 进行 db 分区
    lv = lv + 1
    offt_start = offt_end;
    offt_end = offt_start + db_internal;
    print (lv)
    remote_exec_cmd( "parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
        disk_name=devices['nvmes'][i], start=offt_start, end=offt_end))
    disk['db'] = "/dev/{disk_name}p{lv}".format(disk_name=devices['nvmes'][i], lv=lv)
    local_exec_cmd( "sleep 1")

    # 数据分区
    lv = lv + 1
    offt_start = offt_end;
    offt_end = 4000
    print (lv)
    remote_exec_cmd( "parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
        disk_name=devices['nvmes'][i], start=offt_start, end=offt_end))
    disk['data'] = "/dev/{disk_name}p{lv}".format(disk_name=devices['nvmes'][i], lv=lv)
    ready_disks.append(disk.copy())

# 检查创建成功
local_exec_cmd( "sleep 1")
remote_exec_cmd("parted -l")

for i in range( len(devices['nvmes']) ):
    print (i)
    remote_exec_cmd("rm -rf /var/lib/ceph/osd/ceph-{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 1")
    remote_exec_cmd("/bin/bash -c ulimit -n 32768;ceph-volume raw prepare --bluestore "
        "--data {data_path} "
        "--block.wal {wal_path} "
        "--block.db {db_path} "
        "--no-tmpfs "
        "--osd_id {osd_id}".format(
            data_path=ready_disks[i]['data'],
            wal_path=ready_disks[i]['wal'],
            db_path=ready_disks[i]['db'],
            osd_id=i) )
    local_exec_cmd("sleep 10")

# 激活 osd磁盘
# ceph-volume lvm activate -h
for i in range( len(devices['nvmes']) ):
    print (i)
    remote_exec_cmd("/bin/bash -c ulimit -n 32768;ceph-volume raw activate "
        "--device {data_path} "
        "--block.wal {wal_path} "
        "--block.db {db_path} "
        "--no-tmpfs --no-systemd".format(
            data_path=ready_disks[i]['data'],
            wal_path=ready_disks[i]['wal'],
            db_path=ready_disks[i]['db']) )
    local_exec_cmd("sleep 10")
'''

for i in range( len(devices['nvmes']) ):
    print (i)
    remote_exec_cmd("rm -rf /var/lib/ceph/osd/ceph-{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 1")
    remote_exec_cmd("/bin/bash -c ulimit -n 32768;ceph-volume raw prepare --bluestore "
        "--data /dev/{data_path} "
        "--no-tmpfs "
        "--osd_id {osd_id}".format(
        data_path=devices['nvmes'][i],
        osd_id=i) )
    local_exec_cmd("sleep 10")

for i in range( len(devices['nvmes']) ):
    print (i)
    remote_exec_cmd("/bin/bash -c ulimit -n 32768;ceph-volume raw activate "
        "--device /dev/{data_path} "
        "--no-tmpfs --no-systemd".format(
        data_path=devices['nvmes'][i]) )
    local_exec_cmd("sleep 10")

for i in range( len(devices['nvmes']) ):
    remote_exec_cmd("systemctl enable ceph-osd@{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 1")
    remote_exec_cmd("systemctl start ceph-osd@{osd_id}".format(osd_id=i) )
    local_exec_cmd("sleep 2")
