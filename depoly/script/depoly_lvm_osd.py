import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd

data_disks=['sda', 'sdb', 'sdc', 'sdd', 'sde', 'sdf', 'sdh', 'sdi', 'sdj', 'sdk']
meta_disks=['nvme0n1', 'nvme1n1']
cache_disks=[]
devices={"data": data_disks, 'meta':meta_disks, 'cache':cache_disks}

data_disk_len = len(devices['data'])
meta_disk_len = len(devices['meta'])
cache_disk_len = len(devices['cache'])
if meta_disk_len != 0:
    meta_zone_len = data_disk_len // meta_disk_len
if cache_disk_len != 0:
    cache_zone_len = data_disk_len // cache_disk_len

# 清除数据格式
for disk in devices['data']:
    remote_exec_cmd("ceph-volume lvm zap /dev/{disk_name} --destroy".format(disk_name=disk))

for disk in devices['meta']:
    remote_exec_cmd("ceph-volume lvm zap /dev/{disk_name} --destroy".format(disk_name=disk))

# 数据进行分区
for disk in devices['data']:
    remote_exec_cmd("pvcreate /dev/{disk_name}".format(disk_name=disk))

# 元数据进行分区
for disk in devices['meta']:
    remote_exec_cmd("pvcreate /dev/{disk_name}".format(disk_name=disk))

# 检查创建成功
remote_exec_cmd("pvs")

#创建卷组
for i in range(data_disk_len):
    disk=devices['data'][i]
    remote_exec_cmd("vgcreate ceph_vg_block_{disk_name} /dev/{disk_name}".format(disk_name=disk))

osd_id = 0
disk_id = 0
ready_disks = []
for i in range(meta_disk_len):
    disksinfo = {}
    disk=devices['meta'][i]
    wal_capacity = 10
    db_capacity = 200

    remote_exec_cmd("pvcreate /dev/{disk_name}".format(disk_name=disk))
    remote_exec_cmd( "vgcreate ceph_vg_db_{disk_name} /dev/{disk_name}".format(disk_name=disk))
    for index in range(meta_zone_len):
        capacity = 100 / (meta_zone_len - ( index % meta_zone_len))

        # 数据
        block_path = "ceph_vg_block_{disk_name}/lv_{group_name}_{osd_id}".format(
            disk_name=devices["data"][disk_id], group_name=i, osd_id=disk_id)
        remote_exec_cmd("lvcreate -l 100%FREE -n lv_{group_name}_{osd_id} ceph_vg_block_{disk_name}".format(
            disk_name=devices["data"][disk_id], group_name=i, osd_id=disk_id))
        # 日志
        wal_path= "ceph_vg_db_{disk_name}/lv_wal_{group_name}_{osd_id}".format(
            disk_name=disk, group_name=i, osd_id=disk_id)
        remote_exec_cmd("lvcreate -L {capacity}G -n lv_wal_{group_name}_{osd_id} ceph_vg_db_{disk_name}".format(
            capacity=wal_capacity, disk_name=disk, group_name=i, osd_id=disk_id))
        # 元数据
        db_path= "ceph_vg_db_{disk_name}/lv_db_{group_name}_{osd_id}".format(
            disk_name=disk, group_name=i, osd_id=disk_id)
        remote_exec_cmd("lvcreate -L {capacity}G -n lv_db_{group_name}_{osd_id} ceph_vg_db_{disk_name}".format(
            capacity=db_capacity, disk_name=disk, group_name=i, osd_id=disk_id))

        disksinfo['uuid'] = local_exec_cmd("uuidgen")
        disksinfo['osd_id'] = osd_id
        disksinfo['data'] = block_path
        disksinfo['wal']=wal_path
        disksinfo['db']=db_path
        osd_id = osd_id + 1
        disk_id = disk_id + 1
        ready_disks.append(disksinfo.copy())

# 检查创建成功
remote_exec_cmd("vgdisplay")
remote_exec_cmd("lvdisplay")

# 准备 osd磁盘
for disk in ready_disks:
    remote_exec_cmd("ceph-volume lvm prepare --bluestore "
        "--data {data_path} --block.wal {wal_path} --block.db {db_path} --osd_id {osd_id}".format(
            data_path=disk['data'], wal_path=disk['wal'], db_path=disk['db'], osd_id=disk['osd_id']))
    local_exec_cmd("sleep 1")

# 激活 osd磁盘
remote_exec_cmd("ceph-volume lvm activate --bluestore --all")
