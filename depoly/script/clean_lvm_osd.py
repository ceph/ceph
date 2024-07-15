import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd

target=exec_cmd("hostname")
data=('sda', 'sdb', 'sdc', 'sdd', 'sde', 'sdf', 'sdh', 'sdi', 'sdj', 'sdk')
meta=('nvme0n1', 'nvme1n1')

devices={"data": hdds, 'meta':nvmes}

# 清除磁盘格式
for disk in devices['data']:
    remote_exec_cmd("ceph-volume lvm zap /dev/{disk_name} --destroy".format(disk_name=disk))

for disk in devices['meta']:
    remote_exec_cmd("ceph-volume lvm zap /dev/{disk_name} --destroy".format(disk_name=disk))

# 元数据进行分区
for disk in devices['data']:
    remote_exec_cmd("pvcreate /dev/{disk_name}".format(disk_name=disk))

for disk in devices['meta']:
    remote_exec_cmd("pvcreate /dev/{disk_name}".format(disk_name=disk))

# 检查创建成功
remote_exec_cmd("pvs")

data_disk_len = len(devices['data'])
meta_disk_len =  len(devices['meta'])
meta_zone_len = data_disk_len / meta_disk_len

#创建卷组
for i in range( len(data_disk_len):
    disk=devices['data'][i]
    remote_exec_cmd("vgcreate ceph_vg_block_{disk_name} /dev/{disk_name}".format(disk_name=disk))

for i in range( len(meta_disk_len):
    disk=devices['meta'][i]
    remote_exec_cmd( "vgcreate ceph_vg_db_{disk_name} /dev/{disk_name}".format(disk_name=disk) )
# 检查创建成功
remote_exec_cmd("vgdisplay")

ready_disks=[]
# 创建逻辑卷
osd_id = 0
disk_id = 0
for i in range(meta_disk_len):
    disksinfo={}
    disk = devices['meta']
    for index in meta_zone_len:
        capacity = 100 / (meta_zone_len - (index % meta_zone_len) )

        block_path = "ceph_vg_block_{disk_name}/lv_{group_name}_{osd_id}".format(
            disk_name=devices['data'][disk_id], group_name=i, osd_id=osd_id)
        remote_exec_cmd("lvcreate -l 100%FREE -n lv_{group_name}_{osd_id} ceph_vg_block_{disk_name}".format(
            disk_name=devices['data'][disk_id], group_name=i, osd_id=osd_id))

        wal_path= "ceph_vg_db_{disk_name}/lv_wal_{group_name}_{osd_id}".format(
            disk_name=disk, group_name=i, osd_id=osd_id)
        remote_exec_cmd("lvcreate -L 20G -n lv_wal_{group_name}_{osd_id} ceph_vg_db_{disk_name}".format(
            disk_capacity=capacity, disk_name=disk, group_name=i, osd_id=osd_id))

        db_path= "ceph_vg_db_{disk_name}/lv_db_{group_name}_{osd_id}".format(
            disk_name=disk_nvme, group_name=i, osd_id=osd_id)
        remote_exec_cmd("lvcreate -L 300G -n lv_db_{group_name}_{osd_id} ceph_vg_db_{disk_name}".format(
            disk_capacity=capacity, disk_name=disk, group_name=i, osd_id=osd_id))

    # 记录
    disksinfo['uuid'] = local_exec_cmd("uuidgen")
    disksinfo['osd_id'] = osd_id
    disksinfo['data'] = block_path
    disksinfo['wal']=wal_path
    disksinfo['db']=db_path
    osd_id = osd_id + 1
    disk_id = disk_id + 1
    ready_disks.append(disksinfo)

# 检查创建成功
exec_cmd("lvdisplay")

#exec_cmd("rm -rf /usr/lib64/ceph/")
# 准备 osd磁盘
for disk in ready_disks:
    remote_exec_cmd("ceph-volume lvm prepare --bluestore "
        "--data {data_path} --block.wal {wal_path} --block.db {db_path} --osd_id {osd_id}".format(
            data_path=disk['data'], wal_path=disk['wal'], db_path=disk['db'], osd_id=disk['osd_id']))
    local_exec_cmd("sleep 1")

# 激活 osd磁盘
# ceph-volume lvm activate --bluestore {osd_id} {uuid}
remote_exec_cmd("ceph-volume lvm activate --bluestore --all")
