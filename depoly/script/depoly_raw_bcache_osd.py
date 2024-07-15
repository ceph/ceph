import re
import os
from depoly_tools import remote_exec_cmd
from depoly_tools import local_exec_cmd
from depoly_tools import remote_target
from depoly_tools import cluster

osd_ids=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
data_disks=['sda', 'sdb', 'sdc', 'sdd', 'sde', 'sdf', 'sdh', 'sdi', 'sdj', 'sdk']
meta_disks=['nvme0n1']
cache_disks=['nvme1n1']
#meta_disks=['nvme0n1', 'nvme1n1']
#cache_disks=[]

devices={"data": data_disks, 'meta':meta_disks, "cache":cache_disks}

data_disk_len = len(devices['data'])
meta_disk_len = len(devices['meta'])
cache_disk_len = len(devices['cache'])
if meta_disk_len != 0:
    meta_zone_len = data_disk_len // meta_disk_len

if cache_disk_len != 0:
    cache_zone_len = data_disk_len // cache_disk_len
db_internal = 200
wal_internal = 10
cache_internal = db_internal + wal_internal

# 拷贝证书
remote_exec_cmd("scp /var/lib/ceph/bootstrap-osd/{cluster_name}.keyring root@{node}:/var/lib/ceph/bootstrap-osd/{cluster_name}.keyring".format(
    cluster_name=cluster, node=remote_target))

# 进行元数据分物理区
for i in range(len(devices['meta'])):
    remote_exec_cmd("sdparm -s WCE=0 /dev/{disk_name}".format(disk_name=devices['meta'][i]))
    remote_exec_cmd( "parted /dev/{disk_name} --script mktable gpt".format(disk_name=devices['meta'][i]))
    local_exec_cmd( "sleep 1")

# 进行缓存数据分物理区
for i in range(len(devices['cache'])):
    remote_exec_cmd("sdparm -s WCE=0 /dev/{disk_name}".format(disk_name=devices['cache'][i]))
    remote_exec_cmd( "parted /dev/{disk_name} --script mktable gpt".format(disk_name=devices['cache'][i]))
    local_exec_cmd( "sleep 1")

# 进行元数据逻辑分区
disk_id = 0
ready_disks = []
for i in range(meta_disk_len):
    # 进行db分区
    offt_start = 0
    offt_end = 0
    disk = {}
    #disk = {'osd_id':'', 'disk_id':'', 'wal':'', 'db':'','data':'', 'cache':''}
    lv = 0
    for index in range(meta_zone_len):
        print ("disk_id")
        print (disk_id)
        lv = lv + 1
        offt_start = offt_end;
        offt_end = offt_start + wal_internal;
        # 进行 wal 分区
        print ("lv")
        print (lv)
        remote_exec_cmd( "parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
            disk_name=devices['meta'][i], start=offt_start, end=offt_end))
        disk['wal'] = "/dev/{disk_name}p{lv}".format(disk_name=devices['meta'][i], lv=lv)

        # 进行 db 分区
        lv = lv + 1
        offt_start = offt_end;
        offt_end = offt_start + db_internal;
        print ("lv")
        print (lv)
        remote_exec_cmd( "parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
            disk_name=devices['meta'][i], start=offt_start, end=offt_end))
        disk['db'] = "/dev/{disk_name}p{lv}".format(disk_name=devices['meta'][i], lv=lv)

        if cache_disk_len != 0:
            # 缓存盘后端建立
            disk['disk_id'] = disk_id
            remote_exec_cmd("sdparm -s WCE=0 /dev/{disk_name}".format(disk_name=devices['data'][disk_id]))
            remote_exec_cmd("make-bcache -B /dev/{disk_name} -w 4K".format(disk_name=devices['data'][disk_id]))
            disk['data'] = "/dev/bcache{cache_id}".format(cache_id=disk_id)
        else:
            # 数据盘
            disk['data'] = "/dev/{disk_name}".format(disk_name=devices['data'][disk_id])

        # osd 
        disk['osd_id'] = osd_ids[disk_id]
        disk['disk_id'] = disk_id
        # diskid
        disk_id = disk_id + 1
        ready_disks.append(disk.copy())

    local_exec_cmd( "sleep 1")
    # 预留进程创建测试盘
    offt_start = offt_end;
    offt_end = offt_start + 20;
    remote_exec_cmd( "parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
        disk_name=devices['meta'][i], start=offt_start, end=offt_end))
    local_exec_cmd( "sleep 1")

'''
disk_id = 0
# 缓存盘处理
for i in range(cache_disk_len):
    # backing device hard sector size of SSD -w
    # -bucket aching device的 erase block size
    # lsblk -o NAME, PHY-SeC
    remote_exec_cmd("make-bcache -C /dev/{disk_name} -w 4K -b 4M".format(
        disk_name=devices['cache'][i]))
    local_exec_cmd("sleep 2")

    cset_out = remote_exec_cmd('bcache-super-show /dev/{disk_name}|grep "cset.uuid" '.format(
        disk_name=devices['cache'][i]))

    tmp_cset_uuid = re.findall(r'\S+-\S+-\S+-\S+-*', cset_out)
    cset_uuid = tmp_cset_uuid[0]
    for index in range(cache_zone_len):
        remote_exec_cmd('echo {uuid} >/sys/block/bcache{cache_id}/bcache/attach'.format(
            uuid=cset_uuid, cache_id=ready_disks[disk_id]['disk_id']))
        disk_id = disk_id + 1
        local_exec_cmd("sleep 2")
'''

disk_id = 0
for i in range(cache_disk_len):
    offt_start = 0
    offt_end = 0
    lv = 0
    for index in range(cache_zone_len):
        print ("disk_id")
        print (disk_id)
        disk=ready_disks[disk_id]
        lv = lv + 1
        offt_start = offt_end;
        offt_end = offt_start + cache_internal;
        # 进行 wal 分区
        print ("lv")
        print (lv)
        remote_exec_cmd("parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
            disk_name=devices['cache'][i], start=offt_start, end=offt_end))
        cache_divice = "{disk_name}p{lv}".format(disk_name=devices['cache'][i], lv=lv)
        local_exec_cmd("sleep 1")
        remote_exec_cmd("make-bcache -C /dev/{disk_name} -w 4K -b 4M".format(
            disk_name=cache_divice))
        local_exec_cmd("sleep 2")
        cset_out = remote_exec_cmd('bcache-super-show /dev/{disk_name}|grep "cset.uuid" '.format(
            disk_name=cache_divice))
        tmp_cset_uuid = re.findall(r'\S+-\S+-\S+-\S+-*', cset_out)
        cset_uuid = tmp_cset_uuid[0]
        remote_exec_cmd('echo {uuid} >/sys/block/bcache{cache_id}/bcache/attach'.format(
            uuid=cset_uuid, cache_id=ready_disks[disk_id]['disk_id']))
        disk_id = disk_id + 1
        local_exec_cmd("sleep 2")

    # 预留进程创建测试盘
    offt_start = offt_end;
    offt_end = offt_start + 20;
    remote_exec_cmd("parted /dev/{disk_name} --script mkpart primary {start}G {end}G".format(
        disk_name=devices['cache'][i], start=offt_start, end=offt_end))
    local_exec_cmd("sleep 1")
        
# 检查创建成功
local_exec_cmd("sleep 1")
remote_exec_cmd("parted -l")

# 准备 osd磁盘
for disk in ready_disks:
    print (disk['disk_id'])
    remote_exec_cmd("/bin/bash -c ulimit -n 32768;ceph-volume raw prepare --bluestore "
        "--data {data_path} "
        "--block.wal {wal_path} "
        "--block.db {db_path} "
        "--no-tmpfs "
        "--osd_id {osd_id}".format(
            data_path=disk['data'],
            wal_path=disk['wal'],
            db_path=disk['db'],
            osd_id=disk['osd_id']))
    local_exec_cmd("sleep 5")

# 激活 osd磁盘
# ceph-volume lvm activate -h
for disk in ready_disks:
    print (disk['disk_id'])
    remote_exec_cmd("/bin/bash -c ulimit -n 32768;ceph-volume raw activate "
        "--device {data_path} "
        "--block.wal {wal_path} "
        "--block.db {db_path} "
        "--no-tmpfs --no-systemd".format(
            data_path=disk['data'],
            wal_path=disk['wal'],
            db_path=disk['db']))
    local_exec_cmd("sleep 5")

for disk in ready_disks:
    remote_exec_cmd("systemctl enable ceph-osd@{osd_id}".format(osd_id=disk['osd_id']))
    local_exec_cmd("sleep 1")
    remote_exec_cmd("systemctl start ceph-osd@{osd_id}".format(osd_id=disk['osd_id']))
    local_exec_cmd("sleep 2")

