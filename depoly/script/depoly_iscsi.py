import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd

# 安装 iscsi 软件包

# 部署 iscsi 服务

# 将节点加入 ha 服务中
# ha 服务 由 mgr 触发

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
