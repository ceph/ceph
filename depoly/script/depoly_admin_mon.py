import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd
from depoly_tools import public_ip
from depoly_tools import remote_target
from depoly_tools import cluster
from depoly_tools import cluster_id

#target=remote_exec_cmd("hostname")
target="node163"
cluster_public_ip=public_ip
source_dir = '../source'
remote_node=remote_target

# 设置集群名称
#exec_cmd("touch /etc/sysconfig/ceph")

# 为此集群创建密钥环、并生成mon密钥
remote_exec_cmd("rm -rf /tmp/{cluster_name}.mon.keyring".format(cluster_name=cluster))
#remote_exec_cmd("scp /tmp/{cluster_name}.mon.keyring root@{host}:/tmp/{cluster_name}.mon.keyring".format(
#    cluster_name=cluster, host=target) )

remote_exec_cmd("ceph-authtool --create-keyring /tmp/{cluster_name}.mon.keyring "
        "--gen-key -n mon. --cap mon 'allow *'".format(cluster_name=cluster))
local_exec_cmd("sleep 2")

# 生成管理员密钥环
remote_exec_cmd("rm -rf /etc/ceph/{cluster_name}.client.admin.keyring".format(cluster_name=cluster))
remote_exec_cmd("ceph-authtool --create-keyring --gen-key -n client.admin "
        "--cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *' --cap mgr 'allow *' "
        "/etc/ceph/{cluster_name}.client.admin.keyring".format(cluster_name=cluster))
local_exec_cmd("sleep 2")

remote_exec_cmd("ceph-authtool --create-keyring /var/lib/ceph/bootstrap-osd/{cluster_name}.keyring "
        "--gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd'"
        " --cap mgr 'allow r'".format(cluster_name=cluster))

# 加入 ceph.mon.keyring
remote_exec_cmd("ceph-authtool /tmp/{cluster_name}.mon.keyring "
        "--import-keyring /etc/ceph/{cluster_name}.client.admin.keyring".format(cluster_name=cluster))
remote_exec_cmd("ceph-authtool /tmp/{cluster_name}.mon.keyring "
        "--import-keyring /var/lib/ceph/bootstrap-osd/{cluster_name}.keyring".format(cluster_name=cluster))

# 生成一个监视器图
remote_exec_cmd("rm -rf /tmp/{cluster_name}_monmap.1223".format(cluster_name=cluster))
remote_exec_cmd("monmaptool --create --clobber "
        "--addv {host} [v2:{public_add}:3300,v1:{public_add}:6798] "
        "--fsid {cid} --print /tmp/{cluster_name}_monmap.1223".format(
            cluster_name=cluster, host=target, cid=cluster_id, public_add=cluster_public_ip))
local_exec_cmd("sleep 2")

# 创建数据目录
remote_exec_cmd("rm -rf /var/lib/ceph/mon/{cluster_name}-{host}".format(cluster_name=cluster, host=target))
remote_exec_cmd("mkdir -p /var/lib/ceph/mon/{cluster_name}-{host}".format(cluster_name=cluster, host=target))

# 拷贝对应的配置文件
local_exec_cmd("scp {source}/ceph.conf root@{node}:/etc/ceph/{cluster_name}.conf".format(source=source_dir, cluster_name=cluster, node=remote_node))
remote_exec_cmd("cat /etc/ceph/{cluster_name}.conf".format(cluster_name=cluster))
local_exec_cmd("sleep 1")

# 初始数据
remote_exec_cmd("ceph-mon --cluster {cluster_name} --mkfs -i {host} "
        "-c /etc/ceph/{cluster_name}.conf "
        "--monmap /tmp/{cluster_name}_monmap.1223 "
        "--keyring=/tmp/{cluster_name}.mon.keyring".format(cluster_name=cluster, host=target))
#标志创建完成
local_exec_cmd("sleep 5")
remote_exec_cmd("touch /var/lib/ceph/mon/{cluster_name}-{host}/done".format(cluster_name=cluster, host=target))

#remote_exec_cmd("ceph-mon --cluster {cluster_name} -i {host} -c /etc/ceph/{cluster_name}.conf "
#        "--keyring=/tmp/{cluster_name}.mon.keyring".format(cluster_name=cluster, host=target))

#systemctl stop ceph-mon@${target}
local_exec_cmd("sleep 1")
remote_exec_cmd("systemctl enable ceph-mon@{host}".format(host=target))
local_exec_cmd("sleep 1")
remote_exec_cmd("systemctl stop ceph-mon@{host}".format(host=target))
local_exec_cmd("sleep 1")
remote_exec_cmd("systemctl start ceph-mon@{host}".format(host=target))
local_exec_cmd("sleep 1")
remote_exec_cmd("systemctl status ceph-mon@{host}".format(host=target))
local_exec_cmd("sleep 1")
#remote_exec_cmd("for i in $(pgrep ceph-mon) ; do taskset -pc $i ; done")

remote_exec_cmd("ceph mon getmap -o monmap --cluster {cluster_name}".format(cluster_name=cluster))
remote_exec_cmd("monmaptool --print monmap")
local_exec_cmd("sleep 1")
remote_exec_cmd("ceph auth get mon. --cluster {cluster_name}".format(cluster_name=cluster))
local_exec_cmd("sleep 1")

# 删除默认的rule
remote_exec_cmd("ceph osd crush rule rm replicated_rule")
remote_exec_cmd("ceph osd crush remove default")

local_exec_cmd("sleep 2")
# 创建 mgr key
remote_exec_cmd("rm -rf /var/lib/ceph/mgr/{cluster_name}-{host}".format(cluster_name=cluster, host=target))
remote_exec_cmd("ceph auth get-or-create mgr.{host} mon 'allow profile mgr' osd 'allow *' mds 'allow *'".format(
    host=target))
remote_exec_cmd("mkdir /var/lib/ceph/mgr/{cluster_name}-{host}".format(cluster_name=cluster, host=target))
remote_exec_cmd("ceph auth get-or-create mgr.{host} -o /var/lib/ceph/mgr/ceph-{host}/keyring".format(
    host=target))
#exec_cmd("ceph config set mgr mgr/restful/{host}/server_port 42159".format(host=target))

# 启动服务
remote_exec_cmd("systemctl enable ceph-mgr@{host}".format(host=target))
local_exec_cmd("sleep 1")
remote_exec_cmd("systemctl start ceph-mgr@{host}".format(host=target))
local_exec_cmd("sleep 1")
remote_exec_cmd("systemctl status ceph-mgr@{host}".format(host=target))
local_exec_cmd("sleep 1")

