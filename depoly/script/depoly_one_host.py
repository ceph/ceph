import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd

# 
#local_exec_cmd("yum install sshpass -y")

# 清理机器
resource_array = ["/tmp/*.keyring", "/etc/ceph/*",
    "/var/lib/ceph/*", "/usr/lib64/ceph/*",
    "/usr/lib/ceph/*", 
    "/usr/lib64/python3.6/site-packages/ceph*",
    "/usr/lib/python3.6/site-packages/ceph*"]

# 安装软件
version="15.2.4-0"
el = 'el8'
# "ceph-mgr-modules-core"
remote_pkgdir="/tmp"
local_pkgdir="/root"
base_rpm_array=["sshpass"]
x86_rpm_array=["ceph-base", "ceph-common",
    "ceph-mon", "ceph-mgr",
    "ceph-osd", "librados2",
    "libradosstriper1",
    "librbd1", "python3-ceph-argparse",
    "python3-rados",
    "python3-rbd", "python3-ceph-common"]
noarch_rpm_array=["ceph-mgr-modules-core"]

for pkg in base_rpm_array:
    local_exec_cmd("yum install -y {pkgs}".format(pkgs=pkg))

node = '10.121.137.163'
local_exec_cmd("ssh-copy-id root@{node}".format(node=node))

# 拷贝软件
for pkg in x86_rpm_array:
    local_exec_cmd("scp {local}/{pkg_name}-{rpm_version}.{rpm_el}.x86_64.rpm root@{node}:/{remote}/".format(
        local=local_pkgdir, node=node, remote=remote_pkgdir, pkg_name=pkg, rpm_version=version,
        rpm_el=el) )

for pkg in noarch_rpm_array:
    local_exec_cmd("scp {local}/{pkg_name}-{rpm_version}.{rpm_el}.noarch.rpm root@{node}:{remote}/".format(
        local=local_pkgdir, node=node, remote=remote_pkgdir, pkg_name=pkg, rpm_version=version,
        rpm_el=el) )

# 安装软件
pkgs = ""
for pkg in x86_rpm_array:
    pkgs += " {rpm_dir}/{rpm_name}-{rpm_version}.{rpm_el}.x86_64.rpm".format(
        rpm_dir=remote_pkgdir, rpm_name=pkg, rpm_version=version, rpm_el=el)

for pkg in noarch_rpm_array:
    pkgs += " {rpm_dir}/{rpm_name}-{rpm_version}.{rpm_el}.noarch.rpm".format(
        rpm_dir=remote_pkgdir, rpm_name=pkg, rpm_version=version, rpm_el=el)

print (pkgs)
remote_exec_cmd("yum install -y {pkgs}".format(pkgs=pkgs) )
remote_exec_cmd("yum install -y fio sdparm")

