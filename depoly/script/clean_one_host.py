import os
from depoly_tools import local_exec_cmd
from depoly_tools import remote_exec_cmd

# 安装软件
remote_pkgdir="/tpm/"
x86_rpm_array=["ceph-base", "ceph-common",
        "ceph-mon", "ceph-mgr",
        "ceph-osd", "librados2",
        "libradosstriper1",
        "ceph-mgr-modules-core",
        "librbd1", "python3-ceph-argparse",
        "python3-rados",
        "python3-rbd", "python3-ceph-common"
        ]

# 安装软件
rpm_name = " "
for pkg in x86_rpm_array:
    rpm_name += pkg + " "

remote_exec_cmd("yum remove -y {pkg_name}".format(pkg_name=rpm_name) )

#remote_exec_cmd("yum remove -y fio")

# 清理机器
resource_array = ["/tmp/*.keyring", "/etc/ceph/*",
    "/var/lib/ceph/*", "/usr/lib64/ceph/*",
    "/usr/lib/ceph/*",
    "/usr/lib64/python3.6/site-packages/ceph*",
    "/usr/lib/python3.6/site-packages/ceph*"]

for resource in resource_array:
    remote_exec_cmd("rm -rf {source}".format(source=resource) )

