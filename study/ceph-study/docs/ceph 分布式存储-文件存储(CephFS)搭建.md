# 1. 创建元数据服务器
## 1.1 安装mds
``
PG数量的预估
集群中单个池的PG数计算公式如下：PG 总数 = (OSD 数 * 100) / 最大副本数 / 池数 (结果必须舍入到最接近2的N次幂的值)
``
```
#ceph-deploy install mds_server
 
$ ceph-deploy install ceph-xxx-osd03.gz01

[ceph_deploy.conf][DEBUG ] found configuration file at: /root/.cephdeploy.conf
[ceph_deploy.cli][INFO  ] Invoked (1.5.39): /bin/ceph-deploy install ceph-xxx-osd03.gz01
[ceph_deploy.cli][INFO  ] ceph-deploy options:
[ceph_deploy.cli][INFO  ]  verbose                       : False
[ceph_deploy.cli][INFO  ]  testing                       : None
[ceph_deploy.cli][INFO  ]  cd_conf                       : <ceph_deploy.conf.cephdeploy.Conf instance at 0xcaa830>
[ceph_deploy.cli][INFO  ]  cluster                       : ceph
[ceph_deploy.cli][INFO  ]  dev_commit                    : None
[ceph_deploy.cli][INFO  ]  install_mds                   : False
[ceph_deploy.cli][INFO  ]  stable                        : None
[ceph_deploy.cli][INFO  ]  default_release               : False
[ceph_deploy.cli][INFO  ]  username                      : None
[ceph_deploy.cli][INFO  ]  adjust_repos                  : True
[ceph_deploy.cli][INFO  ]  func                          : <function install at 0xc22050>
[ceph_deploy.cli][INFO  ]  install_mgr                   : False
[ceph_deploy.cli][INFO  ]  install_all                   : False
[ceph_deploy.cli][INFO  ]  repo                          : False
[ceph_deploy.cli][INFO  ]  host                          : ['ceph-xxx-osd03.gz01']
[ceph_deploy.cli][INFO  ]  install_rgw                   : False
[ceph_deploy.cli][INFO  ]  install_tests                 : False
[ceph_deploy.cli][INFO  ]  repo_url                      : None
[ceph_deploy.cli][INFO  ]  ceph_conf                     : None
[ceph_deploy.cli][INFO  ]  install_osd                   : False
[ceph_deploy.cli][INFO  ]  version_kind                  : stable
[ceph_deploy.cli][INFO  ]  install_common                : False
[ceph_deploy.cli][INFO  ]  overwrite_conf                : False
[ceph_deploy.cli][INFO  ]  quiet                         : False
[ceph_deploy.cli][INFO  ]  dev                           : master
[ceph_deploy.cli][INFO  ]  nogpgcheck                    : False
[ceph_deploy.cli][INFO  ]  local_mirror                  : None
[ceph_deploy.cli][INFO  ]  release                       : None
[ceph_deploy.cli][INFO  ]  install_mon                   : False
[ceph_deploy.cli][INFO  ]  gpg_url                       : None
[ceph_deploy.install][DEBUG ] Installing stable version jewel on cluster ceph hosts ceph-xxx-osd03.gz01
[ceph_deploy.install][DEBUG ] Detecting platform for host ceph-xxx-osd03.gz01 ...
[ceph-xxx-osd03.gz01][DEBUG ] connected to host: ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] detect platform information from remote host
[ceph-xxx-osd03.gz01][DEBUG ] detect machine type
[ceph_deploy.install][INFO  ] Distro info: CentOS Linux 7.2.1511 Core
[ceph-xxx-osd03.gz01][INFO  ] installing Ceph on ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum clean all
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository ceph is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Cleaning repos: base didi_update epel extras tmprepo updates
[ceph-xxx-osd03.gz01][DEBUG ] Cleaning up everything
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum -y install epel-release
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository ceph is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Determining fastest mirrors
[ceph-xxx-osd03.gz01][DEBUG ] 132 packages excluded due to repository priority protections
[ceph-xxx-osd03.gz01][DEBUG ] Package matching epel-release-7-9.noarch already installed. Checking for update.
[ceph-xxx-osd03.gz01][DEBUG ] Nothing to do
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum -y install yum-plugin-priorities
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository ceph is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Loading mirror speeds from cached hostfile
[ceph-xxx-osd03.gz01][DEBUG ] 132 packages excluded due to repository priority protections
[ceph-xxx-osd03.gz01][DEBUG ] Package yum-plugin-priorities-1.1.31-42.el7.noarch already installed and latest version
[ceph-xxx-osd03.gz01][DEBUG ] Nothing to do
[ceph-xxx-osd03.gz01][DEBUG ] Configure Yum priorities to include obsoletes
[ceph-xxx-osd03.gz01][WARNIN] check_obsoletes has been enabled for Yum priorities plugin
[ceph-xxx-osd03.gz01][INFO  ] Running command: rpm --import https://download.ceph.com/keys/release.asc
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum remove -y ceph-release
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository ceph is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Resolving Dependencies
[ceph-xxx-osd03.gz01][DEBUG ] --> Running transaction check
[ceph-xxx-osd03.gz01][DEBUG ] ---> Package ceph-release.noarch 0:1-1.el7 will be erased
[ceph-xxx-osd03.gz01][DEBUG ] --> Finished Dependency Resolution
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Dependencies Resolved
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ]  Package              Arch           Version            Repository         Size
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ] Removing:
[ceph-xxx-osd03.gz01][DEBUG ]  ceph-release         noarch         1-1.el7            installed         535
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Transaction Summary
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ] Remove  1 Package
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Installed size: 535
[ceph-xxx-osd03.gz01][DEBUG ] Downloading packages:
[ceph-xxx-osd03.gz01][DEBUG ] Running transaction check
[ceph-xxx-osd03.gz01][DEBUG ] Running transaction test
[ceph-xxx-osd03.gz01][DEBUG ] Transaction test succeeded
[ceph-xxx-osd03.gz01][DEBUG ] Running transaction
[ceph-xxx-osd03.gz01][DEBUG ]   Erasing    : ceph-release-1-1.el7.noarch                                  1/1
[ceph-xxx-osd03.gz01][DEBUG ] warning: /etc/yum.repos.d/ceph.repo saved as /etc/yum.repos.d/ceph.repo.rpmsave
[ceph-xxx-osd03.gz01][DEBUG ]   Verifying  : ceph-release-1-1.el7.noarch                                  1/1
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Removed:
[ceph-xxx-osd03.gz01][DEBUG ]   ceph-release.noarch 0:1-1.el7
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Complete!
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum install -y https://download.ceph.com/rpm-jewel/el7/noarch/ceph-release-1-0.el7.noarch.rpm
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Examining /var/tmp/yum-root-Y2Avp6/ceph-release-1-0.el7.noarch.rpm: ceph-release-1-1.el7.noarch
[ceph-xxx-osd03.gz01][DEBUG ] Marking /var/tmp/yum-root-Y2Avp6/ceph-release-1-0.el7.noarch.rpm to be installed
[ceph-xxx-osd03.gz01][DEBUG ] Resolving Dependencies
[ceph-xxx-osd03.gz01][DEBUG ] --> Running transaction check
[ceph-xxx-osd03.gz01][DEBUG ] ---> Package ceph-release.noarch 0:1-1.el7 will be installed
[ceph-xxx-osd03.gz01][DEBUG ] --> Finished Dependency Resolution
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Dependencies Resolved
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ]  Package          Arch       Version     Repository                        Size
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ] Installing:
[ceph-xxx-osd03.gz01][DEBUG ]  ceph-release     noarch     1-1.el7     /ceph-release-1-0.el7.noarch     535
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Transaction Summary
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ] Install  1 Package
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Total size: 535
[ceph-xxx-osd03.gz01][DEBUG ] Installed size: 535
[ceph-xxx-osd03.gz01][DEBUG ] Downloading packages:
[ceph-xxx-osd03.gz01][DEBUG ] Running transaction check
[ceph-xxx-osd03.gz01][DEBUG ] Running transaction test
[ceph-xxx-osd03.gz01][DEBUG ] Transaction test succeeded
[ceph-xxx-osd03.gz01][DEBUG ] Running transaction
[ceph-xxx-osd03.gz01][DEBUG ]   Installing : ceph-release-1-1.el7.noarch                                  1/1
[ceph-xxx-osd03.gz01][DEBUG ]   Verifying  : ceph-release-1-1.el7.noarch                                  1/1
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Installed:
[ceph-xxx-osd03.gz01][DEBUG ]   ceph-release.noarch 0:1-1.el7
[ceph-xxx-osd03.gz01][DEBUG ]
[ceph-xxx-osd03.gz01][DEBUG ] Complete!
[ceph-xxx-osd03.gz01][WARNIN] ensuring that /etc/yum.repos.d/ceph.repo contains a high priority
[ceph-xxx-osd03.gz01][WARNIN] altered ceph.repo priorities to contain: priority=1
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum -y install ceph ceph-radosgw
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Loading mirror speeds from cached hostfile
[ceph-xxx-osd03.gz01][DEBUG ] 133 packages excluded due to repository priority protections
[ceph-xxx-osd03.gz01][DEBUG ] Package matching 1:ceph-10.2.10-0.el7.x86_64 already installed. Checking for update.
[ceph-xxx-osd03.gz01][DEBUG ] Package matching 1:ceph-radosgw-10.2.10-0.el7.x86_64 already installed. Checking for update.
[ceph-xxx-osd03.gz01][DEBUG ] Nothing to do
[ceph-xxx-osd03.gz01][INFO  ] Running command: ceph --version
[ceph-xxx-osd03.gz01][DEBUG ] ceph version 12.2.0 (32ce2a3ae5239ee33d6150705cdb24d43bab910c) luminous (rc)
```

## 1.2 拷贝ceph配置及秘钥

```
$ cd /etc/ceph/
$ ceph-deploy admin ceph-xxx-osd03.gz01
[ceph_deploy.conf][DEBUG ] found configuration file at: /root/.cephdeploy.conf
[ceph_deploy.cli][INFO  ] Invoked (1.5.39): /bin/ceph-deploy admin ceph-xxx-osd03.gz01
[ceph_deploy.cli][INFO  ] ceph-deploy options:
[ceph_deploy.cli][INFO  ]  username                      : None
[ceph_deploy.cli][INFO  ]  verbose                       : False
[ceph_deploy.cli][INFO  ]  overwrite_conf                : False
[ceph_deploy.cli][INFO  ]  quiet                         : False
[ceph_deploy.cli][INFO  ]  cd_conf                       : <ceph_deploy.conf.cephdeploy.Conf instance at 0x2ac9b90>
[ceph_deploy.cli][INFO  ]  cluster                       : ceph
[ceph_deploy.cli][INFO  ]  client                        : ['ceph-xxx-osd03.gz01']
[ceph_deploy.cli][INFO  ]  func                          : <function admin at 0x2a24c08>
[ceph_deploy.cli][INFO  ]  ceph_conf                     : None
[ceph_deploy.cli][INFO  ]  default_release               : False
[ceph_deploy.admin][DEBUG ] Pushing admin keys and conf to ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] connected to host: ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] detect platform information from remote host
[ceph-xxx-osd03.gz01][DEBUG ] detect machine type
[ceph-xxx-osd03.gz01][DEBUG ] write cluster configuration to /etc/ceph/{cluster}.conf
```

## 1.3 修改秘钥文件权限，保证有可读权限

```
sudo chmod +r /etc/ceph/ceph.client.admin.keyring
```

## 1.4 创建元数据服务器

```
$ ceph-deploy --overwrite-conf mds create ceph-xxx-osd03.gz01

[ceph_deploy.conf][DEBUG ] found configuration file at: /root/.cephdeploy.conf
[ceph_deploy.cli][INFO  ] Invoked (1.5.39): /bin/ceph-deploy --overwrite-conf mds create ceph-xxx-osd03.gz01
[ceph_deploy.cli][INFO  ] ceph-deploy options:
[ceph_deploy.cli][INFO  ]  username                      : None
[ceph_deploy.cli][INFO  ]  verbose                       : False
[ceph_deploy.cli][INFO  ]  overwrite_conf                : True
[ceph_deploy.cli][INFO  ]  subcommand                    : create
[ceph_deploy.cli][INFO  ]  quiet                         : False
[ceph_deploy.cli][INFO  ]  cd_conf                       : <ceph_deploy.conf.cephdeploy.Conf instance at 0xc61518>
[ceph_deploy.cli][INFO  ]  cluster                       : ceph
[ceph_deploy.cli][INFO  ]  func                          : <function mds at 0xc428c0>
[ceph_deploy.cli][INFO  ]  ceph_conf                     : None
[ceph_deploy.cli][INFO  ]  mds                           : [('ceph-xxx-osd03.gz01', 'ceph-xxx-osd03.gz01')]
[ceph_deploy.cli][INFO  ]  default_release               : False
[ceph_deploy.mds][DEBUG ] Deploying mds, cluster ceph hosts ceph-xxx-osd03.gz01:ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] connected to host: ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] detect platform information from remote host
[ceph-xxx-osd03.gz01][DEBUG ] detect machine type
[ceph_deploy.mds][INFO  ] Distro info: CentOS Linux 7.2.1511 Core
[ceph_deploy.mds][DEBUG ] remote host will use systemd
[ceph_deploy.mds][DEBUG ] deploying mds bootstrap to ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] write cluster configuration to /etc/ceph/{cluster}.conf
[ceph-xxx-osd03.gz01][DEBUG ] create path if it doesn't exist
[ceph-xxx-osd03.gz01][INFO  ] Running command: ceph --cluster ceph --name client.bootstrap-mds --keyring /var/lib/ceph/bootstrap-mds/ceph.keyring auth get-or-create mds.ceph-xxx-osd03.gz01 osd allow rwx mds allow mon allow profile mds -o /var/lib/ceph/mds/ceph-ceph-xxx-osd03.gz01/keyring
[ceph-xxx-osd03.gz01][INFO  ] Running command: systemctl enable ceph-mds@ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][WARNIN] Created symlink from /etc/systemd/system/ceph-mds.target.wants/ceph-mds@ceph-xxx-osd03.gz01.service to /usr/lib/systemd/system/ceph-mds@.service.
[ceph-xxx-osd03.gz01][INFO  ] Running command: systemctl start ceph-mds@ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][INFO  ] Running command: systemctl enable ceph.target
```

# 2. 存储池
## 2.1 创建存储池数据
```
#ceph osd pool create cephfs_data <pg_num>
 
$ ceph osd pool create cephfs_data 128
pool 'cephfs_data' created
```
## 2.2 创建存储池元数据
```
#ceph osd pool create cephfs_metadata <pg_num>
 
$ ceph osd pool create cephfs_data 128
pool 'cephfs_metadata' created
```

## 2.3 查看存储池
```shell
$ ceph osd lspools
1 rbd,2 test_data,3 test_metadata,5 test,6 benmark_test,7 .rgw.root,8 default.rgw.control,9 default.rgw.meta,10 default.rgw.log,11 default.rgw.buckets.index,12 web-services,13 test_pool,15 cephfs_data,16 cephfs_metadata
```

# 3. 文件系统
## 3.1 创建文件系统
```shell
#ceph fs new <fs_name> cephfs_metadata cephfs_data
 
$ ceph fs new test_fs cephfs_metadata cephfs_data
new fs with metadata pool 16 and data pool 15
```
 
## 3.2 查看文件系统
```
$ ceph fs ls
name: test_fs, metadata pool: test_metadata, data pools: [test_data ]
```

# 4. mds状态
## 4.1 查看mds状态
```
$ ceph mds stat
test_fs-1/1/1 up [test_fs:0]=ceph-bench-osd00=up:active}
```
**说明：**
*   `e8` : e标识epoch，8是epoch号
*   `tstfs-1/1/1 up` : `tstfs`是cephfs名字，后面的三个1分别是`[mds_map.in/mds_map.up/mds_map.max_mds](http://mds_map.in/mds_map.up/mds_map.max_mds)`，`up`是cephfs状态
*   `{[tstfs:0]=mds-daemon-1=up:active}` : `[tstfs:0]`指tstfs的rank 0，`mds-daemon-1`是服务tstfs的mds daemon name，`up:active`是cephfs的状态为 up & active
*   test_fs-1是active的，它的mds daemon为ceph-xxx-osd03.gz01
*   又添加一个新的mds daemon后，它会处于standby状态，若前两个mds daemon出问题，它会顶替上去

# 5. 内核驱动挂载ceph文件系统
## 5.1 查看秘钥
```
$ cat /etc/ceph/ceph.client.admin.keyring
[client.admin]
key = AQBpxadZjXLcBxAArU8dYn75aAZYfdGdogYRYg==
```
## 5.2 创建目录
```
$ sudo mkdir /mnt/mycephfs
```
## 5.3 挂载文件系统
```
#指定用户名、密钥
#sudo mount -t {ip-address-of-monitor}:6789 /mycephfs -o name=xxxx,secret=xxxx
 
$ sudo mount -t ceph 10.1.178.32:6789:/ /mnt/mycephfs -o name=admin,secret=AQBpxadZjXLcBxAArU8dYn75aAZYfdGdogYRYg==
$ df -h
10.1.178.32:6789:/  201T  1.9T  199T   1% /mnt/mycephfs
```

# 6. 用户空间挂载ceph文件系统
## 6.1 安装ceph-fuse
```
$ yum install ceph-fuse
```

## 6.2 创建目录
```
$ sudo mkdir /mnt/mycephfs-fuse
```

## 6.3 挂载文件系统
```
#Ceph 存储集群默认要求认证，需指定相应的密钥环文件
#sudo ceph-fuse -k keyring -m {ip-address-of-monitor}:6789 /mycephfs
 
$ sudo ceph-fuse -k /etc/ceph/ceph.client.admin.keyring -m 10.1.178.32:6789 /mnt/mycephfs-fuse
2017-11-15 14:26:45.323695 7fef1e1e2ec0 -1 init, newargv = 0x7fef29cb68a0 newargc=11ceph-fuse[3300741]:
starting ceph client

$ df -h
ceph-fuse 201T 1.9T 199T 1% /mnt/mycephfs-fuse
```
