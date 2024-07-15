# 1. 相关软件包
## 1.1 安装软件包
``
PG数量的预估
集群中单个池的PG数计算公式如下：PG 总数 = (OSD 数 * 100) / 最大副本数 / 池数 (结果必须舍入到最接近2的N次幂的值)
``
```
#在管理节点的工作目录下，给 Ceph 对象网关节点安装Ceph对象所需的软件包
#ceph-deploy install --rgw <node1> [<node2> ...]
 
$ ceph-deploy install --rgw ceph-xxx-osd03.gz01
[ceph_deploy.conf][DEBUG ] found configuration file at: /root/.cephdeploy.conf
[ceph_deploy.cli][INFO  ] Invoked (1.5.39): /bin/ceph-deploy install --rgw ceph-xxx-osd03.gz01
[ceph_deploy.cli][INFO  ] ceph-deploy options:
[ceph_deploy.cli][INFO  ]  verbose                       : False
[ceph_deploy.cli][INFO  ]  testing                       : None
[ceph_deploy.cli][INFO  ]  cd_conf                       : <ceph_deploy.conf.cephdeploy.Conf instance at 0xc42830>
[ceph_deploy.cli][INFO  ]  cluster                       : ceph
[ceph_deploy.cli][INFO  ]  dev_commit                    : None
[ceph_deploy.cli][INFO  ]  install_mds                   : False
[ceph_deploy.cli][INFO  ]  stable                        : None
[ceph_deploy.cli][INFO  ]  default_release               : False
[ceph_deploy.cli][INFO  ]  username                      : None
[ceph_deploy.cli][INFO  ]  adjust_repos                  : True
[ceph_deploy.cli][INFO  ]  func                          : <function install at 0xbba050>
[ceph_deploy.cli][INFO  ]  install_mgr                   : False
[ceph_deploy.cli][INFO  ]  install_all                   : False
[ceph_deploy.cli][INFO  ]  repo                          : False
[ceph_deploy.cli][INFO  ]  host                          : ['ceph-xxx-osd03.gz01']
[ceph_deploy.cli][INFO  ]  install_rgw                   : True
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
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Cleaning repos: Ceph Ceph-noarch base ceph-source didi_update epel extras
[ceph-xxx-osd03.gz01][DEBUG ]               : tmprepo updates
[ceph-xxx-osd03.gz01][DEBUG ] Cleaning up everything
[ceph-xxx-osd03.gz01][DEBUG ] Cleaning up list of fastest mirrors
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum -y install epel-release
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Determining fastest mirrors
[ceph-xxx-osd03.gz01][DEBUG ] 133 packages excluded due to repository priority protections
[ceph-xxx-osd03.gz01][DEBUG ] Package matching epel-release-7-9.noarch already installed. Checking for update.
[ceph-xxx-osd03.gz01][DEBUG ] Nothing to do
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum -y install yum-plugin-priorities
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Loading mirror speeds from cached hostfile
[ceph-xxx-osd03.gz01][DEBUG ] 133 packages excluded due to repository priority protections
[ceph-xxx-osd03.gz01][DEBUG ] Package yum-plugin-priorities-1.1.31-42.el7.noarch already installed and latest version
[ceph-xxx-osd03.gz01][DEBUG ] Nothing to do
[ceph-xxx-osd03.gz01][DEBUG ] Configure Yum priorities to include obsoletes
[ceph-xxx-osd03.gz01][WARNIN] check_obsoletes has been enabled for Yum priorities plugin
[ceph-xxx-osd03.gz01][INFO  ] Running command: rpm --import https://download.ceph.com/keys/release.asc
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum remove -y ceph-release
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
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
[ceph-xxx-osd03.gz01][DEBUG ]  Package         Arch      Version       Repository                        Size
[ceph-xxx-osd03.gz01][DEBUG ] ================================================================================
[ceph-xxx-osd03.gz01][DEBUG ] Removing:
[ceph-xxx-osd03.gz01][DEBUG ]  ceph-release    noarch    1-1.el7       @/ceph-release-1-0.el7.noarch    535
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
[ceph-xxx-osd03.gz01][INFO  ] Running command: yum -y install ceph-radosgw
[ceph-xxx-osd03.gz01][DEBUG ] Loaded plugins: fastestmirror, langpacks, priorities
[ceph-xxx-osd03.gz01][WARNIN] Repository epel is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-debuginfo is listed more than once in the configuration
[ceph-xxx-osd03.gz01][WARNIN] Repository epel-source is listed more than once in the configuration
[ceph-xxx-osd03.gz01][DEBUG ] Loading mirror speeds from cached hostfile
[ceph-xxx-osd03.gz01][DEBUG ] 133 packages excluded due to repository priority protections
[ceph-xxx-osd03.gz01][DEBUG ] Package matching 1:ceph-radosgw-10.2.10-0.el7.x86_64 already installed. Checking for update.
[ceph-xxx-osd03.gz01][DEBUG ] Nothing to do
[ceph-xxx-osd03.gz01][INFO  ] Running command: ceph --version
[ceph-xxx-osd03.gz01][DEBUG ] ceph version 12.2.0 (32ce2a3ae5239ee33d6150705cdb24d43bab910c) luminous (rc)
```
# 2. 管理RGW节点
## 2.1 添加管理节点权限
```
#为了让你的 Ceph 对象网关节点成为管理节点，可以在管理节点的工作目录下执行以下命令
#ceph-deploy admin <node-name>
 
$ cd /etc/ceph/
$ ceph-deploy admin ceph-xxx-osd03.gz01
[ceph_deploy.conf][DEBUG ] found configuration file at: /root/.cephdeploy.conf
[ceph_deploy.cli][INFO  ] Invoked (1.5.39): /bin/ceph-deploy admin ceph-xxx-osd03.gz01
[ceph_deploy.cli][INFO  ] ceph-deploy options:
[ceph_deploy.cli][INFO  ]  username                      : None
[ceph_deploy.cli][INFO  ]  verbose                       : False
[ceph_deploy.cli][INFO  ]  overwrite_conf                : False
[ceph_deploy.cli][INFO  ]  quiet                         : False
[ceph_deploy.cli][INFO  ]  cd_conf                       : <ceph_deploy.conf.cephdeploy.Conf instance at 0x1078b90>
[ceph_deploy.cli][INFO  ]  cluster                       : ceph
[ceph_deploy.cli][INFO  ]  client                        : ['ceph-xxx-osd03.gz01']
[ceph_deploy.cli][INFO  ]  func                          : <function admin at 0xfd3c08>
[ceph_deploy.cli][INFO  ]  ceph_conf                     : None
[ceph_deploy.cli][INFO  ]  default_release               : False
[ceph_deploy.admin][DEBUG ] Pushing admin keys and conf to ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] connected to host: ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] detect platform information from remote host
[ceph-xxx-osd03.gz01][DEBUG ] detect machine type
[ceph-xxx-osd03.gz01][DEBUG ] write cluster configuration to /etc/ceph/{cluster}.conf
```
# 3. RGW实例
## 3.1 安装RGW实例
```
#ceph-deploy rgw create <gateway-node1>
 
$ ceph-deploy rgw create ceph-xxx-osd03.gz01
[ceph_deploy.conf][DEBUG ] found configuration file at: /root/.cephdeploy.conf
[ceph_deploy.cli][INFO  ] Invoked (1.5.39): /bin/ceph-deploy rgw create ceph-xxx-osd03.gz01
[ceph_deploy.cli][INFO  ] ceph-deploy options:
[ceph_deploy.cli][INFO  ]  username                      : None
[ceph_deploy.cli][INFO  ]  verbose                       : False
[ceph_deploy.cli][INFO  ]  rgw                           : [('ceph-xxx-osd03.gz01', 'rgw.ceph-xxx-osd03.gz01')]
[ceph_deploy.cli][INFO  ]  overwrite_conf                : False
[ceph_deploy.cli][INFO  ]  subcommand                    : create
[ceph_deploy.cli][INFO  ]  quiet                         : False
[ceph_deploy.cli][INFO  ]  cd_conf                       : <ceph_deploy.conf.cephdeploy.Conf instance at 0x27c4560>
[ceph_deploy.cli][INFO  ]  cluster                       : ceph
[ceph_deploy.cli][INFO  ]  func                          : <function rgw at 0x272ca28>
[ceph_deploy.cli][INFO  ]  ceph_conf                     : None
[ceph_deploy.cli][INFO  ]  default_release               : False
[ceph_deploy.rgw][DEBUG ] Deploying rgw, cluster ceph hosts ceph-xxx-osd03.gz01:rgw.ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] connected to host: ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] detect platform information from remote host
[ceph-xxx-osd03.gz01][DEBUG ] detect machine type
[ceph_deploy.rgw][INFO  ] Distro info: CentOS Linux 7.2.1511 Core
[ceph_deploy.rgw][DEBUG ] remote host will use systemd
[ceph_deploy.rgw][DEBUG ] deploying rgw bootstrap to ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][DEBUG ] write cluster configuration to /etc/ceph/{cluster}.conf
[ceph-xxx-osd03.gz01][DEBUG ] create path recursively if it doesn't exist
[ceph-xxx-osd03.gz01][INFO  ] Running command: ceph --cluster ceph --name client.bootstrap-rgw --keyring /var/lib/ceph/bootstrap-rgw/ceph.keyring auth get-or-create client.rgw.ceph-xxx-osd03.gz01 osd allow rwx mon allow rw -o /var/lib/ceph/radosgw/ceph-rgw.ceph-xxx-osd03.gz01/keyring
[ceph-xxx-osd03.gz01][INFO  ] Running command: systemctl enable ceph-radosgw@rgw.ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][INFO  ] Running command: systemctl start ceph-radosgw@rgw.ceph-xxx-osd03.gz01
[ceph-xxx-osd03.gz01][INFO  ] Running command: systemctl enable ceph.target
[ceph_deploy.rgw][INFO  ] The Ceph Object Gateway (RGW) is now running on host ceph-xxx-osd03.gz01 and default port 7480
```

## 3.2 查看服务
```
$ curl http://ceph-xxx-osd03.gz01:7480 -v
* About to connect() to ceph-xxx-osd03.gz01 port 7480 (#0)
*   Trying 10.69.7.31...
* Connected to ceph-xxx-osd03.gz01 (10.69.7.31) port 7480 (#0)
> GET / HTTP/1.1
> User-Agent: curl/7.29.0
> Host: ceph-xxx-osd03.gz01:7480
> Accept: */*
>
< HTTP/1.1 200 OK
< x-amz-request-id: tx000000000000000000002-005a0be6f6-1e598-default
< Content-Type: application/xml
< Content-Length: 214
< Date: Wed, 15 Nov 2017 07:04:22 GMT
<
* Connection #0 to host ceph-xxx-osd03.gz01 left intact
<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>anonymous</ID><DisplayName></DisplayName></Owner><Buckets></Buckets></ListAllMyBucketsResult>
```
# 4. S3用户
## 4.1 创建S3用户
```
#想正常的访问RGW，需要创建相应的RGW用户，并赋予相应的权限，radosgw-admin命令实现了这些功能。
#创建一个名为testuser的用户
 
$ radosgw-admin user create --uid="testuser" --display-name="First User"
2017-11-15 15:06:56.718127 7f33b5844c40  0 WARNING: detected a version of libcurl which contains a bug in curl_multi_wait(). enabling a workaround that may degrade performance slightly.
{
    "user_id": "testuser",
    "display_name": "First User",
    "email": "",
    "suspended": 0,
    "max_buckets": 1000,
    "auid": 0,
    "subusers": [
        {
            "id": "testuser:swift",
            "permissions": "full-control"
        }
    ],
    "keys": [
        {
            "user": "testuser",
            "access_key": "GI1GHD6ZOTIVF2R24GQ6",
            "secret_key": "UeEubdXgOegAqgzYRsDNPycFLE98ninUN3fFgbia"
        }
    ],
    "swift_keys": [
        {
            "user": "testuser:swift",
            "secret_key": "cOSLCYgZzvUmIMtSngr2BG553nb3LsJ0Z7PPNgiW"
        }
    ],
    "caps": [],
    "op_mask": "read, write, delete",
    "default_placement": "",
    "placement_tags": [],
    "bucket_quota": {
        "enabled": false,
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    },
    "user_quota": {
        "enabled": false,
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    },
    "temp_url_keys": [],
    "type": "rgw"
}
 
#ps：需要记住返回结果中keys->access_key和keys->secret_key的值，用于S3接口访问确认
```

## 4.2 测试S3接口
### 4.2.1 安装依赖库
```
#说明：需要创建一个Python测试脚本来测试S3访问。该脚本会连接RGW，创建一个bucket并列出所有的bucket。
#     其中，变量access_key和secret_access的值，来自于创建S3用户命令时，radosgw-admin命令返回的keys->access_key和keys->secret_key。 执行以下步骤，首先安装python-boto库，该库用于连接S3：
 
$ yum install python-boto
```
### 4.2.2 创建测试DEMO
```
$ cat s3_test.py
import boto.s3.connection
access_key = 'GI1GHD6ZOTIVF2R24GQ6'
secret_key = 'UeEubdXgOegAqgzYRsDNPycFLE98ninUN3fFgbia'
conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host='ceph-xxx-osd03.gz01', port=7480,
        is_secure=False, calling_format=boto.s3.connection.OrdinaryCallingFormat(),
       )
bucket = conn.create_bucket('my-new-bucket')
for bucket in conn.get_all_buckets():
    print "{name} {created}".format(
        name=bucket.name,
        created=bucket.creation_date,
    )
```
### 4.2.3 执行测试
```
$ python s3_test.py
my-new-bucket 2017-10-12T08:48:36.201Z
```

# 5. Swift用户
## 5.1 创建Swift用户
```
#Swift用户是作为子用户subuser被创建的，执行以下命令
 
$ radosgw-admin subuser create --uid=testuser --subuser=testuser:swift --access=full
2017-11-15 15:12:11.073416 7fa91ace2c40  0 WARNING: detected a version of libcurl which contains a bug in curl_multi_wait(). enabling a workaround that may degrade performance slightly.
{
    "user_id": "testuser",
    "display_name": "First User",
    "email": "",
    "suspended": 0,
    "max_buckets": 1000,
    "auid": 0,
    "subusers": [
        {
            "id": "testuser:swift",
            "permissions": "full-control"
        }
    ],
    "keys": [
        {
            "user": "testuser",
            "access_key": "GI1GHD6ZOTIVF2R24GQ6",
            "secret_key": "UeEubdXgOegAqgzYRsDNPycFLE98ninUN3fFgbia"
        }
    ],
    "swift_keys": [
        {
            "user": "testuser:swift",
            "secret_key": "96jl0Cg12T1izmD0CWKUqQFRrnjEBrJGoW2KYNUT"
        }
    ],
    "caps": [],
    "op_mask": "read, write, delete",
    "default_placement": "",
    "placement_tags": [],
    "bucket_quota": {
        "enabled": false,
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    },
    "user_quota": {
        "enabled": false,
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    },
    "temp_url_keys": [],
    "type": "rgw"
}
 
#ps：需要记住返回结果中swift_keys->secret_key的值，用于Swift接口访问确认。
```
## 5.2 测试Swift接口
### 5.2.1 安装依赖包
```
sudo yum install python-setuptools
sudo easy_install pip
sudo pip install python-swiftclient
```
### 5.2.2 执行测试
```
`#swift -A [http://{IP](http://%7Bip/) ADDRESS}:{port}/auth/1.0 -U testuser:swift -K '{swift_secret_key}' list`

`#替换{IP ADDRESS}、{port}、{swift_secret_key}等相关参数，其中{swift_secret_key}为创建Swift用户时，radosgw-admin命令返回的swift_keys->secret_key的值`

`$ swift -A http:``//ceph-xxx-osd03``.gz01:7480``/auth/1``.0 -U testuser:swift -K ``'96jl0Cg12T1izmD0CWKUqQFRrnjEBrJGoW2KYNUT'` `list`

`my-new-bucket`

```
