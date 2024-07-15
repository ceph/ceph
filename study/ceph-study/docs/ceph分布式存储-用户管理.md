# 一、 说明
Ceph 把数据以对象的形式存于各存储池中。Ceph 用户必须具有访问存储池的权限才能够读写数据。

另外，Ceph 用户必须具有执行权限才能够使用 Ceph 的管理命令。

# 二、 授权能力
Ceph 用 “能力”（ capabilities, caps ）这个术语来描述给认证用户的授权，这样才能使用 Mon、 OSD 和 MDS 的功能。

能力也用于限制对某一存储池内的数据或某个命名空间的访问。 Ceph 管理员用户可在创建或更新普通用户时赋予他相应的能力。

能力的语法符合下面的形式：
```
{daemon-type} 'allow {capability}' [{daemon-type} 'allow {capability}']
```
Monitor 能力： Monitor 能力包括 r 、 w 、 x 和 allow profile {cap}，例如：
```
mon 'allow rwx'
mon 'allow profile osd'
```
OSD 能力： OSD 能力包括 r 、 w 、 x 、 class-read 、 class-write 和 profile osd 。

另外， OSD 能力还支持存储池和命名空间的配置。
```
osd 'allow {capability}' [pool={poolname}] [namespace={namespace-name}]
```
MDS 能力： MDS 能力比较简单，只需要 allow 或者空白，也不会解析更多选项。
```
mds 'allow'
```
注意： Ceph 对象网关守护进程（ radosgw ）是 Ceph 存储集群的一种客户端，所以它没被表示成一种独立的 Ceph 存储集群守护进程类型。
下面描述了各种能力。
allow
描述: 在守护进程的访问设置之前，仅对 MDS 隐含 rw 。
r
描述: 授予用户读权限，对 monitor 具有读权限才能获取 CRUSH map。
w
描述: 授予用户写对象的权限。
x
描述: 授予用户调用类方法的能力，即同时有读和写，且能在 monitor 上执行 auth 操作。
class-read
描述: 授予用户调用类读取方法的能力， x 的子集。
class-write
描述: 授予用户调用类写入方法的能力， x 的子集。
*
描述: 授权此用户读、写和执行某守护进程/存储池，且允许执行管理命令。
profile osd
描述: 授权一个用户以 OSD 身份连接其它 OSD 或 Monitor。授予 OSD 们允许其它 OSD 处理复制、心跳流量和状态报告。
profile mds
描述: 授权一个用户以 MDS 身份连接其它 MDS 或 Monitor。
profile bootstrap-osd
描述: 授权用户自举引导一个 OSD 。授予部署工具，像 ceph-disk 、ceph-deploy 等等，这样它们在自举引导 OSD 时就有权限增加密钥了。
profile bootstrap-mds
描述: 授权用户自举引导一个 MDS。授予例如 ceph-deploy 的部署工具，这样它们在自举引导 MDS 时就有权限增加密钥了。

# 三、管理用户
用户管理功能可以让 Ceph 存储集群的管理员有能力去创建、更新和删除集群的普通用户。当创建或删除一个用户时，可能需要把 keys 分发给各客户端，以便它们可以加入到 keyring 文件中。

## 3.1 罗列用户
可以使用下面的命令罗列用户：
```
ceph auth list
```
Ceph 会列出集群中的所有用户。例如，在一个只有 2 个 OSD 的简单环境中，ceph auth list 将会输入类似下面的内容：
```
installed auth entries:
 
osd.0
        key: AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==
        caps: [mon] allow profile osd
        caps: [osd] allow *
osd.1
        key: AQC4CbtTCFJBChAAVq5spj0ff4eHZICxIOVZeA==
        caps: [mon] allow profile osd
        caps: [osd] allow *
client.admin
        key: AQBHCbtT6APDHhAA5W00cBchwkQjh3dkKsyPjw==
        caps: [mds] allow
        caps: [mon] allow *
        caps: [osd] allow *
client.bootstrap-mds
        key: AQBICbtTOK9uGBAAdbe5zcIGHZL3T/u2g6EBww==
        caps: [mon] allow profile bootstrap-mds
client.bootstrap-osd
        key: AQBHCbtT4GxqORAADE5u7RkpCN/oo4e5W0uBtw==
        caps: [mon] allow profile bootstrap-osd
```

注意 TYPE.ID 这种用户表示方法，比如 osd.0 表示用户类型是 osd 且其 ID 是 0 ， client.admin 表示用户类型是 client 且其 ID 是 admin （即默认的 client.admin 用户）。

另外，每个用户条目都有一个 key: <value> 对，一个或多个 caps: 条目。


## 3.2 获取用户
获取某一特定用户的信息：
```
ceph auth get {TYPE.ID}
```
例如：
```
ceph auth get client.admin
```
还可以给命令加上 -o {filename} 选项把输入保存到一个文件中。

## 3.3 增加用户
增加一个用户就是创建一个用户名（即 TYPE.ID）、一个密钥和任何包含在创建命令中的能力。有以下几种方式可以新增一个用户：

ceph auth add：此命令是最权威的新增用户方式。它回新建一个用户，产生一个 key ，并赋予用户任何给定的能力。
ceph auth get-or-create：这种方式通常是最便捷的一种，因为它的返回值是包含用户名（在方括号内）和密钥的格式。如果用户已经存在，该命令会返回密钥文件格式的用户名和密钥信息。可以使用 -o {filename} 选项把输入保存到一个文件中。
ceph auth get-or-create-key：该命令可以很方便地创建用户，但是只会返回用户的密钥。对于某些只需要密钥的用户（如 libvirt ）来说是很有用的。如果用户已经存在，该命令仅仅返回用户的密钥。可以使用 -o {filename} 选项把输入保存到一个文件中。
下面是几个例子：
```
ceph auth add client.john mon 'allow r' osd 'allow rw pool=liverpool'
ceph auth get-or-create client.paul mon 'allow r' osd 'allow rw pool=liverpool'
ceph auth get-or-create client.george mon 'allow r' osd 'allow rw pool=liverpool' -o george.keyring
ceph auth get-or-create-key client.ringo mon 'allow r' osd 'allow rw pool=liverpool' -o ringo.key
```
## 3.4 修改用户的能力
ceph auth caps 命令允许你指定用户并改变用户的能力。设定新的能力会覆盖当前的能力。

查看当前的能力可以使用 ceph auth get USERTYPE.USERID 命令。

要增加能力，你应该在如下格式的命令中包含当前已经存在的能力：
```
ceph auth caps USERTYPE.USERID {daemon} 'allow [r|w|x|*|...] [pool={pool-name}] [namespace={namespace-name}]' [{daemon} 'allow [r|w|x|*|...] [pool={pool-name}] [namespace={namespace-name}]']
```
例如：
```
ceph auth get client.john
ceph auth caps client.john mon 'allow r' osd 'allow rw pool=liverpool'
ceph auth caps client.paul mon 'allow rw' osd 'allow rwx pool=liverpool'
ceph auth caps client.brian-manager mon 'allow *' osd 'allow *'
```
要移除某个能力，你可能需要进行重置。如果你想取消某个用户对特定守护进程的所有访问权限，可以指定一个空的字符串。比如：
```
ceph auth caps client.ringo mon ' ' osd ' '
```
## 3.5 删除用户
想要删除一个用户，可以用 ceph auth del 命令：
```
ceph auth del {TYPE}.{ID}
```
其中， {TYPE} 是 client，osd，mon 或 mds 的其中一种。{ID} 是用户的名字或守护进程的 ID 。

## 3.6 打印用户的密钥
打印某个用户的授权密钥到标准输出界面，可以执行如下命令
```
ceph auth print-key {TYPE}.{ID}
```
其中， {TYPE} 是 client，osd，mon 或 mds 的其中一种。{ID} 是用户的名字或守护进程的 ID 。

当需要往客户端软件中注入 Ceph 用户（如 libvirt）的密钥时，打印用户的密钥时很有用的。
```
mount -t ceph serverhost:/ mountpoint -o name=client.user,secret=`ceph auth print-key client.user`
```
## 3.7 导入用户
使用 ceph auth import 命令并指定密钥文件，可以导入一个或多个用户：
```
ceph auth import -i /path/to/keyring
```
比如：
```
sudo ceph auth import -i /etc/ceph/ceph.keyring
```
# 四、秘钥管理
当你通过客户端访问 Ceph 集群时，Ceph 客户端会使用本地的 keyring 文件。默认使用下列路径和名称的 keyring 文件：
 - /etc/ceph/$cluster.$name.keyring
 - /etc/ceph/$cluster.keyring
 - /etc/ceph/keyring
 - /etc/ceph/keyring.bin
你也可以在 ceph.conf 中另行指定 keyring 的路径，但不推荐这样做。

$cluster 元变量是你的 Ceph 集群名称，默认名称是 ceph 。$name 元变量是用户类型和 ID 。比如用户是 client.admin，那就得到 ceph.client.admin.keyring 。

本小节介绍如何使用 ceph-authtool 工具来从客户端管理 keyring 。

## 4.1 创建密钥
创建一个空的 keyring 文件，使用 --create-keyring 或 -C 选项。比如：
```
ceph-authtool --create-keyring /path/to/keyring
```
创建一个包含多个用户的 keyring 文件，推荐使用 $cluster.keyring 作为文件名，并存放于 /etc/ceph 目录下。

这样就无需在本地的 ceph.conf 文件中指定 keyring 的路径了。
```
sudo ceph-authtool -C /etc/ceph/ceph.keyring
```
创建仅包含一个用户的 keyring 文件时，建议使用集群名、用户类型和用户名称作为文件名，并存放于 /etc/ceph 目录下。

## 4.2 给密钥文件中增加用户
为了获取某个用户的 keyring 文件，可以使用 ceph auth get 命令加 -o 选项，

以 keyring 文件格式来保存输出。比如：
```
sudo ceph auth get client.admin -o /etc/ceph/ceph.client.admin.keyring
```
当你想要向 keyring 文件中导入某个用户时，可以使用 ceph-authtool 来指定目的和源 keyring 文件。比如：
```
sudo ceph-authtool /etc/ceph/ceph.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring
```
## 4.3 创建用户
可以在 Ceph 客户端直接创建用户、密钥和能力。然后再导入 Ceph 集群。比如：
```
sudo ceph-authtool -n client.ringo --cap osd 'allow rwx' --cap mon 'allow rwx' /etc/ceph/ceph.keyring
```
创建 keyring 文件、增加用户也可以同时进行。比如：
```
sudo ceph-authtool -C /etc/ceph/ceph.keyring -n client.ringo --cap osd 'allow rwx' --cap mon 'allow rwx' --gen-key
```
上述步骤中，新用户 client.ringo 仅存在于 keyring 文件中，还需要把新用户加入到 Ceph 集群中。
```
sudo ceph auth add client.ringo -i /etc/ceph/ceph.keyring
```
## 4.4 修改用户能力
修改记录在 keyring 文件中的用户能力时，需要指定 keyring 、用户和新的能力选项。比如：
```
sudo ceph-authtool /etc/ceph/ceph.keyring -n client.ringo --cap osd 'allow rwx' --cap mon 'allow rwx'
```
更新 Ceph 存储集群的用户，你必须更新 keyring 文件中对应用户入口的信息。
```
sudo ceph auth import -i /etc/ceph/ceph.keyring
```
# 五、命令行使用方法
Ceph 支持通过下列方式使用用户名称和密钥。

--id | --user

描述： Ceph 通过类型和 ID 来确定用户，如 TYPE.ID 或 client.admin, client.user1 。使用 --id 或 --user 选项指定用户的 ID，比如，指定使用 client.foo 用户：
```
ceph --id foo --keyring /path/to/keyring health
ceph --user foo --keyring /path/to/keyring health
```
--name | -n

描述： 使用 --name 或 -n 选项指定用户的全名（ TYPE.ID ），比如：
```
ceph --name client.foo --keyring /path/to/keyring health
ceph -n client.foo --keyring /path/to/keyring health
```
--keyring

描述： 指定包含一个或多个用户及其密钥的 keyring 文件路径。 --secret 选项提供同样的功能，但对 Ceph RADOS Gateway 不生效（ --secret 选项有其他的作用）。比如：
```
sudo rbd map --id foo --keyring /path/to/keyring mypool/myimage
```
