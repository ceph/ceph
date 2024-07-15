# 1. 增加 OSD（手动）

要增加一个 OSD，要依次创建数据目录、把硬盘挂载到数据目录、把 OSD 加入集群、然后把它加入 CRUSH Map。

**Tip：** Ceph 喜欢统一的硬件，与存储池无关。如果你要新增容量不一的硬盘驱动器，还需调整它们的权重。但是，为实现最佳性能，CRUSH 的分级结构最好按类型、容量来组织。

1. 创建 OSD。如果未指定 UUID， OSD 启动时会自动生成一个。下列命令会输出 OSD 号，后续步骤你会用到。
```
ceph osd create [{uuid} [{id}]]
```

如果指定了可选参数 {id} ，那么它将作为 OSD id 。要注意，如果此数字已使用，此命令会出错。

**警告：** 一般来说，我们不建议指定 {id} 。因为 ID 是按照数组分配的，跳过一些依然会浪费内存；尤其是跳过太多、或者集群很大时，会更明显。若未指定 {id} ，将用最小可用数字。

2. 在新 OSD 主机上创建数据目录。
```
ssh {new-osd-host}
sudo mkdir /var/lib/ceph/osd/ceph-{osd-number}
```

3. 如果准备用于 OSD 的是单独的磁盘而非系统盘，先把它挂载到刚创建的目录下：
```
ssh {new-osd-host}
sudo mkfs -t {fstype} /dev/{drive}
sudo mount -o user_xattr /dev/{hdd} /var/lib/ceph/osd/ceph-{osd-number}
```

4. 初始化 OSD 数据目录。
```
ssh {new-osd-host}
ceph-osd -i {osd-num} --mkfs --mkkey
```
在启动 `ceph-osd` 前，数据目录必须是空的。

5. 注册 OSD 认证密钥， `ceph-{osd-num}` 路径里的 `ceph` 值应该是 `$cluster-$id` ，如果你的集群名字不是 `ceph` ，那就用自己集群的名字。
```
ceph auth add osd.{osd-num} osd 'allow *' mon 'allow rwx' -i /var/lib/ceph/osd/ceph-{osd-num}/keyring
```

6. 把新 OSD 加入 CRUSH Map 中，以便它可以开始接收数据。用 `ceph osd crush add` 命令把 OSD 加入 CRUSH 分级结构的合适位置。如果你指定了不止一个 bucket，此命令会把它加入你所指定的 bucket 中最具体的一个，并且把此 bucket 挪到你指定的其它 bucket 之内。
```
ceph osd crush add {id-or-name} {weight} [{bucket-type}={bucket-name} ...]
```
比如：
```
ceph osd crush add 21 0.08800 pool=ssd_root rack=ssd_rack01 host=ssd_ceph4
```

你也可以反编译 CRUSH Map、把 OSD 加入设备列表、以 bucket 的形式加入主机（如果它没在 CRUSH Map 里）、以条目形式把设备加入主机、分配权重、重编译并应用它，详情参见本手册第一部分 [9. 修改 Crushmap](./modify_crushmap.md) 。

7. 启动 OSD。把 OSD 加入 Ceph 后， OSD 就在配置里了。然而它还没运行，它现在的状态为 `down & out` 。你必须先启动 OSD 它才能收数据。在 Ubuntu 上执行：
```
sudo start ceph-osd id={osd-num}
```

一旦你启动了 OSD ，其状态就变成了 `up & in` 。

# 2. 增加 OSD（ ceph-deploy ）

还可以通过 `ceph-deploy` 工具很方便的增加 OSD。

1. 登入 `ceph-deploy` 工具所在的 Ceph admin 节点，进入工作目录。
```
ssh {ceph-deploy-node}
cd /path/ceph-deploy-work-path
```

2. 列举磁盘。

执行下列命令列举一节点上的磁盘：
```
ceph-deploy disk list {node-name [node-name]...}
```

3. 格式化磁盘。

用下列命令格式化（删除分区表）磁盘，以用于 Ceph ：
```
ceph-deploy disk zap {osd-server-name}:{disk-name}
ceph-deploy disk zap osdserver1:sdb
```

**重要：** 这会删除磁盘上的所有数据。

4. 准备 OSD。
```
ceph-deploy osd prepare {node-name}:{data-disk}[:{journal-disk}]
ceph-deploy osd prepare osdserver1:sdb:/dev/ssd
ceph-deploy osd prepare osdserver1:sdc:/dev/ssd
```
`prepare` 命令只准备 OSD 。在大多数操作系统中，硬盘分区创建后，不用 `activate` 命令也会自动执行 `activate` 阶段（通过 Ceph 的 `udev` 规则）。

前例假定一个硬盘只会用于一个 OSD 守护进程，以及一个到 SSD 日志分区的路径。我们建议把日志存储于另外的驱动器以最优化性能；你也可以指定一单独的驱动器用于日志（也许比较昂贵）、或者把日志放到 OSD 数据盘（不建议，因为它有损性能）。前例中我们把日志存储于分好区的固态硬盘。

**注意：** 在一个节点运行多个 OSD 守护进程、且多个 OSD 守护进程共享一个日志分区时，你应该考虑整个节点的最小 CRUSH 故障域，因为如果这个 SSD 坏了，所有用其做日志的 OSD 守护进程也会失效。

5. 准备好 OSD 后，可以用下列命令激活它。
```
ceph-deploy osd activate {node-name}:{data-disk-partition}[:{journal-disk-partition}]
ceph-deploy osd activate osdserver1:/dev/sdb1:/dev/ssd1
ceph-deploy osd activate osdserver1:/dev/sdc1:/dev/ssd2
```

`activate` 命令会让 OSD 进入 `up` 且 `in` 状态。该命令使用的分区路径是前面 `prepare` 命令创建的。

# 3. 删除 OSD（手动）

要想缩减集群尺寸或替换硬件，可在运行时删除 OSD 。在 Ceph 里，一个 OSD 通常是一台主机上的一个 `ceph-osd` 守护进程、它运行在一个硬盘之上。如果一台主机上有多个数据盘，你得逐个删除其对应 `ceph-osd` 。通常，操作前应该检查集群容量，看是否快达到上限了，确保删除 OSD 后不会使集群达到 `near full` 比率。

**警告：** 删除 OSD 时不要让集群达到 `full ratio` 值，删除 OSD 可能导致集群达到或超过 `full ratio` 值。

1、停止需要剔除的 OSD 进程，让其他的 OSD 知道这个 OSD 不提供服务了。停止 OSD 后，状态变为 `down` 。
```
ssh {osd-host}
sudo stop ceph-osd id={osd-num}
```

2. 将 OSD 标记为 `out` 状态，这个一步是告诉 mon，这个 OSD 已经不能服务了，需要在其他的 OSD 上进行数据的均衡和恢复了。
```
ceph osd out {osd-num}
```
执行完这一步后，会触发数据的恢复过程。此时应该等待数据恢复结束，集群恢复到 `HEALTH_OK` 状态，再进行下一步操作。

3. 删除 CRUSH Map 中的对应 OSD 条目，它就不再接收数据了。你也可以反编译 CRUSH Map、删除 device 列表条目、删除对应的 host 桶条目或删除 host 桶（如果它在 CRUSH Map 里，而且你想删除主机），重编译 CRUSH Map 并应用它。详情参见本手册第一部分 [9. 修改 Crushmap](./modify_crushmap.md) 。
```
ceph osd crush remove {name}
```
该步骤会触发数据的重新分布。等待数据重新分布结束，整个集群会恢复到 `HEALTH_OK` 状态。

4. 删除 OSD 认证密钥：
```
	ceph auth del osd.{osd-num}
```

5. 删除 OSD 。
```
ceph osd rm {osd-num}
#for example
ceph osd rm 1
```

6. 卸载 OSD 的挂载点。
```	
sudo umount /var/lib/ceph/osd/$cluster-{osd-num}
```

7. 登录到保存 `ceph.conf` 主拷贝的主机。
```
    ssh {admin-host}
    cd /etc/ceph
    vim ceph.conf
```

8. 从 `ceph.conf` 配置文件里删除对应条目。
```
	[osd.1]
        	host = {hostname}
```
9. 从保存 `ceph.conf` 主拷贝的主机，把更新过的 `ceph.conf` 拷贝到集群其他主机的 `/etc/ceph` 目录下。

如果在 `ceph.conf` 中没有定义各 OSD 入口，就不必执行第 7 ~ 9 步。
