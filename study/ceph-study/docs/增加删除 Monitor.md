# 1. 介绍

一个集群可以只有一个 monitor，我们推荐生产环境至少部署 3 个。 Ceph 使用 Paxos 算法的一个变种对各种 map 、以及其它对集群来说至关重要的信息达成共识。建议（但不是强制）部署奇数个 monitor 。Ceph 需要 mon 中的大多数在运行并能够互相通信，比如单个 mon，或 2 个中的 2 个，3 个中的 2 个，4 个中的 3 个等。初始部署时，建议部署 3 个 monitor。后续如果要增加，请一次增加 2 个。

# 2. 增加 Monitor（手动）

1. 在目标节点上，新建 mon 的默认目录。`{mon-id}` 一般取为节点的 hostname 。
```
ssh {new-mon-host}
sudo mkdir /var/lib/ceph/mon/ceph-{mon-id}
```

2. 创建一个临时目录（和第 1 步中的目录不同，添加 mon 完毕后需要删除该临时目录），来存放新增 mon 所需的各种文件。
```
	mkdir {tmp}
```
3. 获取 mon 的 keyring 文件，保存在临时目录下。
```
ceph auth get mon. -o {tmp}/{key-filename}
```

4. 获取集群的 mon map 并保存到临时目录下。
```
ceph mon getmap -o {tmp}/{map-filename}
```

5. 格式化在第 1 步中建立的 mon 数据目录。需要指定 mon map 文件的路径（获取法定人数的信息和集群的 `fsid` ）和 keyring 文件的路径。
```
sudo ceph-mon -i {mon-id} --mkfs --monmap {tmp}/{map-filename} --keyring {tmp}/{key-filename}
```

6. 启动节点上的 mon 进程，它会自动加入集群。守护进程需要知道绑定到哪个 IP 地址，可以通过 `--public-addr {ip:port}` 选择指定，或在 `ceph.conf` 文件中进行配置 `mon addr`。
```
ceph-mon -i {mon-id} --public-addr {ip:port}
```

# 3. 增加 Monitor（ ceph-deploy ）

还可以通过 `ceph-deploy` 工具很方便地增加 MON。

1. 登入 `ceph-deploy` 工具所在的 Ceph admin 节点，进入工作目录。
```
ssh {ceph-deploy-node}
cd /path/ceph-deploy-work-path
```

2. 执行下列命令，新增 Monitor：
```
ceph-deploy mon create {host-name [host-name]...}
```

**注意：** 在某一主机上新增 Mon 时，如果它不是由 `ceph-deploy new` 命令所定义的，那就必须把 `public network` 加入 `ceph.conf` 配置文件。

# 4. 删除 Monitor（手动）

当你想要删除一个 mon 时，需要考虑删除后剩余的 mon 个数是否能够达到法定人数。

1. 停止 mon 进程。
```
stop ceph-mon id={mon-id}
```

2. 从集群中删除 monitor。
```
ceph mon remove {mon-id}
```

3. 从 ceph.conf 中移除 mon 的入口部分（如果有）。

# 5. 删除 Monitor（从不健康的集群中）

本小节介绍了如何从一个不健康的集群（比如集群中的 monitor 无法达成法定人数）中删除 `ceph-mon` 守护进程。

1. 停止集群中所有的 `ceph-mon` 守护进程。
```
ssh {mon-host}
service ceph stop mon || stop ceph-mon-all
# and repeat for all mons
```

2. 确认存活的 mon 并登录该节点。
```
ssh {mon-host}
```

3. 提取 mon map。
```
ceph-mon -i {mon-id} --extract-monmap {map-path}
# in most cases, that's
ceph-mon -i `hostname` --extract-monmap /tmp/monmap
```

4. 删除未存活或有问题的的 monitor。比如，有 3 个 monitors，`mon.a` 、`mon.b` 和 `mon.c`，现在仅有 `mon.a` 存活，执行下列步骤：
```
monmaptool {map-path} --rm {mon-id}
# for example,
monmaptool /tmp/monmap --rm b
monmaptool /tmp/monmap --rm c
```

5. 向存活的 monitor(s) 注入修改后的 mon map。比如，把 mon map 注入 `mon.a`，执行下列步骤：
```
ceph-mon -i {mon-id} --inject-monmap {map-path}
# for example,
ceph-mon -i a --inject-monmap /tmp/monmap
```

6. 启动存活的 monitor。

7. 确认 monitor 是否达到法定人数（ `ceph -s` ）。

8. 你可能需要把已删除的 monitor 的数据目录 `/var/lib/ceph/mon` 归档到一个安全的位置。或者，如果你确定剩下的 monitor 是健康的且数量足够，也可以直接删除数据目录。

# 6. 删除 Monitor（ ceph-deploy ）

1. 登入 `ceph-deploy` 工具所在的 Ceph admin 节点，进入工作目录。
```
ssh {ceph-deploy-node}
cd /path/ceph-deploy-work-path
```
2. 如果你想删除集群中的某个 Mon ，可以用 `destroy` 选项。
```
ceph-deploy mon destroy {host-name [host-name]...}
```
**注意：** 确保你删除某个 Mon 后，其余 Mon 仍能达成一致。如果不可能，删除它之前可能需要先增加一个。
