# 2. 常见 OSD 故障处理

----------

进行 OSD 排障前，先检查一下 monitors 和网络。如果 `ceph health` 或 `ceph -s` 返回的是健康状态，这意味着 monitors 形成了法定人数。如果 monitor 还没达到法定人数、或者 monitor 状态错误，要先解决 monitor 的问题。核实下你的网络，确保它在正常运行，因为网络对 OSD 的运行和性能有显著影响。

### 2.1 收集 OSD 数据

开始 OSD 排障的第一步最好先收集信息，另外还有监控 OSD 时收集的，如 `ceph osd tree` 。

#### Ceph 日志

如果你没改默认路径，可以在 `/var/log/ceph` 下找到 Ceph 的日志：

	ls /var/log/ceph

如果看到的日志还不够详细，可以增大日志级别。请参考[1.12 日志和调试]，查阅如何保证看到大量日志又不影响集群运行。

#### 管理套接字

用管理套接字工具检索运行时信息。列出节点上所有 Ceph 套接字：

	ls /var/run/ceph

然后，执行下例命令显示可用选项，把 `{daemon-name}` 换成实际的守护进程（如 osd.0 ）：

	ceph daemon osd.0 help

或者，你也可以指定一个 `{socket-file}` （如 `/var/run/ceph` 下的文件）：

	ceph daemon {socket-file} help

和其它手段相比，管理套接字允许你：

- 在运行时列出配置
- 列出历史操作
- 列出操作的优先队列状态
- 列出在进行的操作
- 列出性能计数器

#### 显示可用空间

可能会引起文件系统问题。用 `df` 命令显示文件系统的可用空间。

	df -h

其它用法见 `df --help` 。

#### I/O 统计信息

用 `iostat` 工具定位 I/O 相关问题。

	iostat -x

#### 诊断信息

要查看诊断信息，配合 `less` 、 `more` 、 `grep` 或 `tail` 使用 `dmesg` ，例如：

	dmesg | grep scsi

### 2.2 停止数据向外重平衡

你得周期性地对集群的子集进行维护，或解决某个故障域的问题（如某个机架）。如果你不想在停机维护 OSD 时让 CRUSH 自动重均衡，首先设置集群的 `noout` 标志：

	ceph osd set noout

设置了 noout 后，你就可以停机维护失败域内的 OSD 了。

	stop ceph-osd id={num}

**注意：**在定位某故障域内的问题时，停机的 OSD 内的 PG 状态会变为 `degraded` 。

维护结束后，重启 OSD 。

	start ceph-osd id={num}

最后，解除 `noout` 标志。

	ceph osd unset noout

### 2.3 OSD 没运行

通常情况下，简单地重启 `ceph-osd` 进程就可以让它重回集群并恢复。

#### OSD 起不来

如果你重启了集群，但其中一个 OSD 起不来，依次检查：

- **配置文件**： 如果你新装的 OSD 不能启动，检查下配置文件，确保它符合规定（比如 `host` 而非 `hostname` ，等等）。

- **检查路径**： 检查配置文件的路径，以及 OSD 的数据和日志分区路径。如果你分离了 OSD 的数据和日志分区、而配置文件和实际挂载点存在出入，启动 OSD 时就可能失败。如果你想把日志存储于一个块设备，应该为日志硬盘分区并为各 OSD 分别指定一个分区。

- **检查最大线程数**： 如果你的节点有很多 OSD ，也许就会触碰到默认的最大线程数限制（如通常是 32k 个），尤其是在恢复期间。你可以用 `sysctl` 增大线程数，把最大线程数更改为支持的最大值（即 4194303 ），看看是否有用。例如：

	sysctl -w kernel.pid_max=4194303

如果增大最大线程数解决了这个问题，你可以把此配置 `kernel.pid_max` 写入配置文件 `/etc/sysctl.conf`，使之永久生效，例如：

	kernel.pid_max = 4194303

- **内核版本**： 确认你使用的内核版本和发布版本。 Ceph 默认依赖一些第三方工具，这些工具可能有缺陷或者与特定发布版和/或内核版本冲突（如 Google perftools ）。检查下[操作系统推荐表](http://docs.ceph.com/docs/master/start/os-recommendations/)，确保你已经解决了内核相关的问题。

- **段错误**： 如果有了段错误，提高日志级别（如果还没提高），再试试。如果重现了，联系 ceph-devel 邮件列表并提供你的配置文件、monitor 输出和日志文件内容。

#### OSD 失败

当 `ceph-osd` 挂掉时，monitor 可通过活着的 `ceph-osd` 了解到此情况，并通过 `ceph health` 命令报告：

	ceph health
	HEALTH_WARN 1/3 in osds are down

特别地，有 `ceph-osd` 进程标记为 `in` 且 `down` 的时候，你也会得到警告。你可以用下面的命令得知哪个 `ceph-osd` 进程挂了：

    ceph health detail
    HEALTH_WARN 1/3 in osds are down
    osd.0 is down since epoch 23, last address 192.168.106.220:6800/11080

如果有硬盘失败或其它错误使 `ceph-osd` 不能正常运行或重启，将会在日志文件 `/var/log/ceph/` 里输出一条错误信息。

如果守护进程因心跳失败、或者底层核心文件系统无响应而停止，查看 `dmesg` 获取硬盘或者内核错误。



如果是软件错误（失败的断言或其它意外错误），应该向 ceph-devel 邮件列表报告。

#### 硬盘没剩余空间

Ceph 不允许你向满的 OSD 写入数据，以免丢失数据。在运行着的集群中，你应该能收到集群空间将满的警告。 `mon osd full ratio` 默认为 0.95 ，或达到 95% 的空间使用率时它将阻止客户端写入数据。 `mon osd nearfull ratio` 默认为 0.85 ，也就是说达到容量的 85% 时它会产生健康警告。

满载集群问题一般产生于测试小型 Ceph 集群上如何处理 OSD 失败时。当某一节点使用率较高时，集群能够很快掩盖将满和占满率。如果你在测试小型集群上的 Ceph 如何应对 OSD 失败，应该保留足够的可用磁盘空间，然后试着临时降低 `mon osd full ratio` 和 `mon osd nearfull ratio` 值。

`ceph health` 会报告将满的 `ceph-osds` ：

	ceph health
	HEALTH_WARN 1 nearfull osds
	osd.2 is near full at 85%

或者：

	ceph health
	HEALTH_ERR 1 nearfull osds, 1 full osds
	osd.2 is near full at 85%
	osd.3 is full at 97%

处理这种情况的最好方法就是在出现 `near full` 告警时尽快增加新的 `ceph-osd` ，这允许集群把数据重分布到新 OSD 里。

如果因满载而导致 OSD 不能启动，你可以试着删除那个 OSD 上的一些数据。但是这时有个问题，当一个 OSD 使用比例达到 95% 时，集群将不接受任何 Ceph Client 端的读写数据的请求。这时 `rbd rm` 删除命令将不会得到响应。

让集群能够读写是首先要做的事情。最容易想到的就是调高 `mon osd full ratio` 和 `mon osd nearfull ratio` 值，但是对于生产环境，一旦调整这个全局比例，可能会导致整个集群的数据都会动起来，引发更多的数据迁移。因此另一种折衷方法就是单独调整已满 OSD 的 near full 和 full 比例；也可以使用调低  OSD 的 crush weight 的方法，使已满 OSD 上的数据迁移一部分出去。

    # 调整单个 osd 的比例
	ceph tell osd.id injectargs '--mon-osd-full-ratio .98'
    ceph tell osd.id injectargs '--mon-osd-full-ratio 0.98'
    
    # 调整 osd 的 crush weight 值
    ceph osd crush reweight osd.id {a-little-lower-weight-value}

### 2.4 OSD 龟速或无响应

一个反复出现的问题是 OSD 龟速或无响应。在深入性能问题前，你应该先确保不是其他故障。例如，确保你的网络运行正常、且 OSD 在运行，还要检查 OSD 是否被恢复流量拖住了。

**Tip：** 较新版本的 Ceph 能更好地处理恢复，可防止恢复进程耗尽系统资源而导致 `up` 且 `in` 的 OSD 不可用或响应慢。

#### 网络问题

Ceph 是一个分布式存储系统，所以它依赖于网络来互联 OSD 们、复制对象、从错误中恢复和检查心跳。网络问题会导致 OSD 延时和震荡（反复经历 up and down，详情可参考下文中的相关小节） 。

确保 Ceph 进程和 Ceph 依赖的进程已建立连接和/或在监听。

	netstat -a | grep ceph
	netstat -l | grep ceph
	sudo netstat -p | grep ceph

检查网络统计信息。

	netstat -s

#### 驱动器配置

一个存储驱动器应该只用于一个 OSD 。如果有其它进程共享驱动器，顺序读写吞吐量会成为瓶颈，包括日志、操作系统、monitor 、其它 OSD 和非 Ceph 进程。

Ceph 在日志记录完成*之后*才会确认写操作，所以使用 `ext4` 或 `XFS` 文件系统时高速的 SSD 对降低响应延时很有吸引力。与之相比， `btrfs` 文件系统可以同时读写日志和数据分区。

**注意：** 给驱动器分区并不能改变总吞吐量或顺序读写限制。把日志分离到单独的分区可能有帮助，但最好是另外一块硬盘的分区。

#### 扇区损坏 / 碎片化硬盘

检修下硬盘是否有坏扇区和碎片。这会导致总吞吐量急剧下降。

#### MON 和 OSD 共存 

Monitor 通常是轻量级进程，但它们会频繁调用 `fsync()` ，这会妨碍其它工作负载，特别是 Mon 和 OSD 共享驱动器时。另外，如果你在 OSD 主机上同时运行 Mon，遭遇的性能问题可能和这些相关：

- 运行较老的内核（低于 3.0 ）
- Argonaut 版运行在老的 glibc 之上
- 运行的内核不支持 syncfs(2) 系统调用

在这些情况下，同一主机上的多个 OSD 会相互拖垮对方。它们经常导致爆炸式写入。

#### 进程共存

共用同一套硬件、并向 Ceph 写入数据的进程（像基于云的解决方案、虚拟机和其他应用程序）会导致 OSD 延时大增。一般来说，我们建议用单独的主机跑 Ceph 、其它主机跑其它进程。实践证明把 Ceph 和其他应用程序分开可提高性能、并简化故障排除和维护。

#### 日志记录级别

如果你为追踪某问题提高过日志级别，结束后又忘了调回去，这个 OSD 将向硬盘写入大量日志。如果你想始终保持高日志级别，可以考虑给默认日志路径（即 `/var/log/ceph/$cluster-$name.log` ）挂载一个单独的硬盘。

#### 恢复限流

根据你的配置， Ceph 可以降低恢复速度来维持性能，否则它会加快恢复速度而影响 OSD 的性能。检查下 OSD 是否正在恢复。

#### 内核版本

检查下你在用的内核版本。较老的内核也许没有反合能提高 Ceph 性能的代码。

#### 内核与 SYNCFS 问题

试试在一个主机上只运行一个 OSD ，看看能否提升性能。老内核未必支持有 `syncfs(2)` 系统调用的 `glibc` 。

#### 文件系统问题

当前，我们推荐基于 `xfs` 部署集群。 `btrfs` 有很多诱人的功能，但文件系统内的缺陷可能会导致性能问题。我们不推荐使用 `ext4` ，因为 xattr 大小的限制破坏了对长对象名的支持（ RGW 需要）。

#### 内存不足

我们建议为每 OSD 进程规划 1GB 内存。你也许注意到了，通常情况下 OSD 仅会使用一小部分（如 100 - 200MB ）。你也许想用这些空闲内存跑一些其他应用，如虚拟机等等。然而当 OSD 进入恢复状态时，其内存利用率将激增。如果没有足够的可用内存，此 OSD 的性能将会明显下降。

### OLD REQUESTS 或 SLOW REQUESTS

如果某 `ceph-osd` 守护进程对一请求响应很慢，它会生成日志消息来抱怨请求耗费的时间过长。默认警告阀值是 30 秒，可以通过 `osd op complaint time` 选项来配置。这种情况发生时，集群日志会收到这些消息。

很老的版本抱怨 “old requests” ：

	osd.0 192.168.106.220:6800/18813 312 : [WRN] old request osd_op(client.5099.0:790 fatty_26485_object789 [write 0~4096] 2.5e54f643) v4 received at 2012-03-06 15:42:56.054801 currently waiting for sub ops

较新版本的 Ceph 抱怨 “slow requests” ：

	{date} {osd.num} [WRN] 1 slow requests, 1 included below; oldest blocked for > 30.005692 secs
	{date} {osd.num}  [WRN] slow request 30.005692 seconds old, received at {date-time}: osd_op(client.4240.0:8 benchmark_data_ceph-1_39426_object7 [write 0~4194304] 0.69848840) v4 currently waiting for subops from [610]

可能的原因有：

- 坏驱动器（查看 `dmesg` 输出）
- 内核文件系统缺陷（查看 `dmesg` 输出）
- 集群过载（检查系统负载、 iostat 等等）
- ceph-osd 守护进程的 bug 

可能的解决方法：

- 从 Ceph 主机分离 VM 云解决方案
- 升级内核
- 升级 Ceph
- 重启 OSD

### 2.5 震荡的 OSD

我们建议同时部署 public（前端）网络和 cluster（后端）网络，这样能更好地满足对象复制的网络性能需求。另一个优点是你可以运营一个不连接互联网的集群，以此避免某些拒绝服务攻击。 OSD 们互联和检查心跳时会优选 cluster（后端）网络。

![](http://i.imgur.com/1NvAPXa.png)

然而，如果 cluster（后端）网络失败、或出现了明显的延时，同时 public（前端）网络却运行良好， OSD 目前不能很好地处理这种情况。这时 OSD 们会向 monitor 报告邻居 `down` 了、同时报告自己是 `up` 的，我们把这种情形称为震荡（ flapping ）。

如果有原因导致 OSD 震荡（反复地被标记为 `down` ，然后又 `up` ），你可以强制 monitor 停止这种震荡状态：

	ceph osd set noup      # prevent OSDs from getting marked up
	ceph osd set nodown    # prevent OSDs from getting marked down

这些标记记录在 osdmap 数据结构里：

	ceph osd dump | grep flags
	flags no-up,no-down

可用下列命令清除标记：

	ceph osd unset noup
	ceph osd unset nodown

Ceph 还支持另外两个标记 `noin` 和 `noout` ，它们可防止正在启动的 OSD 被标记为 `in` （可以分配数据），或被误标记为 `out` （不管 `mon osd down out interval` 的值是多少）。

**注意：** `noup` 、 `noout` 和 `nodown` 从某种意义上说是临时的，一旦标记被清除了，被它们阻塞的动作短时间内就会发生。另一方面， `noin` 标记阻止 OSD 启动后加入集群，但其它守护进程都维持原样。
