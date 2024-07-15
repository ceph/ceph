# 1. 常见 MON 故障处理

----------

Monitor 维护着 Ceph 集群的信息，如果 Monitor 无法正常提供服务，那整个 Ceph 集群就不可访问。一般来说，在实际运行中，Ceph Monitor的个数是 2n + 1 ( n >= 0) 个，在线上至少3个，只要正常的节点数 >= n+1，Ceph 的 Paxos 算法就能保证系统的正常运行。所以，当 Monitor 出现故障的时候，不要惊慌，冷静下来，一步一步地处理。

### 1.1 开始排障

在遭遇 Monitor 故障时，首先回答下列几个问题：

**Mon 进程在运行吗？**

我们首先要确保 Mon 进程是在正常运行的。很多人往往忽略了这一点。

**是否可以连接 Mon Server？**

有时候我们开启了防火墙，导致无法与 Monitor 的 IP 或端口进行通信。尝试使用 `ssh` 连接服务器，如果成功，再尝试用其他工具（如 `telnet` ， `nc` 等）连接 monitor 的端口。

**ceph -s 命令是否能运行并收到集群回复？**

如果答案是肯定的，那么你的集群已启动并运行着。你可以认为如果已经形成法定人数，monitors 就只会响应 `status` 请求。
  
如果 `ceph -s` 阻塞了，并没有收到集群的响应且输出了很多 `fault` 信息，很可能此时你的 monitors 全部都 down 掉了或只有部分在运行（但数量不足以形成法定人数）。

**ceph -s 没完成是什么情况？**

如果你到目前为止没有完成前述的几步，请返回去完成。然后你需要 `ssh` 登录到服务器上并使用 monitor 的管理套接字。

### 1.2 使用 Mon 的管理套接字

通过管理套接字，你可以用 Unix 套接字文件直接与指定守护进程交互。这个文件位于你 Mon 节点的 `run` 目录下，默认配置它位于 `/var/run/ceph/ceph-mon.ID.asok `，但要是改过配置就不一定在那里了。如果你在那里没找到它，请检查 `ceph.conf` 里是否配置了其它路径，或者用下面的命令获取：

    ceph-conf --name mon.ID --show-config-value admin_socket

请牢记，只有在 Mon 运行时管理套接字才可用。Mon 正常关闭时，管理套接字会被删除；如果 Mon 不运行了、但管理套接字还存在，就说明 Mon 不是正常关闭的。不管怎样，Mon 没在运行，你就不能使用管理套接字， `ceph` 命令会返回类似 `Error 111: Connection Refused` 的错误消息。

访问管理套接字很简单，就是让 `ceph` 工具使用 `asok` 文件。对于 Dumpling 之前的版本，命令是这样的：

	ceph --admin-daemon /var/run/ceph/ceph-mon.<id>.asok <command>

对于 Dumpling 及后续版本，你可以用另一个（推荐的）命令：

	ceph daemon mon.<id> <command>

`ceph` 工具的 `help` 命令会显示管理套接字支持的其它命令。请仔细了解一下 `config get` 、 `config show` 、 `mon_status` 和 `quorum_status` 命令，在排除 Mon 故障时它们会很有用。

### 1.3 理解 MON_STATUS

当集群形成法定人数后，或在没有形成法定人数时通过管理套接字， 用 `ceph` 工具可以获得 `mon_status` 信息。命令会输出关于 monitor 的大多数信息，包括部分 `quorum_status` 命令的输出内容。

下面是 `mon_status` 的输出样例：

	{
		"name": "c",
  		"rank": 2,
  		"state": "peon",
  		"election_epoch": 38,
  		"quorum": [
        	1,
        	2
		],
  		"outside_quorum": [],
  		"extra_probe_peers": [],
  		"sync_provider": [],
  		"monmap": { 
			"epoch": 3,
      		"fsid": "5c4e9d53-e2e1-478a-8061-f543f8be4cf8",
      		"modified": "2013-10-30 04:12:01.945629",
      		"created": "2013-10-29 14:14:41.914786",
      		"mons": [
            	{ 	
					"rank": 0,
              		"name": "a",
              		"addr": "127.0.0.1:6789\/0"
				},
            	{ 
					"rank": 1,
              		"name": "b",
              		"addr": "127.0.0.1:6790\/0"
				},
            	{ 
					"rank": 2,
              		"name": "c",
              		"addr": "127.0.0.1:6795\/0"
				}
			]
		}
	}

从上面的信息可以看出， monmap 中包含 3 个monitor （ *a*，*b* 和 *c*），只有 2 个 monitor 形成了法定人数， *c* 是法定人数中的 *peon* 角色（非 *leader* 角色）。

还可以看出， **a** 并不在法定人数之中。请看 `quorum` 集合。在集合中只有 *1* 和 *2* 。这不是 monitor 的名字，而是它们加入当前 monmap 后确定的等级。丢失了等级 0 的 monitor，根据 monmap ，这个 monitor 就是 `mon.a` 。

那么，monitor 的等级是如何确定的？

当加入或删除 monitor 时，会（重新）计算等级。计算时遵循一个简单的规则： `IP:PORT` 的组合值越**大**， 等级越**低**（等级数字越大，级别越低）。因此在上例中， `127.0.0.1:6789` 比其他 `IP:PORT` 的组合值都小，所以 `mon.a` 的等级是 0 。

### 1.4 最常见的 Mon 问题

#### 达到了法定人数但是有至少一个 Monitor 处于 Down 状态

当此种情况发生时，根据你运行的 Ceph 版本，可能看到类似下面的输出：

	root@OPS-ceph1:~# ceph health detail
	HEALTH_WARN 1 mons down, quorum 1,2 b,c
	mon.a (rank 0) addr 127.0.0.1:6789/0 is down (out of quorum)

##### 如何解决？

首先，确认 mon.a 进程是否运行。

其次，确定可以从其他 monitor 节点连到 `mon.a` 所在节点。同时检查下端口。如果开了防火墙，还需要检查下所有 monitor 节点的 `iptables` ，以确定没有丢弃/拒绝连接。

如果前两步没有解决问题，请继续往下走。

首先，通过管理套接字检查问题 monitor 的 `mon_status` 。考虑到该 monitor 并不在法定人数中，它的状态应该是  `probing` ， `electing` 或 `synchronizing` 中的一种。如果它恰巧是 `leader` 或 `peon` 角色，它会认为自己在法定人数中，但集群中其他 monitor 并不这样认为。或者在我们处理故障的过程中它加入了法定人数，所以再次使用 `ceph -s` 确认下集群状态。如果该 monitor 还没加入法定人数，继续。

**`probing` 状态是什么情况？**

这意味着该 monitor 还在搜寻其他 monitors 。每次你启动一个 monitor，它会去搜寻 `monmap` 中的其他 monitors ，所以会有一段时间处于该状态。此段时间的长短不一。例如，单节点 monitor 环境， monitor 几乎会立即通过该阶段。在多 monitor 环境中，monitors 在找到足够的节点形成法定人数之前，都会处于该状态，这意味着如果 3 个 monitor 中的 2 个 down 了，剩下的 1 个会一直处于该状态，直到你再启动一个 monitor 。

如果你的环境已经形成法定人数，只要 monitor 之间可以互通，新 monitor 应该可以很快搜寻到其他 monitors 。如果卡在 probing 状态，并且排除了连接的问题，那很有可能是该 monitor 在尝试连接一个错误的 monitor 地址。可以根据 `mon_status` 命令输出中的 `monmap` 内容，检查其他 monitor 的地址是否和实际相符。如果不相符，请跳至**恢复 Monitor 损坏的 monmap**。如果相符，这些 monitor 节点间可能存在严重的时钟偏移问题，请首先参考**时钟偏移**，如果没有解决问题，可以搜集相关的日志并向社区求助。

**`electing` 状态是什么情况？**

这意味着该 monitor 处于选举过程中。选举应该很快就可以完成，但偶尔也会卡住，这往往是 monitors 节点时钟偏移的一个标志，跳转至**时钟偏移**获取更多信息。如果时钟是正确同步的，可以搜集相关日志并向社区求助。此种情况除非是一些（*非常*）古老的 bug ，往往都是由时钟不同步引起的。

**`synchronizing` 状态是什么情况？**

这意味着该 monitor 正在和集群中的其他 monitor 进行同步以便加入法定人数。Monitor 的数据库越小，同步过程的耗时就越短。

然而，如果你注意到 monitor 的状态从 synchronizing 变为 electing 后又变回 synchronizing ，那么就有问题了：集群的状态更新的太快（即产生新的 maps ），同步过程已经无法追赶上了。这种情况在早期版本中可以见到，但现在经过代码重构和增强，在较新版本中已经基本见不到了。

**`leader` 或 `peon` 状态是什么情况？**

这种情况不应该发生，但还是有一定概率会发生，这常和时钟偏移有关。如果你并没有时钟偏移的问题，请搜集相关的日志并向社区求助。

#### 恢复 Monitor 损坏的 monmap

monmap 通常看起来是下面的样子，这取决于 monitor 的个数：

    epoch 3
    fsid 5c4e9d53-e2e1-478a-8061-f543f8be4cf8
    last_changed 2013-10-30 04:12:01.945629
    created 2013-10-29 14:14:41.914786
    0: 127.0.0.1:6789/0 mon.a
    1: 127.0.0.1:6790/0 mon.b
    2: 127.0.0.1:6795/0 mon.c

不过也不一定就是这样的内容。比如，在早期版本的 Ceph 中，有一个严重 bug 会导致 `monmap` 的内容全为 0 。这意味着即使用 `monmaptool` 也不能读取它，因为全 0 的内容没有任何意义。另外一些情况，某个 monitor 所持有的 monmap 已严重过期，以至于无法搜寻到集群中的其他 monitors 。在这些状况下，你有两种可行的解决方法：

**销毁 monitor 然后新建**

只有在你确定不会丢失保存在该 monitor 上的数据时，你才能够采用这个方法。也就是说，集群中还有其他运行正常的 monitors，以便新 monitor 可以和其他 monitors 达到同步。请谨记，销毁一个 monitor 时，如果没有其上数据的备份，可能会丢失数据。

**给 monitor 手动注入 monmap**

通常是最安全的做法。你应该从剩余的 monitor 中抓取 monmap，然后手动注入 monmap 有问题的 monitor 节点。

下面是基本步骤：

1、是否已形成法定人数？如果是，从法定人数中抓取 monmap ：

	ceph mon getmap -o /tmp/monmap

2、没有形成法定人数？直接从其他 monitor 节点上抓取 monmap （这里假定你抓取 monmap 的 monitor 的 id 是 ID-FOO 并且守护进程已经停止运行）：

	ceph-mon -i ID-FOO --extract-monmap /tmp/monmap

3、停止你想要往其中注入 monmap 的 monitor。

4、注入 monmap 。

	ceph-mon -i ID --inject-monmap /tmp/monmap

5、启动 monitor 。

请记住，能够注入 monmap 是一个很强大的特性，如果滥用可能会对 monitor 造成大破坏，因为这样做会覆盖 monitor 持有的最新 monmap 。

#### 时钟偏移

Monitor 节点间明显的时钟偏移会对 monitor 造成严重的影响。这通常会导致一些奇怪的问题。为了避免这些问题，在 monitor 节点上应该运行时间同步工具。

**允许的最大时钟偏移量是多少？**

默认最大允许的时钟偏移量是 `0.05 秒`。

**如何增加最大时钟偏移量？**

通过 mon-clock-drift-allowed 选项来配置。尽管你 ***可以*** 修改但不代表你 ***应该*** 修改。时钟偏移机制之所以是合理的，是因为有时钟偏移的 monitor 可能会表现不正常。未经测试而修改该值，尽管没有丢失数据的风险，但仍可能会对 monitors 的稳定性和集群的健康造成不可预知的影响。

**如何知道是否存在时钟偏移？**

Monitor 会用 `HEALTH_WARN` 的方式警告你。 `ceph health detail` 应该输出如下格式的信息：

	mon.c addr 10.10.0.1:6789/0 clock skew 0.08235s > max 0.05s (latency 0.0045s)

这表示 `mon.c` 已被标记出正在遭受时钟偏移。

**如果存在时钟偏移该怎么处理？**

同步各 monitor 节点的时钟。运行 NTP 客户端会有帮助。如果你已经启动了 NTP 服务，但仍遭遇此问题，检查一下使用的 NTP 服务器是否离你的网络太过遥远，然后可以考虑在你的网络环境中运行自己的 NTP 服务器。最后这种选择可趋于减少 monitor 时钟偏移带来的问题。

#### 客户端无法连接或挂载

检查 IP 过滤表。某些操作系统安装工具会给 `iptables` 增加一条 `REJECT` 规则。这条规则会拒绝所有尝试连接该主机的客户端（除了 `ssh` ）。如果你的 monitor 主机设置了这条防火墙 `REJECT` 规则，客户端从其他节点连接过来时就会超时失败。你需要定位出拒绝客户端连接 Ceph 守护进程的那条 `iptables` 规则。比如，你需要对类似于下面的这条规则进行适当处理：

	REJECT all -- anywhere anywhere reject-with icmp-host-prohibited

你还需要给 Ceph 主机的 IP 过滤表增加规则，以确保客户端可以访问 Ceph monitor （默认端口 6789 ）和 Ceph OSD （默认 6800 ~ 7300 ）的相关端口。

	iptables -A INPUT -m multiport -p tcp -s {ip-address}/{netmask} --dports 6789,6800:7300 -j ACCEPT

或者，如果你的环境**允许**，也可以直接关闭主机的防火墙。

### 1.5 Monitor 数据库失败

#### 数据库崩溃的表现

Ceph monitor 把集群的各种 map 信息存放在 key/value 数据库中，如 LevelDB 。如果 monitor 是因为数据库崩溃而失败，在 monitor 的 log 日志中应该会有如下错误信息：

	Corruption: error in middle of record

或：

	Corruption: 1 missing files; e.g.: /var/lib/ceph/mon/mon.0/store.db/1234567.ldb

#### 通过健康的 Monitor(s) 恢复

如果还有幸存的 monitor，我们通常可以用新的数据库替换崩溃的数据库。并且在启动后，新加入的成员会和其他健康的伙伴进行同步，一旦同步完成，它就可以为客户端提供服务了。

#### 通过 OSDs 恢复

但是万一所有的 monitors 都同时失败了该怎么办？由于建议用户在部署集群时至少安装 3 个 monitors，同时失效的可能性较小。但是数据中心意外的断电，再加上磁盘/文件系统配置不当，可能会引起底层文件系统失败，从而杀掉所有的 monitors 。这种情况下，我们可以通过存放在 OSDs 上的信息来恢复 monitor 的数据库：

	ms=/tmp/mon-store
	mkdir $ms
	# collect the cluster map from OSDs
	for host in $hosts; do
  		rsync -avz $ms user@host:$ms
  		rm -rf $ms
  		ssh user@host <<EOF
    		for osd in /var/lib/osd/osd-*; do
      			ceph-objectstore-tool --data-path $osd --op update-mon-db --mon-store-path $ms
    		done
  		EOF
  		rsync -avz user@host:$ms $ms
	done
	# rebuild the monitor store from the collected map, if the cluster does not
	# use cephx authentication, we can skip the following steps to update the
	# keyring with the caps, and there is no need to pass the "--keyring" option.
	# i.e. just use "ceph-monstore-tool /tmp/mon-store rebuild" instead
	ceph-authtool /path/to/admin.keyring -n mon. \
	  --cap mon allow 'allow *'
	ceph-authtool /path/to/admin.keyring -n client.admin \
	  --cap mon allow 'allow *' --cap osd 'allow *' --cap mds 'allow *'
	ceph-monstore-tool /tmp/mon-store rebuild -- --keyring /path/to/admin.keyring
	# backup corrupted store.db just in case
	mv /var/lib/ceph/mon/mon.0/store.db /var/lib/ceph/mon/mon.0/store.db.corrupted
	mv /tmp/mon-store/store.db /var/lib/ceph/mon/mon.0/store.db

上面的这些步骤：

1. 从所有的 OSD 主机上收集 map 信息。
2. 重建数据库。
3. 用恢复副本替换 mon.0 上崩溃的数据库。

**已知的限制**

下面这些信息无法通过上述步骤恢复：

- **一些新增的 keyring** ： 通过 `ceph auth add` 命令增加的所有 OSD keyrings 都可以恢复。用 `ceph-monstore-tool` 可以导入 `client.admin` 的 keyring 。但是 MDS 和其他 keyrings 在被恢复的那个 monitor 数据库中就会丢失。你可能需要手动重新添加一下。  
- **pg 的设置**：通过 `ceph pg set_full_ratio` 和 `ceph pg set_nearfull_ratio` 命令设置的 `full ratio` 和 `nearfull ratio` 值会丢失。  
- **MDS Maps**：MDS maps 会丢失。

#### 磁盘空间不足导致 MON DOWN

当 monitor 进程检测到本地可用磁盘空间不足时，会停止 monitor 服务。Monitor 的日志中应该会有类似如下信息的输出：

	2016-09-01 16:45:54.994488 7fb1cac09700  0 mon.jyceph01@0(leader).data_health(62) update_stats avail 5% total 297 GB, used 264 GB, avail 18107 MB
	2016-09-01 16:45:54.994747 7fb1cac09700 -1 mon.jyceph01@0(leader).data_health(62) reached critical levels of available space on local monitor storage -- shutdown!

清理本地磁盘，增大可用空间，重启 monitor 进程，即可恢复正常。

#### 通过 Mon 数据库的备份恢复

具体请参考本手册第三部分 [2. Monitor 的备份和恢复](../Advance_usage/mon_bakcup.md) 。
