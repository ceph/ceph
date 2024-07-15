# 3. 常见 PG 故障处理

----------

### 3.1 PG 无法达到 CLEAN 状态

创建一个新集群后，PG 的状态一直处于 `active` ， `active + remapped` 或 `active + degraded` 状态， 而无法达到 `active + clean` 状态 ，那很可能是你的配置有问题。

你可能需要检查下集群中有关 Pool 、 PG 和 CRUSH 的配置项，做以适当的调整。

一般来说，你的集群中需要多于 1 个 OSD，并且存储池的 size 要大于 1 副本。

#### 单节点集群

有时候，我们需要搭建一个单节点的 Ceph 实验环境。此时，在开始创建 monitor 和 OSD 之前，你需要把 Ceph 配置文件中的 `osd crush chooseleaf type` 选项从默认值 `1` （表示 `host` 或 `node`）修改为 `0` （表示 `osd`）。这样做是告诉 Ceph 允许把数据的不同副本分布到同一 host 的 OSDs 上。

#### OSD 个数小于副本数

如果你已经启动了 2 个 OSD，它们都处于 `up` 和 `in` 的状态，但 PG 仍未达到 `active + clean` 状态，那可能是给 `osd pool default size` 设置了一个大于 `2` 的值。

如果你想要在 `active + degraded` 状态（ 2 副本）操作你的集群，可以设置 `osd pool default min size` 为 2 ，这样你就可以对处于 `active + degraded` 的对象写入数据。然后你还可以把 `osd pool default size` 的值改为 2 ，这样集群就可以达到 `active + clean` 状态了。

另外，修改参数 `osd pool default size/min_size`后，只会对后面新建的 pool 起作用。如果想修改已存在的 pool 的 `size/min_size` ，可用下面的命令：

	ceph osd pool set <poolname> size|min_size <val>

**注意：** 你可以在运行时修改参数值。如果是在 Ceph 配置文件中进行的修改，你可能需要重启集群。

#### POOL SIZE = 1

如果你设置了 `osd pool default size` 的值为 `1` ，那你就仅有对象的单份拷贝。OSD 依赖于其他 OSD 告诉自己应该保存哪些对象。如果第一个 OSD 持有对象的拷贝，并且没有第二份拷贝，那么也就没有第二个 OSD 去告诉第一个 OSD 它应该保管那份拷贝。对于每一个映射到第一个 OSD 上的 PG （参考 `ceph pg dump` 的输出），你可以强制第一个 OSD 关注它应该保存的 PGs ：

	ceph pg force_create_pg <pgid>

#### CRUSH MAP 错误

PG 达不到 clean 状态的另一个可能的原因就是集群的 CRUSH Map 有错误，导致 PG 不能映射到正确的地方。

### 3.2 卡住的 PGs

有失败发生后，PG 会进入“degraded”（降级）或“peering”（连接建立中）状态，这种情况时有发生。通常这些状态意味着正常的失败恢复正在进行。然而，如果一个 PG 长时间处于这些状态中的某个，就意味着有更大的问题。因此 monitor 在 PG 卡 （ stuck ） 在非最优状态时会告警。我们具体检查：

- `inactive` （不活跃）—— PG 长时间不是 `active` （即它不能提供读写服务了）；
- `unclean` （不干净）—— PG 长时间不是 `clean` （例如它未能从前面的失败完全恢复）；
- `stale` （不新鲜）—— PG 状态没有被 `ceph-osd` 更新，表明存储这个 PG 的所有节点可能都 `down` 了。

你可以用下列命令显式地列出卡住的 PGs：

	ceph pg dump_stuck stale
	ceph pg dump_stuck inactive
	ceph pg dump_stuck unclean

卡在 `stale` 状态的 PG 通过重启 ceph-osd 进程通常可以修复；卡在 `inactive` 状态的 PG 通常是互联问题（参见  ***PG 挂了 —— 互联失败*** ）；卡在 `unclean` 状态的 PG 通常是由于某些原因阻止了恢复的完成，像未找到的对象（参见 ***未找到的对象*** ）。

### 3.3 PG 挂了 —— 互联失败

在某些情况下， `ceph-osd` *互联*进程会遇到问题，阻值 PG 达到活跃、可用的状态。例如， `ceph health` 也许显示：

	ceph health detail
	HEALTH_ERR 7 pgs degraded; 12 pgs down; 12 pgs peering; 1 pgs recovering; 6 pgs stuck unclean; 114/3300 degraded (3.455%); 1/3 in osds are down
	...
	pg 0.5 is down+peering
	pg 1.4 is down+peering
	...
	osd.1 is down since epoch 69, last address 192.168.106.220:6801/8651

可以查询到 PG 为何被标记为 `down` ：

	ceph pg 0.5 query  

	{ "state": "down+peering",
  	  ...
  	  "recovery_state": [
           { "name": "Started\/Primary\/Peering\/GetInfo",
         	 "enter_time": "2012-03-06 14:40:16.169679",
         	 "requested_info_from": []},
       	   { "name": "Started\/Primary\/Peering",
         	 "enter_time": "2012-03-06 14:40:16.169659",
         	 "probing_osds": [
               	   0,
                   1],
         	 "blocked": "peering is blocked due to down osds",
         	 "down_osds_we_would_probe": [
                   1],
         	 "peering_blocked_by": [
               	   { "osd": 1,
                 	 "current_lost_at": 0,
                 	 "comment": "starting or marking this osd lost may let us proceed"}]},
       	   { "name": "Started",
         	 "enter_time": "2012-03-06 14:40:16.169513"}
      ]
    }

`recovery_state` 段告诉我们互联过程因 `ceph-osd` 进程挂了而被阻塞，本例是 `osd.1` 挂了，启动这个进程应该就可以恢复。

或者，如果 `osd.1` 发生了灾难性的失败（如硬盘损坏），我们可以告诉集群它丢失（ `lost` ）了，让集群尽力完成副本拷贝。

**重要：** 集群不能保证其它数据副本是一致且最新的，就会很危险！

让 Ceph 无论如何都继续：

	ceph osd lost 1

恢复将继续进行。

### 3.4 未找到的对象

某几种失败相组合，可能导致 Ceph 抱怨有找不到（ `unfound` ）的对象：

	ceph health detail
	HEALTH_WARN 1 pgs degraded; 78/3778 unfound (2.065%)
	pg 2.4 is active+degraded, 78 unfound

这意味着存储集群知道一些对象（或者存在对象的较新副本）存在，却没有找到它们的副本。下例展示了这种情况是如何发生的，一个 PG 的数据存储在 ceph-osd 1 和 2 上：

- 1 挂了
- 2 独自处理一些写动作
- 1 起来了
- 1 和 2 重新互联， 1 上面丢失的对象加入队列准备恢复
- 新对象还未拷贝完， 2 挂了

这时， 1 知道这些对象存在，但是活着的 `ceph-osd` 都没有这些副本。这种情况下，读写这些对象的 IO 就会被阻塞，集群只能指望 down 掉的节点尽早恢复。这样处理是假设比直接给用户返回一个 IO 错误要好一些。

首先，你应该确认哪些对象找不到了：

	ceph pg 2.4 list_missing [starting offset, in json]

	{ "offset": { "oid": "",
     	"key": "",
     	"snapid": 0,
     	"hash": 0,
     	"max": 0},
	"num_missing": 0,
	"num_unfound": 0,
	"objects": [
       { "oid": "object 1",
      	 "key": "",
         "hash": 0,
         "max": 0 },
       ...
	],
	"more": 0}

如果在一次查询里列出的对象太多， `more` 这个字段将为 `true` ，你就可以查询更多。

其次，你可以找出哪些 OSD 上探测到、或可能包含数据：

	ceph pg 2.4 query

	"recovery_state": [
     	{ "name": "Started\/Primary\/Active",
       	  "enter_time": "2012-03-06 15:15:46.713212",
       	  "might_have_unfound": [
             	{ "osd": 1,
               	  "status": "osd is down"}]},

本例中，集群知道 `osd.1` 可能有数据，但它挂了（ `down` ）。所有可能的状态有：

- 已经探测到了
- 在查询
- OSD 挂了
- 尚未查询

有时候集群要花一些时间来查询可能的位置。

还有一种可能性，对象存在于其它位置却未被列出。例如，集群里的一个 ceph-osd 停止且被剔出集群，然后集群完全恢复了；后来一系列的失败导致了未找到的对象，它也不会觉得早已死亡的 ceph-osd 上仍可能包含这些对象。（这种情况几乎不太可能发生）。

如果所有可能的位置都查询过了但仍有对象丢失，那就得放弃丢失的对象了。这仍可能是罕见的失败组合导致的，集群在写操作恢复后，未能得知写入是否已执行。以下命令把未找到的（ `unfound` ）对象标记为丢失（ `lost` ）。

	ceph pg 2.5 mark_unfound_lost revert|delete

上述最后一个参数告诉集群应如何处理丢失的对象。

- `delete` 选项将导致完全删除它们。
- `revert` 选项（纠删码存储池不可用）会回滚到前一个版本或者（如果它是新对象的话）删除它。要慎用，它可能迷惑那些期望对象存在的应用程序。

### 3.5 无家可归的 PG

拥有 PG 拷贝的 OSD 可能会全部失败，这种情况下，那一部分的对象存储不可用， monitor 也就不会收到那些 PG 的状态更新了。为检测这种情况，monitor 会把任何主 OSD 失败的 PG 标记为 `stale` （不新鲜），例如：

	ceph health
	HEALTH_WARN 24 pgs stale; 3/300 in osds are down

可以找出哪些 PG 是 `stale` 状态，和存储这些归置组的最新 OSD ，命令如下：

    ceph health detail
    HEALTH_WARN 24 pgs stale; 3/300 in osds are down
    ...
    pg 2.5 is stuck stale+active+remapped, last acting [2,0]
    ...
    osd.10 is down since epoch 23, last address 192.168.106.220:6800/11080
    osd.11 is down since epoch 13, last address 192.168.106.220:6803/11539
    osd.12 is down since epoch 24, last address 192.168.106.220:6806/11861

如果想使 PG 2.5 重新上线，例如，上面的输出告诉我们它最后由 `osd.0` 和 `osd.2` 管理，重启这些 `ceph-osd` 将恢复之（可以假定还有其它的很多 PG 也会进行恢复 ）。

### 3.6 只有几个 OSD 接收数据

如果你的集群有很多节点，但只有其中几个接收数据，**检查**下存储池里的 PG 数量。因为 PG 是映射到多个 OSD 的，较少的 PG 将不能均衡地分布于整个集群。试着创建个新存储池，设置 PG 数量是 OSD 数量的若干倍。更详细的信息可以参考 Ceph 官方文档 —— [Placement Groups](http://docs.ceph.com/docs/master/rados/operations/placement-groups/) 。

### 3.7 不能写入数据

如果你的集群已启动，但一些 OSD 没起来，导致不能写入数据，确认下运行的 OSD 数量满足 PG 要求的最低 OSD 数。如果不能满足， Ceph 就不会允许你写入数据，因为 Ceph 不能保证复制能如愿进行。这个最低 OSD 个数是由参数 `osd pool default min size` 限定的。

### 3.8 PG 不一致
 
如果收到 `active + clean + inconsistent` 这样的状态，很可能是由于在对 PG 做擦洗（ scrubbing ）时发生了错误。如果是由于磁盘错误导致的不一致，请检查磁盘，如果磁盘有损坏，可能需要将这个磁盘对应的 OSD 踢出集群，然后进行更换。生产环境中遇到过不一致的问题，就是由于磁盘坏道导致的。

当集群中出现 PG 不一致的问题时，执行 `ceph -s` 命令会出现下面的信息：

	root@mon:~# ceph -s
	    cluster 614e77b4-c997-490a-a3f9-e89aa0274da3
	     health HEALTH_ERR
	            1 pgs inconsistent
	            1 scrub errors
	     monmap e5: 1 mons at {osd1=10.95.2.43:6789/0}
	            election epoch 796, quorum 0 osd1
	     osdmap e1079: 3 osds: 3 up, 3 in
	            flags sortbitwise
	      pgmap v312153: 384 pgs, 6 pools, 1148 MB data, 311 objects
	            3604 MB used, 73154 MB / 76759 MB avail
	                 383 active+clean
	                   1 active+clean+inconsistent

1、查找处于 `inconsistent` 状态的问题 PG ：

	root@mon:~# ceph health detail
	HEALTH_ERR 1 pgs inconsistent; 1 scrub errors
	pg 9.14 is active+clean+inconsistent, acting [1,2,0]
	1 scrub errors

这个有问题的 PG 分布在 `osd.1` 、 `osd.2` 和 `osd.0` 上，其中 `osd.1` 是主 OSD。

2、去主 OSD（ osd.1 ）的日志中查找不一致的具体对象 。

	root@osd0:~# grep -Hn 'ERR' /var/log/ceph/ceph-osd.1.log
	/var/log/ceph/ceph-osd.1.log:30:2016-11-10 13:49:07.848804 7f628c5e6700 -1 log_channel(cluster) log [ERR] : 9.14 shard 0: soid 9:29b4ad99:::rbd_data.1349f035c101d9.0000000000000001:head missing attr _
	/var/log/ceph/ceph-osd.1.log:31:2016-11-10 13:49:07.849803 7f628c5e6700 -1 log_channel(cluster) log [ERR] : 9.14 scrub 0 missing, 1 inconsistent objects
	/var/log/ceph/ceph-osd.1.log:32:2016-11-10 13:49:07.849824 7f628c5e6700 -1 log_channel(cluster) log [ERR] : 9.14 scrub 1 errors

从日志中可以知道，是 `rbd_data.1349f035c101d9.0000000000000001` 这个对象的属性 `_` 丢失了，所以在 scrub 的过程中产生了 error 。

3、执行 `ceph pg repair` 命令修复问题 PG 。

	root@mon:~# ceph pg repair 9.14
	instructing pg 9.14 on osd.1 to repair

4、检查 Ceph 集群是否恢复到 `HEALTH_OK` 状态。

	root@mon:~# ceph -s
	    cluster 614e77b4-c997-490a-a3f9-e89aa0274da3
	     health HEALTH_OK
	     monmap e5: 1 mons at {osd1=10.95.2.43:6789/0}
	            election epoch 796, quorum 0 osd1
	     osdmap e1079: 3 osds: 3 up, 3 in
	            flags sortbitwise
	      pgmap v312171: 384 pgs, 6 pools, 1148 MB data, 311 objects
	            3604 MB used, 73154 MB / 76759 MB avail
	                 384 active+clean

osd.1 的日志里也提示修复成功：

	2016-11-10 14:04:31.732640 7f628c5e6700  0 log_channel(cluster) log [INF] : 9.14 repair starts
	2016-11-10 14:04:31.827951 7f628edeb700 -1 log_channel(cluster) log [ERR] : 9.14 shard 0: soid 9:29b4ad99:::rbd_data.1349f035c101d9.0000000000000001:head missing attr _
	2016-11-10 14:04:31.828117 7f628edeb700 -1 log_channel(cluster) log [ERR] : 9.14 repair 0 missing, 1 inconsistent objects
	2016-11-10 14:04:31.828273 7f628edeb700 -1 log_channel(cluster) log [ERR] : 9.14 repair 1 errors, 1 fixed

如果经过前面的步骤，Ceph 仍没有达到 `HEALTH_OK` 状态，可以尝试用下面这种方式进行修复。

1、停掉不一致的 object 所属的 osd 。

	stop ceph-osd id=xxx

2、刷新该 osd 的日志。

	ceph-osd -i xx --flush-journal

3、将不一致的 object 移除。

	mv /var/lib/ceph/osd/ceph-{osd-id}/current/{pg.id}_head/ rbd\\udata.xxx /home

4、重新启动该 osd 。

	start ceph-osd id=xx

5、重新执行修复命令。

	ceph pg repair {pg_id}

6、检查 Ceph 集群是否恢复到 `HEALTH_OK` 状态。


### 3.9 Too Many/Few PGs per OSD

有时候，我们在 ceph -s 的输出中可以看到如下的告警信息：

	root@node241:~# ceph -s
    	cluster 3b37db44-f401-4409-b3bb-75585d21adfe
	     health HEALTH_WARN
	            too many PGs per OSD (652 > max 300)
	     monmap e1: 1 mons at {node241=192.168.2.41:6789/0}
	            election epoch 1, quorum 0 node241
	     osdmap e408: 5 osds: 5 up, 5 in
	      pgmap v23049: 1088 pgs, 16 pools, 256 MB data, 2889 objects
	            6100 MB used, 473 GB / 479 GB avail
	                 1088 active+clean

这是因为集群 OSD 数量较少，测试过程中建立了多个存储池，每个存储池都要建立一些 PGs 。而目前 Ceph 配置的默认值是每 OSD 上最多有 300 个 PGs 。在测试环境中，为了快速解决这个问题，可以调大集群的关于此选项的告警阀值。方法如下：

在 monitor 节点的 ceph.conf 配置文件中添加:

    [global]
    .......
    mon_pg_warn_max_per_osd = 1000

然后重启 monitor 进程。

或者直接用 `tell` 命令在运行时更改参数的值而不用重启服务：

	ceph tell mon.* injectargs '--mon_pg_warn_max_per_osd 1000'

而另一种情况， `too few PGs per OSD （16 < min 20）` 这样的告警信息则往往出现在集群刚刚建立起来，除了默认的 rbd 存储池，还没建立自己的存储池，再加上 OSD 个数较多，就会出现这个提示信息。这通常不是什么问题，也无需修改配置项，在建立了自己的存储池后，这个告警信息就会消失。
