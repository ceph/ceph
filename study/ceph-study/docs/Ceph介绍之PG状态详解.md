# 1. PG介绍
继上次分享的[《Ceph介绍及原理架构分享》](https://www.jianshu.com/p/cc3ece850433)，这次主要来分享Ceph中的PG各种状态详解，PG是最复杂和难于理解的概念之一，PG的复杂如下：
 - 在架构层次上，PG位于RADOS层的中间。 
     a. 往上负责接收和处理来自客户端的请求。
     b. 往下负责将这些数据请求翻译为能够被本地对象存储所能理解的事务。
 - 是组成存储池的基本单位，存储池中的很多特性，都是直接依托于PG实现的。
 - 面向容灾域的备份策略使得一般而言的PG需要执行跨节点的分布式写，因此数据在不同节点之间的同步、恢复时的数据修复也都是依赖PG完成。 


# 2. PG状态表
正常的PG状态是 100%的active + clean， 这表示所有的PG是可访问的，所有副本都对全部PG都可用。
如果Ceph也报告PG的其他的警告或者错误状态。PG状态表：

| 状态   |      描述      |
|----------|---------------|
| Activating |  Peering已经完成，PG正在等待所有PG实例同步并固化Peering的结果(Info、Log等)  |
| Active | 活跃态。PG可以正常处理来自客户端的读写请求 |
| Backfilling | 正在后台填充态。 backfill是recovery的一种特殊场景，指peering完成后，如果基于当前权威日志无法对Up Set当中的某些PG实例实施增量同步(例如承载这些PG实例的OSD离线太久，或者是新的OSD加入集群导致的PG实例整体迁移) 则通过完全拷贝当前Primary所有对象的方式进行全量同步 |
| Backfill-toofull | 某个需要被Backfill的PG实例，其所在的OSD可用空间不足，Backfill流程当前被挂起 |
| Backfill-wait| 等待Backfill 资源预留 |
| Clean | 干净态。PG当前不存在待修复的对象， Acting Set和Up Set内容一致，并且大小等于存储池的副本数 |
| Creating | PG正在被创建 |
| Deep | PG正在或者即将进行对象一致性扫描清洗 |
| Degraded | 降级状态。Peering完成后，PG检测到任意一个PG实例存在不一致(需要被同步/修复)的对象，或者当前ActingSet 小于存储池副本数 |
| Down | Peering过程中，PG检测到某个不能被跳过的Interval中(例如该Interval期间，PG完成了Peering，并且成功切换至Active状态，从而有可能正常处理了来自客户端的读写请求),当前剩余在线的OSD不足以完成数据修复  |
| Incomplete | Peering过程中， 由于 a. 无非选出权威日志 b. 通过choose_acting选出的Acting Set后续不足以完成数据修复，导致Peering无非正常完成 |
| Inconsistent | 不一致态。集群清理和深度清理后检测到PG中的对象在副本存在不一致，例如对象的文件大小不一致或Recovery结束后一个对象的副本丢失 |
| Peered | Peering已经完成，但是PG当前ActingSet规模小于存储池规定的最小副本数(min_size) |
| Peering | 正在同步态。PG正在执行同步处理 |
| Recovering | 正在恢复态。集群正在执行迁移或同步对象和他们的副本 |
| Recovering-wait | 等待Recovery资源预留 |
| Remapped | 重新映射态。PG活动集任何的一个改变，数据发生从老活动集到新活动集的迁移。在迁移期间还是用老的活动集中的主OSD处理客户端请求，一旦迁移完成新活动集中的主OSD开始处理 |
| Repair | PG在执行Scrub过程中，如果发现存在不一致的对象，并且能够修复，则自动进行修复状态 |
| Scrubbing | PG正在或者即将进行对象一致性扫描 |
| Unactive | 非活跃态。PG不能处理读写请求 |
| Unclean | 非干净态。PG不能从上一个失败中恢复 |
| Stale | 未刷新态。PG状态没有被任何OSD更新，这说明所有存储这个PG的OSD可能挂掉, 或者Mon没有检测到Primary统计信息(网络抖动) |
| Undersized | PG当前Acting Set小于存储池副本数 |

# 3. 状态详解及故障模拟复现
## 3.1 Degraded
### 3.1.1 说明
 - 降级：由上文可以得知，每个PG有三个副本，分别保存在不同的OSD中，在非故障情况下，这个PG是active+clean 状态，那么，如果PG 的 副本osd.4 挂掉了，这个 PG 是降级状态。

### 3.1.2 故障模拟
 a. 停止osd.1
```
$ systemctl stop ceph-osd@1
```
b. 查看PG状态
```
$ bin/ceph pg stat
20 pgs: 20 active+undersized+degraded; 14512 kB data, 302 GB used, 6388 GB / 6691 GB avail; 12/36 objects degraded (33.333%)
```
c. 查看集群监控状态
```
$ bin/ceph health detail
HEALTH_WARN 1 osds down; Degraded data redundancy: 12/36 objects degraded (33.333%), 20 pgs unclean, 20 pgs degraded; application not enabled on 1 pool(s)
OSD_DOWN 1 osds down
    osd.1 (root=default,host=ceph-xx-cc00) is down
PG_DEGRADED Degraded data redundancy: 12/36 objects degraded (33.333%), 20 pgs unclean, 20 pgs degraded
    pg 1.0 is active+undersized+degraded, acting [0,2]
    pg 1.1 is active+undersized+degraded, acting [2,0]
```
d. 客户端IO操作
```
#写入对象
$ bin/rados -p test_pool put myobject ceph.conf

#读取对象到文件
$ bin/rados -p test_pool get myobject.old

#查看文件
$ ll ceph.conf*
-rw-r--r-- 1 root root 6211 Jun 25 14:01 ceph.conf
-rw-r--r-- 1 root root 6211 Jul  3 19:57 ceph.conf.old
```

**故障总结**：
为了模拟故障，(size = 3, min_size = 2) 我们手动停止了 osd.1，然后查看PG状态，可见，它此刻的状态是active+undersized+degraded,当一个 PG 所在的 OSD 挂掉之后，这个 PG 就会进入undersized+degraded 状态，而后面的[0,2]的意义就是还有两个副本存活在 osd.0 和 osd.2 上, 并且这个时候客户端可以正常读写IO。

### 3.1.3 总结
- 降级就是在发生了一些故障比如OSD挂掉之后，Ceph 将这个 OSD 上的所有 PG 标记为 Degraded。
- 降级的集群可以正常读写数据，降级的 PG 只是相当于小毛病而已，并不是严重的问题。
- Undersized的意思就是当前存活的PG 副本数为 2，小于副本数3，将其做此标记，表明存货副本数不足，也不是严重的问题。

## 3.2 Peered
### 3.2.1 说明
 - Peering已经完成，但是PG当前Acting Set规模小于存储池规定的最小副本数(min_size)。

### 3.2.2 故障模拟

a. 停掉两个副本osd.1,osd.0
```
$ systemctl stop ceph-osd@1
$ systemctl stop ceph-osd@0
```
b. 查看集群健康状态
```
$ bin/ceph health detail
HEALTH_WARN 1 osds down; Reduced data availability: 4 pgs inactive; Degraded data redundancy: 26/39 objects degraded (66.667%), 20 pgs unclean, 20 pgs degraded; application not enabled on 1 pool(s)
OSD_DOWN 1 osds down
    osd.0 (root=default,host=ceph-xx-cc00) is down
PG_AVAILABILITY Reduced data availability: 4 pgs inactive
    pg 1.6 is stuck inactive for 516.741081, current state undersized+degraded+peered, last acting [2]
    pg 1.10 is stuck inactive for 516.737888, current state undersized+degraded+peered, last acting [2]
    pg 1.11 is stuck inactive for 516.737408, current state undersized+degraded+peered, last acting [2]
    pg 1.12 is stuck inactive for 516.736955, current state undersized+degraded+peered, last acting [2]
PG_DEGRADED Degraded data redundancy: 26/39 objects degraded (66.667%), 20 pgs unclean, 20 pgs degraded
    pg 1.0 is undersized+degraded+peered, acting [2]
    pg 1.1 is undersized+degraded+peered, acting [2]
```
c. 客户端IO操作(夯住) 
``` 
#读取对象到文件，夯住IO
$ bin/rados -p test_pool get myobject  ceph.conf.old
```
**故障总结：**
- 现在pg 只剩下osd.2上存活，并且 pg 还多了一个状态：peered，英文的意思是仔细看，这里我们可以理解成协商、搜索。
- 这时候读取文件，会发现指令会卡在那个地方一直不动，为什么就不能读取内容了，因为我们设置的 min_size=2 ，如果存活数少于2，比如这里的 1 ，那么就不会响应外部的IO请求。

d. 调整min_size=1可以解决IO夯住问题
``` 
#设置min_size = 1
$ bin/ceph osd pool set test_pool min_size 1
set pool 1 min_size to 1
``` 
e. 查看集群监控状态
```
$ bin/ceph health detail
HEALTH_WARN 1 osds down; Degraded data redundancy: 26/39 objects degraded (66.667%), 20 pgs unclean, 20 pgs degraded, 20 pgs undersized; application not enabled on 1 pool(s)
OSD_DOWN 1 osds down
    osd.0 (root=default,host=ceph-xx-cc00) is down
PG_DEGRADED Degraded data redundancy: 26/39 objects degraded (66.667%), 20 pgs unclean, 20 pgs degraded, 20 pgs undersized
    pg 1.0 is stuck undersized for 65.958983, current state active+undersized+degraded, last acting [2]
    pg 1.1 is stuck undersized for 65.960092, current state active+undersized+degraded, last acting [2]
    pg 1.2 is stuck undersized for 65.960974, current state active+undersized+degraded, last acting [2]
```
f. 客户端IO操作
```
#读取对象到文件中
$ ll -lh ceph.conf*
-rw-r--r-- 1 root root 6.1K Jun 25 14:01 ceph.conf
-rw-r--r-- 1 root root 6.1K Jul  3 20:11 ceph.conf.old
-rw-r--r-- 1 root root 6.1K Jul  3 20:11 ceph.conf.old.1
```
**故障总结：**
- 可以看到，PG状态Peered没有了，并且客户端文件IO可以正常读写了。
- 当min_size=1时，只要集群里面有一份副本活着，那就可以响应外部的IO请求。

### 3.2.3 总结
 - Peered状态我们这里可以将它理解成它在等待其他副本上线。
 - 当min_size = 2 时，也就是必须保证有两个副本存活的时候就可以去除Peered这个状态。
 - 处于 Peered 状态的 PG 是不能响应外部的请求的并且IO被挂起。

## 3.3 Remapped
### 3.3.1 说明
 - Peering完成，PG当前Acting Set与Up Set不一致就会出现Remapped状态。

### 3.3.2 故障模拟
a. 停止osd.x
```
$ systemctl stop ceph-osd@x
```
b. 间隔5分钟，启动osd.x
```
$ systemctl start ceph-osd@x
```
c. 查看PG状态
```
$ ceph pg stat
1416 pgs: 6 active+clean+remapped, 1288 active+clean, 3 stale+active+clean, 119 active+undersized+degraded; 74940 MB data, 250 GB used, 185 TB / 185 TB avail; 1292/48152 objects degraded (2.683%)
$ ceph pg dump | grep remapped
dumped all
13.cd         0                  0        0         0       0         0    2        2      active+clean+remapped 2018-07-03 20:26:14.478665       9453'2   20716:11343    [10,23]         10 [10,23,14]             10       9453'2 2018-07-03 20:26:14.478597          9453'2 2018-07-01 13:11:43.262605
3.1a         44                  0        0         0       0 373293056 1500     1500      active+clean+remapped 2018-07-03 20:25:47.885366  20272'79063  20716:109173     [9,23]          9  [9,23,12]              9  20272'79063 2018-07-03 03:14:23.960537     20272'79063 2018-07-03 03:14:23.960537
5.f           0                  0        0         0       0         0    0        0      active+clean+remapped 2018-07-03 20:25:47.888430          0'0   20716:15530     [23,8]         23  [23,8,22]             23          0'0 2018-07-03 06:44:05.232179             0'0 2018-06-30 22:27:16.778466
3.4a         45                  0        0         0       0 390070272 1500     1500      active+clean+remapped 2018-07-03 20:25:47.886669  20272'78385  20716:108086     [7,23]          7  [7,23,17]              7  20272'78385 2018-07-03 13:49:08.190133      7998'78363 2018-06-28 10:30:38.201993
13.102        0                  0        0         0       0         0    5        5      active+clean+remapped 2018-07-03 20:25:47.884983       9453'5   20716:11334     [1,23]          1  [1,23,14]              1       9453'5 2018-07-02 21:10:42.028288          9453'5 2018-07-02 21:10:42.028288
13.11d        1                  0        0         0       0   4194304 1539     1539      active+clean+remapped 2018-07-03 20:25:47.886535  20343'22439   20716:86294     [4,23]          4  [4,23,15]              4  20343'22439 2018-07-03 17:21:18.567771     20343'22439 2018-07-03 17:21:18.567771#2分钟之后查询$ ceph pg stat
1416 pgs: 2 active+undersized+degraded+remapped+backfilling, 10 active+undersized+degraded+remapped+backfill_wait, 1401 active+clean, 3 stale+active+clean; 74940 MB data, 247 GB used, 179 TB / 179 TB avail; 260/48152 objects degraded (0.540%); 49665 kB/s, 9 objects/s recovering$ ceph pg dump | grep remapped
dumped all
13.1e8 2 0 2 0 0 8388608 1527 1527 active+undersized+degraded+remapped+backfill_wait 2018-07-03 20:30:13.999637 9493'38727 20754:165663 [18,33,10] 18 [18,10] 18 9493'38727 2018-07-03 19:53:43.462188 0'0 2018-06-28 20:09:36.303126
```
d. 客户端IO操作
``` 
#rados读写正常
rados -p test_pool put myobject /tmp/test.log
```

### 3.3.3 总结
- 在 OSD 挂掉或者在扩容的时候PG 上的OSD会按照Crush算法重新分配PG 所属的osd编号。并且会把 PG  Remap到别的OSD上去。
- Remapped状态时，PG当前Acting Set与Up Set不一致。
- 客户端IO可以正常读写。

## 3.4 Recovery
### 3.4.1 说明
 - 指PG通过PGLog日志针对数据不一致的对象进行同步和修复的过程。

### 3.4.2 故障模拟 
a. 停止osd.x
```
$ systemctl stop ceph-osd@x
```
b. 间隔1分钟启动osd.x
```
osd$ systemctl start ceph-osd@x
```
c. 查看集群监控状态
```
$ ceph health detail
HEALTH_WARN Degraded data redundancy: 183/57960 objects degraded (0.316%), 17 pgs unclean, 17 pgs degraded
PG_DEGRADED Degraded data redundancy: 183/57960 objects degraded (0.316%), 17 pgs unclean, 17 pgs degraded
    pg 1.19 is active+recovery_wait+degraded, acting [29,9,17]
```
### 3.4.3 总结
 - Recovery是通过记录的PGLog进行恢复数据的。
- 记录的PGLog 在osd_max_pg_log_entries=10000条以内，这个时候通过PGLog就能增量恢复数据。

## 3.5 Backfill
### 3.5.1 说明
 - 当PG的副本无非通过PGLog来恢复数据，这个时候就需要进行全量同步，通过完全拷贝当前Primary所有对象的方式进行全量同步。

### 3.5.2 故障模拟
a. 停止osd.x
```
$ systemctl stop ceph-osd@x
```
b. 间隔10分钟启动osd.x
```
$ osd systemctl start ceph-osd@x
```
c. 查看集群健康状态
```
$ ceph health detail
HEALTH_WARN Degraded data redundancy: 6/57927 objects degraded (0.010%), 1 pg unclean, 1 pg degraded
PG_DEGRADED Degraded data redundancy: 6/57927 objects degraded (0.010%), 1 pg unclean, 1 pg degraded
    pg 3.7f is active+undersized+degraded+remapped+backfilling, acting [21,29] 
```
### 3.5.3 总结
 - 无法根据记录的PGLog进行恢复数据时，就需要执行Backfill过程全量恢复数据。
 - 如果超过osd_max_pg_log_entries=10000条， 这个时候需要全量恢复数据。

## 3.6 Stale
### 3.6.1 说明
 - mon检测到当前PG的Primary所在的osd宕机。
 - Primary超时未向mon上报pg相关的信息(例如网络阻塞)。
 - PG内三个副本都挂掉的情况。

### 3.6.2 故障模拟
a. 分别停止PG中的三个副本osd, 首先停止osd.23
```
$ systemctl stop ceph-osd@23
```
b. 然后停止osd.24
```
$ systemctl stop ceph-osd@24
```
c. 查看停止两个副本PG 1.45的状态(undersized+degraded+peered)
```
$ ceph health detail
HEALTH_WARN 2 osds down; Reduced data availability: 9 pgs inactive; Degraded data redundancy: 3041/47574 objects degraded (6.392%), 149 pgs unclean, 149 pgs degraded, 149 pgs undersized
OSD_DOWN 2 osds down
    osd.23 (root=default,host=ceph-xx-osd02) is down
    osd.24 (root=default,host=ceph-xx-osd03) is down
PG_AVAILABILITY Reduced data availability: 9 pgs inactive
    pg 1.45 is stuck inactive for 281.355588, current state undersized+degraded+peered, last acting [10]
```
d. 在停止PG 1.45中第三个副本osd.10
```
$ systemctl stop ceph-osd@10
```
e. 查看停止三个副本PG 1.45的状态(stale+undersized+degraded+peered)
```
$ ceph health detail
HEALTH_WARN 3 osds down; Reduced data availability: 26 pgs inactive, 2 pgs stale; Degraded data redundancy: 4770/47574 objects degraded (10.026%), 222 pgs unclean, 222 pgs degraded, 222 pgs undersized
OSD_DOWN 3 osds down
    osd.10 (root=default,host=ceph-xx-osd01) is down
    osd.23 (root=default,host=ceph-xx-osd02) is down
    osd.24 (root=default,host=ceph-xx-osd03) is down
PG_AVAILABILITY Reduced data availability: 26 pgs inactive, 2 pgs stale
    pg 1.9 is stuck inactive for 171.200290, current state undersized+degraded+peered, last acting [13]
    pg 1.45 is stuck stale for 171.206909, current state stale+undersized+degraded+peered, last acting [10]
    pg 1.89 is stuck inactive for 435.573694, current state undersized+degraded+peered, last acting [32]
    pg 1.119 is stuck inactive for 435.574626, current state undersized+degraded+peered, last acting [28]
```
f. 客户端IO操作
```
#读写挂载磁盘IO 夯住
ll /mnt/
```
**故障总结：**
先停止同一个PG内两个副本，状态是undersized+degraded+peered。
然后停止同一个PG内三个副本，状态是stale+undersized+degraded+peered。

### 3.6.3 总结
 - 当出现一个PG内三个副本都挂掉的情况，就会出现stale状态。
 - 此时该PG不能提供客户端读写，IO挂起夯住。
 - Primary超时未向mon上报pg相关的信息(例如网络阻塞),也会出现stale状态。


## 3.7 Inconsistent
### 3.7.1 说明
 - PG通过Scrub检测到某个或者某些对象在PG实例间出现了不一致

### 3.7.2 故障模拟
a. 删除PG 3.0中副本osd.34头文件
```
$ rm -rf /var/lib/ceph/osd/ceph-34/current/3.0_head/DIR_0/1000000697c.0000122c__head_19785300__3
```
b. 手动执行PG 3.0进行数据清洗
```
$ ceph pg scrub 3.0
instructing pg 3.0 on osd.34 to scrub
```
c. 检查集群监控状态
```
$ ceph health detail
HEALTH_ERR 1 scrub errors; Possible data damage: 1 pg inconsistent
OSD_SCRUB_ERRORS 1 scrub errors
PG_DAMAGED Possible data damage: 1 pg inconsistent
    pg 3.0 is active+clean+inconsistent, acting [34,23,1]
```
d. 修复PG 3.0
```
$ ceph pg repair 3.0
instructing pg 3.0 on osd.34 to repair

#查看集群监控状态
$ ceph health detail
HEALTH_ERR 1 scrub errors; Possible data damage: 1 pg inconsistent, 1 pg repair
OSD_SCRUB_ERRORS 1 scrub errors
PG_DAMAGED Possible data damage: 1 pg inconsistent, 1 pg repair
    pg 3.0 is active+clean+scrubbing+deep+inconsistent+repair, acting [34,23,1]

#集群监控状态已恢复正常
$ ceph health detail
HEALTH_OK
```
**故障总结：**
当PG内部三个副本有数据不一致的情况，想要修复不一致的数据文件，只需要执行ceph pg repair修复指令，ceph就会从其他的副本中将丢失的文件拷贝过来就行修复数据。

### 3.7.3 故障模拟
 - 当osd短暂挂掉的时候，因为集群内还存在着两个副本，是可以正常写入的，但是 osd.34 内的数据并没有得到更新，过了一会osd.34上线了，这个时候osd.34的数据是陈旧的，就通过其他的OSD 向 osd.34 进行数据的恢复，使其数据为最新的，而这个恢复的过程中，PG的状态会从inconsistent ->recover -> clean,最终恢复正常。
- 这是集群故障自愈一种场景。

## 3.8 Down
### 3.8.1 说明
 - Peering过程中，PG检测到某个不能被跳过的Interval中(例如该Interval期间，PG完成了Peering，并且成功切换至Active状态，从而有可能正常处理了来自客户端的读写请求),当前剩余在线的OSD不足以完成数据修复.

### 3.8.2 故障模拟
a. 查看PG 3.7f内副本数
```
$ ceph pg dump | grep ^3.7f
dumped all
3.7f         43                  0        0         0       0 494927872 1569     1569               active+clean 2018-07-05 02:52:51.512598  21315'80115  21356:111666  [5,21,29]          5  [5,21,29]              5  21315'80115 2018-07-05 02:52:51.512568      6206'80083 2018-06-29 22:51:05.831219
```
b. 停止PG 3.7f 副本osd.21
```
$ systemctl stop ceph-osd@21
```
c. 查看PG 3.7f状态
```
$ ceph pg dump | grep ^3.7f
dumped all
3.7f         66                  0       89         0       0 591396864 1615     1615 active+undersized+degraded 2018-07-05 15:29:15.741318  21361'80161  21365:128307     [5,29]          5     [5,29]              5  21315'80115 2018-07-05 02:52:51.512568      6206'80083 2018-06-29 22:51:05.831219
```
d. 客户端写入数据，一定要确保数据写入到PG 3.7f的副本中[5,29]
```
$ fio -filename=/mnt/xxxsssss -direct=1 -iodepth 1 -thread -rw=read -ioengine=libaio -bs=4M -size=2G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
read-libaio: (g=0): rw=read, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 2048MB)
Jobs: 5 (f=5): [_(5),R(1),_(5),R(1),_(3),R(1),_(2),R(1),_(1),R(1),_(9)] [96.5% done] [1052MB/0KB/0KB /s] [263/0/0 iops] [eta 00m:02s]                                                            s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=32966: Thu Jul  5 15:35:16 2018
  read : io=61440MB, bw=1112.2MB/s, iops=278, runt= 55203msec
    slat (msec): min=18, max=418, avg=103.77, stdev=46.19
    clat (usec): min=0, max=33, avg= 2.51, stdev= 1.45
     lat (msec): min=18, max=418, avg=103.77, stdev=46.19
    clat percentiles (usec):
     |  1.00th=[    1],  5.00th=[    1], 10.00th=[    1], 20.00th=[    2],
     | 30.00th=[    2], 40.00th=[    2], 50.00th=[    2], 60.00th=[    2],
     | 70.00th=[    3], 80.00th=[    3], 90.00th=[    4], 95.00th=[    5],
     | 99.00th=[    7], 99.50th=[    8], 99.90th=[   10], 99.95th=[   14],
     | 99.99th=[   32]
    bw (KB  /s): min=15058, max=185448, per=3.48%, avg=39647.57, stdev=12643.04
    lat (usec) : 2=19.59%, 4=64.52%, 10=15.78%, 20=0.08%, 50=0.03%
  cpu          : usr=0.01%, sys=0.37%, ctx=491792, majf=0, minf=15492
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=15360/w=0/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=61440MB, aggrb=1112.2MB/s, minb=1112.2MB/s, maxb=1112.2MB/s, mint=55203msec, maxt=55203msec
```
e. 停止PG 3.7f中副本osd.29,并且查看PG 3.7f状态(undersized+degraded+peered)
```
#停止该PG副本osd.29
systemctl stop ceph-osd@29
 
#查看该PG 3.7f状态为undersized+degraded+peered
ceph pg dump | grep ^3.7f
dumped all
3.7f         70                  0      140         0       0 608174080 1623     1623 undersized+degraded+peered 2018-07-05 15:35:51.629636  21365'80169  21367:132165        [5]          5        [5]              5  21315'80115 2018-07-05 02:52:51.512568      6206'80083 2018-06-29 22:51:05.831219
```
f. 停止PG 3.7f中副本osd.5,并且查看PG 3.7f状态(undersized+degraded+peered)
```
#停止该PG副本osd.5
$ systemctl stop ceph-osd@5
 
#查看该PG状态undersized+degraded+peered
$ ceph pg dump | grep ^3.7f
dumped all
3.7f         70                  0      140         0       0 608174080 1623     1623 stale+undersized+degraded+peered 2018-07-05 15:35:51.629636  21365'80169  21367:132165        [5]          5        [5]              5  21315'80115 2018-07-05 02:52:51.512568      6206'80083 2018-06-29 22:51:05.831219
```
g. 拉起PG 3.7f中副本osd.21(此时的osd.21数据比较陈旧), 查看PG状态(down)
```
#拉起该PG的osd.21
$ systemctl start ceph-osd@21
 
#查看该PG的状态down
$ ceph pg dump | grep ^3.7f
dumped all
3.7f         66                  0        0         0       0 591396864 1548     1548                          down 2018-07-05 15:36:38.365500  21361'80161  21370:111729       [21]         21       [21]             21  21315'80115 2018-07-05 02:52:51.512568      6206'80083 2018-06-29 22:51:05.831219
```
h. 客户端IO操作
```
#此时客户端IO都会夯住
ll /mnt/
```
**故障总结：**
首先有一个PG 3.7f有三个副本[5,21,29]， 当停掉一个osd.21之后， 写入数据到osd.5, osd.29。 这个时候停掉osd.29, osd.5 ，最后拉起osd.21。 这个时候osd.21的数据比较旧，就会出现PG为down的情况，这个时候客户端IO会夯住，只能拉起挂掉的osd才能修复问题。

### 3.8.3 PG为Down的OSD丢失或无法拉起
  - 修复方式(生产环境已验证)
```
      a. 删除无法拉起的OSD
      b. 创建对应编号的OSD
      c. PG的Down状态就会消失
      d. 对于unfound 的PG ，可以选择delete或者revert 
         ceph pg {pg-id} mark_unfound_lost revert|delete
```
### 3.8.4 结论
  - 典型的场景：A(主)、B、C
```
      a. 首先kill B 
      b. 新写入数据到 A、C 
      c. kill A和C
      d. 拉起B    
```
 - 出现PG为Down的场景是由于osd节点数据太旧，并且其他在线的osd不足以完成数据修复。
 - 这个时候该PG不能提供客户端IO读写， IO会挂起夯住。

## 3.9 Incomplete
Peering过程中， 由于 a. 无非选出权威日志 b. 通过choose_acting选出的Acting Set后续不足以完成数据修复，导致Peering无非正常完成。
常见于ceph集群在peering状态下，来回重启服务器，或者掉电。

### 3.9.1 总结
 - 修复方式 [wanted: command to clear 'incomplete' PGs](http://tracker.ceph.com/issues/10098)
比如：pg 1.1是incomplete，先对比pg 1.1的主副本之间 pg里面的对象数 哪个对象数多 就把哪个pg export出来
然后import到对象数少的pg里面 然后再mark complete，一定要先export pg备份。

**简单方式，数据可能又丢的情况**
```
   a. stop the osd that is primary for the incomplete PG;
   b. run: ceph-objectstore-tool --data-path ... --journal-path ... --pgid $PGID --op mark-complete
   c. start the osd. 
```

**保证数据完整性**
```
#1. 查看pg 1.1主副本里面的对象数，假设主本对象数多，则到主本所在osd节点执行
$ ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0/ --journal-path /var/lib/ceph/osd/ceph-0/journal --pgid 1.1 --op export --file /home/pg1.1

#2. 然后将/home/pg1.1 scp到副本所在节点（有多个副本，每个副本都要这么操作），然后到副本所在节点执行
$ ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-1/ --journal-path /var/lib/ceph/osd/ceph-1/journal --pgid 1.1 --op import --file /home/pg1.1

#3. 然后再makr complete
$ ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-1/ --journal-path /var/lib/ceph/osd/ceph-1/journal --pgid 1.1 --op mark-complete

#4. 最后启动osd
$ start osd
```
**验证方案**
```
#1. 把状态incomplete的pg，标记为complete。建议操作前，先在测试环境验证，并熟悉ceph-objectstore-tool工具的使用。
PS：使用ceph-objectstore-tool之前需要停止当前操作的osd，否则会报错。

#2. 查询pg 7.123的详细信息，在线使用查询
ceph pg 7.123 query > /export/pg-7.123-query.txt

#3. 每个osd副本节点进行查询
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-641/ --type bluestore --pgid 7.123 --op info > /export/pg-7.123-info-osd641.txt
如
pg 7.123 OSD 1 存在1,2,3,4,5 object
pg 7.123 OSD 2 存在1,2,3,6   object
pg 7.123 OSD 2 存在1,2,3,7   object

#4. 查询对比数据
#4.1 导出pg的object清单
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-641/ --type bluestore --pgid 7.123 --op list > /export/pg-7.123-object-list-osd-641.txt

#4.2 查询pg的object数量
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-641/ --type bluestore --pgid 7.123 --op list|wc -l

#4.3 对比所有副本的object是否一致。
diff -u /export/pg-7.123-object-list-osd-1.txt /export/pg-7.123-object-list-osd-2.txt
比如：pg 7.123是incomplete，对比7.123的所有副本之间pg里面的object数量。
 - 如上述情况，diff对比后，每个副本（主从所有副本）的object list是否一致。避免有数据不一致。使用数量最多，并且diff对比后，数量最多的包含所有object的备份。
 - 如上述情况，diff对比后，数量是不一致，最多的不包含所有的object，则需要考虑不覆盖导入，再导出。最终使用完整的所有的object进行导入。注：import是需要提前remove pg后进行导入，等于覆盖导入。
 - 如上述情况，diff对比后，数据是一致，则使用object数量最多的备份，然后import到object数量少的pg里面 然后在所有副本mark complete，一定要先在所有副本的osd节点export pg备份，避免异常后可恢复pg。

#5. 导出备份
查看pg 7.123所有副本里面的object数量，参考上述情况，假设osd-641的object数量多，数据diff对比一致后，则到object数量最多，object list一致的副本osd节点执行（最好是每个副本都进行导出备份,为0也需要导出备份）
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-641/ --type bluestore --pgid 7.123 --op export --file /export/pg1.414-osd-1.obj

#6. 导入备份
然后将/export/pg1.414-osd-1.obj scp到副本所在节点，在对象少的副本osd节点执行导入。（最好是每个副本都进行导出备份,为0也需要导出备份）
将指定的pg元数据导入到当前pg,导入前需要先删除当前pg（remove之前请先export备份一下pg数据）。需要remove当前pg,否则无法导入，提示已存在。
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-57/ --type bluestore --pgid 7.123 --op remove 需要加–force才可以删除。
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-57/ --type bluestore --pgid 7.123 --op import --file /export/pg1.414-osd-1.obj

#7. 标记pg状态，makr complete（主从所有副本执行）
ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-57/ --type bluestore --pgid 7.123 --op mark-complete
```



    
# 作者信息
**作者：**李航
**个人简介：** 多年的底层开发经验，在高性能nginx开发和分布式缓存redis cluster有着丰富的经验，目前从事Ceph工作两年左右。
先后在58同城、汽车之家、优酷土豆集团工作。 
目前供职于滴滴基础平台运维部-技术专家岗位   负责分布式Ceph集群开发及运维等工作。
个人主要关注的技术领域：高性能Nginx开发、分布式缓存、分布式存储。
