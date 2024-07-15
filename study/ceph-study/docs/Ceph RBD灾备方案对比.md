# 一、业内灾备方案

## 1\. Snapshot

### 1.1 介绍

Cluster A & B仍然是独立的Ceph集群，通过RBD的snapshot机制，在Cluster A端，针对image定期通过rbd创建image的snap，

然后通过`rbd export-diff`, `rbd import-diff`命令来完成image备份到Cluster B。

### 1.2 原理

 异步备份，基于RBD的`snapshot`机制

### 1.3 命令和步骤

把 Cluster A 的 pool rbd 下面 image testimage 异步备份到 Cluster B 的 pool rbd 下的相同image上；

1.  在Cluster A/B上创建rbd/testimage
    `rbd create -p rbd --size 10240 testimage`

2.  在准备备份image前，暂停Cluster A端对testimage的IO操作，然后创建个snapshot
    `rbd snap create <snap-name>`

3.  导出Cluster A端的testimage数据，不指定from-snap
    `rbd export-diff <image-name> <path>`

4.  copy上一步中导出的文件到Cluster B，并导入数据到testimage
    `rbd import-diff <path> <image-name>`

后续需周期性的暂停Cluster A端的testimage的IO，然后创建snapshot，通过 `rbd export-diff <image-name> [--from-snap <snap-name>] <path>`命令导出incremental diff，

之后把差异数据文件copy到Cluster B上，然后通过命令`rbd import-diff <path> <image-name>`导入。

【注】：也可不暂停Cluster A端的IO，直接take snapshot；这样并不会引起image的数据不一致，只是有可能会使`rbd export-diff`时导出的数据在take snapshot之后

### 1.4 优缺点

**优点：**

1.  当前Ceph版本就支持rbd snapshot的功能
2.  命令简介方便，通过定制执行脚本就能实现rbd块设备的跨区备份

**缺点：**

1.  每次同步前都需要在源端take snapshot
2.  持续的snapshots可能导致image的读写性能下降
3.  还要考虑后续删除不用的snapshots
4.  snapshot只能保证IO的一致性，并不能保证使用rbd块设备上的系统一致性；

【可以每次暂停image的IO，sync IO数据来保证rbd块设备上的系统一致性，但需要虚拟机支持qemu-guest-agent】

### [1.5 参考资料](http://www.yangguanjun.com/2017/02/22/rbd-data-replication/#%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)
[https://ceph.com/dev-notes/incremental-snapshots-with-rbd/](https://ceph.com/dev-notes/incremental-snapshots-with-rbd/)
[https://www.rapide.nl/blog/item/ceph_-_rbd_replication.html](https://www.rapide.nl/blog/item/ceph_-_rbd_replication.html)
[http://wiki.libvirt.org/page/Qemu_guest_agent](http://wiki.libvirt.org/page/Qemu_guest_agent)
[http://www.zphj1987.com/2016/06/22/rbd](http://www.zphj1987.com/2016/06/22/rbd%E7%9A%84%E5%A2%9E%E9%87%8F%E5%A4%87%E4%BB%BD%E5%92%8C%E6%81%A2%E5%A4%8D/)
[http://ju.outofmemory.cn/entry/243899](http://ju.outofmemory.cn/entry/243899)

## 2. CEPH BackUp
### 2.1 介绍
teralytics是一家国外的大数据公司，这个是他们开源的ceph的备份的工具。
这个软件基于python的实现，可以说作者的实现逻辑是很清晰的，并且提供了配置文件的方式，基本上是各个细节都考虑的比较到位，很容易上手，可以直接拿来使用，或者集成到自己的平台中去，是一个很好的软件

**软件包含以下功能：**
- 支持存储池和多image的只对
- 支持自定义备份目标路径
- 配置文件支持
- 支持备份窗口设置
- 支持压缩选项
- 支持增量和全量备份的配置

### 2.2 原理
异步备份，基于RBD的snapshot机制。

### 2.3 命令和步骤
#### 2.3.1 全量备份配置
上面的配置文件已经写好了，直接执行备份命令就可以了
```
cephbackup
Starting backup for pool rbd
Full ceph backup
Images to backup:
 rbd/zp
Backup folder: /tmp/
Compression: True
Check mode: False
Taking full backup of images: zp
rbd image 'zp':
 size 40960 MB in 10240 objects
 order 22 (4096 kB objects)
 block_name_prefix: rbd_data.25496b8b4567
 format: 2
 features: layering
 flags: 
Exporting image zp to /tmp/rbd/zp/zp_UTC20170119T092933.full
Compress mode activated
# rbd export rbd/zp /tmp/rbd/zp/zp_UTC20170119T092933.full
Exporting image: 100% complete...done.
# tar Scvfz /tmp/rbd/zp/zp_UTC20170119T092933.full.tar.gz /tmp/rbd/zp/zp_UTC20170119T092933.full
tar: Removing leading `/' from member names
```
压缩的如果开了，正好文件也是稀疏文件的话，需要等很久，压缩的效果很好，dd生成的文件可以压缩到很小

检查备份生成的文件
```
ll /tmp/rbd/zp/zp_UTC20170119T092933.full*
-rw-r--r-- 1 root root 42949672960 Jan 19 17:29 /tmp/rbd/zp/zp_UTC20170119T092933.full
-rw-r--r-- 1 root root 0 Jan 19 17:29 /tmp/rbd/zp/zp_UTC20170119T092933.full.tar.gz
```

#### 2.3.2 全量备份的还原
```
rbd import /tmp/rbd/zp/zp_UTC20170119T092933.full zpbk
```
检查数据，没有问题

#### 2.3.3 增量备份配置
写下增量配置的文件，修改下备份模式的选项
```
[rbd]
window size = 7
window unit = day
destination directory = /tmp/
images = zp
compress = yes
ceph config = /etc/ceph/ceph.conf
backup mode = incremental
check mode = no
```
执行多次进行增量备份以后是这样的
```
[root@lab8106 ~]#ll /tmp/rbd/zpbk/
total 146452
-rw-r--r-- 1 root root 42949672960 Jan 19 18:04 zpbk@UTC20170119T100339.full
-rw-r--r-- 1 root root 66150 Jan 19 18:05 zpbk@UTC20170119T100546.diff_from_UTC20170119T100339
-rw-r--r-- 1 root root 68 Jan 19 18:05 zpbk@UTC20170119T100550.diff_from_UTC20170119T100546
-rw-r--r-- 1 root root 68 Jan 19 18:06 zpbk@UTC20170119T100606.diff_from_UTC20170119T100550
-rw-r--r-- 1 root root 68 Jan 19 18:06 zpbk@UTC20170119T100638.diff_from_UTC20170119T100606
```
#### 2.3.4 增量备份的还原
分成多个步骤进行
```
1、进行全量的恢复
# rbd import config@UTC20161130T170848.full dest_image
2、重新创建基础快照
# rbd snap create dest_image@UTC20161130T170848
3、还原增量的快照(多次执行)
# rbd import-diff config@UTC20161130T170929.diff_from_UTC20161130T170848 dest_image
```
本测试用例还原步骤就是
```
rbd import zpbk@UTC20170119T100339.full zpnew
rbd snap create zpnew@UTC20170119T100339
rbd import-diff zpbk@UTC20170119T100546.diff_from_UTC20170119T100339 zpnew
rbd import-diff zpbk@UTC20170119T100550.diff_from_UTC20170119T100546 zpnew
rbd import-diff zpbk@UTC20170119T100606.diff_from_UTC20170119T100550 zpnew
rbd import-diff zpbk@UTC20170119T100638.diff_from_UTC20170119T100606 zpnew
```
检查数据，没有问题

## 3\. RBD Mirroring

### 3.1 介绍

Ceph新的rbd mirror功能支持配置两个Ceph Cluster之间的rbd同步

### 3.2 原理

利用Journal日志进行异步备份，Ceph自身带有的rbd mirror功能

### 3.3 命令和步骤

详见：rbd-mirror

### 3.4 优缺点

**优点：**

1.  Ceph新的功能，不需要额外开发
2.  同步的粒度比较小，为一个块设备的transaction
3.  保证了Crash consistency
4.  可配置pool的备份，也可单独指定image备份

**缺点：**

1.  需要升级线上Ceph到Jewel 10.2.2版本以上
2.  Image Journal日志性能影响较为严重



# 二、结论
## 1. 方案对比
方案 |	详细说明 |	优点 |	缺点 |
---|---|---|---|
Snapshot | 主站点备份时为存储块打快照，将快照的差异部分发送到备站点重新生成新快照 | 1.当前Ceph版本就支持rbd snapshot的功能 <br> 2. 命令简介方便，通过定制执行脚本就能实现rbd块设备的跨区备份 <br> 3. 不需要对集群操作升级降级操作<br>4. 风险较低，简单，易实现 |1. 快照对原块的性能有很大影响，尤其是随机IO <br>2. 快照间的差异部分是在备份时计算出来的，因此很耗时，即使两个快照间没有差异也要花上很长一段时间来扫描差异部分<br> 3. 定期备份存在差异数据丢失
Ceph-backup|	官方社区基于快照的方式，进行包装了下	| 同上	|同上 |
RBD Mirroring| 主要是客户端多写一份日志，然后异步同步到备集群进行实时备份 | 1. Ceph新的功能，不需要额外开发<br>2. 同步的粒度比较小，为一个块设备的transaction<br>3. 保证了Crash consistency <br>4. 可配置pool的备份，也可单独指定image备份<br>5. 实时备份保证数据的一致性 |1. 需要升级线上Ceph到Jewel 10.2.2版本以上<br>2. Image Journal日志性能影响较为严重

## 2. 总结
结合业内的各大公司的灾备方案，以及社区相关的技术文档。个人建议用快照的方式， 简单、便捷、风险较低、易实现。

并且国内云厂商也普遍都是利用快照的方式实现灾备方案，然后加上自己的策略进行包装。

rbd-mirror功能还是比较新 并且官方的文档也有一些措施进行优化，但是效果不佳。

官方也把这块列为todolist，期待下个版本进行优化。
