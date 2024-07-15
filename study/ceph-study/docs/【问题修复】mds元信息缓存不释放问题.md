# 1. 问题：
ceph集群警告信息如下：
```
ceph -s
health HEALTH_WARN
mds0: Client xxx-online00.gz01 failing to respond to cache pressure
```

# 2. 分析问题过程
## 2.1 官方解释
类型 | 描述 |
---|---|
 消息: |  “Client name failing to respond to cache pressure” |
| 代码:	| MDS_HEALTH_CLIENT_RECALL,MDS_HEALTH_CLIENT_RECALL_MANY |
| 描述:	| 客户端有各自的元数据缓存，客户端缓存中的条目（比如索引节点）也会存在于 MDS 缓存中，所以当 MDS 需要削减其缓存时（保持在 mds_cache_size 以下），它也会发消息给客户端让它们削减自己的缓存。如果有客户端没响应或者有缺陷，就会妨碍 MDS 将缓存保持在 mds_cache_size 以下， MDS 就有可能耗尽内存而后崩溃。如果某个客户端的响应时间超过了 mds_recall_state_timeout （默认为 60s ），这条消息就会出现。|

## 2.2  查看客户端session
```
$ ceph daemon mds.ceph-epnfs-mds01.gz01 session ls
[
    {
        "id": 4746087,
        "num_leases": 9,
        "num_caps": 57368,
        "state": "open",
        "replay_requests": 0,
        "completed_requests": 1,
        "reconnecting": false,
        "inst": "client.4746087 10.1.7.1:0\/1700679012",
        "client_metadata": {
            "entity_id": "admin",
            "hostname": "test-hostname00",
            "kernel_version": "3.10.0-514.16.1.el7.x86_64"
        }
    }
]
```
## 2.3  查看客户端inode
跟踪代码发现num_caps就是统计的客户端的inode数量, 大概统计了下已经打开的inode数量。
![image.png](https://upload-images.jianshu.io/upload_images/2099201-96c167e0f8ac3861.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 2.4  尝试mds主从切换
### 2.4.1 执行过程如下

**主从切换流程：**
- handle_mds_map state change up:boot --> up:replay
- handle_mds_map state change up:replay --> up:reconnect
- handle_mds_map state change up:reconnect --> up:rejoin
- handle_mds_map state change up:rejoin --> up:active

## 2.5.  主从mds切换结论
成功切换主从角色


## 2.6. 主从mds切换问题
- mds在切换过程中，导致繁忙cpu很高，在mds_beacon_grace(默认15s)时间内没有向monitor注册，没有及时汇报心跳给mon，导致mds自杀。
- mds主从切换open inode并没有释放

# 3. 深入问题分析
## 3.1 mds切换过程导致mds自杀
问题：mds在切换过程中，导致繁忙cpu很高，在mds_beacon_grace(默认15s)时间内没有向monitor注册，没有及时汇报心跳给mon，导致mds自杀。

**mds存储：**
- 元数据的内存缓存，为了加快元数据的访问。
- 保存了文件系统的元数据(对象里保存了子目录和子文件的名称和inode编号)
- 还保存cephfs日志journal，日志是用来恢复mds里的元数据缓存
- 重启mds的时候会通过replay的方式从osd上加载之前缓存的元数据

**mds冷备/热备：**
- 冷备就是备份的mds，只起到一个进程备份的作用，并不备份lru元数据。主备进程保持心跳关系，一旦主的mds挂了，备份mds replay()元数据到缓存，当然这需要消耗一点时间。
- 热备除了进程备份，元数据缓存还时时刻刻的与主mds保持同步，当 active mds挂掉后，热备的mds直接变成主mds，并且没有replay()的操作，元数据缓存大小和主mds保持一致。
**说明：**
  - rejoin把客户端的inode加载到mds cache
  - replay把从cephfs的journal恢复内存

**mds主备切换策略：**
- 默认每个standby都一样
- 指定后补
  - mds standby for name指定一 MDS 守护进程的名字，此进程将作为它的候补
  - mds standby for rank此 MDS 将作为本机架上 MDS 守护进程的候补
- 优先级最高standby replay

**节点失效机制：**
- 一个活跃的MDS定期向monitor发送交互信息，如果一个MDS在mds_beacon_grace(默认15s)时间内没有向monitor注册，则认为该MDS失效。

**恢复过程：**
- 失效节点的相关日志被读入内存；
- 处理有争议的子树分配问题和涉及多个MDS的transaction；
- 与client重新建立会话并重新保存打开文件的状态；
- 接替失效节点的MDS加入到MDS集群的分布式缓存中

**resolve阶段的事件：**
- 恢复节点向所有MDS发送一个resolve信息，该信息中包含了当前恢复节点管理的子树、在迁移过程中出现故障的子树；
- 其他正常运行的MDS也要将这些信息发送给正在恢复的MDS；
- 恢复中的MDS根据收到的子树信息重建自己缓存中的子树层次结构。

**重建分布式缓存和锁状态：**
- 恢复节点向所有MDS发送一个rejoin信息，该信息包含了恢复节点所知道的接受节点拥有的元数据副本信息并宣称自己没有管理的恢复文件；
- 原来有效的节点向恢复节点发送信息，告诉恢复节点自己拥有的元数据副本，并且向恢复节点加入锁状态
- 恢复节点将自己原本不知道的副本信息加入到自己的缓存中

**为啥mds切换导致cpu高？**
- 1. 分析日志(发现执行rejoin_start动作只会就超时)
```
2018-04-27 19:12:21.909280 7f8268805700 1 mds.0.2665 rejoin_start
2018-04-27 19:12:37.294438 7f826a809700 1 heartbeat_map is_healthy 'MDSRank' had timed out after 15
2018-04-27 19:12:40.961787 7f82656fe700 1 heartbeat_map is_healthy 'MDSRank' had timed out after 15
2018-04-27 19:12:40.961796 7f82656fe700 1 mds.beacon.ceph-xxx-mds01.gz01 _send skipping beacon, heartbeat map not healthy
2018-04-27 19:12:42.294507 7f826a809700 1 heartbeat_map is_healthy 'MDSRank' had timed out after 15
```
- 2.跟踪代码分析(在执行process_imported_caps超时了， 这个函数主要是打开inodes 加载到cache中)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-1652420b5cfd4d8f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

- 3. 跟踪官方bug列表发现补丁(解决主从mds切换超时自杀, 以及merge到目标版本13.0.0) [https://github.com/ceph/ceph/pull/21144](https://github.com/ceph/ceph/pull/21144)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-e9a88d18f2b61b2b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

- 4.跟踪补丁代码分析(inode到了1000个，mds 心跳reset, 禁止自杀行为)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-348e433c658229e6.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-06ca87bcd4d4ad0d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 3.2 mds主从切换open inode没有释放
问题：mds主从切换open inode没有释放，mds集群显示mds0: Client xxx-online00.gz01 failing to respond to cache pressure
解决方式：(由于inode都缓存在client端,所以必须的想办法释放inode)

*   方案1：evict client(主动踢出有问题的客户端)
*   方案2：client remount(有问题的客户端重新mount挂载)
*   方案3：drop_cache(官方提供的mds 主动删除cache，补丁在review过程中个，目标版本是ceph-14.0.0) [https://github.com/ceph/ceph/pull/21566](https://github.com/ceph/ceph/pull/21566)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-5279fc57af33fb76.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

