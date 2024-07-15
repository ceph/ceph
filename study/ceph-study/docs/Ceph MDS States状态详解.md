# MDS States
元数据服务器（MDS）在CephFS的正常操作过程中经历多个状态。例如，一些状态指示MDS从MDS的先前实例从故障转移中恢复。在这里，我们将记录所有这些状态，并包括状态图来可视化转换。

## State Descriptions
# Common states
| 状态 | 说明 |
----|----|
up:active | This is the normal operating state of the MDS. It indicates that the MDS and its rank in the file system is available.<br><br>这个状态是正常运行的状态。 这个表明该mds在rank中是可用的状态。
up:standby | The MDS is available to takeover for a failed rank (see also [:ref:`mds-standby`](https://github.com/ceph/ceph/blob/master/doc/cephfs/mds-states.rst#id1)). The monitor will automatically assign an MDS in this state to a failed rank once available.<br><br>这个状态是灾备状态，用来接替主挂掉的情况。
up:standby_replay | The MDS is following the journal of another up:active MDS. Should the active MDS fail, having a standby MDS in replay mode is desirable as the MDS is replaying the live journal and will more quickly takeover. A downside to having standby replay MDSs is that they are not available to takeover for any other MDS that fails, only the MDS they follow.<br><br>灾备守护进程就会持续读取某个处于 up 状态的 rank 的元数据日志。这样它就有元数据的热缓存，在负责这个 rank 的守护进程失效时，可加速故障切换。<br><br> 一个正常运行的 rank 只能有一个灾备重放守护进程（ standby replay daemon ），如果两个守护进程都设置成了灾备重放状态，那么其中任意一个会取胜，另一个会变为普通的、非重放灾备状态。<br><br>一旦某个守护进程进入灾备重放状态，它就只能为它那个 rank 提供灾备。如果有另外一个 rank 失效了，即使没有灾备可用，这个灾备重放守护进程也不会去顶替那个失效的。
up:boot | This state is broadcast to the Ceph monitors during startup. This state is never visible as the Monitor immediately assign the MDS to an available rank or commands the MDS to operate as a standby. The state is documented here for completeness. <br><br> 此状态在启动期间被广播到CEPH监视器。这种状态是不可见的，因为监视器立即将MDS分配给可用的秩或命令MDS作为备用操作。这里记录了完整性的状态。
up:creating | The MDS is creating a new rank (perhaps rank 0) by constructing some per-rank metadata (like the journal) and entering the MDS cluster.
up:starting | The MDS is restarting a stopped rank. It opens associated per-rank metadata and enters the MDS cluster.
up:stopping | When a rank is stopped, the monitors command an active MDS to enter the `up:stopping` state. In this state, the MDS accepts no new client connections, migrates all subtrees to other ranks in the file system, flush its metadata journal, and, if the last rank (0), evict all clients and shutdown (see also [:ref:`cephfs-administration`](https://github.com/ceph/ceph/blob/master/doc/cephfs/mds-states.rst#id3)).
up:replay | The MDS taking over a failed rank. This state represents that the MDS is recovering its journal and other metadata.<br><br>日志恢复阶段，他将日志内容读入内存后，在内存中进行回放操作。
up:resolve | The MDS enters this state from up:replay if the Ceph file system has multiple ranks (including this one), i.e. it's not a single active MDS cluster. The MDS is resolving any uncommitted inter-MDS operations. All ranks in the file system must be in this state or later for progress to be made, i.e. no rank can be failed/damaged or up:replay. <br><br>用于解决跨多个mds出现权威元数据分歧的场景，对于服务端包括子树分布、Anchor表更新等功能，客户端包括rename、unlink等操作。
up:reconnect | An MDS enters this state from up:replay or up:resolve. This state is to solicit reconnections from clients. Any client which had a session with this rank must reconnect during this time, configurable via mds_reconnect_timeout.<br><br>恢复的mds需要与之前的客户端重新建立连接，并且需要查询之前客户端发布的文件句柄，重新在mds的缓存中创建一致性功能和锁的状态。mds不会同步记录文件打开的信息，原因是需要避免在访问mds时产生多余的延迟，并且大多数文件是以只读方式打开。
up:rejoin | The MDS enters this state from up:reconnect. In this state, the MDS is rejoining the MDS cluster cache. In particular, all inter-MDS locks on metadata are reestablished.<br>If there are no known client requests to be replayed, the MDS directly becomes up:active from this state.<br><br>把客户端的inode加载到mds cache
up:clientreplay | The MDS may enter this state from up:rejoin. The MDS is replaying any client requests which were replied to but not yet durable (not journaled). Clients resend these requests during up:reconnect and the requests are replayed once again. The MDS enters up:active after completing replay. 
down:failed | No MDS actually holds this state. Instead, it is applied to the rank in the file system
down:damaged | No MDS actually holds this state. Instead, it is applied to the rank in the file system
down:stopped | No MDS actually holds this state. Instead, it is applied to the rank in the file system

**主从切换流程：**
- handle_mds_map state change up:boot --> up:replay
- handle_mds_map state change up:replay --> up:reconnect
- handle_mds_map state change up:reconnect --> up:rejoin
- handle_mds_map state change up:rejoin --> up:active

## State Diagram

This state diagram shows the possible state transitions for the MDS/rank. The legend is as follows:

### [Color](https://github.com/ceph/ceph/blob/master/doc/cephfs/mds-states.rst#color)

*   绿色: MDS是活跃的.
*   橙色: MDS处于过渡临时状态，试图变得活跃.
*   红色:  MDS指示一个状态，该状态导致被标记为失败.
*   紫色: MDS和rank为停止.
*   红色: MDS指示一个状态，该状态导致被标记为损坏.

### [Shape](https://github.com/ceph/ceph/blob/master/doc/cephfs/mds-states.rst#shape)

*   圈：MDS保持这种状态.
*   六边形：没有MDS保持这个状态.

### [Lines](https://github.com/ceph/ceph/blob/master/doc/cephfs/mds-states.rst#lines)

*   A double-lined shape indicates the rank is "in".

 
![image.png](https://upload-images.jianshu.io/upload_images/2099201-8c9958250dd4b485.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


**参考：**
https://github.com/ceph/ceph/blob/master/doc/cephfs/mds-states.rst
