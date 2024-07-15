# 1. 心跳介绍
心跳是用于节点间检测对方是否故障的，以便及时发现故障节点进入相应的故障处理流程。

**问题：**
 - 故障检测时间和心跳报文带来的负载之间做权衡。
 - 心跳频率太高则过多的心跳报文会影响系统性能。
 - 心跳频率过低则会延长发现故障节点的时间，从而影响系统的可用性。

**故障检测策略应该能够做到：**
 - 及时：节点发生异常如宕机或网络中断时，集群可以在可接受的时间范围内感知。
 - 适当的压力：包括对节点的压力，和对网络的压力。
 - 容忍网络抖动：网络偶尔延迟。
 - 扩散机制：节点存活状态改变导致的元信息变化需要通过某种机制扩散到整个集群。

# 2. Ceph 心跳检测
![ceph_heartbeat_1.png](https://upload-images.jianshu.io/upload_images/2099201-797b8f8c9e2de4d7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**OSD节点会监听public、cluster、front和back四个端口**
 - public端口：监听来自Monitor和Client的连接。
 - cluster端口：监听来自OSD Peer的连接。
 - front端口：供客户端连接集群使用的网卡, 这里临时给集群内部之间进行心跳。
 - back端口：供客集群内部使用的网卡。集群内部之间进行心跳。
 - hbclient：发送ping心跳的messenger。

# 3. Ceph OSD之间相互心跳检测
![ceph_heartbeat_osd.png](https://upload-images.jianshu.io/upload_images/2099201-a04c96ba04ec47df.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**步骤：**
 - 同一个PG内OSD互相心跳，他们互相发送PING/PONG信息。
 - 每隔6s检测一次(实际会在这个基础上加一个随机时间来避免峰值)。
 - 20s没有检测到心跳回复，加入failure队列。


# 4. Ceph OSD与Mon心跳检测
![ceph_heartbeat_mon.png](https://upload-images.jianshu.io/upload_images/2099201-06fcd181ba5c2671.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**OSD报告给Monitor：**
 - OSD有事件发生时（比如故障、PG变更）。
 - 自身启动5秒内。
 - OSD周期性的上报给Monito
   - OSD检查failure_queue中的伙伴OSD失败信息。
   - 向Monitor发送失效报告，并将失败信息加入failure_pending队列，然后将其从failure_queue移除。
   - 收到来自failure_queue或者failure_pending中的OSD的心跳时，将其从两个队列中移除，并告知Monitor取消之前的失效报告。
   - 当发生与Monitor网络重连时，会将failure_pending中的错误报告加回到failure_queue中，并再次发送给Monitor。
 - Monitor统计下线OSD
   - Monitor收集来自OSD的伙伴失效报告。
   - 当错误报告指向的OSD失效超过一定阈值，且有足够多的OSD报告其失效时，将该OSD下线。


# 5. Ceph心跳检测总结
Ceph通过伙伴OSD汇报失效节点和Monitor统计来自OSD的心跳两种方式判定OSD节点失效。
 - **及时：**伙伴OSD可以在秒级发现节点失效并汇报Monitor，并在几分钟内由Monitor将失效OSD下线。
 - **适当的压力：**由于有伙伴OSD汇报机制，Monitor与OSD之间的心跳统计更像是一种保险措施，因此OSD向Monitor发送心跳的间隔可以长达600秒，Monitor的检测阈值也可以长达900秒。Ceph实际上是将故障检测过程中中心节点的压力分散到所有的OSD上，以此提高中心节点Monitor的可靠性，进而提高整个集群的可扩展性。
 - **容忍网络抖动：**Monitor收到OSD对其伙伴OSD的汇报后，并没有马上将目标OSD下线，而是周期性的等待几个条件：
    - 目标OSD的失效时间大于通过固定量osd_heartbeat_grace和历史网络条件动态确定的阈值。
    - 来自不同主机的汇报达到mon_osd_min_down_reporters。
    - 满足前两个条件前失效汇报没有被源OSD取消。
 - **扩散：**作为中心节点的Monitor并没有在更新OSDMap后尝试广播通知所有的OSD和Client，而是惰性的等待OSD和Client来获取。以此来减少Monitor压力并简化交互逻辑。

# 6. 心跳设置
## 6.1 配置监视器/ OSD互动
您已完成初始Ceph的配置之后，您可以部署和运行的Ceph。当你执行一个命令，如ceph health 或 ceph -s ， [Ceph的监视器](http://ceph.com/docs/master/glossary/#term-ceph-monitor)将报告[CEPH存储集群](http://ceph.com/docs/master/glossary/#term-ceph-storage-cluster)的当前状态。Ceph的监视器通过每个[Ceph的OSD守护](http://ceph.com/docs/master/glossary/#term-ceph-osd-daemon)实例，以及相邻的Ceph OSD守护实例，了解Ceph的存储集群的相关状态。Ceph的监视器如果没有收到报告，或者如果它接收Ceph的存储集群的变化的报告，Ceph的监视器更新的的[CEPH集群](http://ceph.com/docs/master/glossary/#term-ceph-cluster-map)映射图的状态。

Ceph为Ceph的监视器/ Ceph的OSD守护程序交互提供合理的默认设置。但是，您可以覆盖默认值。以下部分描述如何用Ceph的监视器和Ceph的OSD守护实例互动来达到Ceph的存储集群监控的目的。

## 6.2. OSDS检查心跳
每个Ceph的OSD守护程序检查其他Ceph的OSD守护进程的心跳每6秒。Ceph的配置文件下的[OSD]部分加入OSD   osd heartbeat interval ，或通过设定值在运行时，您可以更改心跳间隔。如果在20秒的宽限期内邻居的Ceph的OSD守护进程不显示心跳，Ceph的OSD守护进程可能考虑周边的Ceph OSD守护挂掉，并向一个Ceph的Monitor报告，这将更新的CEPH集群地图。一个OSD   osd heartbeat grace 可以在Ceph的配置文件下的[OSD]部分设置，或在运行时，你通过设置这个值改变这个宽限期。

## 6.3. OSDS报告挂掉的OSD
默认情况下，Ceph的OSD守护程序必须向Ceph的监视器报告三次：另一个Ceph的OSD守护程序已经挂掉，在Ceph的Monitor承认该报告Ceph的OSD守护挂掉之前。在（早期V0.62版本之前）Ceph的配置文件下的[MON]部分添加  osd min down reports setting，或者通过设定值在运行时，您可以更改OSD报告的挂掉的最低数量 。默认情况下，只有一个Ceph的OSD守护进程是必需报告另一个Ceph的OSD守护进程。您可以更改向Ceph监视器报告Ceph的OSD守护进程的Ceph的OSD Daemones 的数量，通过添加一个mon osd min down reporters设置在Ceph的配置文件中，或者通过设定值在运行时。

## 6.4. 凝视失败的OSD报告
Ceph的OSD守护进程如果不能和Ceph的配置文件（或群集地图）中定义的OSD守护同行，它将每30秒ping一个Ceph的监视器，为了最新副本的集群映射图。Ceph的配置文件 下的[OSD]部分加入  osd mon heartbeat interval  设置，或通过在运行时设定值，您可以更改Ceph的监控心跳间隔。 

## 6.5. OSDS报告其状态
Ceph的OSD守护进程如果不向Ceph的监视器报告，至少每120秒一次，Ceph的监视器会考虑Ceph的OSD守护已经挂掉。您可以更改Ceph的监控报告间隔，通过加入 osd mon report interval max 设置在Ceph的配置文件的[OSD]部分，或者通过设置在运行时的值。Ceph的OSD守护进程会尝试报告其状态每30秒。在Ceph的配置文件下的[OSD]部分加入 osd mon report interval min s设置，或者通过设定值在运行时，您可以更改Ceph的OSD守护报告间隔。

# 7. 配置设置
修改心跳设置时，你应该将它们包括在 您的配置文件的[global]部分。

## 7.1 监视器MONITOR设置
| 参数 | 说明 | 类型 | 默认值 |
|:---:|:---:|:---:|:---:|
|mon OSD min up ratio  | Ceph的OSD未挂掉的最低比率在Ceph的OSD守护程序被仍定挂掉之前 | double | 0.3 |
|mon OSD min in ratio | Ceph的OSD实例的最低比率在Ceph的OSD守护程序被仍定出局之前 | double | 0.3 |
|mon osd laggy halflife | laggy估计会腐烂的秒数 | int | 60 * 60 |
|mon osd laggy weight | laggy估计衰减的新样本的权重 | double | 0.3 |
|mon osd adjust heartbeat grace | 如果设置为true，Ceph将在laggy估计的基础上扩展 | bool | true |
|mon osd adjust down out interval | 如果设置为true，Ceph基于laggy估计扩展 | bool | true |
|mon osd auto mark in | Ceph将标记任何引导的Ceph的OSD守护进程作为在 CEPH存储集群 | bool | false |
|mon osd auto mark auto out in| Ceph的标记引导Ceph的OSD守护 Ceph的存储集群，集群中的自动标记 | bool | true |
| mon osd auto mark new in | 头孢将迎来启动新的Ceph的OSD守护在 Ceph的存储集群 | bool | true |
| mon osd down out subtree limit| 最大的[CRUSH](http://ceph.com/docs/master/glossary/#term-crush)单位Ceph的类型，会自动标记出来 | String | rack |
|mon osd report timeout | 宽限期秒下来在声明反应迟钝Ceph的OSD守护前 | 32-bit Integer | 900 |
|mon osd min down reporters | Ceph的OSD守护报告向下 Ceph的OSD守护所需的最低数量 | 32-bit Integer | 1 |
| mon osd min down reports | Ceph的OSD守护的最低次数必须报告说，另一个Ceph的OSD守护下来 | 32-bit Integer | 3 |

## 7.2 OSD设置
| 参数 | 说明 | 类型 | 默认值 |
|:---:|:---:|:---:|:---:|
|OSD heartbeat address| 一个Ceph的OSD守护进程的网络地址的心跳 | Address | The host address |
|OSD heartbeat interval | 多久Ceph的OSD守护坪及其同行（以秒计）| 32-bit Integer | 6 |
|OSD heartbeat grace| Ceph的OSD当一个守护进程并没有表现出心跳Ceph的存储集群认为，经过时间的 | 32-bit Integer | 20 |
| OSD mon heartbeat interval | Ceph的的OSD守护坪一个Ceph的监视器如果它没有的CEPH OSD守护同行，多久 | 32-bit Integer | 30 |
| OSD mon report interval max | Ceph的OSD守护进程报告Ceph的监视器Ceph的监视器前认为Ceph的OSD守护下来的时间以秒为单位的最大 | 32-bit Integer | 120 |
| OSD mon report inteval min | 秒为Ceph的OSD的守护Ceph的监视器，以防止Ceph的监视器考虑Ceph的OSD守护的最低数量 | 32-bit Integer | 5 (有效范围：应小于OSD 周一 报告 间隔 最大)|
| OSD mon ACK timeout | 等待的秒数为Ceph的监视器确认请求统计 |  32-bit Integer | 30 |



