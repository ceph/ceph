# 1\. 节点故障检测概述

节点的故障检测是分布式系统无法回避的问题，集群需要感知节点的存活，并作出适当的调整。通常我们采用心跳的方式来进行故障检测，并认为能正常与外界保持心跳的节点便能够正常提供服务。一个好的故障检测策略应该能够做到：

*   **及时**：节点发生异常如宕机或网络中断时，集群可以在可接受的时间范围内感知；
*   **适当的压力**：包括对节点的压力，和对网络的压力；
*   **容忍网络抖动**
*   **扩散机制**：节点存活状态改变导致的元信息变化需要通过某种机制扩散到整个集群；

不同的分布式系统由于其本身的结构不同，以及对一致性、可用性、可扩展性的需求不同，会针对以上几点作出不同的抉择或取舍。下面我们就来看看Ceph是怎么做的。

# 2\. Ceph故障检测机制

Ceph作为有中心的分布式结构，元信息的维护和更新自然的都由其中心节点Ceph Monitor来负责。节点的存活状态发生改变时，也需要Monitor来发现并更新元信息并通知给所有的OSD节点。最自然的，我们可以想到让中心节点Monitor保持与所有OSD节点之间频繁的心跳，但如此一来，当有成百上千的OSD节点时Monitor变会有比较大的压力。之前在[Ceph Monitor and Paxos](http://catkang.github.io/2016/07/17/ceph-monitor-and-paxos.html)中介绍过Ceph的设计思路是通过更智能的OSD和Client来减少对中心节点Monitor的压力。同样的，在节点的故障检测方面也需要OSD和Monitor的配合完成。下面的介绍基于当前最新的11.0.0版本。

## **2.1 OSD之间心跳**

属于同一个pg的OSD我们称之为伙伴OSD，他们会相互发送PING\PONG信息，并且记录发送和接收的时间。OSD在cron中发现有伙伴OSD相应超时后，会将其加入failure_queue队列，等待后续汇报。

**参数：**
 - osd_heartbeat_interval(6): 向伙伴OSD发送ping的时间间隔。实际会在这个基础上加一个随机时间来避免峰值。
 - osd_heartbeat_grace(20)：多久没有收到回复可以认为对方已经down

## 2.2 OSD向Monitor汇报伙伴OSD失效
1. OSD发送错误报告
 - OSD周期性的检查failure_queue中的伙伴OSD失败信息；
 - 向Monitor发送失效报告，并将失败信息加入failure_pending队列，然后将其从failure_queue移除；
 - 收到来自failure_queue或者failure_pending中的OSD的心跳时，将其从两个队列中移除，并告知Monitor取消之前的失效报告；
 - 当发生与Monitor网络重连时，会将failure_pending中的错误报告加回到failure_queue中，并再次发送给Monitor。

2. Monitor统计下线OSD
 - Monitor收集来自OSD的伙伴失效报告；
 - 当错误报告指向的OSD失效超过一定阈值，且有足够多的OSD报告其失效时，将该OSD下线。

**参数:**
 - osd_heartbeat_grace(20): 可以确认OSD失效的时间阈值；
 - mon_osd_reporter_subtree_level(“host”)：在哪一个级别上统计错误报告数，默认为host，即计数来自不同主机的osd报告
 - mon_osd_min_down_reporters(2): 最少需要多少来自不同的mon_osd_reporter_subtree_level的osd的错误报告
 - mon_osd_adjust_heartbeat_grace(true)：在计算确认OSD失效的时间阈值时，是否要考虑该OSD历史上的延迟，因此失效的时间阈值通常会大于osd_heartbeat_grace指定的值

## 2.3 OSD到Monitor心跳
 - OSD当有pg状态改变等事件发生，或达到一定的时间间隔后，会向Monitor发送MSG_PGSTATS消息，这里称之为OSD到Monitor的心跳。
 - Monitor收到消息，回复MSG_PGSTATSACK，并记录心跳时间到last_osd_report。
 - Monitor周期性的检查所有OSD的last_osd_report，发现失效的节点，并标记为Down。

**参数：**
 - mon_osd_report_timeout(900)：多久没有收到osd的汇报，Monitor会将其标记为Down；
 - osd_mon_report_interval_max(600)：OSD最久多长时间向Monitor汇报一次；
 - osd_mon_report_interval_min(5)：OSD向Monitor汇报的最小时间间隔

# 3. 总结
可以看出，Ceph中可以通过伙伴OSD汇报失效节点和Monitor统计来自OSD的心跳两种方式发现OSD节点失效。回到在文章开头提到的一个合格的故障检测机制需要做到的几点，结合Ceph的实现方式来理解其设计思路。

 - **及时**：伙伴OSD可以在秒级发现节点失效并汇报Monitor，并在几分钟内由Monitor将失效OSD下线。当然，由于Ceph对一致性的要求，这个过程中客户端写入会不可避免的被阻塞；
 - **适当的压力**：由于有伙伴OSD汇报机制，Monitor与OSD之间的心跳统计更像是一种保险措施，因此OSD向Monitor发送心跳的间隔可以长达600秒，Monitor的检测阈值也可以长达900秒。Ceph实际上是将故障检测过程中中心节点的压力分散到所有的OSD上，以此提高中心节点Monitor的可靠性，进而提高整个集群的可扩展性；
 - **容忍网络抖动**：Monitor收到OSD对其伙伴OSD的汇报后，并没有马上将目标OSD下线，而是周期性的等待几个条件：1，目标OSD的失效时间大于通过固定量osd_heartbeat_grace和历史网络条件动态确定的阈值；2，来自不同主机的汇报达到mon_osd_min_down_reporters。3，满足前两个条件前失效汇报没有被源OSD取消。
 - **扩散**：作为中心节点的Monitor并没有在更新OSDMap后尝试广播通知所有的OSD和Client，而是惰性的等待OSD和Client来获取。以此来减少Monitor压力并简化交互逻辑。
