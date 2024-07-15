# 1. 数据分布算法挑战
 - 数据分布和负载均衡：
   a. 数据分布均衡，使数据能均匀的分布到各个节点上。
   b. 负载均衡，使数据访问读写操作的负载在各个节点和磁盘的负载均衡。
 - 灵活应对集群伸缩
   a. 系统可以方便的增加或者删除节点设备，并且对节点失效进行处理。
   b. 增加或者删除节点设备后，能自动实现数据的均衡，并且尽可能少的迁移数据。
- 支持大规模集群
   a. 要求数据分布算法维护的元数据相对较小，并且计算量不能太大。随着集群规模的增 加，数据分布算法开销相对比较小。

# 2. Ceph CRUSH算法说明
 - CRUSH算法的全称为：Controlled Scalable Decentralized Placement of Replicated Data，可控的、可扩展的、分布式的副本数据放置算法。
 - pg到OSD的映射的过程算法叫做CRUSH 算法。(一个Object需要保存三个副本，也就是需要保存在三个osd上)。
 - CRUSH算法是一个伪随机的过程，他可以从所有的OSD中，随机性选择一个OSD集合，但是同一个PG每次随机选择的结果是不变的，也就是映射的OSD集合是固定的。

# 3. Ceph CRUSH算法原理
**CRUSH算法因子：**
 - 层次化的Cluster Map
反映了存储系统层级的物理拓扑结构。定义了OSD集群具有层级关系的 静态拓扑结构。OSD层级使得 CRUSH算法在选择OSD时实现了机架感知能力，也就是通过规则定义， 使得副本可以分布在不同的机 架、不同的机房中、提供数据的安全性 。
 - Placement Rules
决定了一个PG的对象副本如何选择的规则，通过这些可以自己设定规则，用户可以自定义设置副本在集群中的分布。

## 3.1 层级化的Cluster Map
![ceph_crush.png](https://upload-images.jianshu.io/upload_images/2099201-f0f7321a9e37361f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

CRUSH Map是一个树形结构，OSDMap更多记录的是OSDMap的属性(epoch/fsid/pool信息以及osd的ip等等)。

叶子节点是device（也就是osd），其他的节点称为bucket节点，这些bucket都是虚构的节点，可以根据物理结构进行抽象，当然树形结构只有一个最终的根节点称之为root节点，中间虚拟的bucket节点可以是数据中心抽象、机房抽象、机架抽象、主机抽象等。


## 3.2 数据分布策略Placement Rules
**数据分布策略Placement Rules主要有特点：**
 a. 从CRUSH Map中的哪个节点开始查找
 b. 使用那个节点作为故障隔离域
 c. 定位副本的搜索模式（广度优先 or 深度优先）
```
rule replicated_ruleset  #规则集的命名，创建pool时可以指定rule集
{
    ruleset 0                #rules集的编号，顺序编即可   
    type replicated          #定义pool类型为replicated(还有erasure模式)   
    min_size 1                #pool中最小指定的副本数量不能小1
    max_size 10               #pool中最大指定的副本数量不能大于10       
    step take default         #查找bucket入口点，一般是root类型的bucket    
    step chooseleaf  firstn  0  type  host #选择一个host,并递归选择叶子节点osd     
    step emit        #结束
}
```

## 3.3 Bucket随机算法类型
![ceph_bucket.png](https://upload-images.jianshu.io/upload_images/2099201-ac18dabc9fb44d20.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

 - 一般的buckets：适合所有子节点权重相同，而且很少添加删除item。
 - list buckets：适用于集群扩展类型。增加item，产生最优的数据移动，查找item，时间复杂度O(n)。
 - tree buckets：查找负责度是O (log n), 添加删除叶子节点时，其他节点node_id不变。
 - straw buckets：允许所有项通过类似抽签的方式来与其他项公平“竞争”。定位副本时，bucket中的每一项都对应一个随机长度的straw，且拥有最长长度的straw会获得胜利（被选中），添加或者重新计算，子树之间的数据移动提供最优的解决方案。


# 4. CRUSH算法案例
**说明：**
集群中有部分sas和ssd磁盘，现在有个业务线性能及可用性优先级高于其他业务线，能否让这个高优业务线的数据都存放在ssd磁盘上。

**普通用户：**
![ceph_sas.png](https://upload-images.jianshu.io/upload_images/2099201-1bd6980a2141bc51.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**高优用户：**
![ssd.png](https://upload-images.jianshu.io/upload_images/2099201-127c6f8a40938233.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**配置规则：**
![ceph_crush1.png](https://upload-images.jianshu.io/upload_images/2099201-0084962b3a7847b4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
