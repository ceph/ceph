## 1. 介绍
CRUSH 算法通过计算数据存储位置来确定如何存储和检索。 CRUSH 授权 Ceph 客户端直接连接 OSD ，
而非通过一个中央服务器或代理。数据存储、检索算法的使用，使 Ceph 避免了单点故障、性能瓶颈、和伸缩的物理限制。

CRUSH 需要一张集群的 Map，且使用 CRUSH Map 把数据伪随机地、尽量平均地分布到整个集群的 OSD 里。
CRUSH Map 包含 OSD 列表、把设备汇聚为物理位置的“桶”列表、和指示 CRUSH 如何复制存储池里的数据的规则列表。

完全手动管理 CRUSH Map 也是可能的，在配置文件中设定：
```
osd crush update on start = false
```

## 2. 操作 CRUSH Map
### 2.1 提取CRUSH Map
```
#提取最新crush图#ceph osd getcrushmap -o {compiled-crushmap-filename}
  
$ ceph osd getcrushmap -o /tmp/crush
  
#反编译crush图
# crushtool -d {compiled-crushmap-filename} -o {decompiled-crushmap-filename}
$ crushtool -d /tmp/crush -o /tmp/decompiled_crush
```

### 2.2 注入CRUSH Map
```
#编译crush图#crushtool -c {decompiled-crush-map-filename} -o {compiled-crush-map-filename}
  
$ crushtool -c /tmp/decompiled_crush -o /tmp/crush_new
#注入crush图
# ceph osd setcrushmap -i {compiled-crushmap-filename}
$ ceph osd setcrushmap -i /tmp/crush_new
```

## 3. CRUSH Map 参数

**CRUSH Map 主要有 4 个段落。**
- 设备：由任意对象存储设备组成，即对应一个 ceph-osd进程的存储器。 Ceph 配置文件里的每个 OSD 都应该有一个设备。
- 桶类型： 定义了 CRUSH 分级结构里要用的桶类型（ types ），桶由逐级汇聚的存储位置（如行、机柜、机箱、主机等等）及其权重组成。
- 桶实例： 定义了桶类型后，还必须声明主机的桶类型、以及规划的其它故障域。
- 规则： 由选择桶的方法组成。

### 3.1 CRUSH Map 之设备
为把 PG 映射到 OSD ， CRUSH Map 需要 OSD 列表（即配置文件所定义的 OSD 守护进程名称），所以它们首先出现在 CRUSH Map 里。
要在 CRUSH Map 里声明一个设备，在设备列表后面新建一行，输入 device 、之后是唯一的数字 ID 、之后是相应的 ceph-osd 守护进程实例名字。
```
# devices
device {num} {osd.name}
 
#例如：
# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2
device 3 osd.3
```

### 3.2 CRUSH Map 之桶类型
CRUSH Map 里的第二个列表定义了 bucket （桶）类型，桶简化了节点和叶子层次。节点（或非叶子）桶在分级结构里一般表示物理位置，
节点汇聚了其它节点或叶子，叶桶表示 ceph-osd 守护进程及其对应的存储媒体。

要往 CRUSH Map 中增加一种 bucket 类型，在现有桶类型列表下方新增一行，输入 type 、之后是惟一数字 ID 和一个桶名。按惯例，
会有一个叶子桶为 type 0 ，然而你可以指定任何名字（如 osd 、 disk 、 drive 、 storage 等等）：
```
# types
type {num} {bucket-name}
 
#例如：
# types
type 0 osd
type 1 host
type 2 chassis
type 3 rack
type 4 row
type 5 pdu
type 6 pod
type 7 room
type 8 datacenter
type 9 region
type 10 root
```

### 3.3 CRUSH Map 之桶层次
CRUSH 算法根据各设备的权重、大致统一的概率把数据对象分布到存储设备中。 CRUSH 根据你定义的集群运行图分布对象及其副本， 
CRUSH Map 表达了可用存储设备以及包含它们的逻辑单元。

要把 PG 映射到跨故障域的 OSD ，一个 CRUSH Map 需定义一系列分级桶类型（即现有 CRUSH Map 的 # type 下）。
创建桶分级结构的目的是按故障域隔离叶子节点，像主机、机箱、机柜、电力分配单元、机群、行、房间、和数据中心。
除了表示叶子节点的 OSD ，其它分级结构都是任意的，你可以按需定义。

声明一个桶实例时，你必须指定其类型、惟一名称（字符串）、惟一负整数 ID （可选）、指定和各条目总容量/能力相关的权重、指定桶算法（通常是 straw ）、
和哈希（通常为 0 ，表示哈希算法 rjenkins1 ）。一个桶可以包含一到多个条目，这些条目可以由节点桶或叶子组成，它们可以有个权重用来反映条目的相对权重。

你可以按下列语法声明一个节点桶：
```
[bucket-type] [bucket-name] {
        id [a unique negative numeric ID]
        weight [the relative capacity/capability of the item(s)]
        alg [the bucket type: uniform | list | tree | straw ]
        hash [the hash type: 0 by default]
        item [item-name] weight [weight]
}
 
#例如，我们可以定义两个主机桶和一个机柜桶，机柜桶包含两个主机桶， OSD 被声明为主机桶内的条目
host node1 {
        id -1
        alg straw
        hash 0
        item osd.0 weight 1.00
        item osd.1 weight 1.00
}
 
host node2 {
        id -2
        alg straw
        hash 0
        item osd.2 weight 1.00
        item osd.3 weight 1.00
}
 
rack rack1 {
        id -3
           alg straw
        hash 0
        item node1 weight 2.00
        item node2 weight 2.00
}
```

#### 3.3.1 调整桶的权重
Ceph 用双精度类型数据表示桶权重。权重和设备容量不同，我们建议用 1.00 作为 1TB 存储设备的相对权重，这样 0.5 的权重大概代表 500GB 、
3.00 大概代表 3TB 。较高级桶的权重是所有叶子桶的权重之和。

一个桶的权重是一维的，你也可以计算条目权重来反映存储设备性能。例如，如果你有很多 1TB 的硬盘，其中一些数据传输速率相对低、其他的数据传输率相对高，
即使它们容量相同，也应该设置不同的权重（如给吞吐量较低的硬盘设置权重 0.8 ，较高的设置 1.20 ）。

## 3.4 CRUSH Map 之规则
CRUSH Map 支持“ CRUSH 规则”的概念，用以确定一个存储池里数据的分布。CRUSH 规则定义了归置和复制策略、或分布策略，
用它可以规定 CRUSH 如何放置对象副本。对大型集群来说，你可能创建很多存储池，且每个存储池都有它自己的 CRUSH 规则集和规则。
默认的 CRUSH Map 里，每个存储池有一条规则、一个规则集被分配到每个默认存储池。

注意： 大多数情况下，你都不需要修改默认规则。新创建存储池的默认规则集是 0 。

规则格式如下：
```
rule <rulename> {
 
        ruleset <ruleset>
        type [ replicated | erasure ]
        min_size <min-size>
        max_size <max-size>
        step take <bucket-type>
        step [choose|chooseleaf] [firstn|indep] <N> <bucket-type>
        step emit
}
```

**参数说明：**
- ruleset：区分一条规则属于某个规则集的手段。给存储池设置规则集后激活。
- type：规则类型，目前仅支持 replicated 和 erasure ，默认是 replicated 。
- min_size：可以选择此规则的存储池最小副本数。
- max_size：可以选择此规则的存储池最大副本数。
- step take <bucket-name>：选取起始的桶名，并迭代到树底。
- step choose firstn {num} type {bucket-type}：选取指定类型桶的数量，这个数字通常是存储池的副本数（即 pool size ）。如果 {num} == 0 ， 选择 pool-num-replicas 个桶（所有可用的）；如果 {num} > 0 && < pool-num-replicas ，就选择那么多的桶；如果 {num} < 0 ，它意味着选择 pool-num-replicas - {num} 个桶。
- step chooseleaf firstn {num} type {bucket-type}：选择 {bucket-type} 类型的桶集合，并从各桶的子树里选择一个叶子节点。桶集合的数量通常是存储池的副本数（即 pool size ）。如果 {num} == 0 ，选择 pool-num-replicas 个桶（所有可用的）；如果 {num} > 0 && < pool-num-replicas ，就选择那么多的桶；如果 {num} < 0 ，它意味着选择 pool-num-replicas - {num}个桶。
- step emit：输出当前值并清空堆栈。通常用于规则末尾，也适用于相同规则应用到不同树的情况。

## 4. 主亲和性
某个 Ceph 客户端读写数据时，总是连接 acting set 里的主 OSD （如 [2, 3, 4] 中， osd.2 是主的）。
有时候某个 OSD 与其它的相比并不适合做主 OSD （比如其硬盘慢、或控制器慢）。最大化硬件利用率时为防止性能瓶颈（特别是读操作），
你可以调整 OSD 的主亲和性，这样 CRUSH 就尽量不把它用作 acting set 里的主 OSD 了。
```
ceph osd primary-affinity <osd-id> <weight>
```
主亲和性默认为 1 （就是说此 OSD 可作为主 OSD ）。此值合法范围为 0-1 ，其中 0 意为此 OSD 不能用作主的， 1 意为 OSD 可用作主的。

此权重 < 1 时， CRUSH 选择主 OSD 时选中它的可能性就较低。

## 5. 增加/移动 OSD
要增加或移动在线集群里 OSD 所对应的 CRUSH Map 条目，执行 ceph osd crush set 命令。
```
ceph osd crush set {id-or-name} {weight} {bucket-type}={bucket-name} [{bucket-type}={bucket-name} ...]
```

## 6. 调整 OSD 的 CRUSH 权重
要调整在线集群中某个 OSD 的 CRUSH 权重，执行命令：
```
ceph osd crush reweight {name} {weight}
```

## 7. 删除 OSD
要从在线集群里把某个 OSD 彻底踢出 CRUSH Map，或仅踢出某个指定位置的 OSD，执行命令：
```
#从 crush map 中删除一个 osd
$ ceph osd crush rm osd.0
```

## 8. 增加桶
要在运行集群的 CRUSH Map 中新建一个桶，用 ceph osd crush add-bucket 命令：
```
ceph osd crush add-bucket {bucket-name} {bucket-type}
```

## 9. 移动桶
要把一个桶移动到 CRUSH Map 里的不同位置，执行命令：
```
ceph osd crush move {bucket-name} {bucket-type}={bucket-name} [{bucket-type}={bucket-name} ...]
```

10. 删除桶
要把一个桶从 CRUSH Map 的分级结构中删除，可用此命令：
```
ceph osd crush remove {bucket-name}
```
注意：从 CRUSH 分级结构里删除时必须是空桶。
 
 
