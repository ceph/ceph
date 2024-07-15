#### Object
- Ceph最底层的存储单元是Object对象，每个Object包含元数据和原始数据。

#### OSD
- OSD全称Object Storage Device，也就是负责响应客户端请求返回具体数据的进程。一个Ceph集群一般都有很多个OSD。

#### PG
- PG全称Placement Grouops，是一个逻辑的概念，一个PG包含多个OSD。引入PG这一层其实是为了更好的分配数据和定位数据。

#### Monitor
- 一个Ceph集群需要多个Monitor组成的小集群，它们通过Paxos同步数据，用来保存OSD的元数据。

#### RADOS
- RADOS全称Reliable Autonomic Distributed Object Store，是Ceph集群的精华，用户实现数据分配、Failover等集群操作。

#### Libradio
- Librados是Rados提供库，因为RADOS是协议很难直接访问，因此上层的RBD、RGW和CephFS都是通过librados访问的，目前提供PHP、Ruby、Java、Python、C和C++支持。

#### CRUSH
- CRUSH是Ceph使用的数据分布算法，类似一致性哈希，让数据分配到预期的地方。

#### RBD
- RBD全称RADOS block device，是Ceph对外提供的块设备服务。

#### RGW
- RGW全称RADOS gateway，是Ceph对外提供的对象存储服务，接口与S3和Swift兼容。

#### MDS
- MDS全称Ceph Metadata Server，是CephFS服务依赖的元数据服务。

#### CephFS
- CephFS全称Ceph File System，是Ceph对外提供的文件系统服务。
