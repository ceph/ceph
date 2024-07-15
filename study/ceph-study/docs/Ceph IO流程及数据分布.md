# 1. Ceph IO流程及数据分布
![rados_io_1.png](https://upload-images.jianshu.io/upload_images/2099201-db0fd6e3e3f49f68.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 1.1 正常IO流程图
![ceph_io_2.png](https://upload-images.jianshu.io/upload_images/2099201-2c47144a5118bcf0.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**步骤：**
 1. client 创建cluster handler。
 2. client 读取配置文件。
 3. client 连接上monitor，获取集群map信息。
 4. client 读写io 根据crshmap 算法请求对应的主osd数据节点。
 5. 主osd数据节点同时写入另外两个副本节点数据。
 6. 等待主节点以及另外两个副本节点写完数据状态。
 7. 主节点及副本节点写入状态都成功后，返回给client，io写入完成。


## 1.2 新主IO流程图
**说明：**
如果新加入的OSD1取代了原有的 OSD4成为 Primary OSD, 由于 OSD1 上未创建 PG , 不存在数据，那么 PG 上的 I/O 无法进行，怎样工作的呢？
![ceph_io_3.png](https://upload-images.jianshu.io/upload_images/2099201-9cc1013f7e3dc8f9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**步骤：**
 1. client连接monitor获取集群map信息。
 2. 同时新主osd1由于没有pg数据会主动上报monitor告知让osd2临时接替为主。
 3. 临时主osd2会把数据全量同步给新主osd1。
 4. client IO读写直接连接临时主osd2进行读写。
 5. osd2收到读写io，同时写入另外两副本节点。
 6. 等待osd2以及另外两副本写入成功。
 7. osd2三份数据都写入成功返回给client, 此时client io读写完毕。
 8. 如果osd1数据同步完毕，临时主osd2会交出主角色。
 9. osd1成为主节点，osd2变成副本。

## 1.3 Ceph IO算法流程
![ceph_io_4.png](https://upload-images.jianshu.io/upload_images/2099201-b24c72ac8bbf1a19.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

 1. File用户需要读写的文件。File->Object映射：
   a. ino (File的元数据，File的唯一id)。
   b. ono(File切分产生的某个object的序号，默认以4M切分一个块大小)。
   c. oid(object id: ino + ono)。
 
2. Object是RADOS需要的对象。Ceph指定一个静态hash函数计算oid的值，将oid映射成一个近似均匀分布的伪随机值，然后和mask按位相与，得到pgid。Object->PG映射：
  a. hash(oid) & mask-> pgid 。
  b. mask = PG总数m(m为2的整数幂)-1 。
 
3. PG(Placement Group),用途是对object的存储进行组织和位置映射, (类似于redis cluster里面的slot的概念) 一个PG里面会有很多object。采用CRUSH算法，将pgid代入其中，然后得到一组OSD。PG->OSD映射： 
  a. CRUSH(pgid)->(osd1,osd2,osd3) 。


## 1.4 Ceph IO伪代码流程
```
locator = object_name
obj_hash =  hash(locator)
pg = obj_hash % num_pg
osds_for_pg = crush(pg)    # returns a list of osds
primary = osds_for_pg[0]
replicas = osds_for_pg[1:]
```

## 1.5 Ceph RBD IO流程
![ceph_rbd_io.png](https://upload-images.jianshu.io/upload_images/2099201-ed51d7d8050dbf64.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**步骤：**
 1. 客户端创建一个pool，需要为这个pool指定pg的数量。
 2. 创建pool/image rbd设备进行挂载。
 3. 用户写入的数据进行切块，每个块的大小默认为4M，并且每个块都有一个名字，名字就是object+序号。
 4. 将每个object通过pg进行副本位置的分配。
 5. pg根据cursh算法会寻找3个osd，把这个object分别保存在这三个osd上。
 6. osd上实际是把底层的disk进行了格式化操作，一般部署工具会将它格式化为xfs文件系统。
 7. object的存储就变成了存储一个文rbd0.object1.file。


## 1.6 Ceph RBD IO框架图
![ceph_rbd_io1.png](https://upload-images.jianshu.io/upload_images/2099201-850a745bc0f44494.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**客户端写数据osd过程：**
 1. 采用的是librbd的形式，使用librbd创建一个块设备，向这个块设备中写入数据。
 2. 在客户端本地同过调用librados接口，然后经过pool，rbd，object、pg进行层层映射,在PG这一层中，可以知道数据保存在哪3个OSD上，这3个OSD分为主从的关系。
 3. 客户端与primay OSD建立SOCKET 通信，将要写入的数据传给primary OSD，由primary OSD再将数据发送给其他replica OSD数据节点。

## 1.7 Ceph Pool和PG分布情况
![ceph_pool_pg.png](https://upload-images.jianshu.io/upload_images/2099201-d49d90ae6a918ef2.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**说明：**
 - pool是ceph存储数据时的逻辑分区，它起到namespace的作用。
 - 每个pool包含一定数量(可配置)的PG。
 - PG里的对象被映射到不同的Object上。
 - pool是分布到整个集群的。
 - pool可以做故障隔离域，根据不同的用户场景不一进行隔离。


## 1.8 Ceph 数据扩容PG分布
**场景数据迁移流程：**
 - 现状3个OSD, 4个PG
 - 扩容到4个OSD, 4个PG

**现状：**
![ceph_recory_1.png](https://upload-images.jianshu.io/upload_images/2099201-4dda9e2648dabe90.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**扩容后：**
![ceph_io_recry2.png](https://upload-images.jianshu.io/upload_images/2099201-9e324e87c6d086f3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**说明**
每个OSD上分布很多PG, 并且每个PG会自动散落在不同的OSD上。如果扩容那么相应的PG会进行迁移到新的OSD上，保证PG数量的均衡。
