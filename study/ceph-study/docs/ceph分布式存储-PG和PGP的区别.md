# 一、前言
首先来一段英文关于PG和PGP区别的解释：
```
PG = Placement Group
PGP = Placement Group for Placement purpose

pg_num = number of placement groups mapped to an OSD

When pg_num is increased for any pool, every PG of this pool splits into half, but they all remain mapped to their parent OSD.

Until this time, Ceph does not start rebalancing. Now, when you increase the pgp_num value for the same pool, PGs start to migrate from the parent to some other OSD, and cluster rebalancing starts. This is how PGP plays an important role.
By Karan Singh
```
以上是来自邮件列表的 Karan Singh 的PG和PGP的相关解释，他也是 Learning Ceph 和 Ceph Cookbook的作者，以上的解释没有问题，我们来看下具体在集群里面具体作用

# 二、实践
环境准备，因为是测试环境，我只准备了两台机器，每台机器4个OSD，所以做了一些参数的设置，让数据尽量散列
```
osd_crush_chooseleaf_type = 0
```
以上为修改的参数，这个是让我的环境故障域为OSD分组的

创建测试需要的存储池
我们初始情况只创建一个名为testpool包含6个PG的存储池
```
ceph osd pool create testpool 6 6
pool 'testpool' created
```
我们看一下默认创建完了后的PG分布情况
```
ceph pg dump pgs|grep ^1|awk '{print $1,$2,$15}'
dumped pgs in format plain
1.1 0 [3,6,0]
1.0 0 [7,0,6]
1.3 0 [4,1,2]
1.2 0 [7,4,1]
1.5 0 [4,6,3]
1.4 0 [3,0,4]
```
我们写入一些对象，因为我们关心的不仅是pg的变动，同样关心PG内对象有没有移动,所以需要准备一些测试数据，这个调用原生rados接口写最方便
```
rados -p testpool bench 20 write --no-cleanup
```
我们再来查询一次
```
ceph pg dump pgs|grep ^1|awk '{print $1,$2,$15}'
dumped pgs in format plain
1.1 75 [3,6,0]
1.0 83 [7,0,6]
1.3 144 [4,1,2]
1.2 146 [7,4,1]
1.5 86 [4,6,3]
1.4 80 [3,0,4]
```
可以看到写入了一些数据，其中的第二列为这个PG当中的对象的数目，第三列为PG所在的OSD

## 2.1 增加PG测试
我们来扩大PG再看看
```
ceph osd pool set testpool pg_num 12
set pool 1 pg_num to 12
```
再次查询
```
ceph pg dump pgs|grep ^1|awk '{print $1,$2,$15}'
dumped pgs in format plain
1.1 37 [3,6,0]
1.9 38 [3,6,0]
1.0 41 [7,0,6]
1.8 42 [7,0,6]
1.3 48 [4,1,2]
1.b 48 [4,1,2]
1.7 48 [4,1,2]
1.2 48 [7,4,1]
1.6 49 [7,4,1]
1.a 49 [7,4,1]
1.5 86 [4,6,3]
1.4 80 [3,0,4]
```
可以看到上面新加上的PG的分布还是基于老的分布组合，并没有出现新的OSD组合，
因为我们当前的设置是pgp为6,那么三个OSD的组合的个数就是6个，因为当前为12个pg，
分布只能从6种组合里面挑选，所以会有重复的组合

根据上面的分布情况，可以确定的是，增加PG操作会引起PG内部对象分裂，分裂的份数是根据新增PG组合重复情况来的，比如上面的情况
 - 1.1的对象分成了两份[3,6,0]
 - 1.3的对象分成了三份[4,1,2]
 - 1.4的对象没有拆分[3,0,4]

**结论：**增加PG会引起PG内的对象分裂，也就是在OSD上创建了新的PG目录，然后进行部分对象的move的操作

## 2.2 增加PGP测试
我们将原来的PGP从6调整到12
```
ceph osd pool set testpool pgp_num 12
ceph pg dump pgs|grep ^1|awk '{print $1,$2,$15}'
dumped pgs in format plain
1.a 49 [1,2,6]
1.b 48 [1,6,2]
1.1 37 [3,6,0]
1.0 41 [7,0,6]
1.3 48 [4,1,2]
1.2 48 [7,4,1]
1.5 86 [4,6,3]
1.4 80 [3,0,4]
1.7 48 [1,6,0]
1.6 49 [3,6,7]
1.9 38 [1,4,2]
1.8 42 [1,2,3]
```
可以看到PG里面的对象并没有发生变化，而PG所在的对应关系发生了变化
我们看下与调整PGP前的对比
```
*1.1 37 [3,6,0]          1.1 37 [3,6,0]*
1.9 38 [3,6,0]          1.9 38 [1,4,2]
*1.0 41 [7,0,6]          1.0 41 [7,0,6]*
1.8 42 [7,0,6]          1.8 42 [1,2,3]
*1.3 48 [4,1,2]          1.3 48 [4,1,2]*
1.b 48 [4,1,2]          1.b 48 [1,6,2]
1.7 48 [4,1,2]          1.7 48 [1,6,0]
*1.2 48 [7,4,1]          1.2 48 [7,4,1]*
1.6 49 [7,4,1]          1.6 49 [3,6,7]
1.a 49 [7,4,1]          1.a 49 [1,2,6]
*1.5 86 [4,6,3]          1.5 86 [4,6,3]*
*1.4 80 [3,0,4]          1.4 80 [3,0,4]*
```
可以看到其中最原始的6个PG的分布并没有变化（标注了*号），变化的是后增加的PG，也就是将重复的PG分布进行新分布，这里并不是随机完全打散，而是根据需要去进行重分布

**结论：** 调整PGP不会引起PG内的对象的分裂，但是会引起PG的分布的变动

# 三、结论
 - PG是指定存储池存储对象的目录有多少个，PGP是存储池PG的OSD分布组合个数
 - PG的增加会引起PG内的数据进行分裂，分裂到相同的OSD上新生成的PG当中
 - PGP的增加会引起部分PG的分布进行变化，但是不会引起PG内对象的变动
