# 1.说明
## 1.1介绍
RADOS全称Reliable Autonomic Distributed Object Store，是Ceph集群的精华，用户实现数据分配、Failover等集群操作。

# 2. 常用操作
## 2.1 查看集群多少个pool
```
$ rados lspools
rbd
test_data
test_metadata
test
benmark_test
.rgw.root
default.rgw.control
default.rgw.meta
default.rgw.log
default.rgw.buckets.index
web-services
test_pool
cephfs_data
cephfs_metadata
test_lihang
```

## 2.2 查看集群pool容量情况
```$ rados df
POOL_NAME                 USED   OBJECTS CLONES COPIES  MISSING_ON_PRIMARY UNFOUND DEGRADED RD_OPS  RD     WR_OPS   WR
.rgw.root                   1113       4      0      12                  0       0        0    1578  1052k        4   4096
benmark_test               8131M 1975366      0 5926098                  0       0        0       0      0        0      0
cephfs_data                    0       0      0       0                  0       0        0       0      0        0      0
cephfs_metadata             2246      21      0      63                  0       0        0       0      0       42   8192
default.rgw.buckets.index      0       1      0       3                  0       0        0       0      0        7      0
default.rgw.control            0       8      0      24                  0       0        0       0      0        0      0
default.rgw.log                0     191      0     573                  0       0        0 4707751  4597M  3135023      0
default.rgw.meta            1014       6      0      18                  0       0        0     107 100352       47  11264
rbd                       29047M   19619    176   58857                  0       0        0 1014831   835M   354618 11460M
test                        3606      10      0      30                  0       0        0      34  31744        0      0
test_data                   130G  292511      0  877533                  0       0        0   44647  5692k 59192711 56594M
test_lihang                    0       0      0       0                  0       0        0       0      0        0      0
test_metadata             42105k     164      0     492                  0       0        0   25575   491M   177975  2779M
test_pool                   305G  100678      0  302034                  0       0        0    3966   709M   337597   305G
web-services                  36       1      0       3                  0       0        0       0      0        1   1024
total_objects    2388580
total_used       1914G
total_avail      154T
total_space      156T

```

## 2.3 创建pool
```
$ rados mkpool test_lihang1
successfully created pool test_lihang1
```

## 2.4 查看pool中object对象
```
$ rados ls -p test_data | more
10000026de5.00000000
1000005f1f3.00000000
100000664db.00000000
1000007461f.00000000
10000021bdf.00000000
1000005ef12.00000000
10000000fc8.00000000
1000002afd7.00000000
100000143b0.00000000
1000000179d.00000000
10000001b2f.00000000
10000073faa.00000000
10000072576.00000000
1000002a9f0.00000000
```

## 2.5 创建一个对象object
```
$ rados create test-object -p test_data
 
$ rados -p test_data ls
```

## 2.6 删除一个对象object
```
$ rados rm test-object -p test_data
```

# 3. 参数梳理
## 3.1 参数介绍


