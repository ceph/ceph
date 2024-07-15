# 1.说明
## 1.1介绍
pool是ceph存储数据时的逻辑分区，它起到namespace的作用。其他分布式存储系统，比如Mogilefs、Couchbase、Swift都有pool的概念，只是叫法不同。
每个pool包含一定数量的PG，PG里的对象被映射到不同的OSD上，因此pool是分布到整个集群的。

# 2. 常用操作
## 2.1 查看pool数量
```
$ ceph osd lspools
1 rbd,2 test_data,3 test_metadata,5 test,6 benmark_test,7 .rgw.root,8 default.rgw.control,9 default.rgw.meta,10 default.rgw.log,11 default.rgw.buckets.index,12 web-services,13 test_pool,15 cephfs_data,16 cephfs_metadata,
```

## 2.2 创建pool
```
$ ceph osd pool create test_lihang 100  #这里的100指的是PG组
pool 'test_lihang' created
```

## 2.3 为pool配置配额
```
$ ceph osd pool set-quota test_lihang max_objects 10000
set-quota max_objects = 10000 for pool test_lihang
```

## 2.4 删除pool
```
$ ceph osd pool delete test_lihang test_lihang --yes-i-really-really-mean-it    #pool的名字需要重复两次
pool 'test_lihang' removed
```

## 2.5 查看pool详细信息
```
$ rados df
POOL_NAME                 USED   OBJECTS CLONES COPIES  MISSING_ON_PRIMARY UNFOUND DEGRADED RD_OPS  RD     WR_OPS   WR
.rgw.root                   1113       4      0      12                  0       0        0    1578  1052k        4   4096
benmark_test               8131M 1975366      0 5926098                  0       0        0       0      0        0      0
cephfs_data                    0       0      0       0                  0       0        0       0      0        0      0
cephfs_metadata             2246      21      0      63                  0       0        0       0      0       42   8192
default.rgw.buckets.index      0       1      0       3                  0       0        0       0      0        7      0
default.rgw.control            0       8      0      24                  0       0        0       0      0        0      0
default.rgw.log                0     191      0     573                  0       0        0 4706893  4596M  3134451      0
default.rgw.meta            1014       6      0      18                  0       0        0     107 100352       47  11264
rbd                       29047M   19619    176   58857                  0       0        0 1014831   835M   354490 11459M
test                        3606      10      0      30                  0       0        0      34  31744        0      0
test_data                   130G  292511      0  877533                  0       0        0   44647  5692k 59192695 56594M
test_lihang                    0       0      0       0                  0       0        0       0      0        0      0
test_metadata             42036k     164      0     492                  0       0        0   25567   491M   177927  2779M
test_pool                   305G  100678      0  302034                  0       0        0    3966   709M   337549   305G
web-services                  36       1      0       3                  0       0        0       0      0        1   1024
total_objects    2388580
total_used       1915G
total_avail      154T
total_space      156T
```

## 2.6 给一个pool创建快照
```
$ ceph osd pool delete test_lihang test_lihang --yes-i-really-really-mean-it    #pool的名字需要重复两次
pool 'test_lihang' removed
```

## 2.7 创建pool快照
```
$ ceph osd pool mksnap test_lihang date-snap
created pool test_lihang snap date-snap
```

## 2.8 删除pool快照
```
$ ceph osd pool rmsnap test_lihang date-snap
removed pool test_lihang snap date-snap
```

## 2.9 查看pool池pg数量
```
$ ceph osd pool get test_lihang pg_num
pg_num: 100
```

## 2.10 设置pool池副本数
```
$ ceph osd pool set test_lihang size 3
set pool 18 size to 3
```

## 2.11 查看pool池副本数
```
$ ceph osd pool get test_lihang size
size: 3
```

## 2.12 设置pool池写最小副本
```
#设置pool池写操作最小副本为2
 
$ ceph osd pool set test_lihang min_size 2
set pool 18 min_size to 2
```

## 2.13 查看集群所有pool副本尺寸
```
$ ceph osd dump | grep 'replicated size'
pool 1 'rbd' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 2048 pgp_num 2048 last_change 5493 lfor 0/187 flags hashpspool stripe_width 0 application rbd
pool 2 'test_data' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 512 pgp_num 512 last_change 1575 lfor 0/227 flags hashpspool stripe_width 0 application cephfs
```

## 2.14 获取pool的pg数量
```
$ ceph osd dump | grep 'replicated size'
pool 1 'rbd' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 2048 pgp_num 2048 last_change 5493 lfor 0/187 flags hashpspool stripe_width 0 application rbd
pool 2 'test_data' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 512 pgp_num 512 last_change 1575 lfor 0/227 flags hashpspool stripe_width 0 application cephfs
```

## 2.15 设置pool的pg数量
```
$ ceph osd pool set test_lihang pg_num 100
specified pg_num 100 <= current 100
 
$ ceph osd pool get test_lihang pg_num
pg_num: 100
```

## 2.16 设置pool的pgp数量
```
$ ceph osd pool set test_lihang pgp_num 100
set pool 18 pgp_num to 100
 
$ ceph osd pool get test_lihang pgp_num
pgp_num: 100
```

## 2.17 设置存储池类型
```
$ ceph osd pool application enable rbd rbd
enabled application 'rbd' on pool 'rbd' 
```

## 2.18 设置存储池crush rule
```
$ ceph osd pool set <poolname> crush_ruleset <ruleset>
ceph osd pool set ssd crush_ruleset 4
```

## 2.19 获取存储池crush rule
```
$ ceph osd pool get <poolname> crush_rule
ceph osd pool get test_pool crush_rule
crush_rule: replicated_rule
```

## 2.20 获取pool->pg->osd 关系
```
$ ceph osd getmap -o om
 
$ ceph osd getcrushmap -o cm
 
$ osdmaptool om --import-crush cm --test-map-pgs-dump --pool {pool_id}
```

## 2.21 设置 pool 及关联的文件系统目录的 quota
```
#create pool
ceph osd pool create sns_data 64

#add pool
ceph mds add_data_pool sns_data


# 设置 pool 名为 sns_data 的 quota，如: 120T
$ ceph osd pool set-quota sns_data max_bytes $((120 * 1024 * 1024 * 1024 * 1024))

# mount cephfs的根目录
$ ceph-fuse /mnt/

# 3. 设置根目录里的子目录 sns_data 的 quato(子目录 sns_data 关联的pool为 sns_data)
$ setfattr -n ceph.quota.max_bytes -v  $((120 * 1024 * 1024 * 1024 * 1024)) /mnt/sns_data

# change layout
$setfattr -n ceph.dir.layout.pool -v sns_data /mnt/sns_data

#add user
ceph auth get-or-create client.trade mon 'allow r' mds 'allow r, allow rw path=/trade' osd 'allow rw pool=cephfs_data'

#modify user caps
ceph auth caps client.sns mds 'allow rw path=/mnt/sns_data' mon 'allow r' osd 'allow rw pool=sns_data'

# 查看子目录 sns_data 
$ ceph-fuse -r /sns_data /test --user sns
$ df -h
```


# 3. 参数梳理
## 3.1 参数介绍


















