# 1. 设置不同的object size进行测试
## 1.1 object size 4M

```
#上传到挂载目录
$ ll -lh /mnt/object_size_4M/
total 2.7G
-rw-r--r-- 1 root root 832M Jun 21 11:09 alchemy-api.error.log._test_1G
-rw-r--r-- 1 root root 1.9G Jun 21 10:59 alchemy-api.info.log.201806211029
 
 
#查看所有块
$ rados -p cephfs_data ls
10000006977.00000a79
10000006977.00000557
10000006977.00000171
10000006977.000003c6
10000006977.0000058e
10000006977.00000849
10000006977.00000bc0
10000006977.000001a9
10000006977.0000063b
10000006977.00000943
 
#查看块大小
$ rados -p cephfs_data get 10000006977.00000943 /tmp/ss.log
$ ll -lh /tmp/ss.log
-rw-r--r-- 1 root root 4.0M Jun 21 10:37 /tmp/ss.log
 
#搜索1.9G文件内容
$ time grep "lihangxxxx" /mnt/object_size_4M/alchemy-api.info.log.201806211029
real    0m3.088s
user    0m0.154s
sys 0m0.670s
 
#搜索1G文件内容
time grep "lihangxxxx" /mnt/object_size_4M/alchemy-api.error.log._test_1G
real    0m1.325s
user    0m0.066s
sys 0m0.337s
```

## 1.2 object size 16M

```
#设置块大小
$ mkdir /mnt/object_size_16M/
$ setfattr -n ceph.dir.layout -v "stripe_unit=16777216 stripe_count=16 object_size=16777216 pool=cephfs_data" /mnt/object_size_16M/
 
#19G文件上传到挂载目录
$ ll  -lh  /mnt/object_size_16M/
total 2.7G
-rw-r--r-- 1 root root 832M Jun 21 11:12 alchemy-api.error.log._test_1G
-rw-r--r-- 1 root root 1.9G Jun 21 11:12 alchemy-api.info.log.201806211029
 
#查看所有块
$ rados -p cephfs_data ls
10000006979.000002da
10000006979.000000f8
10000006979.000001bb
10000006979.000003c6
10000006979.0000024c
10000006979.000002f2
10000006979.00000448
10000006979.0000025a
10000006979.000000f5
10000006979.000002ed
 
#查看块大小
$ rados -p cephfs_data get 10000006979.000002ed /tmp/ss.log
$ ll -lh /tmp/ss.log
-rw-r--r-- 1 root root 16M Jun 21 10:45 /tmp/ss.log
 
 
#搜索1.9G文件内容
$ time grep "lihangxxxx" /mnt/object_size_16M/alchemy-api.info.log.201806211029
real    0m3.701s
user    0m0.148s
sys 0m0.662s
 
 
 
#搜索1G文件内容
time grep "lihangxxxx" /mnt/object_size_16M/alchemy-api.error.log._test_1G
real    0m1.360s
user    0m0.063s
sys 0m0.348s
 
```

## 1.3. object size 64M

```
#设置块大小
$ mkdir /mnt/object_size_64M/
$ setfattr -n ceph.dir.layout -v "stripe_unit=67108864 stripe_count=64 object_size=67108864 pool=cephfs_data"  /mnt/object_size_64M/
 
 
#19G文件上传到挂载目录
$ ll  -lh   /mnt/object_size_64M/
total 2.7G
-rw-r--r-- 1 root root 832M Jun 21 11:12 alchemy-api.error.log._test_1G
-rw-r--r-- 1 root root 1.9G Jun 21 11:12 alchemy-api.info.log.201806211029
 
 
#查看所有块
$ rados -p cephfs_data ls
10000006979.000002da
10000006979.000000f8
10000006979.000001bb
10000006979.000003c6
10000006979.0000024c
10000006979.000002f2
10000006979.00000448
10000006979.0000025a
10000006979.000000f5
10000006979.000002ed
 
 
#查看块大小
$ rados -p cephfs_data get 10000006979.000002ed /tmp/ss.log
$ ll -lh /tmp/ss.log
-rw-r--r-- 1 root root 64M Jun 21 10:45 /tmp/ss.log
 
 
#搜索1.9G文件内容
$ time grep "lihangxxxx"  /mnt/object_size_64M//alchemy-api.info.log.201806211029
real    0m4.830s
user    0m0.137s
sys 0m0.710s
 
 
 
#搜索1G文件内容
$ time grep "lihangxxxx" /mnt/object_size_64M/alchemy-api.error.log._test_1G
real    0m1.708s
user    0m0.062s
sys 0m0.316s
```

# 2. 测试结论
 - 通过测试结论发现，设置越大的object size对检索读取文件性能有一定的影响，目前可以看出默认4M性能最优。

| 文件大小 | object size | unit | count | 耗时 |
|:---:|:---:|:---:|:---:|:---:|
| 1.9G | 4M | | |	 3.088s |
| 1.9G | 16M | 16M | 16 |	3.701s |
| 1.9G | 64M | 64M | 64 | 4.830s |
| 1G	| 4M	| | | 1.325s |
| 1G | 16M | 16M |16 | 1.360s |
| 1G | 64M | 64M | 64 | 1.708s |
| 11G | 4M | | | 16.880s |
| 11G | 16M | 16M | 16 | 1m17.401s |
| 11G | 64M | 64M | 64 | 2m22.575s |

	 	 	 
| 文件大小 | object size | unit | count | 耗时 |
|:---:|:---:|:---:|:---:|:---:|
| 1G | 4M | 512K | 2,4,6,8 | 1.303s, 2.206s, 2.084s, 1.112s |
| 1.9G | 4M | 512K | 2,4,6,8 | 3.014s, 2.488s, 3.012s, 2.509s |
| 11G | 4M | 512K | 2,4,6,8 | 15.482s,15.852s,15.310s,14.424s  |
| | | | | |
| 1G | 16M | 512K | 2,4,6,8 | 1.282s, 1.207s，1.229s，1.238s |
| 1.9G | 16M | 512K	 | 2,4,6,8	| 2.481s, 4.047s，3.820s，4.004s |
| 11G | 16M | 512K	| 2,4,6,8	| 13.925s, 1m46.600s，54.475s，1m23.614s|
| | | | | | 
| 1G	| 64M | 512K	| 2,4,6,8	| 1.684s,1.451s |
| 1.9G | 64M | 512K	| 2,4,6,8	| 2m17.855s,6.389s |
| 11G | 64M | 512K	| 2,4,6,8	| 4m6.544s,23.964s |
 	 	 	 	 
# 3. 官方测试资料
https://rc.coepp.org.au/_media/cephfs-fio-analysis.pdf
