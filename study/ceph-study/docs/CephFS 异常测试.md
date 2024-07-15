# 1. Cephfs 异常测试方案
CephFS允许客户端缓存metadata 30s，所以这里测试对MDS stop/start的时间间隔取为：2s，10s，60s。

| 测试组件 | 测试场景 | 挂载方式 | 测试方法 | 
|:---:|:---:|:---:|:---:|
| MDS | 单MDS | fuse/kernel | 单个MDS挂掉情况 2s/10s/60s IO情况 |
| | 主从MDS时 | fuse/kernel | 单主挂掉情况 2s/10s/60s IO情况 |
| | 主从MDS时 | fuse/kernel | 主从同时挂掉情况 2s/10s/60s IO情况 |
| MON | 单个MON | fuse/kernel | 单个MON挂掉情况 2s/10s/60s IO情况 |
| | 所有MON | fuse/kernel | 所有MON挂掉情况 2s/10s/60s IO情况 |
| OSD | 单个OSD | fuse/kernel | 单个OSD挂掉情况 2s/10s/60s IO情况	|
| | 集群一半OSD |	fuse/kernel | 一半OSD挂掉情况 2s/10s/60s IO情况 |
| | 集群所有OSD挂掉 | fuse/kernel | 所有OSD挂掉情况 2s/10s/60s IO情况 |

# 2. 测试环境
 - **mon：**  ceph-xxx-osd01.ys, ceph-xxx-osd02.ys, ceph-xxx-osd03.ys
 - **osd：** ceph-xxx-osd01.ys, ceph-xxx-osd02.ys, ceph-xxx-osd03.ys
 - **mds：** ceph-xxx-osd04.ys, ceph-xxx-osd05.ys

# 3. 测试工具
## fio
fio也是我们性能测试中常用的一个工具，详细介绍Google之。

**我们测试中固定配置：**
-filename=tstfile   指定测试文件的name
-size=20G           指定测试文件的size为20G
-direct=1           指定测试IO为DIRECT IO
-thread             指定使用thread模式
-name=fio-tst-name  指定job name

**测试bandwidth时：**
-ioengine=libaio/sync
-bs=512k/1M/4M/16M
-rw=write/read
-iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8

**测试iops时：**
-ioengine=libaio
-bs=4k
-runtime=300
-rw=randwrite/randread
-iodepth=64 -iodepth_batch=1 -iodepth_batch_complete=1

# 4. 测试步骤
## 4.1 MDS
### 4.1.1 单MDS挂掉
不需要测试，目前都是主从结构。

### 4.1.2 主从MDS主挂掉
```
#测试多个文件
#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
```
### 4.1.3 结论
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 停掉主MDS, io会出现稍微的抖动 |
| | 多个文件 | 停掉主MDS,会发生1-2秒的io夯住 |
| kernel | 单个文件 | 停掉主MDS, io会出现稍微的抖动 |
| | 多个文件 |  停掉主MDS,会发生1-2秒的io夯住 |

**单个文件：**
![image.png](https://upload-images.jianshu.io/upload_images/2099201-b5f7a0431d4e73be.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### 4.1.4 主从MDS都挂掉
```
#测试多个文件
#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
 
#18:19:24 fio
#18:19:28 sh mdsstop.sh
```
### 4.1.5 结论：
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 停掉主从MDS,40s左右io夯死 |
| | 多个文件 | 停掉主从MDS, io立马夯死 |
| kernel | 单个文件 | 停掉主从MDS,40s左右io夯死 |
| |多个文件 |停掉主从MDS, io立马夯死 |

**单个文件模式：**
![image.png](https://upload-images.jianshu.io/upload_images/2099201-d61d5b8fe18ea85f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 4.2 MON
### 4.2.1 单个MON挂掉
```
#测试多个文件
#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
```
### 4.2.2 结论
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 停掉单个MON，客户端写入无影响. |
| | 多个文件 | 停掉单个MON，客户端写入无影响. |
| kernel | 单个文件 | 停掉单个MON，客户端写入无影响. |
| | 多个文件 | 停掉单个MON，客户端写入无影响. |

### 4.2.3 所有MON挂掉
```
#测试多个文件
#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
```
### 4.2.4 结论
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 所有的MON都挂掉,会在60秒左右IO夯死 |
| | 多个文件 | 所有的MON挂掉,会在挂掉后立刻IO夯死 |
| kernel | 单个文件 | 所有的MON都挂掉,会在60秒左右IO夯死 |
| | 多个文件 | 所有的MON挂掉，会在挂掉后10秒左右IO夯死. |

**单个文件模式(内核模式)：**
![image.png](https://upload-images.jianshu.io/upload_images/2099201-3c32ba07c96d4621.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


**单个文件模式(fuse模式)：**
![image.png](https://upload-images.jianshu.io/upload_images/2099201-7a71576adc5120d7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 4.3 OSD
### 4.3.1 单个OSD挂掉
```
#测试多个文件
#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
```
### 4.3.2 结论
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 停掉一个osd，MDS客户端的写入无影响 |
| | 多个文件 | 停掉一个osd，MDS客户端的写入无影响. |
| kernel | 单个文件 | 停掉一个osd，MDS客户端的写入无影响 |
| | 多个文件 | 停掉一个osd，MDS客户端的写入无影响. |

### 4.3.3 集群一半OSD挂掉
```
#测试多个文件
#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
```
### 4.3.4 结论
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 集群2/3 的osd 挂掉,MDS客户端立刻会夯死. |
| | 多个文件 | 集群2/3 的osd 挂掉,MDS客户端立刻会夯死. |
| kernel  | 单个文件	 | 集群2/3 的osd 挂掉,MDS客户端立刻会夯死. |
| | 多个文件 | 集群2/3 的osd 挂掉,MDS客户端立刻会夯死. |

### 4.3.5 集群所有OSD挂掉
```

#!/bin/bash
while true
do
  curtime=`date --date='0 days ago' +%s`
  fio -filename=/test/$curtime -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4m -size=20G -runtime=1 -group_reporting -name=mytest
done
 
#测试单个文件
fio -filename-testfile -size=20G -direct=1 -thread -name=/test/fio-test-name -ioengine=libaio -bs=512k/1M/4M/16M -rw=rw  -write_bw_log=rw -iodepth=64 -iodepth_batch=8 -iodepth_batch_complete=8
```
### 4.3.6 结论
| 挂载方式 |  写入方式 | 故障描述 |
|:---:|:---:|:---:|
| fuse | 单个文件 | 集群所有的osd 挂掉,MDS客户端会立刻夯死 |
|  | 多个文件 | 集群所有的osd 挂掉,MDS客户端会立刻夯死. |
| kernel | 单个文件	| 集群所有的osd 挂掉,MDS客户端会立刻夯死 |
| |多个文件 | 集群所有 的osd 挂掉,MDS客户端立刻会夯死. |
