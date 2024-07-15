# 1. 环境介绍
## 1.1 软件环境

**ceph集群：**
 - mon：ceph-xxx-osd02.ys,ceph-xxx-osd03.ys,ceph-xxx-osd01.ys
 - osd: 36
 - mds：ceph-xxx-osd04.ys=up:active

**ceph版本：**
 - ceph version 12.2.2.3 (277c813c8cdeb79eba8a11bfe08faf1bf7809f07) luminous (stable)

**os系统：**
 - CentOS Linux release 7.2.1511 (Core)
 - 3.10.0-514.16.1.el7.x86_64

## 1.2 硬件环境
**cpu型号：** Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
**cpu核数：** 40
**硬盘：** hdd

## 2. 测试场景
### 2.1 单个磁盘的能力
下表以测试随机写IOPS的命令为例，说明命令中各种参数的含义。

| 参数 | 说明 |
|:---|:---|
| -direct=1 | 表示测试时忽略I/O缓存，数据直写。 |
| -iodepth=128 | 表示使用AIO时，同时发出I/O数的上限为128。 | 
| -rw=randwrite | 表示测试时的读写策略为随机写（random writes）。作其它测试时可以设置为：<br/>1. randread（随机读random reads）<br/> 2. read（顺序读sequential reads）<br/> 3. write（顺序写sequential writes <br/> 4. randrw（混合随机读写mixed random reads and writes）|
| -ioengine=libaio | 表示测试方式为libaio（Linux AIO，异步I/O）。应用使用I/O通常有二种方式：同步和异步。同步的I/O一次只能发出一个I/O请求，等待内核完成才返回。这样对于单个线程iodepth总是小于1，但是可以透过多个线程并发执行来解决。通常会用 16−32 根线程同时工作将iodepth塞满。异步则通常使用libaio这样的方式一次提交一批I/O请求，然后等待一批的完成，减少交互的次数，会更有效率。|
| -bs=4k | 表示单次I/O的块文件大小为4k。未指定该参数时的默认大小也是4k。测试IOPS时，建议将bs设置为一个比较小的值，如本示例中的4k。测试吞吐量时，建议将bs设置为一个较大的值，如本示例中的1024k。|
| -size=1G | 表示测试文件大小为1G。|
| -numjobs=1 | 表示测试线程数为1。|
| -runtime=1000 | 表示测试时间为1000秒。如果未配置，则持续将前述-size指定大小的文件，以每次-bs值为分块大小写完。|
| -group_reporting | 表示测试结果里汇总每个进程的统计信息，而非以不同job汇总展示信息。|
| -name=Rand_Write_Testing | 表示测试任务名称为Rand_Write_Testing，可以随意设定。|

### 2.1.1 顺序读(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：377MB/s  平均IOPS： 89  运行时长：2848ms   耗时：1249ms
```
$ fio -direct=1 -iodepth=128 -rw=read -ioengine=libaio -bs=4M -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test0012 -name=Read_PPS_Testing
Read_PPS_Testing: (g=0): rw=read, bs=(R) 4096KiB-4096KiB, (W) 4096KiB-4096KiB, (T) 4096KiB-4096KiB, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Jobs: 1 (f=1): [R(1)][30.0%][r=232MiB/s,w=0KiB/s][r=58,w=0 IOPS][eta 00m:07s]
Read_PPS_Testing: (groupid=0, jobs=1): err= 0: pid=26652: Thu Jun  7 16:34:27 2018
   read: IOPS=89, BW=360MiB/s (377MB/s)(1024MiB/2848msec)
    slat (usec): min=623, max=54269, avg=10938.52, stdev=8604.56
    clat (msec): min=46, max=2750, avg=1238.56, stdev=741.72
     lat (msec): min=47, max=2751, avg=1249.50, stdev=743.03
    clat percentiles (msec):
     |  1.00th=[   48],  5.00th=[   56], 10.00th=[   65], 20.00th=[  342],
     | 30.00th=[  802], 40.00th=[ 1200], 50.00th=[ 1401], 60.00th=[ 1586],
     | 70.00th=[ 1787], 80.00th=[ 1972], 90.00th=[ 2140], 95.00th=[ 2165],
     | 99.00th=[ 2198], 99.50th=[ 2198], 99.90th=[ 2735], 99.95th=[ 2735],
     | 99.99th=[ 2735]
   bw (  KiB/s): min=155648, max=253952, per=57.11%, avg=210261.33, stdev=50053.95, samples=3
   iops        : min=   38, max=   62, avg=51.33, stdev=12.22, samples=3
  lat (msec)   : 50=1.95%, 100=13.67%, 250=3.12%, 500=5.08%, 750=5.08%
  lat (msec)   : 1000=5.86%, 2000=46.48%, >=2000=18.75%
  cpu          : usr=0.00%, sys=9.41%, ctx=188, majf=0, minf=802
  IO depths    : 1=0.4%, 2=0.8%, 4=1.6%, 8=3.1%, 16=6.2%, 32=12.5%, >=64=75.4%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=99.2%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.8%
     issued rwt: total=256,0,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
   READ: bw=360MiB/s (377MB/s), 360MiB/s-360MiB/s (377MB/s-377MB/s), io=1024MiB (1074MB), run=2848-2848msec
Disk stats (read/write):
  sde: ios=6616/0, merge=0/0, ticks=359717/0, in_queue=366490, util=96.03%
```
### 2.1.2 随机读(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：289MB/s  平均IOPS： 68  运行时长：3717ms   耗时：1423ms
```
$ fio -direct=1 -iodepth=128 -rw=randread -ioengine=libaio -bs=4M -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test0012 -name=Rand_Read_Testing
Rand_Read_Testing: (g=0): rw=randread, bs=(R) 4096KiB-4096KiB, (W) 4096KiB-4096KiB, (T) 4096KiB-4096KiB, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Jobs: 1 (f=1): [r(1)][30.0%][r=272MiB/s,w=0KiB/s][r=68,w=0 IOPS][eta 00m:07s]
Rand_Read_Testing: (groupid=0, jobs=1): err= 0: pid=29439: Thu Jun  7 16:36:49 2018
   read: IOPS=68, BW=275MiB/s (289MB/s)(1024MiB/3717msec)
    slat (usec): min=764, max=41253, avg=14163.74, stdev=9476.51
    clat (msec): min=90, max=2159, avg=1409.08, stdev=569.89
     lat (msec): min=106, max=2166, avg=1423.24, stdev=570.40
    clat percentiles (msec):
     |  1.00th=[  123],  5.00th=[  347], 10.00th=[  523], 20.00th=[  768],
     | 30.00th=[ 1062], 40.00th=[ 1418], 50.00th=[ 1703], 60.00th=[ 1770],
     | 70.00th=[ 1854], 80.00th=[ 1905], 90.00th=[ 1938], 95.00th=[ 2005],
     | 99.00th=[ 2056], 99.50th=[ 2056], 99.90th=[ 2165], 99.95th=[ 2165],
     | 99.99th=[ 2165]
   bw (  KiB/s): min=90112, max=327680, per=88.57%, avg=249856.00, stdev=110414.87, samples=4
   iops        : min=   22, max=   80, avg=61.00, stdev=26.96, samples=4
  lat (msec)   : 100=0.39%, 250=2.73%, 500=6.25%, 750=9.38%, 1000=8.20%
  lat (msec)   : 2000=67.97%, >=2000=5.08%
  cpu          : usr=0.03%, sys=7.56%, ctx=255, majf=0, minf=801
  IO depths    : 1=0.4%, 2=0.8%, 4=1.6%, 8=3.1%, 16=6.2%, 32=12.5%, >=64=75.4%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=99.2%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.8%
     issued rwt: total=256,0,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
   READ: bw=275MiB/s (289MB/s), 275MiB/s-275MiB/s (289MB/s-289MB/s), io=1024MiB (1074MB), run=3717-3717msec
Disk stats (read/write):
  sde: ios=8064/0, merge=0/0, ticks=508040/0, in_queue=515960, util=97.27%
```

### 2.1.3 顺序写(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：224MB/s  平均IOPS： 53  运行时长：4785ms   耗时：1819ms
```
$ fio -direct=1 -iodepth=128 -rw=write -ioengine=libaio -bs=4M -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test00123 -name=Write_PPS_Testing
Write_PPS_Testing: (g=0): rw=write, bs=(R) 4096KiB-4096KiB, (W) 4096KiB-4096KiB, (T) 4096KiB-4096KiB, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Write_PPS_Testing: Laying out IO file (1 file / 1024MiB)
Jobs: 1 (f=1): [W(1)][50.0%][r=0KiB/s,w=228MiB/s][r=0,w=57 IOPS][eta 00m:05s]
Write_PPS_Testing: (groupid=0, jobs=1): err= 0: pid=3653: Thu Jun  7 16:50:53 2018
  write: IOPS=53, BW=214MiB/s (224MB/s)(1024MiB/4785msec)
    slat (usec): min=544, max=41043, avg=18384.66, stdev=3151.59
    clat (msec): min=77, max=2476, avg=1801.54, stdev=762.46
     lat (msec): min=95, max=2496, avg=1819.93, stdev=762.97
    clat percentiles (msec):
     |  1.00th=[  113],  5.00th=[  288], 10.00th=[  518], 20.00th=[  978],
     | 30.00th=[ 1418], 40.00th=[ 1888], 50.00th=[ 2299], 60.00th=[ 2333],
     | 70.00th=[ 2400], 80.00th=[ 2433], 90.00th=[ 2467], 95.00th=[ 2467],
     | 99.00th=[ 2467], 99.50th=[ 2467], 99.90th=[ 2467], 99.95th=[ 2467],
     | 99.99th=[ 2467]
   bw (  KiB/s): min=49152, max=237568, per=87.48%, avg=191692.80, stdev=80181.23, samples=5
   iops        : min=   12, max=   58, avg=46.80, stdev=19.58, samples=5
  lat (msec)   : 100=0.78%, 250=3.12%, 500=5.47%, 750=5.86%, 1000=5.47%
  lat (msec)   : 2000=21.88%, >=2000=57.42%
  cpu          : usr=1.28%, sys=2.72%, ctx=260, majf=0, minf=30
  IO depths    : 1=0.4%, 2=0.8%, 4=1.6%, 8=3.1%, 16=6.2%, 32=12.5%, >=64=75.4%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=99.2%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.8%
     issued rwt: total=0,256,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
  WRITE: bw=214MiB/s (224MB/s), 214MiB/s-214MiB/s (224MB/s-224MB/s), io=1024MiB (1074MB), run=4785-4785msec
Disk stats (read/write):
  sde: ios=0/7899, merge=0/0, ticks=0/652613, in_queue=658268, util=92.57%
```

### 2.1.4 随机写(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：197MB/s  平均IOPS： 46   运行时长：5454ms 耗时：ss50ms
```
$ fio -direct=1 -iodepth=128 -rw=randwrite -ioengine=libaio -bs=4M -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test001234 -name=Rand_Write_Testing
Rand_Write_Testing: (g=0): rw=randwrite, bs=(R) 4096KiB-4096KiB, (W) 4096KiB-4096KiB, (T) 4096KiB-4096KiB, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Rand_Write_Testing: Laying out IO file (1 file / 1024MiB)
Jobs: 1 (f=1): [w(1)][54.5%][r=0KiB/s,w=144MiB/s][r=0,w=36 IOPS][eta 00m:05s]
Rand_Write_Testing: (groupid=0, jobs=1): err= 0: pid=6773: Thu Jun  7 16:53:28 2018
  write: IOPS=46, BW=188MiB/s (197MB/s)(1024MiB/5454msec)
    slat (usec): min=539, max=100202, avg=20492.25, stdev=15576.03
    clat (msec): min=206, max=3281, avg=2230.30, stdev=822.98
     lat (msec): min=211, max=3291, avg=2250.79, stdev=821.50
    clat percentiles (msec):
     |  1.00th=[  247],  5.00th=[  531], 10.00th=[  768], 20.00th=[ 1452],
     | 30.00th=[ 2005], 40.00th=[ 2366], 50.00th=[ 2534], 60.00th=[ 2635],
     | 70.00th=[ 2769], 80.00th=[ 2937], 90.00th=[ 3037], 95.00th=[ 3071],
     | 99.00th=[ 3205], 99.50th=[ 3239], 99.90th=[ 3272], 99.95th=[ 3272],
     | 99.99th=[ 3272]
   bw (  KiB/s): min=114688, max=188416, per=83.80%, avg=161109.33, stdev=26248.51, samples=6
   iops        : min=   28, max=   46, avg=39.33, stdev= 6.41, samples=6
  lat (msec)   : 250=1.17%, 500=3.52%, 750=5.08%, 1000=3.52%, 2000=16.41%
  lat (msec)   : >=2000=70.31%
  cpu          : usr=1.23%, sys=2.38%, ctx=262, majf=0, minf=30
  IO depths    : 1=0.4%, 2=0.8%, 4=1.6%, 8=3.1%, 16=6.2%, 32=12.5%, >=64=75.4%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=99.2%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.8%
     issued rwt: total=0,256,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
  WRITE: bw=188MiB/s (197MB/s), 188MiB/s-188MiB/s (197MB/s-197MB/s), io=1024MiB (1074MB), run=5454-5454msec
Disk stats (read/write):
  sde: ios=0/8140, merge=0/0, ticks=0/717641, in_queue=725290, util=93.59%
```

### 2.1.5 顺序读(block size 4k)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：264MB/s  平均IOPS： 64.4k  运行时长：4068ms 耗时：1.9ms
```
$ fio -direct=1 -iodepth=128 -rw=read -ioengine=libaio -bs=4k -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test0012 -name=Read_PPS_Testing
Read_PPS_Testing: (g=0): rw=read, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Jobs: 1 (f=1): [R(1)][100.0%][r=269MiB/s,w=0KiB/s][r=68.9k,w=0 IOPS][eta 00m:00s]
Read_PPS_Testing: (groupid=0, jobs=1): err= 0: pid=20688: Thu Jun  7 16:28:49 2018
   read: IOPS=64.4k, BW=252MiB/s (264MB/s)(1024MiB/4068msec)
    slat (usec): min=2, max=571, avg= 3.75, stdev= 3.81
    clat (usec): min=136, max=1011.5k, avg=1981.49, stdev=17975.31
     lat (usec): min=140, max=1011.5k, avg=1985.32, stdev=17975.32
    clat percentiles (usec):
     |  1.00th=[    338],  5.00th=[    429], 10.00th=[    603],
     | 20.00th=[    898], 30.00th=[    979], 40.00th=[   1844],
     | 50.00th=[   1926], 60.00th=[   1942], 70.00th=[   1975],
     | 80.00th=[   2024], 90.00th=[   2442], 95.00th=[   2507],
     | 99.00th=[   2835], 99.50th=[   5211], 99.90th=[  13173],
     | 99.95th=[  34866], 99.99th=[1002439]
   bw (  KiB/s): min=203744, max=359656, per=98.11%, avg=252880.00, stdev=46299.89, samples=8
   iops        : min=50936, max=89914, avg=63219.75, stdev=11575.00, samples=8
  lat (usec)   : 250=0.01%, 500=8.59%, 750=2.98%, 1000=19.20%
  lat (msec)   : 2=47.72%, 4=20.85%, 10=0.46%, 20=0.10%, 50=0.05%
  lat (msec)   : 2000=0.03%
  cpu          : usr=4.25%, sys=26.01%, ctx=43067, majf=0, minf=163
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.1%
     issued rwt: total=262144,0,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
   READ: bw=252MiB/s (264MB/s), 252MiB/s-252MiB/s (264MB/s-264MB/s), io=1024MiB (1074MB), run=4068-4068msec
Disk stats (read/write):
  sde: ios=239348/0, merge=0/0, ticks=490468/0, in_queue=490489, util=97.52%
```

### 2.1.6 随机读(block size 4k)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：2900kB/s  平均IOPS： 707  运行时长：370303ms   耗时：180ms
```
fio -direct=1 -iodepth=128 -rw=randread -ioengine=libaio -bs=4k -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test00 -name=Rand_Read_Testing
Rand_Read_Testing: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Jobs: 1 (f=1): [r(1)][99.7%][r=8072KiB/s,w=0KiB/s][r=2018,w=0 IOPS][eta 00m:01s]
Rand_Read_Testing: (groupid=0, jobs=1): err= 0: pid=38517: Thu Jun  7 16:15:43 2018
   read: IOPS=707, BW=2832KiB/s (2900kB/s)(1024MiB/370303msec)
    slat (usec): min=2, max=855, avg= 3.61, stdev= 2.17
    clat (msec): min=11, max=962, avg=180.81, stdev=55.54
     lat (msec): min=11, max=962, avg=180.81, stdev=55.54
    clat percentiles (msec):
     |  1.00th=[   66],  5.00th=[  133], 10.00th=[  140], 20.00th=[  148],
     | 30.00th=[  153], 40.00th=[  159], 50.00th=[  167], 60.00th=[  176],
     | 70.00th=[  188], 80.00th=[  207], 90.00th=[  245], 95.00th=[  284],
     | 99.00th=[  393], 99.50th=[  443], 99.90th=[  558], 99.95th=[  609],
     | 99.99th=[  726]
   bw (  KiB/s): min= 1704, max= 8384, per=99.80%, avg=2825.44, stdev=400.58, samples=740
   iops        : min=  426, max= 2096, avg=706.35, stdev=100.15, samples=740
  lat (msec)   : 20=0.01%, 50=0.49%, 100=1.75%, 250=88.63%, 500=8.90%
  lat (msec)   : 750=0.22%, 1000=0.01%
  cpu          : usr=0.08%, sys=0.31%, ctx=65772, majf=0, minf=162
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.1%
     issued rwt: total=262144,0,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
   READ: bw=2832KiB/s (2900kB/s), 2832KiB/s-2832KiB/s (2900kB/s-2900kB/s), io=1024MiB (1074MB), run=370303-370303msec
Disk stats (read/write):
  sde: ios=261680/152, merge=0/1, ticks=47370387/6, in_queue=47374260, util=100.00%
```
### 2.1.7 顺序写(block size 4k)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：349MB/s  平均IOPS： 85.3k  运行时长：3073ms  耗时：1.4ms
```
 fio -direct=1 -iodepth=128 -rw=write -ioengine=libaio -bs=4k -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test0012 -name=Write_PPS_Testing
Write_PPS_Testing: (g=0): rw=write, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Write_PPS_Testing: Laying out IO file (1 file / 1024MiB)
Jobs: 1 (f=1): [W(1)][-.-%][r=0KiB/s,w=330MiB/s][r=0,w=84.4k IOPS][eta 00m:00s]
Write_PPS_Testing: (groupid=0, jobs=1): err= 0: pid=17836: Thu Jun  7 16:26:45 2018
  write: IOPS=85.3k, BW=333MiB/s (349MB/s)(1024MiB/3073msec)
    slat (usec): min=3, max=337, avg= 6.05, stdev= 4.49
    clat (usec): min=655, max=4742, avg=1493.19, stdev=573.07
     lat (usec): min=660, max=4774, avg=1499.35, stdev=572.69
    clat percentiles (usec):
     |  1.00th=[  775],  5.00th=[  824], 10.00th=[  873], 20.00th=[ 1020],
     | 30.00th=[ 1074], 40.00th=[ 1123], 50.00th=[ 1205], 60.00th=[ 1647],
     | 70.00th=[ 1893], 80.00th=[ 2040], 90.00th=[ 2278], 95.00th=[ 2507],
     | 99.00th=[ 2999], 99.50th=[ 3228], 99.90th=[ 3654], 99.95th=[ 3785],
     | 99.99th=[ 4015]
   bw (  KiB/s): min=276064, max=444488, per=100.00%, avg=341630.67, stdev=55834.75, samples=6
   iops        : min=69016, max=111122, avg=85407.67, stdev=13958.69, samples=6
  lat (usec)   : 750=0.41%, 1000=16.84%
  lat (msec)   : 2=59.88%, 4=22.86%, 10=0.01%
  cpu          : usr=8.04%, sys=50.75%, ctx=103073, majf=0, minf=31
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.1%
     issued rwt: total=0,262144,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
  WRITE: bw=333MiB/s (349MB/s), 333MiB/s-333MiB/s (349MB/s-349MB/s), io=1024MiB (1074MB), run=3073-3073msec
Disk stats (read/write):
  sde: ios=0/247342, merge=0/0, ticks=0/309854, in_queue=310008, util=96.66%
```

### 2.1.8 随机写(block size 4k)
//block size是4M, 30个线程并发，持续时间200s
测试结果：带宽：11.2MB/s  平均IOPS： 2730  运行时长：95995ms   平均耗时：46ms
```
$ fio -direct=1 -iodepth=128 -rw=randwrite -ioengine=libaio -bs=4k -size=1G -numjobs=1 -runtime=1000 -group_reporting -filename=/var/lib/ceph/osd/ceph-3/test00 -name=Rand_Write_Testing
Rand_Write_Testing: (g=0): rw=randwrite, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=128
fio-3.1
Starting 1 process
Jobs: 1 (f=1): [w(1)][100.0%][r=0KiB/s,w=17.1MiB/s][r=0,w=4368 IOPS][eta 00m:00s]
Rand_Write_Testing: (groupid=0, jobs=1): err= 0: pid=29959: Thu Jun  7 16:03:16 2018
  write: IOPS=2730, BW=10.7MiB/s (11.2MB/s)(1024MiB/95995msec)
    slat (usec): min=3, max=8086, avg= 5.42, stdev=21.91
    clat (usec): min=781, max=176463, avg=46865.68, stdev=21344.90
     lat (usec): min=786, max=176468, avg=46871.22, stdev=21344.98
    clat percentiles (usec):
     |  1.00th=[  1500],  5.00th=[ 15926], 10.00th=[ 22938], 20.00th=[ 30278],
     | 30.00th=[ 35390], 40.00th=[ 40109], 50.00th=[ 44827], 60.00th=[ 50070],
     | 70.00th=[ 55837], 80.00th=[ 63177], 90.00th=[ 73925], 95.00th=[ 84411],
     | 99.00th=[107480], 99.50th=[120062], 99.90th=[145753], 99.95th=[156238],
     | 99.99th=[173016]
   bw (  KiB/s): min= 5864, max=61248, per=99.73%, avg=10893.37, stdev=4353.23, samples=191
   iops        : min= 1466, max=15312, avg=2723.34, stdev=1088.31, samples=191
  lat (usec)   : 1000=0.46%
  lat (msec)   : 2=2.48%, 4=0.23%, 10=0.31%, 20=3.75%, 50=52.98%
  lat (msec)   : 100=38.14%, 250=1.65%
  cpu          : usr=0.70%, sys=1.92%, ctx=236086, majf=0, minf=31
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.1%
     issued rwt: total=0,262144,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128
Run status group 0 (all jobs):
  WRITE: bw=10.7MiB/s (11.2MB/s), 10.7MiB/s-10.7MiB/s (11.2MB/s-11.2MB/s), io=1024MiB (1074MB), run=95995-95995msec
Disk stats (read/write):
  sde: ios=4/261751, merge=0/0, ticks=334/12270799, in_queue=12272756, util=99.93%
```

# 3. 测试结论
| 操作 | block size | 吞吐量 | IOPS | 耗时 |
|:---:|:---:|:---:|:---:|:---:|
|顺序读 | 4M | 377MB/s  | 89 | 1249ms |
| 随机读 | 4M | 289MB/s | 68 | 1423ms |
| 顺序写 | 4M | 224MB/s | 53 | 1819ms |
| 随机写 | 4M | 197MB/s | 46 | 2250ms |
| 顺序读 | 4k | 164MB/s | 64.4k | 1.9ms |
| 随机读 | 4k | 2900kB/s | 707 | 180ms |
| 顺序写 | 4k | 349MB/s | 85.3k | 1.4ms |
| 随机写 | 4k | 11.2MB/s | 2730 | 46ms |

# 4. 集群能力评估
**参考：**
 - 由于Ceph存储结构不同于物理硬件，所以影响其IOPS的因素主要有网络、副本数量、日志、OSD(硬盘)数量、OSD服务器数量、OSD IOPS等。

 - 这里给出一个来自Mirantis的经验公式：
    - IOPS = 硬盘IOPS * 硬盘数量 * 0.88 / 副本数量
    - 其中0.88为4-8k随机读操作占比(88%)，如果OSD不是以硬盘为单位而是RAID组，则替换公式中对应参数。

 - 关于Ceph的IOPS计算仅供参考，计算结果可能会跟物理环境实测有较大偏差。

## 4.1 IOPS估算
参考上述公式，结合测试报告，推算公式如下：
 - IOPS = 硬盘IOPS * 硬盘数量  / 副本数量(只针对写)
 - 随机读写：
    - 磁盘IOPS = (随机读+随机写）/  2   (按照平均值计算)
    - 副本数量 =  1~3  (按照读写计算)

**根据公式估算测试集群rbdbench(36个osd) iops：**
 - 4M块大小：IOPS = （68+46)/2 * 36  / 3 = （最小值：684 ~ 最高值：2052）
 - 4k块大小： IOPS = (707+2730)/2 * 36  / 3  =  （最小值：61866 ~ 最高值：20622)

## 4.2 吞吐量估算
参考上述公式，结合测试报告，推算公式如下：
 - IOPS = 硬盘吞吐量 * 硬盘数量  / 副本数量(只针对写)
 - 顺序读写：
    - 硬盘吞吐量 = (顺序读+顺序写）/  2   (按照平均值计算)
    - 副本数量 =  1~3  (按照读写计算)
 

**根据公式估算测试集群rbdbench(36个osd) 吞吐量：**
 - 4M块大小：IOPS = （377+224)/2 * 36  / 3 =(最小值： 3606MB/s ~  最高值：10818MB/s)
 - 4k块大小： IOPS = （164+349)/2 * 36  / 3  =  (最小值： 3078MB/s ~  最高值：9234MB/s)






