# 一、 集群环境
## 1.1 部署环境说明
**mon组件部署：** ceph-xxx-osd00, ceph-xxx-osd01, ceph-xxx-osd02
**osd组件部署：** ceph-xxx-osd00, ceph-xxx-osd01, ceph-xxx-osd02，ceph-xxx-osd03, ceph-xxx-osd04
**磁盘：** SATA
**ceph版本：** ceph 12.2.2 bluestore

# 二、 性能测试
## 2.1 rados bench测试
### 2.1.1 写测试
//默认block size是4M，30个线程并发，测试时间200s
测试结果：30线程并发写，带宽：1119.68 MB/s  平均IOPS：279  平均延迟：0.107s
```
rados bench -p test_pool 200 write -t 30 --no-cleanup  --cluster=test
Total time run:         200.075896
Total writes made:      56005
Write size:             4194304
Object size:            4194304
Bandwidth (MB/sec):     1119.68
Stddev Bandwidth:       13.3296
Max bandwidth (MB/sec): 1148
Min bandwidth (MB/sec): 1048
Average IOPS:           279
Stddev IOPS:            3
Max IOPS:               287
Min IOPS:               262
Average Latency(s):     0.107161
Stddev Latency(s):      0.0469331
Max latency(s):         0.50955
Min latency(s):         0.0251761
```
### 2.1.2 顺序读
//默认block size是4M，30个线程并发，测试时间200s
测试结果：30线程并发，带宽：1121.07 MB/s  平均IOPS：280  平均延迟0.106s
```
rados bench -p test_pool 200 seq -t 30 --no-cleanup  --cluster=test
Total time run:       199.827279
Total reads made:     56005
Read size:            4194304
Object size:          4194304
Bandwidth (MB/sec):   1121.07
Average IOPS:         280
Stddev IOPS:          2
Max IOPS:             288
Min IOPS:             260
Average Latency(s):   0.106283
Max latency(s):       0.780835
Min latency(s):       0.0198169
```
### 2.1.3 随机读
//默认block size是4M，30个线程并发，测试时间200s
测试结果：30线程并发，带宽：1109.64 MB/s  平均IOPS：279  平均延迟：0.106s
```
rados bench -p test_pool 200 rand -t 30 --no-cleanup --cluster=test
Total time run:       200.074550
Total reads made:     56003
Read size:            4194304
Object size:          4194304
Bandwidth (MB/sec):   1119.64
Average IOPS:         279
Stddev IOPS:          3
Max IOPS:             286
Min IOPS:             245
Average Latency(s):   0.106435
Max latency(s):       0.732702
Min latency(s):       0.0216828
```
## 2.2 rbd性能测试
### 2.2.1 顺序读写
//默认block size是4k，30个线程并发
测试结果：30线程并发，带宽：297 MB/s  平均IOPS：72681.76
```
rbd bench-write test_image  --io-threads 30  --pool=test_pool   --io-pattern seq --io-total 17199730000 --cluster=test
elapsed:    57  ops:  4199153  ops/sec: 72681.76  bytes/sec: 297704487.46
```
//block size是4M，30个线程并发
测试结果：30线程并发，带宽：844MB/s  平均IOPS：206.16
```
rbd bench-write test_image  --io-threads 30  --pool=test_pool   --io-pattern seq --io-total 17199730000 --io-size 4096000 --cluster=test 
elapsed:    20  ops:     4200  ops/sec:   206.16  bytes/sec: 844451109.43
```
### 2.2.2 随机读写
//默认block size是4k，30个线程并发
测试结果：30线程并发，带宽：36 MB/s  平均IOPS：8925.40
```
rbd bench-write test_image  --io-threads 30  --pool=test_pool   --io-pattern rand --io-total 17199730000  --cluster=test
elapsed:   470  ops:  4199153  ops/sec:  8925.40  bytes/sec: 36558420.10
```
//block size是4M，30个线程并发
测试结果：30线程并发，带宽：860 MB/s  平均IOPS： 210.18
```
rbd bench-write test_image  --io-threads 30  --pool=test_pool   --io-pattern rand --io-total 17199730000 --io-size 4096000 --cluster=test
elapsed:    19  ops:     4200  ops/sec:   210.18  bytes/sec: 860894998.27
```
## 2.3 fio+libaio测试
### 2.3.1 顺序读(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：2365.5 MB/s  平均IOPS： 591 耗时：50.58ms
```
fio -filename=/mnt/test/xxx -direct=1 -iodepth 1 -thread -rw=read -ioengine=libaio -bs=4M -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=read, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [R(30)] [100.0% done] [2580MB/0KB/0KB /s] [645/0/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=32265: Tue Jan 16 14:42:45 2018
  read : io=30720MB, bw=2365.5MB/s, iops=591, runt= 12987msec
    slat (usec): min=381, max=111307, avg=39421.29, stdev=12150.39
    clat (usec): min=806, max=99391, avg=11156.56, stdev=8561.94
     lat (msec): min=2, max=127, avg=50.58, stdev=11.41
    clat percentiles (usec):
     |  1.00th=[ 4128],  5.00th=[ 6496], 10.00th=[ 7072], 20.00th=[ 7072],
     | 30.00th=[ 7072], 40.00th=[ 7136], 50.00th=[ 7136], 60.00th=[ 7200],
     | 70.00th=[ 9152], 80.00th=[14528], 90.00th=[21888], 95.00th=[29568],
     | 99.00th=[47360], 99.50th=[49920], 99.90th=[54528], 99.95th=[94720],
     | 99.99th=[99840]
    bw (KB  /s): min=67764, max=105233, per=3.33%, avg=80770.99, stdev=4432.59
    lat (usec) : 1000=0.04%
    lat (msec) : 2=0.25%, 4=0.55%, 10=69.62%, 20=17.27%, 50=11.76%
    lat (msec) : 100=0.52%
  cpu          : usr=0.01%, sys=0.79%, ctx=23227, majf=0, minf=15436
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=7680/w=0/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=30720MB, aggrb=2365.5MB/s, minb=2365.5MB/s, maxb=2365.5MB/s, mint=12987msec, maxt=12987msec
Disk stats (read/write):
  nbd0: ios=243854/2, merge=0/1, ticks=1801606/27, in_queue=1802231, util=99.28%
```
### 2.3.1 随机读(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：1172.9 MB/s  平均IOPS： 293 耗时：101.27ms
```
fio -filename=/mnt/test/xxx -direct=1 -iodepth 1 -thread -rw=randread -ioengine=libaio -bs=4M -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=randread, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 19 (f=18): [_(1),r(3),_(1),r(3),_(1),r(2),_(1),r(1),_(1),r(1),_(1),r(1),_(2),r(5),_(1),r(3),_(2)] [96.4% done] [1312MB/0KB/0KB /s] [328/0/0 iops] [eta 00m:01s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=34255: Tue Jan 16 14:50:14 2018
  read : io=30720MB, bw=1172.9MB/s, iops=293, runt= 26194msec
    slat (usec): min=320, max=215905, avg=76016.00, stdev=31251.51
    clat (usec): min=774, max=249806, avg=25251.42, stdev=16090.29
     lat (msec): min=1, max=341, avg=101.27, stdev=35.33
    clat percentiles (usec):
     |  1.00th=[ 1544],  5.00th=[ 8256], 10.00th=[12992], 20.00th=[17024],
     | 30.00th=[19584], 40.00th=[21632], 50.00th=[23680], 60.00th=[25728],
     | 70.00th=[28032], 80.00th=[31360], 90.00th=[36608], 95.00th=[43264],
     | 99.00th=[66048], 99.50th=[102912], 99.90th=[228352], 99.95th=[238592],
     | 99.99th=[248832]
    bw (KB  /s): min=19200, max=81593, per=3.34%, avg=40089.91, stdev=6232.06
    lat (usec) : 1000=0.03%
    lat (msec) : 2=2.71%, 4=0.86%, 10=2.83%, 20=25.34%, 50=65.53%
    lat (msec) : 100=2.20%, 250=0.51%
  cpu          : usr=0.01%, sys=0.43%, ctx=23282, majf=0, minf=15444
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=7680/w=0/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=30720MB, aggrb=1172.9MB/s, minb=1172.9MB/s, maxb=1172.9MB/s, mint=26194msec, maxt=26194msec
Disk stats (read/write):
  nbd0: ios=242695/4, merge=0/1, ticks=3682263/186, in_queue=3683531, util=99.68%
```
### 2.3.2 顺序写(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：577 MB/s  平均IOPS： 141 耗时：180.71ms
```
fio -filename=/mnt/test/xxxxxx -direct=1 -iodepth 1 -thread -rw=write -ioengine=libaio -bs=4M -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=write, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 3 (f=3): [_(14),W(1),_(3),W(2),_(10)] [88.7% done] [0KB/595.5MB/0KB /s] [0/148/0 iops] [eta 00m:07s]                                                     s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=39754: Tue Jan 16 15:06:52 2018
  write: io=30720MB, bw=577632KB/s, iops=141, runt= 54459msec
    slat (usec): min=432, max=11789K, avg=146240.74, stdev=890871.23
    clat (usec): min=906, max=243958, avg=34464.33, stdev=18648.43
     lat (msec): min=2, max=11820, avg=180.71, stdev=889.82
    clat percentiles (usec):
     |  1.00th=[ 1832],  5.00th=[ 2160], 10.00th=[ 3312], 20.00th=[27520],
     | 30.00th=[34048], 40.00th=[36608], 50.00th=[38656], 60.00th=[40192],
     | 70.00th=[42240], 80.00th=[44800], 90.00th=[48384], 95.00th=[51456],
     | 99.00th=[59648], 99.50th=[69120], 99.90th=[238592], 99.95th=[242688],
     | 99.99th=[244736]
    bw (KB  /s): min=  350, max=191625, per=11.71%, avg=67662.34, stdev=44038.16
    lat (usec) : 1000=0.01%
    lat (msec) : 2=2.62%, 4=9.49%, 10=5.60%, 20=0.35%, 50=75.18%
    lat (msec) : 100=6.29%, 250=0.46%
  cpu          : usr=0.17%, sys=0.42%, ctx=17901, majf=0, minf=48
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=0/w=7680/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
  WRITE: io=30720MB, aggrb=577632KB/s, minb=577632KB/s, maxb=577632KB/s, mint=54459msec, maxt=54459msec
Disk stats (read/write):
  nbd0: ios=0/245786, merge=0/40, ticks=0/6934238, in_queue=6934029, util=99.88%
```
### 2.3.3 随机写(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：574 MB/s  平均IOPS： 140 耗时：175.80ms
```
fio -filename=/mnt/test/xxxxxx1 -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4M -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=randwrite, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 6 (f=6): [_(6),w(1),_(4),w(1),_(5),w(1),_(3),w(1),_(3),w(1),_(2),w(1),_(1)] [87.3% done] [0KB/567.5MB/0KB /s] [0/141/0 iops] [eta 00m:08s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=40883: Tue Jan 16 15:10:16 2018
  write: io=30720MB, bw=574531KB/s, iops=140, runt= 54753msec
    slat (usec): min=419, max=10185K, avg=140967.10, stdev=907840.86
    clat (msec): min=1, max=70, avg=34.83, stdev=15.47
     lat (msec): min=2, max=10224, avg=175.80, stdev=907.34
    clat percentiles (usec):
     |  1.00th=[ 1848],  5.00th=[ 2192], 10.00th=[ 3440], 20.00th=[32384],
     | 30.00th=[35584], 40.00th=[37632], 50.00th=[39168], 60.00th=[41216],
     | 70.00th=[42752], 80.00th=[45312], 90.00th=[48384], 95.00th=[51456],
     | 99.00th=[58112], 99.50th=[63232], 99.90th=[69120], 99.95th=[69120],
     | 99.99th=[70144]
    bw (KB  /s): min=  410, max=128000, per=12.24%, avg=70326.68, stdev=40733.29
    lat (msec) : 2=2.17%, 4=9.19%, 10=5.94%, 20=0.07%, 50=75.76%
    lat (msec) : 100=6.88%
  cpu          : usr=0.17%, sys=0.43%, ctx=17921, majf=0, minf=53
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=0/w=7680/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
  WRITE: io=30720MB, aggrb=574530KB/s, minb=574530KB/s, maxb=574530KB/s, mint=54753msec, maxt=54753msec
Disk stats (read/write):
  nbd0: ios=0/245594, merge=0/35, ticks=0/7007163, in_queue=7007071, util=99.88%
```
### 2.3.4 顺序读写(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：30线程并发，
读（带宽：980 MB/s  平均IOPS： 239 耗时：69.67ms）
写（带宽：762 MB/s  平均IOPS： 186 耗时：66.22ms）
```
fio -filename=/mnt/test/xxxxxx2 -direct=1 -iodepth 1 -thread -rw=rw -ioengine=libaio -bs=4M -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=rw, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 1 (f=1): [_(2),M(1),_(27)] [100.0% done] [891.2MB/647.4MB/0KB /s] [222/161/0 iops] [eta 00m:00s]         s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=1220: Tue Jan 16 15:13:14 2018
  read : io=17280MB, bw=980534KB/s, iops=239, runt= 18046msec
    slat (usec): min=328, max=437663, avg=58992.25, stdev=66315.88
    clat (usec): min=2, max=137801, avg=10681.00, stdev=12378.44
     lat (msec): min=1, max=465, avg=69.67, stdev=66.54
    clat percentiles (usec):
     |  1.00th=[ 1384],  5.00th=[ 1704], 10.00th=[ 2384], 20.00th=[ 3344],
     | 30.00th=[ 4256], 40.00th=[ 5152], 50.00th=[ 5984], 60.00th=[ 7264],
     | 70.00th=[ 9408], 80.00th=[15168], 90.00th=[27776], 95.00th=[35584],
     | 99.00th=[59136], 99.50th=[70144], 99.90th=[103936], 99.95th=[113152],
     | 99.99th=[138240]
    bw (KB  /s): min= 5333, max=106496, per=3.46%, avg=33881.20, stdev=15479.85
  write: io=13440MB, bw=762638KB/s, iops=186, runt= 18046msec
    slat (usec): min=462, max=443741, avg=50250.17, stdev=69529.56
    clat (usec): min=378, max=175843, avg=15969.00, stdev=14417.80
     lat (msec): min=2, max=477, avg=66.22, stdev=71.78
    clat percentiles (usec):
     |  1.00th=[ 1512],  5.00th=[ 1768], 10.00th=[ 2224], 20.00th=[ 3536],
     | 30.00th=[ 4640], 40.00th=[ 6240], 50.00th=[10304], 60.00th=[18816],
     | 70.00th=[24704], 80.00th=[29824], 90.00th=[35072], 95.00th=[39168],
     | 99.00th=[46848], 99.50th=[50432], 99.90th=[162816], 99.95th=[175104],
     | 99.99th=[175104]
    bw (KB  /s): min= 4708, max=89932, per=3.52%, avg=26814.32, stdev=13250.05
    lat (usec) : 4=0.01%, 250=0.01%, 500=0.01%, 750=0.04%, 1000=0.14%
    lat (msec) : 2=7.30%, 4=18.29%, 10=36.30%, 20=12.47%, 50=24.34%
    lat (msec) : 100=0.91%, 250=0.16%
  cpu          : usr=0.17%, sys=0.88%, ctx=21190, majf=0, minf=49
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=4320/w=3360/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=17280MB, aggrb=980534KB/s, minb=980534KB/s, maxb=980534KB/s, mint=18046msec, maxt=18046msec
  WRITE: io=13440MB, aggrb=762637KB/s, minb=762637KB/s, maxb=762637KB/s, mint=18046msec, maxt=18046msec
Disk stats (read/write):
  nbd0: ios=138078/107366, merge=0/3, ticks=1045037/1365373, in_queue=2410297, util=99.42%
```
### 2.3.5 随机读写(block size 4M)
//block size是4M, 30个线程并发，持续时间200s
测试结果：30线程并发，
读（带宽：512 MB/s  平均IOPS： 125 耗时：110.69ms）
写（带宽：398 MB/s  平均IOPS： 97   耗时：160.91ms）
```
fio -filename=/mnt/test/xxxxxx3 -direct=1 -iodepth 1 -thread -rw=randrw -ioengine=libaio -bs=4M -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=randrw, bs=4M-4M/4M-4M/4M-4M, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 18 (f=18): [m(4),_(2),m(1),_(2),m(1),_(2),m(4),_(4),m(2),_(1),m(1),_(1),m(5)] [94.6% done] [651.4MB/423.6MB/0KB /s] [162/105/0 iops] [eta 00m:02s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=2998: Tue Jan 16 15:19:09 2018
  read : io=17280MB, bw=512653KB/s, iops=125, runt= 34516msec
    slat (usec): min=330, max=1534.7K, avg=86957.58, stdev=113303.64
    clat (msec): min=1, max=1063, avg=23.73, stdev=24.36
     lat (msec): min=1, max=1552, avg=110.69, stdev=115.99
    clat percentiles (usec):
     |  1.00th=[ 1592],  5.00th=[ 3504], 10.00th=[ 7456], 20.00th=[10432],
     | 30.00th=[12992], 40.00th=[16064], 50.00th=[19328], 60.00th=[22912],
     | 70.00th=[28032], 80.00th=[34048], 90.00th=[43264], 95.00th=[55040],
     | 99.00th=[86528], 99.50th=[101888], 99.90th=[216064], 99.95th=[246784],
     | 99.99th=[1056768]
    bw (KB  /s): min= 2161, max=68776, per=3.84%, avg=19673.38, stdev=10392.27
  write: io=13440MB, bw=398730KB/s, iops=97, runt= 34516msec
    slat (usec): min=404, max=1522.5K, avg=128656.15, stdev=193442.37
    clat (msec): min=1, max=1261, avg=32.25, stdev=68.96
     lat (msec): min=2, max=1554, avg=160.91, stdev=211.14
    clat percentiles (usec):
     |  1.00th=[ 1704],  5.00th=[ 1912], 10.00th=[ 2352], 20.00th=[ 6048],
     | 30.00th=[16768], 40.00th=[23680], 50.00th=[28288], 60.00th=[32128],
     | 70.00th=[35584], 80.00th=[39168], 90.00th=[44800], 95.00th=[50432],
     | 99.00th=[415744], 99.50th=[544768], 99.90th=[1236992], 99.95th=[1253376],
     | 99.99th=[1253376]
    bw (KB  /s): min= 2210, max=44441, per=3.97%, avg=15818.67, stdev=8092.93
    lat (msec) : 2=4.05%, 4=6.25%, 10=10.43%, 20=22.85%, 50=50.48%
    lat (msec) : 100=5.07%, 250=0.39%, 500=0.21%, 750=0.20%, 2000=0.08%
  cpu          : usr=0.10%, sys=0.44%, ctx=21453, majf=0, minf=54
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=4320/w=3360/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=17280MB, aggrb=512652KB/s, minb=512652KB/s, maxb=512652KB/s, mint=34516msec, maxt=34516msec
  WRITE: io=13440MB, aggrb=398729KB/s, minb=398729KB/s, maxb=398729KB/s, mint=34516msec, maxt=34516msec
Disk stats (read/write):
  nbd0: ios=138208/107507, merge=0/16, ticks=2145787/2611698, in_queue=4757309, util=99.75%
```
### 2.3.6 顺序读(block size 4k)
//block size是4k, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：231 MB/s  平均IOPS： 57839 耗时：0.518ms
```
fio -filename=/mnt/test/xxxxxx5 -direct=1 -iodepth 1 -thread -rw=read -ioengine=libaio -bs=4k -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=read, bs=4K-4K/4K-4K/4K-4K, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [R(30)] [100.0% done] [211.8MB/0KB/0KB /s] [54.3K/0/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=6145: Tue Jan 16 15:28:45 2018
  read : io=30720MB, bw=231356KB/s, iops=57839, runt=135969msec
    slat (usec): min=2, max=1476, avg= 5.39, stdev= 2.81
    clat (usec): min=2, max=31844, avg=512.62, stdev=367.96
     lat (usec): min=53, max=31848, avg=518.07, stdev=367.95
    clat percentiles (usec):
     |  1.00th=[  346],  5.00th=[  374], 10.00th=[  394], 20.00th=[  422],
     | 30.00th=[  446], 40.00th=[  474], 50.00th=[  498], 60.00th=[  532],
     | 70.00th=[  556], 80.00th=[  580], 90.00th=[  620], 95.00th=[  644],
     | 99.00th=[  780], 99.50th=[  852], 99.90th=[ 1528], 99.95th=[ 1928],
     | 99.99th=[24704]
    bw (KB  /s): min= 6376, max= 8792, per=3.34%, avg=7719.35, stdev=404.15
    lat (usec) : 4=0.01%, 50=0.01%, 100=0.01%, 250=0.01%, 500=50.03%
    lat (usec) : 750=48.60%, 1000=1.09%
    lat (msec) : 2=0.23%, 4=0.01%, 10=0.01%, 20=0.01%, 50=0.02%
  cpu          : usr=0.23%, sys=1.65%, ctx=7865037, majf=0, minf=81
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=7864320/w=0/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=30720MB, aggrb=231356KB/s, minb=231356KB/s, maxb=231356KB/s, mint=135969msec, maxt=135969msec
Disk stats (read/write):
  nbd0: ios=7860030/37, merge=0/16, ticks=4014182/80, in_queue=4012440, util=99.98%
```
### 2.3.7 随机读(block size 4k)
//block size是4k, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：69 MB/s  平均IOPS： 17491 耗时：1.714ms
```
fio -filename=/mnt/test/xxxxxx6 -direct=1 -iodepth 1 -thread -rw=randread -ioengine=libaio -bs=4k -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=randread, bs=4K-4K/4K-4K/4K-4K, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [r(30)] [100.0% done] [81256KB/0KB/0KB /s] [20.4K/0/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=8166: Tue Jan 16 15:35:15 2018
  read : io=13665MB, bw=69966KB/s, iops=17491, runt=200002msec
    slat (usec): min=2, max=328, avg= 5.04, stdev= 2.22
    clat (usec): min=37, max=2512.8K, avg=1709.01, stdev=15457.78
     lat (usec): min=41, max=2512.8K, avg=1714.11, stdev=15457.78
    clat percentiles (usec):
     |  1.00th=[  532],  5.00th=[ 1032], 10.00th=[ 1240], 20.00th=[ 1352],
     | 30.00th=[ 1416], 40.00th=[ 1464], 50.00th=[ 1496], 60.00th=[ 1544],
     | 70.00th=[ 1592], 80.00th=[ 1656], 90.00th=[ 1736], 95.00th=[ 1816],
     | 99.00th=[ 2128], 99.50th=[ 4128], 99.90th=[17280], 99.95th=[29312],
     | 99.99th=[765952]
    bw (KB  /s): min=    2, max= 3696, per=3.57%, avg=2494.55, stdev=507.53
    lat (usec) : 50=0.02%, 100=0.05%, 250=0.01%, 500=0.57%, 750=1.72%
    lat (usec) : 1000=2.09%
    lat (msec) : 2=93.94%, 4=1.08%, 10=0.30%, 20=0.13%, 50=0.05%
    lat (msec) : 100=0.01%, 250=0.01%, 500=0.01%, 750=0.01%, 1000=0.01%
    lat (msec) : 2000=0.01%, >=2000=0.01%
  cpu          : usr=0.11%, sys=0.48%, ctx=3498696, majf=0, minf=85
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=3498327/w=0/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=13665MB, aggrb=69965KB/s, minb=69965KB/s, maxb=69965KB/s, mint=200002msec, maxt=200002msec
Disk stats (read/write):
  nbd0: ios=3498053/50, merge=0/16, ticks=5969304/9480, in_queue=5978090, util=100.00%
```
### 2.3.8 顺序写(block size 4k)
//block size是4k, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：51 MB/s  平均IOPS： 12991 耗时：2.308ms
```
fio -filename=/mnt/test/xxxxxx7 -direct=1 -iodepth 1 -thread -rw=write -ioengine=libaio -bs=4k -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=write, bs=4K-4K/4K-4K/4K-4K, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [W(30)] [100.0% done] [0KB/30488KB/0KB /s] [0/7622/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=11056: Tue Jan 16 15:45:24 2018
  write: io=10150MB, bw=51965KB/s, iops=12991, runt=200003msec
    slat (usec): min=3, max=4307, avg= 6.44, stdev=34.00
    clat (usec): min=121, max=10589, avg=2302.09, stdev=1246.93
     lat (usec): min=130, max=10596, avg=2308.58, stdev=1247.30
    clat percentiles (usec):
     |  1.00th=[  486],  5.00th=[  588], 10.00th=[  692], 20.00th=[ 1032],
     | 30.00th=[ 1368], 40.00th=[ 1752], 50.00th=[ 2160], 60.00th=[ 2608],
     | 70.00th=[ 3088], 80.00th=[ 3568], 90.00th=[ 4128], 95.00th=[ 4448],
     | 99.00th=[ 4768], 99.50th=[ 4896], 99.90th=[ 5152], 99.95th=[ 5920],
     | 99.99th=[ 7328]
    bw (KB  /s): min=  842, max= 4464, per=3.34%, avg=1734.14, stdev=778.17
    lat (usec) : 250=0.01%, 500=1.53%, 750=10.32%, 1000=7.19%
    lat (msec) : 2=26.99%, 4=41.87%, 10=12.09%, 20=0.01%
  cpu          : usr=0.06%, sys=0.39%, ctx=2603560, majf=0, minf=55
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=0/w=2598273/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
  WRITE: io=10150MB, aggrb=51964KB/s, minb=51964KB/s, maxb=51964KB/s, mint=200003msec, maxt=200003msec
Disk stats (read/write):
  nbd0: ios=0/2595504, merge=0/109, ticks=0/5967580, in_queue=5967134, util=99.99% 
```
2.3.9 随机写(block size 4k)
//block size是4k, 30个线程并发，持续时间200s
测试结果：30线程并发，带宽：64 MB/s  平均IOPS： 16108 耗时：1.861ms
```
fio -filename=/mnt/test/xxxxxx8 -direct=1 -iodepth 1 -thread -rw=randwrite -ioengine=libaio -bs=4k -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=randwrite, bs=4K-4K/4K-4K/4K-4K, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [w(30)] [100.0% done] [0KB/63904KB/0KB /s] [0/15.1K/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=12970: Tue Jan 16 15:51:49 2018
  write: io=12585MB, bw=64433KB/s, iops=16108, runt=200002msec
    slat (usec): min=3, max=30770, avg= 9.15, stdev=162.50
    clat (usec): min=71, max=237287, avg=1852.00, stdev=3475.88
     lat (usec): min=96, max=237298, avg=1861.23, stdev=3483.81
    clat percentiles (usec):
     |  1.00th=[  382],  5.00th=[  548], 10.00th=[ 1400], 20.00th=[ 1608],
     | 30.00th=[ 1704], 40.00th=[ 1784], 50.00th=[ 1832], 60.00th=[ 1896],
     | 70.00th=[ 1960], 80.00th=[ 2024], 90.00th=[ 2128], 95.00th=[ 2224],
     | 99.00th=[ 2448], 99.50th=[ 2576], 99.90th=[12992], 99.95th=[119296],
     | 99.99th=[150528]
    bw (KB  /s): min= 1048, max= 3121, per=3.34%, avg=2151.39, stdev=252.21
    lat (usec) : 100=0.01%, 250=0.01%, 500=3.60%, 750=3.56%, 1000=0.83%
    lat (msec) : 2=67.91%, 4=23.94%, 10=0.05%, 20=0.01%, 50=0.02%
    lat (msec) : 100=0.01%, 250=0.06%
  cpu          : usr=0.12%, sys=0.59%, ctx=3244801, majf=0, minf=43
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=0/w=3221693/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
  WRITE: io=12585MB, aggrb=64433KB/s, minb=64433KB/s, maxb=64433KB/s, mint=200002msec, maxt=200002msec
Disk stats (read/write):
  nbd0: ios=0/3220841, merge=0/13128, ticks=0/6009641, in_queue=6008914, util=100.00%
```
### 2.4.0 顺序读写(block size 4k)
//block size是4k, 30个线程并发，持续时间200s
测试结果：30线程并发
读：（带宽：48 MB/s  平均IOPS： 12075 耗时：1.046ms）
写：（带宽：48 MB/s  平均IOPS： 12069 耗时：1.437ms）
```
fio -filename=/mnt/test/xxxxxx9 -direct=1 -iodepth 1 -thread -rw=rw -ioengine=libaio -bs=4k -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=rw, bs=4K-4K/4K-4K/4K-4K, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [M(30)] [100.0% done] [45720KB/42072KB/0KB /s] [11.5K/10.6K/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=19624: Tue Jan 16 16:11:06 2018
  read : io=9433.8MB, bw=48300KB/s, iops=12075, runt=200001msec
    slat (usec): min=2, max=2412, avg= 5.37, stdev=12.36
    clat (usec): min=5, max=287292, avg=1041.16, stdev=2820.85
     lat (usec): min=72, max=287299, avg=1046.59, stdev=2820.94
    clat percentiles (usec):
     |  1.00th=[  278],  5.00th=[  362], 10.00th=[  398], 20.00th=[  454],
     | 30.00th=[  532], 40.00th=[  636], 50.00th=[  772], 60.00th=[  948],
     | 70.00th=[ 1176], 80.00th=[ 1480], 90.00th=[ 1960], 95.00th=[ 2384],
     | 99.00th=[ 3344], 99.50th=[ 3792], 99.90th=[ 5216], 99.95th=[14016],
     | 99.99th=[209920]
    bw (KB  /s): min=   45, max= 3800, per=3.34%, avg=1613.92, stdev=654.35
  write: io=9429.4MB, bw=48278KB/s, iops=12069, runt=200001msec
    slat (usec): min=3, max=2720, avg= 6.09, stdev=15.34
    clat (usec): min=44, max=9849, avg=1431.03, stdev=946.09
     lat (usec): min=51, max=9854, avg=1437.17, stdev=946.08
    clat percentiles (usec):
     |  1.00th=[  161],  5.00th=[  358], 10.00th=[  462], 20.00th=[  612],
     | 30.00th=[  780], 40.00th=[  972], 50.00th=[ 1192], 60.00th=[ 1448],
     | 70.00th=[ 1752], 80.00th=[ 2160], 90.00th=[ 2800], 95.00th=[ 3344],
     | 99.00th=[ 4320], 99.50th=[ 4512], 99.90th=[ 4704], 99.95th=[ 4832],
     | 99.99th=[ 4960]
    bw (KB  /s): min=   15, max= 3480, per=3.34%, avg=1612.75, stdev=639.39
    lat (usec) : 10=0.01%, 50=0.01%, 100=0.17%, 250=1.26%, 500=18.03%
    lat (usec) : 750=19.06%, 1000=13.40%
    lat (msec) : 2=31.55%, 4=15.34%, 10=1.16%, 20=0.02%, 50=0.01%
    lat (msec) : 250=0.01%, 500=0.01%
  cpu          : usr=0.11%, sys=0.69%, ctx=4830416, majf=0, minf=78
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=2415036/w=2413920/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=9433.8MB, aggrb=48300KB/s, minb=48300KB/s, maxb=48300KB/s, mint=200001msec, maxt=200001msec
  WRITE: io=9429.4MB, aggrb=48278KB/s, minb=48278KB/s, maxb=48278KB/s, mint=200001msec, maxt=200001msec
Disk stats (read/write):
  nbd0: ios=2413583/2412391, merge=0/39, ticks=2508070/3446958, in_queue=5954029, util=100.00%
```
### 2.4.1 随机读写(block size 4k)
//block size是4k, 30个线程并发，持续时间200s
读：（带宽：29 MB/s  平均IOPS： 7347 耗时：2.919ms）
写：（带宽：29 MB/s  平均IOPS： 7325 耗时：1.164ms）
```
fio -filename=/mnt/test/xxxxxx10 -direct=1 -iodepth 1 -thread -rw=randrw -ioengine=libaio -bs=4k -size=1G -numjobs=30 -runtime=200 -group_reporting -name=read-libaio
 
read-libaio: (g=0): rw=randrw, bs=4K-4K/4K-4K/4K-4K, ioengine=libaio, iodepth=1
...
fio-2.2.8
Starting 30 threads
read-libaio: Laying out IO file(s) (1 file(s) / 1024MB)
Jobs: 30 (f=30): [m(30)] [100.0% done] [34044KB/33808KB/0KB /s] [8511/8452/0 iops] [eta 00m:00s]
read-libaio: (groupid=0, jobs=30): err= 0: pid=21889: Tue Jan 16 16:19:14 2018
  read : io=5740.4MB, bw=29390KB/s, iops=7347, runt=200003msec
    slat (usec): min=2, max=1655, avg= 6.17, stdev= 9.07
    clat (usec): min=35, max=1221.7K, avg=2912.79, stdev=14872.43
     lat (usec): min=39, max=1221.7K, avg=2919.05, stdev=14872.92
    clat percentiles (usec):
     |  1.00th=[  370],  5.00th=[  516], 10.00th=[  740], 20.00th=[ 1192],
     | 30.00th=[ 1496], 40.00th=[ 1752], 50.00th=[ 1880], 60.00th=[ 1976],
     | 70.00th=[ 2064], 80.00th=[ 2192], 90.00th=[ 2448], 95.00th=[ 4192],
     | 99.00th=[20608], 99.50th=[42752], 99.90th=[214016], 99.95th=[305152],
     | 99.99th=[593920]
    bw (KB  /s): min=    5, max= 2424, per=3.40%, avg=1000.66, stdev=276.02
  write: io=5722.9MB, bw=29301KB/s, iops=7325, runt=200003msec
    slat (usec): min=3, max=1660, avg= 6.93, stdev= 9.13
    clat (usec): min=37, max=161642, avg=1157.90, stdev=2459.66
     lat (usec): min=42, max=162186, avg=1164.92, stdev=2461.31
    clat percentiles (usec):
     |  1.00th=[   54],  5.00th=[   69], 10.00th=[  151], 20.00th=[  540],
     | 30.00th=[  860], 40.00th=[ 1176], 50.00th=[ 1336], 60.00th=[ 1416],
     | 70.00th=[ 1496], 80.00th=[ 1576], 90.00th=[ 1672], 95.00th=[ 1736],
     | 99.00th=[ 1896], 99.50th=[ 1944], 99.90th=[ 2160], 99.95th=[ 4256],
     | 99.99th=[146432]
    bw (KB  /s): min=    4, max= 2235, per=3.40%, avg=997.39, stdev=278.86
    lat (usec) : 50=0.16%, 100=3.53%, 250=3.21%, 500=4.87%, 750=6.24%
    lat (usec) : 1000=6.61%
    lat (msec) : 2=56.37%, 4=16.42%, 10=1.35%, 20=0.70%, 50=0.30%
    lat (msec) : 100=0.08%, 250=0.12%, 500=0.03%, 750=0.01%, 1000=0.01%
    lat (msec) : 2000=0.01%
  cpu          : usr=0.12%, sys=0.47%, ctx=2935214, majf=0, minf=37
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued    : total=r=1469532/w=1465052/d=0, short=r=0/w=0/d=0, drop=r=0/w=0/d=0
     latency   : target=0, window=0, percentile=100.00%, depth=1
Run status group 0 (all jobs):
   READ: io=5740.4MB, aggrb=29390KB/s, minb=29390KB/s, maxb=29390KB/s, mint=200003msec, maxt=200003msec
  WRITE: io=5722.9MB, aggrb=29300KB/s, minb=29300KB/s, maxb=29300KB/s, mint=200003msec, maxt=200003msec
Disk stats (read/write):
  nbd0: ios=1467674/1463306, merge=0/53, ticks=4274517/1703752, in_queue=5977476, util=100.00%
```
# 三、 总结
## 3.1 测试工具小结
| 工具 | 用途 | 语法 |  说明 | 
|:---:|:---:|:---|:---|
| rados bench | RADOS 性能测试工具 | rados bench -p <pool_name> <seconds> <write,seq,rand> -b <block size> -t --no-cleanup | Ceph 自带的 RADOS 性能测试工具 |
| rbd bench-write | ceph 自带的 rbd 性能测试工具 | rbd bench-write <RBD image name><br/>--io-size：单位 byte，默认 4M<br/>--io-threads：线程数，默认 16<br/>--io-total：总写入字节，默认 1024M<br/>--io-pattern <seq,rand>：写模式，默认为 seq 即顺序写 | 只能对块设备做写测试 |
| fio + libaio | fio 结合IO 引擎的性能测试工具测试rbd | 参考 fio --help | 1. Linux 平台上做 IO 性能测试的瑞士军刀<br/>2. 可以对使用内核内 rbd 和用户空间 librados 进行比较<br/>3. 标准规则 - 顺序和随机 IO<br/>4. 块大小 - 4k，16k，64k，256k<br/>5. 模式 - 读和写<br/>6. 支持混合模式 |

## 3.2 测试结果比较
### 3.2.1 rados bench测试结果
| 客户端数 | 并发数 | 块大小 |  写测试 |  顺序读 | 随机读 | 
|:---:|:---:|:---|:---|:---|:---|
|单个客户端| 30 | 4M | 带宽：1119.68 MB/s <br/>平均IOPS：279 <br/>平均耗时：0.107s | 带宽：1121.07 MB/s<br/>平均IOPS：280<br/>平均耗时：0.106s | 带宽：1109.64 MB/s <br/>平均IOPS：279 <br/>平均耗时：0.106s |
| 两个客户端 | 30/个 | 4M | 带宽：1630.81MB/s <br/>平均IOPS：406 <br/>平均耗时：0.293s | 带宽：2238.71MB/s <br/>平均IOPS：558 <br/>平均耗时：0.212s | 带宽：2237.01MB/s <br/>平均IOPS：558 <br/>平均耗时：0.212s |

### 3.2.2 rbd测试结果
| 客户端数 | 并发数 | 块大小 |   顺序读写 | 随机读写 | 
|:---:|:---:|:---|:---|:---|
|单个客户端 | 30 | 4k | 带宽：297 MB/s <br/>平均IOPS：72681.76 | 带宽：36 MB/s <br/>平均IOPS：8925.40 |
| 单个客户端 | 30 | 4M| 带宽：844MB/s <br/>平均IOPS：206.16 | 带宽：860 MB/s<br/>平均IOPS： 210.18|


