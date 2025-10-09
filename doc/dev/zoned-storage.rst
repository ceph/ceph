=======================
 Zoned Storage Support
=======================

http://zonedstorage.io

Zoned Storage is a class of storage devices that enables host and storage
devices to cooperate to achieve higher storage capacities, increased throughput,
and lower latencies. The zoned storage interface is available through the SCSI
Zoned Block Commands (ZBC) and Zoned Device ATA Command Set (ZAC) standards on
Shingled Magnetic Recording (SMR) hard disks today and is also being adopted for
NVMe Solid State Disks with the upcoming NVMe Zoned Namespaces (ZNS) standard.

This project aims to enable Ceph to work on zoned storage drives and at the same
time explore research problems related to adopting this new interface.  The
first target is to enable non-overwrite workloads (e.g. RGW) on host-managed SMR
(HM-SMR) drives and explore cleaning (garbage collection) policies.  HM-SMR
drives are high capacity hard drives with the ZBC/ZAC interface.  The longer
term goal is to support ZNS SSDs, as they become available, as well as overwrite
workloads.

The first patch in these series enabled writing data to HM-SMR drives.  This
patch introduces ZonedFreelistManger, a FreelistManager implementation that
passes enough information to ZonedAllocator to correctly initialize state of
zones by tracking the write pointer and the number of dead bytes per zone.  We
have to introduce a new FreelistManager implementation because with zoned
devices a region of disk can be in three states (empty, used, and dead), whereas
current BitmapFreelistManager tracks only two states (empty and used).  It is
not possible to accurately initialize the state of zones in ZonedAllocator by
tracking only two states.  The third planned patch will introduce a rudimentary
cleaner to form a baseline for further research.

Currently we can perform basic RADOS benchmarks on an OSD running on an HM-SMR
drives, restart the OSD, and read the written data, and write new data, as can
be seen below.

Please contact Abutalib Aghayev <agayev@psu.edu> for questions.

::
   
  $ sudo zbd report -i -n /dev/sdc
  Device /dev/sdc:
      Vendor ID: ATA HGST HSH721414AL T240
      Zone model: host-managed
      Capacity: 14000.520 GB (27344764928 512-bytes sectors)
      Logical blocks: 3418095616 blocks of 4096 B
      Physical blocks: 3418095616 blocks of 4096 B
      Zones: 52156 zones of 256.0 MB
      Maximum number of open zones: no limit
      Maximum number of active zones: no limit
  52156 / 52156 zones
  $ MON=1 OSD=1 MDS=0 sudo ../src/vstart.sh --new --localhost --bluestore --bluestore-devs /dev/sdc --bluestore-zoned
  <snipped verbose output>
  $ sudo ./bin/ceph osd pool create bench 32 32
  pool 'bench' created
  $ sudo ./bin/rados bench -p bench 10 write --no-cleanup
  hints = 1
  Maintaining 16 concurrent writes of 4194304 bytes to objects of size 4194304 for up to 10 seconds or 0 objects
  Object prefix: benchmark_data_h0.cc.journaling712.narwhal.p_29846
    sec Cur ops   started  finished  avg MB/s  cur MB/s last lat(s)  avg lat(s)
      0       0         0         0         0         0           -           0
      1      16        45        29   115.943       116    0.384175    0.407806
      2      16        86        70   139.949       164    0.259845    0.391488
      3      16       125       109   145.286       156     0.31727    0.404727
      4      16       162       146   145.953       148    0.826671    0.409003
      5      16       203       187   149.553       164     0.44815    0.404303
      6      16       242       226   150.621       156    0.227488    0.409872
      7      16       281       265   151.384       156    0.411896    0.408686
      8      16       320       304   151.956       156    0.435135    0.411473
      9      16       359       343   152.401       156    0.463699    0.408658
     10      15       396       381   152.356       152    0.409554    0.410851
  Total time run:         10.3305
  Total writes made:      396
  Write size:             4194304
  Object size:            4194304
  Bandwidth (MB/sec):     153.333
  Stddev Bandwidth:       13.6561
  Max bandwidth (MB/sec): 164
  Min bandwidth (MB/sec): 116
  Average IOPS:           38
  Stddev IOPS:            3.41402
  Max IOPS:               41
  Min IOPS:               29
  Average Latency(s):     0.411226
  Stddev Latency(s):      0.180238
  Max latency(s):         1.00844
  Min latency(s):         0.108616
  $ sudo ../src/stop.sh
  $ # Notice the lack of "--new" parameter to vstart.sh
  $ MON=1 OSD=1 MDS=0 sudo ../src/vstart.sh --localhost --bluestore --bluestore-devs /dev/sdc --bluestore-zoned  
  <snipped verbose output>
  $ sudo ./bin/rados bench -p bench 10 rand
  hints = 1
    sec Cur ops   started  finished  avg MB/s  cur MB/s last lat(s)  avg lat(s)
      0       0         0         0         0         0           -           0
      1      16        61        45   179.903       180    0.117329    0.244067
      2      16       116       100   199.918       220    0.144162    0.292305
      3      16       174       158   210.589       232    0.170941    0.285481
      4      16       251       235   234.918       308    0.241175    0.256543
      5      16       316       300   239.914       260    0.206044    0.255882
      6      15       392       377   251.206       308    0.137972    0.247426
      7      15       458       443   252.984       264   0.0800146    0.245138
      8      16       529       513   256.346       280    0.103529    0.239888
      9      16       587       571   253.634       232    0.145535      0.2453
     10      15       646       631   252.254       240    0.837727    0.246019
  Total time run:       10.272
  Total reads made:     646
  Read size:            4194304
  Object size:          4194304
  Bandwidth (MB/sec):   251.558
  Average IOPS:         62
  Stddev IOPS:          10.005
  Max IOPS:             77
  Min IOPS:             45
  Average Latency(s):   0.249385
  Max latency(s):       0.888654
  Min latency(s):       0.0103208
  $ sudo ./bin/rados bench -p bench 10 write --no-cleanup
  hints = 1
  Maintaining 16 concurrent writes of 4194304 bytes to objects of size 4194304 for up to 10 seconds or 0 objects
  Object prefix: benchmark_data_h0.aa.journaling712.narwhal.p_64416
    sec Cur ops   started  finished  avg MB/s  cur MB/s last lat(s)  avg lat(s)
      0       0         0         0         0         0           -           0
      1      16        46        30   119.949       120     0.52627    0.396166
      2      16        82        66   131.955       144     0.48087    0.427311
      3      16       123       107   142.627       164      0.3287    0.420614
      4      16       158       142   141.964       140    0.405177    0.425993
      5      16       192       176   140.766       136    0.514565    0.425175
      6      16       224       208   138.635       128     0.69184    0.436672
      7      16       261       245   139.967       148    0.459929    0.439502
      8      16       301       285   142.468       160    0.250846    0.434799
      9      16       336       320   142.189       140    0.621686    0.435457
     10      16       374       358   143.166       152    0.460593    0.436384
  
