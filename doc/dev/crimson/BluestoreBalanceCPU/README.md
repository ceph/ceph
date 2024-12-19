In this report we present side by side the comparison of the three CPU balance strategies for Crimson using
Bluestore as backend.

Notice that there does not seem to be a clear advantage of one strategy over the other.

# 4k randread
## 8 OSD crimson, 5 vs 6 reactor, fixed FIO 8 cores, response latency
![blue_8osd_5reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](blue_8osd_5reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![blue_8osd_6reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](blue_8osd_6reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)

### OSD utilisation
![blue_8osd_5reactor_8fio_randread_osd_cpu](blue_8osd_5reactor_8fio_randread_osd_cpu.png)
![blue_8osd_6reactor_8fio_randread_osd_cpu](blue_8osd_6reactor_8fio_randread_osd_cpu.png)
![blue_8osd_5reactor_8fio_randread_osd_mem](blue_8osd_5reactor_8fio_randread_osd_mem.png)
![blue_8osd_6reactor_8fio_randread_osd_mem](blue_8osd_6reactor_8fio_randread_osd_mem.png)
### FIO utilisation
![blue_8osd_5reactor_8fio_randread_fio_cpu](blue_8osd_5reactor_8fio_randread_fio_cpu.png)
![blue_8osd_6reactor_8fio_randread_fio_cpu](blue_8osd_6reactor_8fio_randread_fio_cpu.png)
![blue_8osd_5reactor_8fio_randread_fio_mem](blue_8osd_5reactor_8fio_randread_fio_mem.png)
![blue_8osd_6reactor_8fio_randread_fio_mem](blue_8osd_6reactor_8fio_randread_fio_mem.png)
-
---
# 4k randwrite
It is interesting to note that the default configuration shows better performance for both five and six reactors, however
the NUMA socket and the OSD-balanced strategies perform worse on six reactors than five.

## 8 OSD crimson, 5 vs 6 reactor, fixed FIO 8 cores, response latency
![blue_8osd_5reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](blue_8osd_5reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![blue_8osd_6reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](blue_8osd_6reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)

### OSD utilisation
![blue_8osd_5reactor_8fio_randwrite_osd_cpu](blue_8osd_5reactor_8fio_randwrite_osd_cpu.png)
![blue_8osd_6reactor_8fio_randwrite_osd_cpu](blue_8osd_6reactor_8fio_randwrite_osd_cpu.png)
![blue_8osd_5reactor_8fio_randwrite_osd_mem](blue_8osd_5reactor_8fio_randwrite_osd_mem.png)
![blue_8osd_6reactor_8fio_randwrite_osd_mem](blue_8osd_6reactor_8fio_randwrite_osd_mem.png)
### FIO utilisation
![blue_8osd_5reactor_8fio_randwrite_fio_cpu](blue_8osd_5reactor_8fio_randwrite_fio_cpu.png)
![blue_8osd_6reactor_8fio_randwrite_fio_cpu](blue_8osd_6reactor_8fio_randwrite_fio_cpu.png)
![blue_8osd_5reactor_8fio_randwrite_fio_mem](blue_8osd_5reactor_8fio_randwrite_fio_mem.png)
![blue_8osd_6reactor_8fio_randwrite_fio_mem](blue_8osd_6reactor_8fio_randwrite_fio_mem.png)
---
# 64k seqwrite
For this workload, both NUMA socket and OSD-balanced strategies perform marginally better than the default CPU allocation.

## 8 OSD crimson, 5 vs 6 reactor, fixed FIO 8 cores, response latency
![blue_8osd_5reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](blue_8osd_5reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![blue_8osd_6reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](blue_8osd_6reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)

### OSD utilisation
![blue_8osd_5reactor_8fio_seqwrite_osd_cpu](blue_8osd_5reactor_8fio_seqwrite_osd_cpu.png)
![blue_8osd_6reactor_8fio_seqwrite_osd_cpu](blue_8osd_6reactor_8fio_seqwrite_osd_cpu.png)
![blue_8osd_5reactor_8fio_seqwrite_osd_mem](blue_8osd_5reactor_8fio_seqwrite_osd_mem.png)
![blue_8osd_6reactor_8fio_seqwrite_osd_mem](blue_8osd_6reactor_8fio_seqwrite_osd_mem.png)
### FIO utilisation
![blue_8osd_5reactor_8fio_seqwrite_fio_cpu](blue_8osd_5reactor_8fio_seqwrite_fio_cpu.png)
![blue_8osd_6reactor_8fio_seqwrite_fio_cpu](blue_8osd_6reactor_8fio_seqwrite_fio_cpu.png)
![blue_8osd_5reactor_8fio_seqwrite_fio_mem](blue_8osd_5reactor_8fio_seqwrite_fio_mem.png)
![blue_8osd_6reactor_8fio_seqwrite_fio_mem](blue_8osd_6reactor_8fio_seqwrite_fio_mem.png)
---
# 64k seqread
## 8 OSD crimson, 5 vs 6 reactor, fixed FIO 8 cores, response latency
![blue_8osd_5reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](blue_8osd_5reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![blue_8osd_6reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](blue_8osd_6reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)

### OSD utilisation
![blue_8osd_5reactor_8fio_seqread_osd_cpu](blue_8osd_5reactor_8fio_seqread_osd_cpu.png)
![blue_8osd_6reactor_8fio_seqread_osd_cpu](blue_8osd_6reactor_8fio_seqread_osd_cpu.png)
![blue_8osd_5reactor_8fio_seqread_osd_mem](blue_8osd_5reactor_8fio_seqread_osd_mem.png)
![blue_8osd_6reactor_8fio_seqread_osd_mem](blue_8osd_6reactor_8fio_seqread_osd_mem.png)
### FIO utilisation
![blue_8osd_5reactor_8fio_seqread_fio_cpu](blue_8osd_5reactor_8fio_seqread_fio_cpu.png)
![blue_8osd_6reactor_8fio_seqread_fio_cpu](blue_8osd_6reactor_8fio_seqread_fio_cpu.png)
![blue_8osd_5reactor_8fio_seqread_fio_mem](blue_8osd_5reactor_8fio_seqread_fio_mem.png)
![blue_8osd_6reactor_8fio_seqread_fio_mem](blue_8osd_6reactor_8fio_seqread_fio_mem.png)
---
