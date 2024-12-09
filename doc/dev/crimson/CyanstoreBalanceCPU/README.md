In this report we summarise the performance impact of the CPU balance strategies: default (no balance), NUMA socket, and OSD-based.

Each configuration set of charts consists to the following:

- A response latency curve (IOPs vs latency) of the three CPU balance strategies,
- OSD utilisation CPU and MEM,
- FIO (using libRBD) utilisation CPU and MEM.

# randread

Some preliminary observations:

- for 4k-randread, there is no much difference in performance by adding a single reactor
- OSD-balance performs overall marginally better, only slightly worse on a configuration with 8 OSD and 3 reactors
- the memory allocation remains constant across all strategies, but assuming there is not a bug in the post processing of
  the results, there is not a clear pattern of when a strategy uses more memory than its counterparts.


## 5 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_3reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](cyan_5osd_3reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_3reactor_8fio_randread_osd_cpu](cyan_5osd_3reactor_8fio_randread_osd_cpu.png)
![cyan_5osd_3reactor_8fio_randread_osd_mem](cyan_5osd_3reactor_8fio_randread_osd_mem.png)
![cyan_5osd_3reactor_8fio_randread_fio_cpu](cyan_5osd_3reactor_8fio_randread_fio_cpu.png)
![cyan_5osd_3reactor_8fio_randread_fio_mem](cyan_5osd_3reactor_8fio_randread_fio_mem.png)
---
## 5 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_4reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](cyan_5osd_4reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_4reactor_8fio_randread_osd_cpu](cyan_5osd_4reactor_8fio_randread_osd_cpu.png)
![cyan_5osd_4reactor_8fio_randread_osd_mem](cyan_5osd_4reactor_8fio_randread_osd_mem.png)
![cyan_5osd_4reactor_8fio_randread_fio_cpu](cyan_5osd_4reactor_8fio_randread_fio_cpu.png)
![cyan_5osd_4reactor_8fio_randread_fio_mem](cyan_5osd_4reactor_8fio_randread_fio_mem.png)
---
## 5 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_5reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](cyan_5osd_5reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_5reactor_8fio_randread_osd_cpu](cyan_5osd_5reactor_8fio_randread_osd_cpu.png)
![cyan_5osd_5reactor_8fio_randread_osd_mem](cyan_5osd_5reactor_8fio_randread_osd_mem.png)
![cyan_5osd_5reactor_8fio_randread_fio_cpu](cyan_5osd_5reactor_8fio_randread_fio_cpu.png)
![cyan_5osd_5reactor_8fio_randread_fio_mem](cyan_5osd_5reactor_8fio_randread_fio_mem.png)
---
## 8 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_3reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](cyan_8osd_3reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_3reactor_8fio_randread_osd_cpu](cyan_8osd_3reactor_8fio_randread_osd_cpu.png)
![cyan_8osd_3reactor_8fio_randread_osd_mem](cyan_8osd_3reactor_8fio_randread_osd_mem.png)
![cyan_8osd_3reactor_8fio_randread_fio_cpu](cyan_8osd_3reactor_8fio_randread_fio_cpu.png)
![cyan_8osd_3reactor_8fio_randread_fio_mem](cyan_8osd_3reactor_8fio_randread_fio_mem.png)
---
## 8 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_4reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](cyan_8osd_4reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_4reactor_8fio_randread_osd_cpu](cyan_8osd_4reactor_8fio_randread_osd_cpu.png)
![cyan_8osd_4reactor_8fio_randread_osd_mem](cyan_8osd_4reactor_8fio_randread_osd_mem.png)
![cyan_8osd_4reactor_8fio_randread_fio_cpu](cyan_8osd_4reactor_8fio_randread_fio_cpu.png)
![cyan_8osd_4reactor_8fio_randread_fio_mem](cyan_8osd_4reactor_8fio_randread_fio_mem.png)
---
## 8 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_5reactor_8fio_randread_bal_vs_unbal_iops_vs_lat](cyan_8osd_5reactor_8fio_randread_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_5reactor_8fio_randread_osd_cpu](cyan_8osd_5reactor_8fio_randread_osd_cpu.png)
![cyan_8osd_5reactor_8fio_randread_osd_mem](cyan_8osd_5reactor_8fio_randread_osd_mem.png)
![cyan_8osd_5reactor_8fio_randread_fio_cpu](cyan_8osd_5reactor_8fio_randread_fio_cpu.png)
![cyan_8osd_5reactor_8fio_randread_fio_mem](cyan_8osd_5reactor_8fio_randread_fio_mem.png)
---
# randwrite

- Surprisingly, the default configuration (no balance) performs better by approx 10% compared to the other two
  stategies.
- In a few configurations (eg. 5 OSD, 5 reactors) the NUMA socket balance strategy performs better than its counterparts.
- Resource utilisation increases proportional to the IO load (as opposed to the constant MEM utilisation in 4k-randread).

## 5 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_3reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](cyan_5osd_3reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_3reactor_8fio_randwrite_osd_cpu](cyan_5osd_3reactor_8fio_randwrite_osd_cpu.png)
![cyan_5osd_3reactor_8fio_randwrite_osd_mem](cyan_5osd_3reactor_8fio_randwrite_osd_mem.png)
![cyan_5osd_3reactor_8fio_randwrite_fio_cpu](cyan_5osd_3reactor_8fio_randwrite_fio_cpu.png)
![cyan_5osd_3reactor_8fio_randwrite_fio_mem](cyan_5osd_3reactor_8fio_randwrite_fio_mem.png)
---
## 5 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_4reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](cyan_5osd_4reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_4reactor_8fio_randwrite_osd_cpu](cyan_5osd_4reactor_8fio_randwrite_osd_cpu.png)
![cyan_5osd_4reactor_8fio_randwrite_osd_mem](cyan_5osd_4reactor_8fio_randwrite_osd_mem.png)
![cyan_5osd_4reactor_8fio_randwrite_fio_cpu](cyan_5osd_4reactor_8fio_randwrite_fio_cpu.png)
![cyan_5osd_4reactor_8fio_randwrite_fio_mem](cyan_5osd_4reactor_8fio_randwrite_fio_mem.png)
---
## 5 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_5reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](cyan_5osd_5reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_5reactor_8fio_randwrite_osd_cpu](cyan_5osd_5reactor_8fio_randwrite_osd_cpu.png)
![cyan_5osd_5reactor_8fio_randwrite_osd_mem](cyan_5osd_5reactor_8fio_randwrite_osd_mem.png)
![cyan_5osd_5reactor_8fio_randwrite_fio_cpu](cyan_5osd_5reactor_8fio_randwrite_fio_cpu.png)
![cyan_5osd_5reactor_8fio_randwrite_fio_mem](cyan_5osd_5reactor_8fio_randwrite_fio_mem.png)
---
## 8 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_3reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](cyan_8osd_3reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_3reactor_8fio_randwrite_osd_cpu](cyan_8osd_3reactor_8fio_randwrite_osd_cpu.png)
![cyan_8osd_3reactor_8fio_randwrite_osd_mem](cyan_8osd_3reactor_8fio_randwrite_osd_mem.png)
![cyan_8osd_3reactor_8fio_randwrite_fio_cpu](cyan_8osd_3reactor_8fio_randwrite_fio_cpu.png)
![cyan_8osd_3reactor_8fio_randwrite_fio_mem](cyan_8osd_3reactor_8fio_randwrite_fio_mem.png)
---
## 8 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_4reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](cyan_8osd_4reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_4reactor_8fio_randwrite_osd_cpu](cyan_8osd_4reactor_8fio_randwrite_osd_cpu.png)
![cyan_8osd_4reactor_8fio_randwrite_osd_mem](cyan_8osd_4reactor_8fio_randwrite_osd_mem.png)
![cyan_8osd_4reactor_8fio_randwrite_fio_cpu](cyan_8osd_4reactor_8fio_randwrite_fio_cpu.png)
![cyan_8osd_4reactor_8fio_randwrite_fio_mem](cyan_8osd_4reactor_8fio_randwrite_fio_mem.png)
---
## 8 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_5reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat](cyan_8osd_5reactor_8fio_randwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_5reactor_8fio_randwrite_osd_cpu](cyan_8osd_5reactor_8fio_randwrite_osd_cpu.png)
![cyan_8osd_5reactor_8fio_randwrite_osd_mem](cyan_8osd_5reactor_8fio_randwrite_osd_mem.png)
![cyan_8osd_5reactor_8fio_randwrite_fio_cpu](cyan_8osd_5reactor_8fio_randwrite_fio_cpu.png)
![cyan_8osd_5reactor_8fio_randwrite_fio_mem](cyan_8osd_5reactor_8fio_randwrite_fio_mem.png)
---
# seqwrite

- Overall the default (unbalanced) strategy performs better than its counterparts. Notice also the OSD uses less CPU overall for 
  small configurations with less than 8 OSD.
## 5 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_3reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](cyan_5osd_3reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_3reactor_8fio_seqwrite_osd_cpu](cyan_5osd_3reactor_8fio_seqwrite_osd_cpu.png)
![cyan_5osd_3reactor_8fio_seqwrite_osd_mem](cyan_5osd_3reactor_8fio_seqwrite_osd_mem.png)
![cyan_5osd_3reactor_8fio_seqwrite_fio_cpu](cyan_5osd_3reactor_8fio_seqwrite_fio_cpu.png)
![cyan_5osd_3reactor_8fio_seqwrite_fio_mem](cyan_5osd_3reactor_8fio_seqwrite_fio_mem.png)
---
## 5 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_4reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](cyan_5osd_4reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_4reactor_8fio_seqwrite_osd_cpu](cyan_5osd_4reactor_8fio_seqwrite_osd_cpu.png)
![cyan_5osd_4reactor_8fio_seqwrite_osd_mem](cyan_5osd_4reactor_8fio_seqwrite_osd_mem.png)
![cyan_5osd_4reactor_8fio_seqwrite_fio_cpu](cyan_5osd_4reactor_8fio_seqwrite_fio_cpu.png)
![cyan_5osd_4reactor_8fio_seqwrite_fio_mem](cyan_5osd_4reactor_8fio_seqwrite_fio_mem.png)
---
## 5 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_5reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](cyan_5osd_5reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_5reactor_8fio_seqwrite_osd_cpu](cyan_5osd_5reactor_8fio_seqwrite_osd_cpu.png)
![cyan_5osd_5reactor_8fio_seqwrite_osd_mem](cyan_5osd_5reactor_8fio_seqwrite_osd_mem.png)
![cyan_5osd_5reactor_8fio_seqwrite_fio_cpu](cyan_5osd_5reactor_8fio_seqwrite_fio_cpu.png)
![cyan_5osd_5reactor_8fio_seqwrite_fio_mem](cyan_5osd_5reactor_8fio_seqwrite_fio_mem.png)
---
## 8 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_3reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](cyan_8osd_3reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_3reactor_8fio_seqwrite_osd_cpu](cyan_8osd_3reactor_8fio_seqwrite_osd_cpu.png)
![cyan_8osd_3reactor_8fio_seqwrite_osd_mem](cyan_8osd_3reactor_8fio_seqwrite_osd_mem.png)
![cyan_8osd_3reactor_8fio_seqwrite_fio_cpu](cyan_8osd_3reactor_8fio_seqwrite_fio_cpu.png)
![cyan_8osd_3reactor_8fio_seqwrite_fio_mem](cyan_8osd_3reactor_8fio_seqwrite_fio_mem.png)
---
## 8 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_4reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](cyan_8osd_4reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_4reactor_8fio_seqwrite_osd_cpu](cyan_8osd_4reactor_8fio_seqwrite_osd_cpu.png)
![cyan_8osd_4reactor_8fio_seqwrite_osd_mem](cyan_8osd_4reactor_8fio_seqwrite_osd_mem.png)
![cyan_8osd_4reactor_8fio_seqwrite_fio_cpu](cyan_8osd_4reactor_8fio_seqwrite_fio_cpu.png)
![cyan_8osd_4reactor_8fio_seqwrite_fio_mem](cyan_8osd_4reactor_8fio_seqwrite_fio_mem.png)
---
## 8 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_5reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat](cyan_8osd_5reactor_8fio_seqwrite_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_5reactor_8fio_seqwrite_osd_cpu](cyan_8osd_5reactor_8fio_seqwrite_osd_cpu.png)
![cyan_8osd_5reactor_8fio_seqwrite_osd_mem](cyan_8osd_5reactor_8fio_seqwrite_osd_mem.png)
![cyan_8osd_5reactor_8fio_seqwrite_fio_cpu](cyan_8osd_5reactor_8fio_seqwrite_fio_cpu.png)
![cyan_8osd_5reactor_8fio_seqwrite_fio_mem](cyan_8osd_5reactor_8fio_seqwrite_fio_mem.png)
---
# seqread

- Overall seems to be a problem with the Memory utilisation for FIO (flat zero, probably a bug in the post processing).
- Again, OSD-balanced achieves best performance overall.

## 5 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_3reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](cyan_5osd_3reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_3reactor_8fio_seqread_osd_cpu](cyan_5osd_3reactor_8fio_seqread_osd_cpu.png)
![cyan_5osd_3reactor_8fio_seqread_osd_mem](cyan_5osd_3reactor_8fio_seqread_osd_mem.png)
![cyan_5osd_3reactor_8fio_seqread_fio_cpu](cyan_5osd_3reactor_8fio_seqread_fio_cpu.png)
![cyan_5osd_3reactor_8fio_seqread_fio_mem](cyan_5osd_3reactor_8fio_seqread_fio_mem.png)
---
## 5 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_4reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](cyan_5osd_4reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_4reactor_8fio_seqread_osd_cpu](cyan_5osd_4reactor_8fio_seqread_osd_cpu.png)
![cyan_5osd_4reactor_8fio_seqread_osd_mem](cyan_5osd_4reactor_8fio_seqread_osd_mem.png)
![cyan_5osd_4reactor_8fio_seqread_fio_cpu](cyan_5osd_4reactor_8fio_seqread_fio_cpu.png)
![cyan_5osd_4reactor_8fio_seqread_fio_mem](cyan_5osd_4reactor_8fio_seqread_fio_mem.png)
---
## 5 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_5osd_5reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](cyan_5osd_5reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![cyan_5osd_5reactor_8fio_seqread_osd_cpu](cyan_5osd_5reactor_8fio_seqread_osd_cpu.png)
![cyan_5osd_5reactor_8fio_seqread_osd_mem](cyan_5osd_5reactor_8fio_seqread_osd_mem.png)
![cyan_5osd_5reactor_8fio_seqread_fio_cpu](cyan_5osd_5reactor_8fio_seqread_fio_cpu.png)
![cyan_5osd_5reactor_8fio_seqread_fio_mem](cyan_5osd_5reactor_8fio_seqread_fio_mem.png)
---
## 8 OSD crimson, 3 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_3reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](cyan_8osd_3reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_3reactor_8fio_seqread_osd_cpu](cyan_8osd_3reactor_8fio_seqread_osd_cpu.png)
![cyan_8osd_3reactor_8fio_seqread_osd_mem](cyan_8osd_3reactor_8fio_seqread_osd_mem.png)
![cyan_8osd_3reactor_8fio_seqread_fio_cpu](cyan_8osd_3reactor_8fio_seqread_fio_cpu.png)
![cyan_8osd_3reactor_8fio_seqread_fio_mem](cyan_8osd_3reactor_8fio_seqread_fio_mem.png)
---
## 8 OSD crimson, 4 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_4reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](cyan_8osd_4reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_4reactor_8fio_seqread_osd_cpu](cyan_8osd_4reactor_8fio_seqread_osd_cpu.png)
![cyan_8osd_4reactor_8fio_seqread_osd_mem](cyan_8osd_4reactor_8fio_seqread_osd_mem.png)
![cyan_8osd_4reactor_8fio_seqread_fio_cpu](cyan_8osd_4reactor_8fio_seqread_fio_cpu.png)
![cyan_8osd_4reactor_8fio_seqread_fio_mem](cyan_8osd_4reactor_8fio_seqread_fio_mem.png)
---
## 8 OSD crimson, 5 reactor, fixed FIO 8 cores, response latency
![cyan_8osd_5reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat](cyan_8osd_5reactor_8fio_seqread_bal_vs_unbal_iops_vs_lat.png)
![cyan_8osd_5reactor_8fio_seqread_osd_cpu](cyan_8osd_5reactor_8fio_seqread_osd_cpu.png)
![cyan_8osd_5reactor_8fio_seqread_osd_mem](cyan_8osd_5reactor_8fio_seqread_osd_mem.png)
![cyan_8osd_5reactor_8fio_seqread_fio_cpu](cyan_8osd_5reactor_8fio_seqread_fio_cpu.png)
![cyan_8osd_5reactor_8fio_seqread_fio_mem](cyan_8osd_5reactor_8fio_seqread_fio_mem.png)
---
