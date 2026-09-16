# System Resources

## Host

- Intel Xeon Platinum 8276M @ 2.20GHz (turbo 4.0GHz)
- 2 sockets, 28 cores/socket, 2 threads/core = 112 logical CPUs
- NUMA node0: cores 0-27, 56-83
- NUMA node1: cores 28-55, 84-111
- L3 cache: 77 MiB (2 instances)
- RAM: 376 GiB (394,294,312 KiB)
- OS: Rocky Linux 9
- Boot disk: sda1 (894.3G) → /

## FDB Resource Limits

- Max CPU cores for FDB: 48
- Max DRAM for FDB: 96 GB

Any config requesting more than these limits will be rejected by the generator script.

## FDB Storage Allowlist

Only these drives/partitions may be used by FDB containers. Everything else is off-limits.

### Storage (SS) drives

- nvme4n1p1 (7.3T) → /mnt/fdb0
- nvme8n1p1 (7.3T) → /mnt/fdb1
- nvme9n1p1 (7.3T) → /mnt/fdb2

### SS Allocation Rule

Valid storage-server counts: 1, 3, 6, 9, 12. SS processes are distributed round-robin across the 3 drives (e.g. count=6: fdb0, fdb1, fdb2, fdb0, fdb1, fdb2). Any other count is rejected by the generator script.

### Log partitions

- nvme1n1p3 (74.5G) → /mnt/fdb-log0
- nvme7n1p3 (74.5G) → /mnt/fdb-log1
- nvme5n1/vg_nvme-lv_fdblog (80G LVM) → /mnt/fdb-log2

## Current FDB Allocation (default config)

- CPU cores: 0, 1, 4, 5, 8, 9, 12, 13, 16, 17, 18, 20 (12 cores, all NUMA node0)
- Memory: 12 containers × 2G mem_limit = 24G container total (12 GiB FDB working set + 3 GiB cache)
