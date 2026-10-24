# FDB Cluster Configuration

Generated from: FDB-Config9.txt
Generated at: 2026-09-08T18:53:41Z

## Resource Budget

| Resource | Requested | Limit |
|----------|-----------|-------|
| CPU cores | 15 | 48 |
| DRAM (GB) | 78 | 96 |

## Storage Servers (9)

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID   |
|----------------|------|--------|-------|-----|----------------|-------|--------------|
| fdb-storage00  | 4500 |      0 |     1 |  6G | /mnt/fdb0      | zone0 | fdb-storage0 |
| fdb-storage10  | 4501 |      1 |     1 |  6G | /mnt/fdb1      | zone1 | fdb-storage1 |
| fdb-storage20  | 4502 |      2 |     1 |  6G | /mnt/fdb2      | zone2 | fdb-storage2 |
| fdb-storage01  | 4503 |      3 |     1 |  6G | /mnt/fdb0      | zone0 | fdb-storage0 |
| fdb-storage11  | 4504 |      4 |     1 |  6G | /mnt/fdb1      | zone1 | fdb-storage1 |
| fdb-storage21  | 4505 |      5 |     1 |  6G | /mnt/fdb2      | zone2 | fdb-storage2 |
| fdb-storage02  | 4506 |      6 |     1 |  6G | /mnt/fdb0      | zone0 | fdb-storage0 |
| fdb-storage12  | 4507 |      7 |     1 |  6G | /mnt/fdb1      | zone1 | fdb-storage1 |
| fdb-storage22  | 4508 |      8 |     1 |  6G | /mnt/fdb2      | zone2 | fdb-storage2 |

## Log Servers (3)

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID |
|----------------|------|--------|-------|-----|----------------|-------|------------|
| fdb-log0       | 4520 |      9 |     1 |  4G | /mnt/fdb-log0  | zone3 | fdb-log0   |
| fdb-log1       | 4521 |     10 |     1 |  4G | /mnt/fdb-log1  | zone4 | fdb-log1   |
| fdb-log2       | 4522 |     11 |     1 |  4G | /mnt/fdb-log2  | zone5 | fdb-log2   |

## Stateless Servers (3)

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID |
|----------------|------|--------|-------|-----|----------------|-------|------------|
| fdb-stateless0 | 4530 |     12 |     1 |  4G | /mnt/fdb-log0  | zone3 | fdb-log0   |
| fdb-stateless1 | 4531 |     13 |     1 |  4G | /mnt/fdb-log1  | zone4 | fdb-log1   |
| fdb-stateless2 | 4532 |     14 |     1 |  4G | /mnt/fdb-log2  | zone5 | fdb-log2   |

## FDB Command Line

SS: `-m 6GiB --cache_memory 3GiB --knob_storage_hard_limit_bytes=1572864000`
LOG: `-m 4GiB --cache_memory 2GiB`
SL: `-m 4GiB --cache_memory 2GiB`

Container mem_limit: SS=12g LOG=8g SL=8g

## FDB Cluster Settings

| Setting | Value |
|---------|-------|
| throttle | disable |
| storage_hard_limit_mb | 1500 |
| engine | ssd-redwood-1 |
| fdbcli_engine | ssd-redwood-1 |
