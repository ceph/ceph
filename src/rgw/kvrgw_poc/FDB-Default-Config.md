# FDB Cluster Configuration

Generated from: FDB-Default-Config.txt
Generated at: 2026-09-15T15:14:53Z

## Resource Budget

| Resource | Requested | Limit |
|----------|-----------|-------|
| CPU cores | 12 | 48 |
| DRAM (GB) | 12 | 96 |

## Storage Servers (6)

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID   |
|----------------|------|--------|-------|-----|----------------|-------|--------------|
| fdb-storage00  | 4500 |      0 |     1 |  1G | /mnt/fdb0      | zone0 | fdb-storage0 |
| fdb-storage10  | 4501 |      1 |     1 |  1G | /mnt/fdb1      | zone1 | fdb-storage1 |
| fdb-storage20  | 4502 |      2 |     1 |  1G | /mnt/fdb2      | zone2 | fdb-storage2 |
| fdb-storage01  | 4503 |      3 |     1 |  1G | /mnt/fdb0      | zone0 | fdb-storage0 |
| fdb-storage11  | 4504 |      4 |     1 |  1G | /mnt/fdb1      | zone1 | fdb-storage1 |
| fdb-storage21  | 4505 |      5 |     1 |  1G | /mnt/fdb2      | zone2 | fdb-storage2 |

## Log Servers (3)

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID |
|----------------|------|--------|-------|-----|----------------|-------|------------|
| fdb-log0       | 4520 |      6 |     1 |  1G | /mnt/fdb-log0  | zone3 | fdb-log0   |
| fdb-log1       | 4521 |      7 |     1 |  1G | /mnt/fdb-log1  | zone4 | fdb-log1   |
| fdb-log2       | 4522 |      8 |     1 |  1G | /mnt/fdb-log2  | zone5 | fdb-log2   |

## Stateless Servers (3)

| Container      | Port | CPUset | Cores | MEM |      Mount     | Zone  | Machine ID |
|----------------|------|--------|-------|-----|----------------|-------|------------|
| fdb-stateless0 | 4530 |      9 |     1 |  1G | /mnt/fdb-log0  | zone3 | fdb-log0   |
| fdb-stateless1 | 4531 |     10 |     1 |  1G | /mnt/fdb-log1  | zone4 | fdb-log1   |
| fdb-stateless2 | 4532 |     11 |     1 |  1G | /mnt/fdb-log2  | zone5 | fdb-log2   |

## FDB Command Line

SS: `-m 1GiB --cache_memory 512MiB --knob_storage_hard_limit_bytes=1572864000`
LOG: `-m 1GiB --cache_memory 512MiB`
SL: `-m 1GiB --cache_memory 512MiB`

Container mem_limit: SS=2g LOG=2g SL=2g

## FDB Cluster Settings

| Setting | Value |
|---------|-------|
| throttle | disable |
| storage_hard_limit_mb | 1500 |
| engine | ssd2 |
| fdbcli_engine | ssd |
