
| 指标 | 说明 | 级别 |
|---|---|---|
|noscrub flag(s) set | 防止集群做清洗操作 | |
|full flag(s) set | 使集群到达设置的full_ratio值。会导致集群阻止写入操作 | |
|nodeep-scrub flag(s) set | 防止集群进行深度清洗操作 | |
|pause flag(s) set | 集群将会阻止读写操作，但不会影响集群的in、out、up或down状态。集群扔保持正常运行,就是客户端无法读写 | |
|noup  flag(s) set | 防止osd进入up状态 | |
|nodown flag(s) set | 防止osd进入down状态 | |
|noout flag(s) set | 防止osd进入out状态 | |
|noin flag(s) set | 防止osd纳入ceph集群。有时候我们新加入OSD，并不想立马加入集群，可以设置该选项 | |
|nobackfill  flag(s) set | 防止集群进行数据回填操作 | |
|norebalance flag(s) set | 防止数据均衡操作 | |
|norecover flag(s) set | 避免关闭OSD的过程中发生数据迁移 | |
|notieragent flag(s) set | | |
|osds exist in the crush map but not in the osdmap | osd crush weight有值但是osd weight无值 | |
|application not enabled on 1 pool(s) | 没有定义池的使用类型 | |
|osds have slow requests | 慢查询 | |
|Monitor clock skew detected | 时钟偏移 | |
|bigdata failing to advance its oldest client/flush tid | 客户端和MDS服务器之间通信使用旧的tid | |
|Many clients (34) failing to respond to cache pressure | 如果某个客户端的响应时间超过了 mds_revoke_cap_timeout （默认为 60s ）这条消息就会出现 | |
|mons down, quorum | Ceph Monitor down | |
|in osds are down| OSD down后会出现 | |
|cache pools are missing hit_sets | 使用cache tier后会出现 | |
|has mon_osd_down_out_interval set to 0 | has mon_osd_down_out_interval set to 0 ||
|is full | pool满后会出现 | |
|near full osd | near full osd | |
|unscrubbed pgs | 有些pg没有scrub  | |
|pgs stuck | PG处于一些不健康状态的时候，会显示出来 | |
|requests are blocked | slow requests会警告 | |
|osds have slow requests | slow requests会警告 | |
| recovery |  需要recovery的时候会报 | |
| at/near target max | 使用cache tier的时候会警告 | |
| too few PGs per OSD | 每个OSD的PG数过少 | |
| too many PGs per OSD | too many PGs per OSD | |
| > pgp_num | > pgp_num | |
| has many more objects per pg than average (too few pgs?) | 每个Pg上的objects数过多 ||
| no osds | 部署完就可以看到，运行过程中不会出现 | |
| full osd | OSD满时出现 | |
| pgs are stuck inactive for more than | Pg处于inactive状态，该Pg读写都不行 | |
| scrub errors | scrub 错误出现，是scrub错误?还是scrub出了不一致的pg | |



