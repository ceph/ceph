# 1. perf dump
## 1.1 cluster
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
perf dump cluster | ceph.cluster.num_mon |mon数量| |
*| ceph.cluster.num_mon_quorum  |法定mon数量| |
*| ceph.cluster.num_osd  |集群osd节点数量| |
*| ceph.cluster.num_osd_up  |up状态的osd节点数量| |
*| ceph.cluster.num_osd_in  |in状态的osd节点数量| |
*| osd_epoch  |osd的epoch号| |
*| ceph.cluster_total_objects  |集群objects总数| |		 	 	 
*|ceph.cluster.num_pg	|集群pg总数	 | |	 	 
*|ceph.cluster.num_pg_peering |	长时间存在peering的pg	 | |	 	 
*|ceph.cluster.num_pool |	集群pool数量	 ||	 	 
*|ceph.cluster_total_objects |	集群objects数量	|| 	 	 
*|ceph.cluster.num_mon |	集群mon节点数	 ||	 	 
*|ceph.cluster.num_mds_up |	up状态mds数量	|| 	 	 
*|ceph.cluster.num_mds_in |	in状态mds数量	 ||	 	 
*|ceph.cluster.num_mds_failed	| failed状态mds数量	|| 	 	 
*| ceph.cluster.osd_bytes	| osd总共大小	 	 ||	 
*|ceph.cluster.osd_bytes_used |	用户占用大小	 ||	 	 
*|ceph.cluster.osd_bytes_avail |	osd可用的大小	|| 	 	 


## 1.2 leveldb
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
perf dump leveldb | ceph.leveldb.leveldb_get |获取的数量| |
*| ceph.leveldb.leveldb_transaction |	处理的数量	| | 	 	 
*| ceph.leveldb.leveldb_get_latency.avgcount |	获取延迟队列里面的平均数量	| |	 	 
*| ceph.leveldb.leveldb_get_latency.sum |	获取延迟队列里面的总数	 ||	 
*| ceph.leveldb.leveldb_submit_latency.avgcount |	提交延迟队列里面的平均数量	 ||	 	 
*| ceph.leveldb.leveldb_submit_latency.sum |	提交延迟队列里面的总数	 ||	 	 
*| ceph.leveldb.leveldb_submit_latency.avgcount | 提交延迟队列里面的平均数量	 ||	 	 
*| ceph.leveldb.leveldb_submit_latency.sum |	提交延迟队列里面的总数	|| 	 	 
*| ceph.leveldb.leveldb_submit_sync_latency.avgcount	| 提交同步延迟队列里面的平均数量 ||	 	 	 
*| ceph.leveldb.leveldb_submit_sync_latency.sum | 提交同步延迟队列里面的总数 ||	 	 	 
*| ceph.leveldb.leveldb_compact |	压缩	 ||	 	 
*| ceph.leveldb.leveldb_compact_range |	压缩范围	|| 	 
*| ceph.leveldb.leveldb_compact_queue_merge |	压缩合并队列	||	 	 
*| ceph.leveldb.leveldb_compact_queue_len | 压缩队列长度	 || 

## 1.3 mon
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
perf dump mon |ceph.mon.num_sessions|当前打开的监视器会话数||	 	 	 
*|ceph.mon.sessions_add| 创建监视器会话数||	 	 	 
*|ceph.mon.sessions_rm|监视器中删除会话调用的次数||	 	 	 
*|ceph.mon.sessions_trim|调整监视器会话数	 ||	 	 
*|ceph.mon.num_elections|选举监察员数量	 ||	 	 
*|ceph.mon.election_cal | 由监视器启动的选举数 ||	 	 	 
*|ceph.mon.election_win |选举赢得选举数	 ||	 	 
*|ceph.mon.election_lose |选举流失的选举数 ||	 	 	 

## 1.4 paxos
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
perf dump paxos|ceph.paxos.start_leader|启动leader角色|| 	 	 
*|ceph.paxos.start_peon|启动peon角色	 ||	 	 
*|ceph.paxos.restart |重启数||	 	 	 
*|ceph.paxos.refresh|	刷新	 ||	 	 
*|ceph.paxos.refresh|	刷新	|| 	 	 
*|ceph.paxos.refresh_latency.avgcount |延迟刷新平均数|| 	 	 
*|ceph.paxos.refresh_latency.sum | 延迟刷新总数 ||	 	 	 
*| ceph.paxos.begin | 开始处理	|| 	 	 
*| ceph.paxos.begin_keys.avgcount | 开始处理keys的平均数量 ||	 	 	 
*| ceph.paxos.begin_keys.sum | 开始处理keys的总数 || 	 	 
*| ceph.paxos.begin_bytes.avgcount | 开始处理bytes的平均数量||	 	 
*|ceph.paxos.begin_bytes.sum | 开始处理bytes的总数 ||	 	 	 
*|ceph.paxos.begin_latency.avgcount | 开始处理延迟的平均数量||	 	 	 
*|ceph.paxos.begin_latency.sum|开始处理延迟的总数|| 	 	 
*|ceph.paxos.commit|提交数	 ||	 	 
*|ceph.paxos.commit_keys.avgcount|提交keys的平均数量 ||	 	 	 
*|ceph.paxos.commit_keys.sum|提交keys的总数	|| 	 	 
*|ceph.paxos.commit_latency.avgcount|提交延迟的平均数量||	 	 	 
*|ceph.paxos.commit_latency.sum|提交延迟的总数||	 	 	 
*|ceph.paxos.collect|收集|| 	 	 
*|ceph.paxos.collect_keys.avgcount|收集的keys的平均数量	 ||	 	 
*|ceph.paxos.collect_keys.sum|收集的keys的总数||	 	 	 
*|ceph.paxos.collect_bytes.avgcount|收集的bytes数的平均数量||	 	 	 
*|ceph.paxos.collect_bytes.sum|收集的bytes数的总数||	 	 	 
*|ceph.paxos.collect_latency.avgcount|收集延迟平均数量||	 	 	 
*|ceph.paxos.collect_latency.sum|收集延迟总数|| 	 	 
*|ceph.paxos.collect_uncommitted|||	 	 	 	 
*|ceph.paxos.collect_timeout|收集超时时间||	 	 	 
*|ceph.paxos.accept_timeout|接受超时时间	||	 	 
*|ceph.paxos.lease_ack_timeout|租约确认超时时间||	 	 	 
*|ceph.paxos.lease_timeout|租约超时时间|| 	 	 
*|ceph.paxos.store_state|存储的状态||	 	 	 
*|ceph.paxos.store_state_keys.avgcount|存储状态中的事务密钥平均数	 ||	 	 
*|ceph.paxos.store_state_keys.sum|存储状态中的事务密钥总数	|| 	 	 
*|ceph.paxos.store_state_bytes.avgcount|存储状态中事务中的数据的平均数	||	 	 
*|ceph.paxos.store_state_bytes.sum|存储状态中事务中的数据的总数	 || 	 
*|ceph.paxos.store_state_latency.avgcount |存储状态延迟平均数||	 	 	 
*|ceph.paxos.store_state_latency.sum|存储状态延迟总数|| 	 	 
*|ceph.paxos.share_state|共享状态||	 	 	 
*|ceph.paxos.share_state_keys.avgcount|共享状态的keys的平均数	||	 	 
*|ceph.paxos.share_state_keys.sum|共享状态的keys的总数	|| 	 	 
*|ceph.paxos.share_state_bytes.avgcount|共享状态数据平均数|| 	 	 
*|ceph.paxos.share_state_bytes.sum|共享状态数据总数||	 	 	 
*|ceph.paxos.new_pn|新建提议号询问||	 	 	 
*|ceph.paxos.new_pn_latency.avgcount|新建提议号询问等待时间的平均数量||	 	 
*|ceph.paxos.new_pn_latency.sum	|新建提议号询问等待时间的总数|| 

## 1.5 throttle
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
perf dump throttle-*|val|当前可用的值||	 	 	 
*|max|最大限制数||	 	 	 
*|get|获取到的值||	 	 	 
*|get_sum|获取到的总数	|| 	 	 
*|get_or_fail_fail|获取或者错误值||	 	 	 
*|get_or_fail_success|获取或者错误成功值||	 	 	 
*|take|接受值||	 	 	 
*|take_sum|接受总数||	 	 	 
*|put	|推送值||	 	 	 
*|put_sum|推送总数	 ||	 	 
*|wait.avgcount|等待平均数量||	 	 	 
*|wait.sum|等待总数||	 	 	 
