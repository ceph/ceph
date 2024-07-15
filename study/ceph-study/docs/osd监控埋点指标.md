# 1. WBThrottle

| 监控类型   |      监控项      |  说明	 |
|----------|:-------------:|:-------------:|
| perf dump WBThrottle |  bytes_dirtied         | 脏数据大小     		  |
|                      |  bytes_wb              | 写入数据大小   		  |
|                      |  ios_dirtied           | 脏数据操作	          |
|                      |  ios_wb                | 写操作               |
|                      |  inodes_dirtied        | 等待写入的条目        |
|                      |  inodes_wb             | 写记录               |

# 2. filestore
| 监控类型   |      监控项      |  说明  |
|----------|:-------------:|:-------------:|
| perf dump filestore  |  journal_queue_max_ops         | 日志队列中的最大操作	    	|
| 		       		   |  journal_queue_ops         	| 日志队列中的操作大小	    	|
| 		       		   |  journal_ops         			| 日志请求数	    			|
| 		               |  journal_bytes         	    | 日志大小	    			|
| 		       		   |  journal_latency.avgcount      | 日志等待队列平均大小	    	|
| 		       		   |  journal_latency.sum      		| 日志等待队列总大小	    	|
| 		       		   |  journal_latency.avgtime       | 日志等待队列平均时间	    	|
| 		       		   |  journal_wr      				| 日志读写io	    			|
| 		       		   |  journal_wr_bytes.avgcount     | 日志读写大小的队列平均数量	|
| 		       		   |  journal_wr_bytes.sum      	| 日志读写大小的队列总数	    |
| 		       		   |  journal_full      			| 日志写满	   				|
| 		       		   |  committing     				| 正在提交数量	       		|
| 		       		   |  commitcycle_interval.avgcount | 提交之间的间隔队列平均数量	|
| 		       		   |  commitcycle_interval.sum     	| 提交之间的间隔队列总数	    |
| 		       		   |  commitcycle_interval.avgtime  | 提交之间的间隔队列平均时间	|
| 		       		   |  commitcycle_latency.avgcount  | 提交延迟队列平均数量	    	|
| 		       		   |  commitcycle_latency.sum     	| 提交延迟队列总数	      		|
| 		       		   |  commitcycle_latency.avgtime   | 提交延迟队列平均时间      	|
| 		       		   |  op_queue_max_ops   			| 队列中最大的操作数      		|
| 		       		   |  op_queue_max_ops   			| 队列队中的操作数      		|
| 		       		   |  ops   						| 操作数      				|
| 		       		   |  op_queue_max_bytes   			| 操作队列最大bytes数      	|
| 		       		   |  op_queue_bytes   				| 操作队列bytes数      		|
| 		       		   |  bytes   						| 写入存储的数据     			|
| 		       		   |  apply_latency.avgcount   		| 申请延迟队列平均数      		|
| 		       		   |  apply_latency.sum   			| 申请延迟队列总数      		|
| 		       		   |  apply_latency.avgtime   		| 申请延迟队列平均时间      	|
| 		       		   |  queue_transaction_latency_avg.avgcount   		| 存储操作等待队列平均数      	|
| 		       		   |  queue_transaction_latency_avg.sum   		| 存储操作等待队列总数      	|
| 		       		   |  queue_transaction_latency_avg.avgtime   		| 存储操作等待队列平均数      	|


# 3. leveldb
| 监控类型   |      监控项      |  说明  |
|----------|:-------------:|:-------------:|
| perf dump leveldb  |  leveldb_get         				 | 获取的数量	    					|
|   				 |  leveldb_get_latency.avgcount         | 获取延迟队列里面的平均数量	    	|
| 					 |  leveldb_get_latency.sum         	 | 获取延迟队列里面的总数	    		|
| 					 |  leveldb_submit_latency.avgcount      | 提交延迟队列里面的平均数量	    	|
| 				     |  leveldb_submit_latency.sum         	 | 提交延迟队列里面的总数	    		|
| 					 |  leveldb_submit_sync_latency.avgcount | 提交同步延迟队列里面的平均数量	    |
| 					 |  leveldb_submit_sync_latency.sum      | 提交同步延迟队列里面的总数	    	|
| 					 |  leveldb_compact      	 		     | 压缩	    					    |
| 					 |  leveldb_compact_range   			 | 压缩范围	    					|
| 					 |  leveldb_compact_queue_merge      	 | 压缩合并队列	    				|
| 					 |  leveldb_compact_queue_len      	 	 | 压缩队列长度	    				|



	
# 4. objecter
| 监控类型   |      监控项      |  说明  |
|----------|:-------------:|:-------------:|
| perf dump objecter  |  op_active         				 	  | 主动操作数	    					|
|   				  |  op_laggy         					  | 消极操作数	    					|
|   				  |  op_send         					  | 发送操作数	    					|
|   				  |  op_send_bytes         				  | 发送操作bytes	    					|
|   				  |  op_resend         				  	  | 重操作数    							|
|   				  |  op_reply         				  	  | 回复操作数	    					|
|   				  |  op         				  		  | 操作数	    						|
|   				  |  op_r         				  		  | 读操作数    							|
|   				  |  op_w         				  		  | 写操作数    							|
|   				  |  op_rmw         				  	  | 读写修改操作数    					|
|   				  |  op_pg         				  		  | PG操作数    							|
|   				  |  osdop_stat         				  | 操作状态    							|
|   				  |  osdop_create         				  | 创建对象操作    						|
|   				  |  osdop_read         				  | 读操作    							|
|   				  |  osdop_write         				  | 写操作    							|
|   				  |  osdop_writefull         			  | 写满对象操作    						|
|   				  |  osdop_writesame         			  | 写相同的对象操作    					|
|   				  |  osdop_append         				  | 追加操作    							|
|   				  |  osdop_zero         				  | 设置对象0操作    						|
|   				  |  osdop_truncate         			  | 截断对象操作    						|
|   				  |  osdop_delete         				  | 删除对象操作    						|
|   				  |  osdop_mapext         				  | 映射范围操作    						|
|   				  |  osdop_sparse_read         			  | 稀少读操作    						|
|   				  |  osdop_clonerange         			  | 克隆范围操作    						|
|   				  |  osdop_getxattr         			  | 获取xattr操作    						|
|   				  |  osdop_setxattr         			  | 设置xattr操作    						|
|   				  |  osdop_cmpxattr         			  | 比较xattr操作    						|
|   				  |  osdop_rmxattr         			  	  | 移除xattr操作    						|
|   				  |  osdop_resetxattrs         			  | 重置xattr操作    						|
|   				  |  osdop_tmap_up         			  	  | tmap更新操作    						|
|   				  |  osdop_tmap_put         			  | tmap推送操作    						|
|   				  |  osdop_tmap_get         			  | tmap获取操作    						|
|   				  |  osdop_call         			  	  | 调用执行操作    						|
|   				  |  osdop_watch         			  	  | 监控对象操作    						|
|   				  |  osdop_notify         			  	  | 对象操作通知    						|
|   				  |  osdop_src_cmpxattr         		  | 多个操作扩展属性    					|
|   				  |  osdop_pgls         		  		  | pg对象操作   							|
|   				  |  osdop_pgls_filter         		  	  | pg过滤对象操作    					|
|   				  |  osdop_other         		  		  | 其他操作    							|
|   				  |  linger_active         		  		  | 主动延迟操作    						|
|   				  |  linger_send         		  		  | 延迟发送操作    						|
|   				  |  linger_resend         		  		  | 延迟重新发送    						|
|   				  |  linger_ping         		  		  | 延迟ping操作    						|
|   				  |  poolop_active         		  		  | 主动池操作    						|
|   				  |  poolop_send         		  		  | 发送池操作   							|
|   				  |  poolop_resend         		  		  | 重新发送池操作	   							|
|   				  |  poolstat_active         		  	  | 主动获取池子统计操作   							|
|   				  |  poolstat_send         		  		  | 发送池子统计操作   							|
|   				  |  poolstat_resend         		  	  | 重新发送池子统计操作   							|
|   				  |  statfs_active         		  		  | fs状态操作   							|
|   				  |  statfs_send         		  		  | 发送fs状态   							|
|   				  |  statfs_resend         		  		  | 重新发送fs状态   							|
|   				  |  command_active         		  	  | 活动的命令   							|
|   				  |  command_send         		  		  | 发送指令  							|
|   				  |  command_resend         		  	  | 重新发送指令  							|
|   				  |  map_epoch         		  	  		  | OSD map epoch  							|
|   				  |  map_full         		  	  		  | 接收满的OSD map  							|
|   				  |  map_inc         		  	  		  | 接收到增量OSD map  							|
|   				  |  osd_sessions         		  	  	  | osd 会话  							|
|   				  |  osd_session_open         		  	  	  | 打开osd会话  							|
|   				  |  osd_session_close         		  	  	  | 关闭osd会话  							|
|   				  |  osd_laggy         		  	  	  	 | 缓慢的osd会话  							|
|   				  |  omap_wr         		  	  	  	 | osd map读写操作  							|
|   				  |  omap_rd         		  	  	  	 | osd map读操作  							|
|   				  |  omap_del         		  	  	  	 | osd map删除操作  							|

# 5. osd
| 监控类型   |      监控项      |  说明  |
|----------|:-------------:|:-------------:|
| perf dump osd  |  ceph.osd.op_wip      |  当前正在处理的复制操作(主节点)	 |
|  |  ceph.osd.op      |  操作数 | 
|  |  ceph.osd.op_in_bytes      |  客户端操作总写入大小 | 
|  |  ceph.osd.op_out_bytes      |  客户端操作总读取大小 | 
|  |  ceph.osd.op_latency.avgcount  |  客户端操作的延迟（包括队列时间）的平均数 | 
|  | ceph.osd.op_latency.sum | 客户端操作的延迟（包括队列时间）总数 |
|  | ceph.osd.op_latency.avgtime | 客户端操作的延迟（包括队列时间）平均时间 |
|  | ceph.osd.op_process_latency.avgcount | 客户端操作的延迟（不包括排队时间）的平均数 |
|  | ceph.osd.op_process_latency.sum |  客户端操作的延迟（不包括排队时间）总数  |
|  | ceph.osd.op_process_latency.avgtime | 客户端操作的延迟（不包括排队时间）平均时间 |
|  | ceph.osd.op_prepare_latency.avgcount | 客户端操作的延迟（不包括排队时间和等待完成）平均数 |
|  | ceph.osd.op_prepare_latency.sum | 客户端操作的延迟（不包括排队时间和等待完成）总数 |
|  | ceph.osd.op_prepare_latency.avgtime | 客户端操作的延迟（不包括排队时间和等待完成）平均时间 |
|  | ceph.osd.op_r | 客户端读取操作 |
|  | ceph.osd.op_r_out_bytes | 客户端数据读取 |
|  | ceph.osd.op_r_latency.avgcount | 读取操作的延迟（包括队列时间）平均数 |
|  | ceph.osd.op_r_latency.sum | 读取操作的延迟（包括队列时间）总数 |
|  | ceph.osd.op_r_latency.avgtime | 读取操作的延迟（包括队列时间）平均时间 |
|  | ceph.osd.op_r_process_latency.avgcount | 读取操作的延迟（不包括排队时间）平均数 |
|  | ceph.osd.op_r_process_latency.sum | 读取操作的延迟（不包括排队时间）总数 |
|  | ceph.osd.op_r_process_latency.avgtime | 读取操作的延迟（不包括排队时间）平均时间 |
|  | ceph.osd.op_r_prepare_latency.avgcount | 读取操作的等待时间（不包括排队时间和等待完成）平均数 |
|  | ceph.osd.op_r_prepare_latency.sum | 读取操作的等待时间（不包括排队时间和等待完成）总数 |
|  | ceph.osd.op_r_prepare_latency.avgtime | 读取操作的等待时间（不包括排队时间和等待完成）平均时间 |
|  | ceph.osd.op_w | 客户端写操作 |
|  | ceph.osd.op_w_in_bytes | 客户端写入数据 |
|  | ceph.osd.op_w_latency.avgcount | 写操作的延迟（包括排队时间）平均数 |
|  | ceph.osd.op_w_latency.sum | 写操作的延迟（包括排队时间）总数 |
|  | ceph.osd.op_w_latency.avgtime | 写操作的延迟（包括排队时间）平均时间 |
|  | ceph.osd.op_w_process_latency.avgcount | 写操作的延迟（不包括排队时间）平均数 |
|  | ceph.osd.op_w_process_latency.sum | 写操作的延迟（不包括排队时间）总数 |
|  | ceph.osd.op_w_process_latency.avgtime | 写操作的延迟（不包括排队时间）平均时间 |
|  | ceph.osd.op_w_prepare_latency.avgcount | 写操作的延迟（不包括排队时间和等待完成）平均数 |
|  | ceph.osd.op_w_prepare_latency.sum | 写操作的延迟（不包括排队时间和等待完成）总数 |
|  | ceph.osd.op_w_prepare_latency.avgcount | 写操作的延迟（不包括排队时间和等待完成）平均时间 |
|  | ceph.osd.rw | 客户端读修改写操作 |
|  | ceph.osd.op_rw_in_bytes | 客户端读取修改写入操作写入 |
|  | ceph.osd.op_rw_out_bytes | 客户端读修改写操作读出 |
|  | ceph.osd.op_rw_latency.avgcount | 读修改写操作的延迟（包括排队时间）平均数 |
|  | ceph.osd.op_rw_latency.sum | 读修改写操作的延迟（包括排队时间）总数 |
|  | ceph.osd.op_rw_latency.avgtime | 读修改写操作的延迟（包括排队时间）平均时间	|
|  | ceph.osd.op_rw_process_latency.avgcount | 读修改写操作的延迟（不包括排队时间）平均数 |
|  | ceph.osd.op_rw_process_latency.sum | 读修改写操作的延迟（不包括排队时间）总数 |
|  | ceph.osd.op_rw_process_latency.avgtime | 读修改写操作的延迟（不包括排队时间）平均时间 |
|  | ceph.osd.op_rw_prepare_latency.avgcount | 读修改写操作的延迟（不包括排队时间和等待完成）平均数 |
|  | ceph.osd.op_rw_prepare_latency.sum | 读修改写操作的延迟（不包括排队时间和等待完成）总数 |
|  | ceph.osd.op_rw_prepare_latency.avgtime | 读修改写操作的延迟（不包括排队时间和等待完成）平均时间 |
|  | ceph.osd.op_before_queue_op_lat.avgcount | 调用队列之前的IO等待时间（在真正进入ShardedOpWq之前,客户端IO在队列OpWQ等待时间之前 ) 平均数 |
|  | ceph.osd.op_before_queue_op_lat.sum | 调用队列之前的IO等待时间（在真正进入ShardedOpWq之前,客户端IO在队列OpWQ等待时间之前 ) 总数 |
|  | ceph.osd.op_before_queue_op_lat.avgtime | 调用队列之前的IO等待时间（在真正进入ShardedOpWq之前,客户端IO在队列OpWQ等待时间之前 ) 平均时间 | 
|  | ceph.osd.op_before_dequeue_op_lat.avgcount | 在调用dequeue_op（已经排队并获得PG锁）之前，IO的等待时间（客户端IO在dequeue_op等待时间之前） 平均数 |
|  | ceph.osd.op_before_dequeue_op_lat.sum | 在调用dequeue_op（已经排队并获得PG锁）之前，IO的等待时间（客户端IO在dequeue_op等待时间之前） 总数 |
|  | ceph.osd.op_before_dequeue_op_lat.avgtime | 在调用dequeue_op（已经排队并获得PG锁）之前，IO的等待时间（客户端IO在dequeue_op等待时间之前） 平均时间 |
|  | ceph.osd.subop | 子操作数 |
|  | ceph.osd.subop_in_bytes | 子操作总大小 |
|  | ceph.osd.subop_latency.avgcount | 子操作延迟 平均数 |
|  | ceph.osd.subop_latency.sum | 子操作延迟 总数 |
|  | ceph.osd.subop_latency.avgtime | 子操作延迟 平均时间 |
|  | ceph.osd.subop_w | 复制写入 |
|  | ceph.osd.subop_w_in_bytes | 复制的写入数据大小 |
|  | ceph.osd.subop_w_latency.avgcount | 复制的写入延迟 平均数 |
|  | ceph.osd.subop_w_latency.avgtime | 复制的写入延迟 平均时间 |
|  | ceph.osd.subop_w_latency.sum | 复制的写入延迟 总数 |
|  | ceph.osd.subop_pull | 子操作拉取请求 |
|  | ceph.osd.subop_pull_latency.avgcount | 子操作拉取延迟 平均数 |
|  | ceph.osd.subop_pull_latency.sum | 子操作拉取延迟 总数 |
|  | ceph.osd.subop_pull_latency.avgtime | 子操作拉取延迟 平均时间 |
|  | ceph.osd.subop_push | 子操作推送消息 |
|  | ceph.osd.subop_push_in_bytes | 子操作推送大小 |
|  | ceph.osd.subop_push_latency.avgcount | 子操作推送延迟 平均数 |
|  | ceph.osd.subop_push_latency.sum | 子操作推送延迟 总数 |
|  | ceph.osd.subop_push_latency.avgtime | 子操作推送延迟 平均时间 |
|  | ceph.osd.pull | 拉取请求发送 |
|  | ceph.osd.push | 推送消息 |
|  | ceph.osd.push_out_bytes | 推送大小 |
|  | ceph.osd.push_in | 入栈推送消息 |
|  | ceph.osd.push_in_bytes | 入栈推送大小 |
|  | ceph.osd.recovery_ops | 恢复操作数 |
|  | ceph.osd.loadavg | cpu load |
|  | ceph.osd.buffer_bytes | 分配的缓冲区大小 |
|  | ceph.osd.history_alloc_Mbytes | 历史分配大小 |
|  | ceph.osd.history_alloc_num | 历史分配数量 | 
|  | ceph.osd.cached_crc | 获取crc_cached获取的总数量 |
|  | ceph.osd.missed_crc | 未名字crc_cached的总数量 |
|  | ceph.osd.numpg | pg数量 |
|  | ceph.osd.numpg_primary | 主pg数量 |
|  | ceph.osd.numpg_replica |	副本pg数量 |	 	  	 
|  |  ceph.osd.numpg_stray| 	删除的pg数量	 |	 	 
|  | ceph.osd.heartbeat_to_peers	| 发送心跳ping数	 | 	 
|  | ceph.osd.heartbeat_from_peers	| 接收心跳ping数	 |	 	 
|  | ceph.osd.map_messages	| osd map 消息	| 	 	 
|  |  ceph.osd.map_message_epochs |	osd map 消息的epochs号	| 	 	 
|  |  ceph.osd.map_message_epoch_dups	|osd map 复制	 |	 	 
|  |  ceph.osd.messages_delayed_for_map |	等待osd map操作	| 	 	 
|  | ceph.osd.stat_bytes |	osd 大小	 |	 	 
|  |  ceph.osd.stat_bytes_used |	osd 占用大小	 |	 	 
|  | ceph.osd.stat_bytes_avail |	osd 可以用大小 |	 	 	 
|  |  ceph.osd.osd_map_cache_hit |	osd map 命中缓存	 |	 	 
|  |  ceph.osd.osd_map_cache_miss | osd map 穿透缓存 |	 	 	 
|  |  ceph.osd.osd_map_cache_miss_low | osd map 穿透缓存下限 |	 	 	 
|  |  ceph.osd.osd_map_cache_miss_low_avg.avgcount | osd map 穿透缓存下限 平均数	|  	 	 
|  |  ceph.osd.osd_map_cache_miss_low_avg.sum | osd map 穿透缓存下限 总数 |	 	 	 
|  | ceph.osd.osd_map_bl_cache_hit | osd map 缓冲区缓存命中	| 	 	 
|  |  ceph.osd.osd_map_bl_cache_miss |	osd map 缓冲区缓存穿透 |	 	 	 
|  |  ceph.osd.copyfrom |	rados 复制操作 |	 	 	 
|  |  ceph.osd.tier_promote |	提升	 	| 	 
|  |  ceph.osd.tier_flush |	刷新	 |	 	 
|  |  ceph.osd.tier_flush_fail |	刷新失败	 | 	 	 
|  |  ceph.osd.tier_try_flush |	尝试刷新	 |	 	 
|  |  ceph.osd.tier_try_flush_fail | 尝试刷新失败 |	 	 	 
|  | ceph.osd.tier_evict | 逐出 |	 	 	 
|  | ceph.osd.tier_whiteout |	白名单	| 	 	 
|  |  ceph.osd.tier_dirty | 设置脏数据标志	 |	 	 
|  |  ceph.osd.tier_clean | 清除设置的脏数据标志	 |	 	 
|  |  ceph.osd.tier_delay	| 延迟	 	| 	 
|  | ceph.osd.tier_proxy_read	| 代理读取	 |	 	 
|  |  ceph.osd.tier_proxy_write| 	代理写入	 	| 	 
|  |  ceph.osd.agent_wake | agent唤醒up	| 	 	 
|  | ceph.osd.agent_skip |	被agent跳过的对象数	 |	 	 
|  |  ceph.osd.agent_flush|	agent刷新|	 	 	 
|  |  ceph.osd.agent_evict | agent逐出	 |	 	 
|  |  ceph.osd.object_ctx_cache_hit	 | 对象内容命中缓存 |	 	 	 
|  | ceph.osd.object_ctx_cache_total	| 查找对象内存缓存	 |	 	 
|  |  ceph.osd.op_cache_hit |	操作命中缓存	 |	 	 
|  |  ceph.osd.osd_tier_flush_lat.avgcount |	对象刷新延迟 平均数	| 	 	 
|  | ceph.osd.osd_tier_flush_lat.sum	| 对象刷新延迟 总数 |	 	 	 
|  | ceph.osd.osd_tier_flush_lat.avgtime |	对象刷新延迟 平均时间	| 	 	 
|  | ceph.osd.osd_tier_promote_lat.avgcount	 | 对象促进延迟 平均数	 |	 	 
|  | ceph.osd.osd_tier_promote_lat.sum |	对象促进延迟 总数 |	 	 	 
|  | ceph.osd.osd_tier_promote_lat.avgtime |	对象促进延迟 平均时间	 |	 	 
|  | ceph.osd.osd_tier_r_lat.avgcount	| 对象代理读取延迟 平均数 |	 	 	 
|  | ceph.osd.osd_tier_r_lat.sum	| 对象代理读取延迟 总数 |	 	 	 
|  | ceph.osd.osd_tier_r_lat.avgtime	| 对象代理读取延迟 平均时间	  |	 	 
|  | ceph.osd.osd_pg_info |	pg 更新的信息	 |	 	 
|  | ceph.osd.osd_pg_fastinfo |	pg 使用fastinfo更新信息	 	| 	 
|  | ceph.osd.osd_pg_biginfo	| pg 更新大信息属性 |


# 6. recoverystate_perf
| 监控类型 | 监控项      |  说明  |
|----------|:-------------:|:-------------:|
| perf dump<br/> recoverystate_perf | ceph.recoverystate_perf.<br/>initial_latency.avgcount |	初始化恢复状态延迟 平均数	 
|  | ceph.recoverystate_perf.<br/>initial_latency.sum |	初始化恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>initial_latency.avgtime	| 初始化恢复状态延迟 平均时间	| 	 	 
|  |  ceph.recoverystate_perf.<br/>started_latency.avgcount |	启动恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>started_latency.sum |	启动恢复状态延迟 总数	 |	 	 
|  | ceph.recoverystate_perf.<br/>started_latency.avgtime |	启动恢复状态延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>reset_latency.avgcount |	复位恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>reset_latency.sum |	复位恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>reset_latency.avgtime	| 复位恢复状态延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>start_latency.avgcount |	启动恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>start_latency.sum |	启动恢复状态延迟 总数	 |	 	 
|  | ceph.recoverystate_perf.<br/>start_latency.avgtime |	启动恢复状态延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>primary_latency.avgcount |	主节点恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>primary_latency.sum |	主节点恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>primary_latency.avgtime |	主节点恢复状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>peering_latency.avgcount |	凝视恢复状态延迟 平均数	| 	 	 
|  |  ceph.recoverystate_perf.<br/>peering_latency.sum |	凝视恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>peering_latency.avgtime |	凝视恢复状态延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>backfilling_latency.avgcount |	回填恢复状态延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>backfilling_latency.sum |	回填恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>backfilling_latency.avgtime |	回填恢复状态延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>waitremotebackfillreserved_latency.avgcount |	等待远程回填保留恢复状态延迟 平均数	 	 	 |
|  |  ceph.recoverystate_perf.<br/>waitremotebackfillreserved_latency.sum |	等待远程回填保留恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>waitremotebackfillreserved_latency.avgtime |	等待远程回填保留恢复状态延迟 平均时间	 	 	 |
|  | ceph.recoverystate_perf.<br/>waitlocalbackfillreserved_latency.avgcount |	等待本地回填保留恢复状态延迟 平均数	 |	 	 
|  |  ceph.recoverystate_perf.<br/>waitlocalbackfillreserved_latency.sum |	等待本地回填保留恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>waitlocalbackfillreserved_latency.avgtime |	等待本地回填保留恢复状态延迟 平均时间	  |	 	 
|  | ceph.recoverystate_perf.<br/>notbackfilling_latency.avgcount |	非回填恢复状态延迟 平均数	  |	 	 
|  | ceph.recoverystate_perf.<br/>notbackfilling_latency.sum |	非回填恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>notbackfilling_latency.avgtime	| 非回填恢复状态延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>repnotrecovering_latency.avgcount |	爬行覆盖恢复状态延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>repnotrecovering_latency.sum |	爬行覆盖恢复状态延迟 总数	 |	 	 
|  |  ceph.recoverystate_perf.<br/>repnotrecovering_latency.avgtime |	爬行覆盖恢复状态延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>repwaitrecoveryreserved_latency.avgcount |	保留等待恢复状态延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>repwaitrecoveryreserved_latency.sum |	保留等待恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>repwaitrecoveryreserved_latency.avgtime	 | 保留等待恢复状态延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>repwaitbackfillreserved_latency.avgcount |	保留等待回填状态延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>repwaitbackfillreserved_latency.sum |	保留等待回填状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>repwaitbackfillreserved_latency.avgtime |	保留等待回填状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>reprecovering_latency.avgcount |	重修恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>reprecovering_latency.sum	| 重修恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>reprecovering_latency.avgtime |	重修恢复状态延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>activating_latency.avgcount |	激活恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>activating_latency.sum |	激活恢复状态延迟 总数	 | 	 	 
|  | ceph.recoverystate_perf.<br/>activating_latency.avgtime |	激活恢复状态延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>waitlocalrecoveryreserved_latency.avgcount |	等待本地恢复保留恢复状态延迟 平均数	 	 	 |
|  | ceph.recoverystate_perf.<br/>waitlocalrecoveryreserved_latency.sum	| 等待本地恢复保留恢复状态延迟 总数|	 	 	 
|  | ceph.recoverystate_perf.<br/>waitlocalrecoveryreserved_latency.avgtime |	等待本地恢复保留恢复状态延迟 平均时间	 	 	 |
|  | ceph.recoverystate_perf.<br/>waitremoterecoveryreserved_latency.avgcount |	等待远程恢复保留恢复状态延迟 平均数	 |	 	 
|  | ceph.recoverystate_perf.<br/>waitremoterecoveryreserved_latency.sum |	等待远程恢复保留恢复状态延迟 总数	 	 	 |
|  | ceph.recoverystate_perf.<br/>waitremoterecoveryreserved_latency.avgtime |	等待远程恢复保留恢复状态延迟 平均时间	| 	 	 
|  |  ceph.recoverystate_perf.<br/>recovering_latency.avgcount|	恢复中的恢复状态延迟 平均数 |	 	 	 
|  |  ceph.recoverystate_perf.<br/>recovering_latency.sum |	恢复中的恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>recovering_latency.avgtime |	恢复中的恢复状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>recovered_latency.avgcount |	已恢复的恢复状态延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>recovered_latency.sum |	已恢复的恢复状态延迟 总数	 | 	 	 
|  | ceph.recoverystate_perf.<br/>recovered_latency.avgtime |	已恢复的恢复状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>clean_latency.avgcount |	清除恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>clean_latency.sum |	清除恢复状态延迟 总数	 | 	 	 
|  | ceph.recoverystate_perf.<br/>clean_latency.avgtime |	清除恢复状态延迟 平均时间	 | 	 	 
|  | ceph.recoverystate_perf.<br/>active_latency.avgcount |	活跃的恢复状态延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>active_latency.sum |	活跃的恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>active_latency.avgtime |	活跃的恢复状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>replicaactive_latency.avgcount |	复制激活恢复状态延迟 平均数	 | 	 	 
|  | ceph.recoverystate_perf.<br/>replicaactive_latency.sum |	复制激活恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>replicaactive_latency.avgtime |	复制激活恢复状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>stray_latency.avgcount	| 无主的恢复状态延迟 平均数	 |	 	 
|  | ceph.recoverystate_perf.<br/>stray_latency.sum |	无主的恢复状态延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>stray_latency.avgtime |	无主的恢复状态延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>getinfo_latency.avgcount	| 恢复状态信息的延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>getinfo_latency.sum |	恢复状态信息的延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>getinfo_latency.avgtime	| 恢复状态信息的延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>getlog_latency.avgcount	| 恢复状态日志的延迟 平均数 |	 	 	 
|  | ceph.recoverystate_perf.getlog_latency.sum |	恢复状态日志的延迟 总数	 	| 	 
|  | ceph.recoverystate_perf.<br/>getlog_latency.avgtime	| 恢复状态日志的延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>waitactingchange_latency.avgcount |	等待改变恢复状态的延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>waitactingchange_latency.sum |	等待改变恢复状态的延迟 总数	| 	 	 
|  | ceph.recoverystate_perf. waitactingchange_latency waitactingchange_latency.avgtime |	等待改变恢复状态的延迟 平均时间	 |	 	 
|  | ceph.recoverystate_perf.<br/>incomplete_latency.avgcount	| 不完全恢复状态的延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>incomplete_latency.sum |	不完全恢复状态的延迟 总数	 |  	 	 
|  | ceph.recoverystate_perf.<br/>incomplete_latency.avgtime |	不完全恢复状态的延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>down_latency.avgcount |	挂掉恢复状态的延迟 平均数 |	 	 	 
|  |  ceph.recoverystate_perf.<br/>down_latency.sum |	挂掉恢复状态的延迟 总数 |	 	 	 
|  |  ceph.recoverystate_perf.<br/>down_latency.avgtime |	挂掉恢复状态的延迟 平均时间	| 	 	 
|  | ceph.recoverystate_perf.<br/>getmissing_latency.avgcount |	缺失恢复状态的延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>getmissing_latency.sum |	缺失恢复状态的延迟 总数 |	 	 	 
|  | ceph.recoverystate_perf.<br/>getmissing_latency.avgtime |	缺失恢复状态的延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>waitupthru_latency.avgcount	| 守候恢复状态的延迟 平均数	 |	 	 
|  |  ceph.recoverystate_perf.<br/>waitupthru_latency.sum|  	守候恢复状态的延迟 总数 |	 	 	 
|  |  ceph.recoverystate_perf.<br/>waitupthru_latency.avgtime |	守候恢复状态的延迟 平均时间 |	 	 	 
|  | ceph.recoverystate_perf.<br/>notrecovering_latency.avgcount	 | 不恢复的恢复状态延迟 平均数	| 	 	 
|  | ceph.recoverystate_perf.<br/>notrecovering_latency.sum |	不恢复的恢复状态延迟 总数	| 	 	 
|  | ceph.recoverystate_perf.<br/>notrecovering_latency.avgtime |	不恢复的恢复状态延迟 平均时间	| 



# 7. throttle
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
|perf dump throttle-*|val|当前可用的值||	 	 	 
|*|max|最大限制数||	 	 	 
|*|get|获取到的值||	 	 	 
|*|get_sum|获取到的总数	|| 	 	 
|*|get_or_fail_fail|获取或者错误值||	 	 	 
|*|get_or_fail_success|获取或者错误成功值||	 	 	 
|*|take|接受值||	 	 	 
|*|take_sum|接受总数||	 	 	 
|*|put	|推送值||	 	 	 
|*|put_sum|推送总数	 ||	 	 
|*|wait.avgcount|等待平均数量||	 	 	 
|*|wait.sum|等待总数||	 	


# 8. ceph osd perf
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
ceph osd perf| osd |osd id||
| | commit_latency | 写入延迟时间，表示写journal的完成时间(毫秒) | |
| | apply_latency | 读取延迟，表示写到osd的buffer cache里的完成时间(毫秒) | |







		


	


		


		


		




