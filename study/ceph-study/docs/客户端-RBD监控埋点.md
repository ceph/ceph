# 1. perf dump
- ceph daemon /var/run/ceph/ceph-client.admin.asok  perf dump 

## 1. RBD Client Metrics Table
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
AsyncMessenger*| msgr_recv_messages |网络接收消息| |
*| msgr_send_messages  |网络发送消息| |
*| msgr_recv_bytes  |网络接收字节| |
*| msgr_send_bytes  |网络发送字节| |
*| msgr_created_connections  |创建连接数| |
*| msgr_active_connections  |有效连接数| |
*| msgr_running_total_time  |线程运行的总时间| |
*| msgr_running_send_time  |消息发送的总时间| |
*| msgr_running_recv_time  |消息接收的总时间| |
*| msgr_running_fast_dispatch_time  |快速调度总时间| |

## 2. RBD Finisher-RadosClient Metrics Table
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
finisher-radosclient| queue_len |队列长度| |
*| complete_latency.avgcount  |完成延迟 平均数| |
*| complete_latency.sum  |完成延迟 总数| |
*| complete_latency.avgtime  |完成延迟 平均时间| |

## 3. RBD ObjectCacher-librbd-{id}-{pool}-{image} Metrics Table
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
objectcacher-librbd-{id}-{pool}-{image}| cache_ops_hit |命中缓存| |
*| cache_ops_miss |穿透缓存| |
*| cache_bytes_hit |命中缓存大小| |
*| cache_bytes_miss |穿透缓存大小| |
*| data_read |读数据大小| |
*| data_written |写缓存数据大小| |
*| data_flushed |数据刷新| |
*| data_overwritten_while_flushing |刷新时数据重写| |
*| write_ops_blocked |肮脏限制延迟的写操作| |
*| write_bytes_blocked |写入脏数据的限制| |
*| write_time_blocked |由于脏数据限制而阻塞写入的时间| |

## 4. RBD librbd-{id}-{pool}-{image} Metrics Table
监控类型 | 监控项 |  说明 | 级别 |
---|---|---|---|
librbd-{id}-{pool}-{image}| rd |读操作数| |
*| rd_bytes |读操作的字节数| |
*| rd_latency.avgcount |读操作延迟队列的平均数| |
*| rd_latency.sum |读操作延迟队列的总数| |
*| rd_latency.avgtime |读操作延迟队列的平均时间| |
*| wr |写操作数| |
*| wr_bytes |写操作字节数| |
*| wr_latency.avgcount |写操作延迟队列的平均数| |
*| wr_latency.sum |写操作延迟队列的总数| |
*| wr_latency.avgtime |写操作延迟队列的平均时间| |
*| discard |丢弃操作数| |
*| discard_bytes |丢弃操作字节数| |
*| discard_latency.avgcount |丢弃操作延迟队列平均数| |
*| discard_latency.sum |丢弃操作延迟队列总数| |
*| discard_latency.avgtime |丢弃操作延迟队列平均时间| |
*| flush |刷新操作数| |
*| aio_flush |异步IO刷新操作数| |
*| aio_flush_latency.avgcount |异步IO刷新操作延迟队列平均数| |
*| aio_flush_latency.sum |异步IO刷新操作延迟队列总数| |
*| aio_flush_latency.avgtime	 |异步IO刷新操作延迟队列平均时间| |
*| ws	 |WriteSame: 清零操作offload到存储，加速块分配、克隆、数据初始化操作| |
*| ws_bytes	 |ws字节数大小| |
*| ws_latency.avgcount	 |ws延迟队列平均数| |
*| ws_latency.sum	 |ws延迟队列总数| |
*| ws_latency.avgtime	 |ws延迟队列平均时间| |
*| cmp	 || |
*| cmp_bytes	 |cmp字节数大小| |
*| cmp_latency.avgcount	|cmp延迟队列平均数||	 	 	 
*| cmp_latency.sum	|cmp延迟队列总数	|| 	 	 
*| cmp_latency.avgtime|	cmp延迟队列平均时间||	 	 	 
*| snap_create |	快照创建数 ||	 	 	 
*| snap_remove |	快照移除数 ||	 	 	 
*| snap_rollback |	快照回滚数 ||	 	 	 
*| snap_rename |	快照重命名数 ||	  	 	 
*| notify |	更新通知数 ||	 	 	 
*| resize |	调整大小	|| 	 	 
*| readahead |	读取头数	|| 	 	 
*| readahead_bytes | 读取头大小 ||	 	 	 
*| invalidate_cache |	缓存失效 ||

## 5. objecter
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


## 6. throttle
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
