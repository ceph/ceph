# 1. 模块简介
Monitor 作为Ceph的 Metada Server 维护了集群的信息，它包括了6个 Map，
分别是 MONMap，OSDMap，PGMap，LogMap，AuthMap，MDSMap。
其中 PGMap 和 OSDMap 是最重要的两张Map。

# 2. 模块的基本结构
![image.png](https://upload-images.jianshu.io/upload_images/2099201-beca04aae58bef45.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

1. Monitor内部使用一套Paxos来实现各种数据的更新，所以所有继承自PaxosService的Monitor
   实现数据更新时需要通过Paxos达成一致后才能进行。
2. PaxosService的dispatch内部调用子类的preprocess_query进行查询相关操作，如果非查询类处理，
再调用子类的prepare_update接口实现数据的更新，所以子类Monitor实现两个接口来处理相关的业务消息。

# 3. Monitor业务消息
## 3.1 Monitor自身
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| CEPH_MSG_PING | MPing | 定期Ping Monitor确认Monitor的存在 | handle_ping |
| CEPH_MSG_MON_GET_MAP | MMonGetMap | 认证前获取MonMap | handle_mon_get_map |
| CEPH_MSG_MON_METADATA | MMonMetadata | 处理保存某个Monitor的系统信息（cpu，内存等）| handle_mon_metadata |
| MSG_MON_COMMAND | MMonCommand | 传递命令行消息给Monitor，Monitor再分发给相应的XXXMonitor进行处理 | handle_command |
| CEPH_MSG_MON_GET_VERSION | MMonGetVersion | 获取cluster map的版本信息 | handle_get_version |
| CEPH_MSG_MON_SUBSCRIBE | MMonSubscribe | Cluster map订阅更新 | handle_subscribe |
| MSG_ROUTE | MRoute | 路由请求转发（待确认） | handle_route |
| MSG_MON_PROBE | MMonProbe | 启动加入时需要向其他Monitor发送Probe请求 | handle_probe |
| MSG_MON_SYNC | MMonSync | 同步Paxos状态数据 | handle_sync |
| MSG_MON_SCRUB | MMonScrub | MonitorDBStore数据一致性检测 | handle_scrub |
| MSG_MON_JOIN | MMonJoin | 如果不在MonMap中申请加入到MonMap | MonmapMonitor::prepare_join |
| MSG_MON_PAXOS | MMonPaxos | 选举完成后，leader会触发Paxos::leader_init，状态置为STATE_RECOVERING，并发起该消息的OP_COLLECT流程 | Paxos::dispatch |
| MSG_MON_ELECTION | MMonElection | 发起选举流程 | Elector::dispatch |
| MSG_FORWARD | MForward | 将请求转发到leader | handle_forward |
| MSG_TIMECHECK | MTimeCheck | Monitor每隔mon_timecheck_interval检测所有Monitor的系统时间来检测节点之间的时间差 | handle_timecheck |
| MSG_MON_HEALTH | MMonHealth | 每隔mon_health_data_update_interval检测存放Monitor上面使用的leveldb数据的状态 | HealthMonitor::service_dispatch |

## 3.2 AuthMonitor
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| MSG_MON_COMMAND | MMonCommand | 处理ceph auth xxx命令行相关处理 | preprocess_command处理ceph auth get/export/list等<br/>prepare_command处理ceph auth import/add/get-or-create/caps等 |
| CEPH_MSG_AUTH | MAuth | 实现认证和授权消息处理 | prep_auth |

## 3.3 OSDMonitor
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| CEPH_MSG_MON_GET_OSDMAP | MMonGetOSDMap | 获取OSDMap | preprocess_get_osdmap |
| MSG_OSD_MARK_ME_DOWN | MOSDMarkMeDown | OSD shutdown之前通知Monitor发送该消息 | preprocess_mark_me_down <br/> prepare_mark_me_down |
| MSG_OSD_FAILURE | MOSDFailure | 1. OSD每隔OSD_TICK_INTERVAL检测心跳无响应的OSD，并将失败的OSD report给Monitor<br/> 2. Monitor判断上报次数>=mon_osd_min_down_reports，那么就将target_osd标识为down | preprocess_failure |
| MSG_OSD_BOOT | MOSDBoot | 新OSD加入时发送请求到Monitor，参考新OSD的加入流程 | preprocess_bootprepare_boot |
| MSG_OSD_ALIVE | MOSDAlive | OSD判断up_thru_wanted决定是否发送请求给Monitor，Monitor发送Incremental OSDMap返回给OSD |preprocess_alive <br/>prepare_alive |
| MSG_OSD_PGTEMP | MOSDPGTemp | Primary OSD处于backfilling状态无法提供读取服务时，会发送该消息到Monitor，将PG临时映射到其他的OSD上提供去服务 | preprocess_pgtemp<br/> prepare_pgtemp |
| MSG_REMOVE_SNAPS | MRemoveSnaps | 删除快照信息 | prepare_remove_snaps |
| CEPH_MSG_POOLOP | MPoolOp | 删除/创建Pool，创建/删除pool快照等 | prepare_pool_op |

## 3.4  PGMonitor
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| CEPH_MSG_STATFS | MStatfs | 返回文件系统osd占用的kb容量 | handle_statfs |
| MSG_PGSTATS | MPGStats | 查询或者更新pg状态 | preprocess_pg_stats <br/>prepare_pg_stats |
| MSG_GETPOOLSTATS | MGetPoolStats | 获取pool汇总状态信息 | preprocess_getpoolstats |
| MSG_MON_COMMAND | MMonCommand | 处理ceph pg xxx相关命令行 | preprocess_command |

## 3.5 MonMapMonitor
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| MSG_MON_JOIN | MMonJoin | 更新MonMap | preprocess_join<br/>prepare_join |
| MSG_MON_COMMAND | MMonCommand | 处理ceph mon xxx相关命令行 | preprocess_command<br/> prepare_command |

## 3.6 MDSMonitor
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| MSG_MDS_BEACON | | | |
| MSG_MDS_OFFLOAD_TARGETS  | | | |

## 3.7 LogMonitor
| 消息类型 | 消息结构体 | 消息作用 | 处理接口 |
|:---:|:---:|:---:|:---:|
| MSG_LOG | | | |
