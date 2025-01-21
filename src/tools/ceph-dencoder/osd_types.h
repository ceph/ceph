#include "osd/OSDMap.h"
TYPE(osd_info_t)
TYPE_FEATUREFUL(osd_xinfo_t)
TYPE_FEATUREFUL_NOCOPY(OSDMap)
TYPE_FEATUREFUL_STRAYDATA(OSDMap::Incremental)
TYPE(PGTempMap)

#include "osd/osd_types.h"
TYPE(osd_reqid_t)
TYPE(object_locator_t)
TYPE(request_redirect_t)
TYPE(pg_t)
TYPE(coll_t)
TYPE_FEATUREFUL(objectstore_perf_stat_t)
TYPE_FEATUREFUL(osd_stat_t)
TYPE(OSDSuperblock)
TYPE_FEATUREFUL(pool_snap_info_t)
TYPE_FEATUREFUL(pg_pool_t)
TYPE(object_stat_sum_t)
TYPE(object_stat_collection_t)
TYPE(pg_stat_t)
TYPE_FEATUREFUL(pool_stat_t)
TYPE(pg_hit_set_info_t)
TYPE(pg_hit_set_history_t)
TYPE(pg_history_t)
TYPE(pg_info_t)
TYPE(PastIntervals)
TYPE(PastIntervals::pg_interval_t)
TYPE_FEATUREFUL(pg_query_t)
TYPE(ObjectModDesc)
TYPE(pg_log_entry_t)
TYPE(pg_log_dup_t)
TYPE(pg_log_t)
TYPE_FEATUREFUL(pg_missing_t)
TYPE(pg_nls_response_t)
TYPE(pg_ls_response_t)
TYPE(object_copy_cursor_t)
TYPE_FEATUREFUL(object_copy_data_t)
TYPE(pg_create_t)
TYPE(OSDSuperblock)
TYPE(SnapSet)
TYPE_FEATUREFUL(watch_info_t)
TYPE_FEATUREFUL(watch_item_t)
TYPE(object_manifest_t)
TYPE_FEATUREFUL(object_info_t)
TYPE(SnapSet)
TYPE_FEATUREFUL(ObjectRecoveryInfo)
TYPE(ObjectRecoveryProgress)
TYPE(PushReplyOp)
TYPE_FEATUREFUL(PullOp)
TYPE_FEATUREFUL(PushOp)
TYPE(ScrubMap::object)
TYPE(ScrubMap)
TYPE_FEATUREFUL(obj_list_watch_response_t)
TYPE(clone_info)
TYPE(obj_list_snap_response_t)
TYPE(pool_pg_num_history_t)
TYPE(store_statfs_t)
TYPE(spg_t)
TYPE(ObjectCleanRegions)
TYPE(pg_notify_t) 
TYPE(pg_merge_meta_t)
TYPE(pg_shard_t)
TYPE(pg_fast_info_t)
TYPE(pg_lease_ack_t)
TYPE(pg_lease_t)
TYPE(pg_log_op_return_item_t)
TYPE(chunk_info_t)
TYPE_FEATUREFUL(pool_opts_t)
TYPE_FEATUREFUL(pg_missing_item)
TYPE(eversion_t)
//TYPE(compact_interval_t) declared in .cc
//TYPE(pg_missing_t::item)

#include "osd/ECUtil.h"
// TYPE(stripe_info_t) non-standard encoding/decoding functions
TYPE(ECUtil::HashInfo)

#include "osd/ECMsgTypes.h"
TYPE_NOCOPY(ECSubWrite)
TYPE(ECSubWriteReply)
TYPE_FEATUREFUL(ECSubRead)
TYPE(ECSubReadReply)

#include "osd/HitSet.h"
TYPE_NONDETERMINISTIC(ExplicitHashHitSet)
TYPE_NONDETERMINISTIC(ExplicitObjectHitSet)
TYPE(BloomHitSet)
TYPE_NONDETERMINISTIC(HitSet)   // because some subclasses are
TYPE(HitSet::Params)

#include "osd/SnapMapper.h"
TYPE(SnapMapper::Mapping)
TYPE(SnapMapper::object_snaps)

#include "os/Transaction.h"
TYPE(ceph::os::Transaction)

#include "os/ObjectStore.h"
TYPE(ObjectStore::Transaction)

#include "os/SequencerPosition.h"
TYPE(SequencerPosition)

#ifdef WITH_BLUESTORE
#include "os/bluestore/bluestore_types.h"
TYPE(bluestore_bdev_label_t)
TYPE(bluestore_cnode_t)
TYPE(bluestore_compression_header_t)
TYPE(bluestore_extent_ref_map_t)
TYPE_FEATUREFUL(bluestore_extent_ref_map_t::record_t)
TYPE(bluestore_pextent_t)
TYPE(bluestore_blob_use_tracker_t)
// TODO: bluestore_blob_t repurposes the "feature" param of encode() for its
// struct_v. at a higher level, BlueStore::ExtentMap encodes the extends using
// a different interface than the normal ones. see
// BlueStore::ExtentMap::encode_some(). maybe we can test it using another
// approach.
// TYPE_FEATUREFUL(bluestore_blob_t)
TYPE(bluestore_shared_blob_t)
TYPE(bluestore_onode_t)
TYPE(bluestore_onode_t::shard_info)
using shard_info = bluestore_onode_t::shard_info;
TYPE(shard_info)
TYPE(bluestore_deferred_op_t)
TYPE(bluestore_deferred_transaction_t)
// TYPE(bluestore_compression_header_t) there is no encode here

#include "os/bluestore/bluefs_types.h"
TYPE(bluefs_extent_t)
TYPE(bluefs_fnode_t)
TYPE(bluefs_super_t)
TYPE(bluefs_transaction_t)
TYPE(bluefs_layout_t)
#endif

#include "mon/AuthMonitor.h"
TYPE_FEATUREFUL(AuthMonitor::Incremental)

#include "mon/PGMap.h"
TYPE_FEATUREFUL_NONDETERMINISTIC(PGMapDigest)
TYPE(PGMapDigest::pg_count)
TYPE_FEATUREFUL_NONDETERMINISTIC(PGMap)
//TYPE(PGMap::Incremental)

#include "mon/MonitorDBStore.h"
TYPE(MonitorDBStore::Transaction)
TYPE(MonitorDBStore::Op)

#include "mon/MonMap.h"
TYPE_FEATUREFUL(MonMap)
TYPE_FEATUREFUL(mon_info_t)

#include "mon/MonCommand.h"
TYPE(MonCommand)

#include "mon/MonCap.h"
TYPE(MonCap)

#include "mon/MgrMap.h"
TYPE_FEATUREFUL(MgrMap)
TYPE(MgrMap::ModuleOption)
TYPE(MgrMap::ModuleInfo)
TYPE(MgrMap::StandbyInfo)

#include "mon/mon_types.h"
TYPE(MonitorDBStoreStats)
TYPE(ScrubResult)
TYPE(mon_feature_t)
TYPE_FEATUREFUL(DataStats)
TYPE_FEATUREFUL(ProgressEvent)
TYPE(FeatureMap)

#include "mon/CreatingPGs.h"
TYPE_FEATUREFUL(creating_pgs_t)
TYPE_FEATUREFUL(creating_pgs_t::pg_create_info)

#include "mgr/ServiceMap.h"
TYPE_FEATUREFUL(ServiceMap)
TYPE_FEATUREFUL(ServiceMap::Service)
TYPE_FEATUREFUL(ServiceMap::Daemon)

#include "mgr/DaemonHealthMetric.h"
TYPE(DaemonHealthMetric)

#include "mgr/MetricTypes.h"
TYPE(OSDMetricPayload)

#include "mgr/OSDPerfMetricTypes.h"
TYPE(OSDPerfMetricQuery)
TYPE(OSDPerfMetricSubKeyDescriptor)
TYPE(PerformanceCounterDescriptor)
TYPE(OSDPerfMetricReport)

#include "mon/ConnectionTracker.h"
TYPE(ConnectionReport);
TYPE(ConnectionTracker);

#include "mon/health_check.h"
TYPE(health_check_t)
TYPE(health_check_map_t)

#include "os/DBObjectMap.h"
TYPE(DBObjectMap::_Header)
TYPE(DBObjectMap::State)

#include "os/kstore/kstore_types.h"
TYPE(kstore_cnode_t)
TYPE(kstore_onode_t)
