#include "include/CompatSet.h"
TYPE(CompatSet)

#include "include/filepath.h"
TYPE(filepath)

#include "include/util.h"
TYPE(ceph_data_stats)

#include "common/bit_vector.hpp"
TYPE(BitVector<2>)

#include "common/bloom_filter.hpp"
TYPE(bloom_filter)
TYPE(compressible_bloom_filter)

#include "common/snap_types.h"
TYPE(SnapContext)
TYPE(SnapRealmInfo)

#include "common/DecayCounter.h"
TYPE(DecayCounter)

#include "common/LogEntry.h"
TYPE(LogEntryKey)
TYPE(LogEntry)
TYPE(LogSummary)

#include "common/SloppyCRCMap.h"
TYPE(SloppyCRCMap)

#include "msg/msg_types.h"
TYPE(entity_name_t)
TYPE(entity_addr_t)

#include "osd/OSDMap.h"
TYPE(osd_info_t)
TYPE(osd_xinfo_t)
TYPE_FEATUREFUL_STRAYDATA(OSDMap)
TYPE_FEATUREFUL_STRAYDATA(OSDMap::Incremental)

#include "crush/CrushWrapper.h"
TYPE_NOCOPY(CrushWrapper)

#include "common/histogram.h"
TYPE(pow2_hist_t)

#include "osd/osd_types.h"
TYPE(osd_reqid_t)
TYPE(object_locator_t)
TYPE(request_redirect_t)
TYPE(pg_t)
TYPE(coll_t)
TYPE(objectstore_perf_stat_t)
TYPE(osd_stat_t)
TYPE(OSDSuperblock)
TYPE_FEATUREFUL(pool_snap_info_t)
TYPE_FEATUREFUL(pg_pool_t)
TYPE(object_stat_sum_t)
TYPE(object_stat_collection_t)
TYPE(pg_stat_t)
TYPE_FEATUREFUL(pool_stat_t)
TYPE(pg_history_t)
TYPE(pg_info_t)
TYPE(pg_interval_t)
TYPE_FEATUREFUL(pg_query_t)
TYPE(pg_log_entry_t)
TYPE(pg_log_t)
TYPE(pg_missing_t::item)
TYPE_FEATUREFUL(pg_missing_t)
TYPE(pg_ls_response_t)
TYPE(pg_nls_response_t)
TYPE(object_copy_cursor_t)
TYPE_FEATUREFUL(object_copy_data_t)
TYPE(pg_create_t)
TYPE(watch_info_t)
TYPE(object_info_t)
TYPE(SnapSet)
TYPE(ObjectRecoveryInfo)
TYPE(ObjectRecoveryProgress)
TYPE(ScrubMap::object)
TYPE(ScrubMap)
TYPE(pg_hit_set_info_t)
TYPE(pg_hit_set_history_t)
TYPE(osd_peer_stat_t)
TYPE(clone_info)
TYPE(obj_list_snap_response_t)
TYPE(PullOp)
TYPE(PushOp)
TYPE(PushReplyOp)

#include "osd/ECUtil.h"
TYPE(ECUtil::HashInfo)

#include "osd/ECMsgTypes.h"
TYPE(ECSubWrite)
TYPE(ECSubWriteReply)
TYPE_FEATUREFUL(ECSubRead)
TYPE(ECSubReadReply)

#include "osd/HitSet.h"
TYPE(ExplicitHashHitSet)
TYPE(ExplicitObjectHitSet)
TYPE(BloomHitSet)
TYPE(HitSet)
TYPE(HitSet::Params)

#include "os/ObjectStore.h"
TYPE(ObjectStore::Transaction)

#include "os/SequencerPosition.h"
TYPE(SequencerPosition)

#include "common/hobject.h"
TYPE(hobject_t)
TYPE(ghobject_t)

#include "mon/AuthMonitor.h"
TYPE(AuthMonitor::Incremental)

#include "mon/PGMap.h"
TYPE(PGMap::Incremental)
TYPE(PGMap)

#include "mon/MonitorDBStore.h"
TYPE(MonitorDBStore::Transaction)
TYPE(MonitorDBStore::Op)

#include "mon/MonMap.h"
TYPE_FEATUREFUL(MonMap)

#include "mon/MonCap.h"
TYPE(MonCap)

#include "mon/mon_types.h"
TYPE(LevelDBStoreStats)

#include "os/DBObjectMap.h"
TYPE(DBObjectMap::_Header)
TYPE(DBObjectMap::State)

#include "mds/JournalPointer.h"
TYPE(JournalPointer)

#include "osdc/Journaler.h"
TYPE(Journaler::Header)

#include "mds/snap.h"
TYPE(SnapInfo)
TYPE(snaplink_t)
TYPE(sr_t)

#include "mds/mdstypes.h"
TYPE(frag_info_t)
TYPE(nest_info_t)
TYPE(client_writeable_range_t)
TYPE(inode_t)
TYPE(old_inode_t)
TYPE(fnode_t)
TYPE(old_rstat_t)
TYPE(session_info_t)
TYPE(string_snap_t)
TYPE(MDSCacheObjectInfo)
TYPE(mds_table_pending_t)
TYPE(inode_load_vec_t)
TYPE(dirfrag_load_vec_t)
TYPE(mds_load_t)
TYPE(cap_reconnect_t)
TYPE(inode_backtrace_t)
TYPE(inode_backpointer_t)
TYPE(quota_info_t)

#include "mds/CInode.h"
TYPE(InodeStore)

#include "mds/MDSMap.h"
TYPE_FEATUREFUL(MDSMap)
TYPE_FEATUREFUL(MDSMap::mds_info_t)

#include "mds/Capability.h"
TYPE_NOCOPY(Capability)

#include "mds/InoTable.h"
TYPE(InoTable)

#include "mds/SnapServer.h"
TYPEWITHSTRAYDATA(SnapServer)

#include "mds/SessionMap.h"
TYPE(SessionMapStore)

#include "mds/events/ECommitted.h"
TYPE(ECommitted)
#include "mds/events/EExport.h"
TYPE(EExport)
#include "mds/events/EFragment.h"
TYPE(EFragment)
#include "mds/events/EImportFinish.h"
TYPE(EImportFinish)
#include "mds/events/EImportStart.h"
TYPE(EImportStart)
#include "mds/events/EMetaBlob.h"
TYPE_NOCOPY(EMetaBlob::fullbit)
TYPE(EMetaBlob::remotebit)
TYPE(EMetaBlob::nullbit)
TYPE(EMetaBlob::dirlump)
TYPE(EMetaBlob)
#include "mds/events/EOpen.h"
TYPE(EOpen)
#include "mds/events/EResetJournal.h"
TYPE(EResetJournal)
#include "mds/events/ESession.h"
TYPE(ESession)
#include "mds/events/ESessions.h"
TYPE(ESessions)
#include "mds/events/ESlaveUpdate.h"
TYPE(link_rollback)
TYPE(rmdir_rollback)
TYPE(rename_rollback::drec)
TYPE(rename_rollback)
TYPE(ESlaveUpdate)
#include "mds/events/ESubtreeMap.h"
TYPE(ESubtreeMap)
#include "mds/events/ETableClient.h"
TYPE(ETableClient)
#include "mds/events/ETableServer.h"
TYPE(ETableServer)
#include "mds/events/EUpdate.h"
TYPE(EUpdate)

#include "librbd/WatchNotifyTypes.h"
TYPE(librbd::WatchNotify::NotifyMessage)
TYPE(librbd::WatchNotify::ResponseMessage)

#include "rbd_replay/ActionTypes.h"
TYPE(rbd_replay::action::Dependency)
TYPE(rbd_replay::action::ActionEntry);

#ifdef WITH_RADOSGW

#include "rgw/rgw_rados.h"
TYPE(RGWObjManifestPart)
TYPE(RGWObjManifest)

#include "rgw/rgw_acl.h"
TYPE(ACLPermission)
TYPE(ACLGranteeType)
TYPE(ACLGrant)
TYPE(RGWAccessControlList)
TYPE(ACLOwner)
TYPE(RGWAccessControlPolicy)

#include "rgw/rgw_cache.h"
TYPE(ObjectMetaInfo)
TYPE(ObjectCacheInfo)
TYPE(RGWCacheNotifyInfo)

#include "cls/rgw/cls_rgw_types.h"
TYPE(rgw_bucket_pending_info)
TYPE(rgw_bucket_dir_entry_meta)
TYPE(rgw_bucket_dir_entry)
TYPE(rgw_bucket_category_stats)
TYPE(rgw_bucket_dir_header)
TYPE(rgw_bucket_dir)
TYPE(rgw_bucket_entry_ver)
TYPE(cls_rgw_obj_key)
TYPE(rgw_bucket_olh_log_entry)

#include "cls/rgw/cls_rgw_ops.h"
TYPE(rgw_cls_obj_prepare_op)
TYPE(rgw_cls_obj_complete_op)
TYPE(rgw_cls_list_op)
TYPE(rgw_cls_list_ret)
TYPE(cls_rgw_gc_defer_entry_op)
TYPE(cls_rgw_gc_list_op)
TYPE(cls_rgw_gc_list_ret)
TYPE(cls_rgw_gc_obj_info)
TYPE(cls_rgw_gc_remove_op)
TYPE(cls_rgw_gc_set_entry_op)
TYPE(cls_rgw_obj)
TYPE(cls_rgw_obj_chain)
TYPE(rgw_cls_tag_timeout_op)
TYPE(cls_rgw_bi_log_list_op)
TYPE(cls_rgw_bi_log_trim_op)
TYPE(cls_rgw_bi_log_list_ret)
TYPE(rgw_cls_link_olh_op)
TYPE(rgw_cls_unlink_instance_op)
TYPE(rgw_cls_read_olh_log_op)
TYPE(rgw_cls_read_olh_log_ret)
TYPE(rgw_cls_trim_olh_log_op)
TYPE(rgw_cls_bucket_clear_olh_op)
TYPE(rgw_cls_check_index_ret)

#include "cls/rgw/cls_rgw_client.h"
TYPE(rgw_bi_log_entry)

#include "cls/user/cls_user_types.h"
TYPE(cls_user_bucket)
TYPE(cls_user_bucket_entry)
TYPE(cls_user_stats)
TYPE(cls_user_header)

#include "cls/user/cls_user_ops.h"
TYPE(cls_user_set_buckets_op)
TYPE(cls_user_remove_bucket_op)
TYPE(cls_user_list_buckets_op)
TYPE(cls_user_list_buckets_ret)
TYPE(cls_user_get_header_op)
TYPE(cls_user_get_header_ret)
TYPE(cls_user_complete_stats_sync_op)

#include "rgw/rgw_common.h"
TYPE(RGWAccessKey)
TYPE(RGWSubUser)
TYPE(RGWUserInfo)
TYPE(rgw_bucket)
TYPE(RGWBucketInfo)
TYPE(RGWBucketEnt)
TYPE(RGWUploadPartInfo)
TYPE(rgw_obj)

#include "rgw/rgw_log.h"
TYPE(rgw_log_entry)

#include "cls/rbd/cls_rbd.h"
TYPE(cls_rbd_parent)
TYPE(cls_rbd_snap)

#endif

#include "cls/lock/cls_lock_types.h"
TYPE(rados::cls::lock::locker_id_t)
TYPE(rados::cls::lock::locker_info_t)

#include "cls/lock/cls_lock_ops.h"
TYPE(cls_lock_lock_op)
TYPE(cls_lock_unlock_op)
TYPE(cls_lock_break_op)
TYPE(cls_lock_get_info_op)
TYPE(cls_lock_get_info_reply)
TYPE(cls_lock_list_locks_reply)
TYPE(cls_lock_assert_op)

#include "cls/replica_log/cls_replica_log_types.h"
TYPE(cls_replica_log_item_marker)
TYPE(cls_replica_log_progress_marker)
TYPE(cls_replica_log_bound)
#include "cls/replica_log/cls_replica_log_ops.h"
TYPE(cls_replica_log_delete_marker_op)
TYPE(cls_replica_log_set_marker_op)
TYPE(cls_replica_log_get_bounds_op)
TYPE(cls_replica_log_get_bounds_ret)

#include "cls/refcount/cls_refcount_ops.h"
TYPE(cls_refcount_get_op)
TYPE(cls_refcount_put_op)
TYPE(cls_refcount_read_op)
TYPE(cls_refcount_read_ret)
TYPE(cls_refcount_set_op)


// --- messages ---
#include "messages/MAuth.h"
MESSAGE(MAuth)
#include "messages/MAuthReply.h"
MESSAGE(MAuthReply)
#include "messages/MCacheExpire.h"
MESSAGE(MCacheExpire)
#include "messages/MClientCapRelease.h"
MESSAGE(MClientCapRelease)
#include "messages/MClientCaps.h"
MESSAGE(MClientCaps)
#include "messages/MClientLease.h"
MESSAGE(MClientLease)
#include "messages/MClientReconnect.h"
MESSAGE(MClientReconnect)
#include "messages/MClientReply.h"
MESSAGE(MClientReply)
#include "messages/MClientRequest.h"
MESSAGE(MClientRequest)
#include "messages/MClientRequestForward.h"
MESSAGE(MClientRequestForward)
#include "messages/MClientSession.h"
MESSAGE(MClientSession)
#include "messages/MClientSnap.h"
MESSAGE(MClientSnap)
#include "messages/MCommand.h"
MESSAGE(MCommand)
#include "messages/MCommandReply.h"
MESSAGE(MCommandReply)
#include "messages/MDentryLink.h"
MESSAGE(MDentryLink)
#include "messages/MDentryUnlink.h"
MESSAGE(MDentryUnlink)
#include "messages/MDirUpdate.h"
MESSAGE(MDirUpdate)
#include "messages/MDiscover.h"
MESSAGE(MDiscover)
#include "messages/MDiscoverReply.h"
MESSAGE(MDiscoverReply)
#include "messages/MExportCaps.h"
MESSAGE(MExportCaps)
#include "messages/MExportCapsAck.h"
MESSAGE(MExportCapsAck)
#include "messages/MExportDir.h"
MESSAGE(MExportDir)
#include "messages/MExportDirAck.h"
MESSAGE(MExportDirAck)
#include "messages/MExportDirCancel.h"
MESSAGE(MExportDirCancel)
#include "messages/MExportDirDiscover.h"
MESSAGE(MExportDirDiscover)
#include "messages/MExportDirDiscoverAck.h"
MESSAGE(MExportDirDiscoverAck)
#include "messages/MExportDirFinish.h"
MESSAGE(MExportDirFinish)
#include "messages/MExportDirNotify.h"
MESSAGE(MExportDirNotify)
#include "messages/MExportDirNotifyAck.h"
MESSAGE(MExportDirNotifyAck)
#include "messages/MExportDirPrep.h"
MESSAGE(MExportDirPrep)
#include "messages/MExportDirPrepAck.h"
MESSAGE(MExportDirPrepAck)
#include "messages/MForward.h"
MESSAGE(MForward)
#include "messages/MGetPoolStats.h"
MESSAGE(MGetPoolStats)
#include "messages/MGetPoolStatsReply.h"
MESSAGE(MGetPoolStatsReply)
#include "messages/MHeartbeat.h"
MESSAGE(MHeartbeat)
#include "messages/MInodeFileCaps.h"
MESSAGE(MInodeFileCaps)
#include "messages/MLock.h"
MESSAGE(MLock)
#include "messages/MLog.h"
MESSAGE(MLog)
#include "messages/MLogAck.h"
MESSAGE(MLogAck)
#include "messages/MMDSBeacon.h"
MESSAGE(MMDSBeacon)
#include "messages/MMDSCacheRejoin.h"
MESSAGE(MMDSCacheRejoin)
#include "messages/MMDSFindIno.h"
MESSAGE(MMDSFindIno)
#include "messages/MMDSFindInoReply.h"
MESSAGE(MMDSFindInoReply)
#include "messages/MMDSFragmentNotify.h"
MESSAGE(MMDSFragmentNotify)
#include "messages/MMDSLoadTargets.h"
MESSAGE(MMDSLoadTargets)
#include "messages/MMDSMap.h"
MESSAGE(MMDSMap)
#include "messages/MMDSResolve.h"
MESSAGE(MMDSResolve)
#include "messages/MMDSResolveAck.h"
MESSAGE(MMDSResolveAck)
#include "messages/MMDSSlaveRequest.h"
MESSAGE(MMDSSlaveRequest)
#include "messages/MMDSTableRequest.h"
MESSAGE(MMDSTableRequest)
#include "messages/MMonCommand.h"
MESSAGE(MMonCommand)
#include "messages/MMonCommandAck.h"
MESSAGE(MMonCommandAck)
#include "messages/MMonElection.h"
MESSAGE(MMonElection)
#include "messages/MMonGetMap.h"
MESSAGE(MMonGetMap)
#include "messages/MMonGetVersion.h"
MESSAGE(MMonGetVersion)
#include "messages/MMonGetVersionReply.h"
MESSAGE(MMonGetVersionReply)
#include "messages/MMonGlobalID.h"
MESSAGE(MMonGlobalID)
#include "messages/MMonJoin.h"
MESSAGE(MMonJoin)
#include "messages/MMonMap.h"
MESSAGE(MMonMap)
#include "messages/MMonPaxos.h"
MESSAGE(MMonPaxos)
#include "messages/MMonProbe.h"
MESSAGE(MMonProbe)
#include "messages/MMonScrub.h"
MESSAGE(MMonScrub)
#include "messages/MMonSync.h"
MESSAGE(MMonSync)
#include "messages/MMonSubscribe.h"
MESSAGE(MMonSubscribe)
#include "messages/MMonSubscribeAck.h"
MESSAGE(MMonSubscribeAck)
#include "messages/MOSDAlive.h"
MESSAGE(MOSDAlive)
#include "messages/MOSDBoot.h"
MESSAGE(MOSDBoot)
#include "messages/MOSDFailure.h"
MESSAGE(MOSDFailure)
#include "messages/MOSDMap.h"
MESSAGE(MOSDMap)
#include "messages/MOSDOp.h"
MESSAGE(MOSDOp)
#include "messages/MOSDOpReply.h"
MESSAGE(MOSDOpReply)
#include "messages/MOSDPGBackfill.h"
MESSAGE(MOSDPGBackfill)
#include "messages/MOSDPGCreate.h"
MESSAGE(MOSDPGCreate)
#include "messages/MOSDPGInfo.h"
MESSAGE(MOSDPGInfo)
#include "messages/MOSDPGLog.h"
MESSAGE(MOSDPGLog)
#include "messages/MOSDPGMissing.h"
MESSAGE(MOSDPGMissing)
#include "messages/MOSDPGNotify.h"
MESSAGE(MOSDPGNotify)
#include "messages/MOSDPGQuery.h"
MESSAGE(MOSDPGQuery)
#include "messages/MOSDPGRemove.h"
MESSAGE(MOSDPGRemove)
#include "messages/MOSDPGScan.h"
MESSAGE(MOSDPGScan)
#include "messages/MOSDPGTemp.h"
MESSAGE(MOSDPGTemp)
#include "messages/MOSDPGTrim.h"
MESSAGE(MOSDPGTrim)
#include "messages/MOSDPing.h"
MESSAGE(MOSDPing)
#include "messages/MOSDRepScrub.h"
MESSAGE(MOSDRepScrub)
#include "messages/MOSDScrub.h"
MESSAGE(MOSDScrub)
#include "messages/MOSDSubOp.h"
MESSAGE(MOSDSubOp)
#include "messages/MOSDSubOpReply.h"
MESSAGE(MOSDSubOpReply)
#include "messages/MPGStats.h"
MESSAGE(MPGStats)
#include "messages/MPGStatsAck.h"
MESSAGE(MPGStatsAck)
#include "messages/MPing.h"
MESSAGE(MPing)
#include "messages/MPoolOp.h"
MESSAGE(MPoolOp)
#include "messages/MPoolOpReply.h"
MESSAGE(MPoolOpReply)
#include "messages/MRemoveSnaps.h"
MESSAGE(MRemoveSnaps)
#include "messages/MRoute.h"
MESSAGE(MRoute)
#include "messages/MStatfs.h"
MESSAGE(MStatfs)
#include "messages/MStatfsReply.h"
MESSAGE(MStatfsReply)
#include "messages/MWatchNotify.h"
MESSAGE(MWatchNotify)
