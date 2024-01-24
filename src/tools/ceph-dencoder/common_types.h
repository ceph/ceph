#include "ceph_time.h"
TYPE(real_time_wrapper)
TYPE(coarse_real_time_wrapper)
TYPE(timespan_wrapper)

#include "include/utime.h"
TYPE(utime_t)

#include "include/uuid.h"
TYPE(uuid_d)

#include "sstring.h"
TYPE(sstring_wrapper)

#include "str.h"
TYPE(string_wrapper)

#include "include/CompatSet.h"
TYPE(CompatSet)

#include "include/filepath.h"
TYPE(filepath)

#include "include/fs_types.h"
TYPE_FEATUREFUL(file_layout_t)
TYPE(inodeno_t)

#include "include/util.h"
TYPE(ceph_data_stats)

#include "include/object.h"
TYPE(object_t)
TYPE(sobject_t)

#include "include/frag.h"
TYPE(fragtree_t)
TYPE(frag_t)

#include "include/types.h"
TYPE(shard_id_t)
TYPE(inline_data_t)
TYPE(sha256_digest_t)
TYPE(errorcode32_t)
TYPE(client_t)

#include "common/bit_vector.hpp"
TYPE(BitVector<2>)
TYPE(ceph::BitVector<2>)

#include "common/bloom_filter.hpp"
TYPE(bloom_filter)
TYPE(compressible_bloom_filter)

#include "common/DecayCounter.h"
TYPE(DecayCounter)

#include "common/entity_name.h"
TYPE(EntityName)

#include "common/histogram.h"
TYPE(pow2_hist_t)

#include "common/hobject.h"
TYPE(hobject_t)
TYPE(ghobject_t)

#include "common/LogEntry.h"
TYPE_FEATUREFUL(LogEntry)
TYPE_FEATUREFUL(LogSummary)

#include "common/SloppyCRCMap.h"
TYPE(SloppyCRCMap)

#include "common/snap_types.h"
TYPE(SnapContext)
TYPE(SnapRealmInfo)

#include "msg/msg_types.h"
TYPE(entity_name_t)
TYPE_FEATUREFUL(entity_addr_t)
TYPE_FEATUREFUL(entity_addrvec_t)
TYPE_FEATUREFUL(entity_inst_t)

#include "crush/CrushWrapper.h"
TYPE_FEATUREFUL_NOCOPY(CrushWrapper)

#include "cls/cas/cls_cas_ops.h"
TYPE(cls_cas_chunk_create_or_get_ref_op)
TYPE(cls_cas_chunk_get_ref_op)
TYPE(cls_cas_chunk_put_ref_op)

#include "cls/cas/cls_cas_internal.h"
TYPE(chunk_refs_t)
TYPE(chunk_refs_count_t)
TYPE(chunk_refs_by_object_t)

#include "cls/lock/cls_lock_types.h"
TYPE(rados::cls::lock::locker_id_t)
TYPE_FEATUREFUL(rados::cls::lock::locker_info_t)
TYPE_FEATUREFUL(rados::cls::lock::lock_info_t)
using namespace rados::cls::lock;
TYPE(locker_id_t)
TYPE_FEATUREFUL(locker_info_t)
TYPE_FEATUREFUL(lock_info_t)

#include "cls/lock/cls_lock_ops.h"
TYPE(cls_lock_lock_op)
TYPE(cls_lock_unlock_op)
TYPE(cls_lock_break_op)
TYPE(cls_lock_get_info_op)
TYPE_FEATUREFUL(cls_lock_get_info_reply)
TYPE(cls_lock_list_locks_reply)
TYPE(cls_lock_assert_op)
TYPE(cls_lock_set_cookie_op)

#include "cls/refcount/cls_refcount_ops.h"
TYPE(cls_refcount_get_op)
TYPE(cls_refcount_put_op)
TYPE(cls_refcount_set_op)
TYPE(cls_refcount_read_op)
TYPE(cls_refcount_read_ret)
TYPE(obj_refcount)

#include "cls/timeindex/cls_timeindex_types.h"
TYPE(cls_timeindex_entry)

#include "cls/timeindex/cls_timeindex_ops.h"
TYPE(cls_timeindex_list_op)
TYPE(cls_timeindex_list_ret)

#include "cls/queue/cls_queue_types.h"
TYPE(cls_queue_entry)
TYPE(cls_queue_marker)
TYPE(cls_queue_head)

#include "cls/queue/cls_queue_ops.h"
TYPE(cls_queue_get_capacity_ret)
TYPE(cls_queue_remove_op)
TYPE(cls_queue_enqueue_op)
TYPE(cls_queue_list_op)
TYPE(cls_queue_list_ret)
TYPE(cls_queue_init_op)

#include "cls/2pc_queue/cls_2pc_queue_ops.h"
TYPE(cls_2pc_queue_abort_op)
TYPE(cls_2pc_queue_commit_op)
TYPE(cls_2pc_queue_expire_op)
TYPE_NONDETERMINISTIC(cls_2pc_queue_reservations_ret)
TYPE(cls_2pc_queue_reserve_op)
TYPE(cls_2pc_queue_reserve_ret)
TYPE(cls_queue_init_op)

#include "cls/2pc_queue/cls_2pc_queue_types.h"
TYPE(cls_2pc_reservation)
TYPE_NONDETERMINISTIC(cls_2pc_urgent_data)

#include "cls/log/cls_log_types.h"
TYPE(cls::log::header)

#include "cls/log/cls_log_ops.h"
TYPE(cls::log::ops::info_op)
TYPE(cls::log::ops::list_op)
TYPE(cls::log::ops::list_ret)
TYPE(cls::log::ops::trim_op)

#include "cls/version/cls_version_ops.h"
TYPE(cls_version_check_op)
TYPE(cls_version_read_ret)
TYPE(cls_version_inc_op)
TYPE(cls_version_set_op)


#include "cls/fifo/cls_fifo_ops.h"
using namespace rados::cls::fifo::op;
TYPE(create_meta)
TYPE(get_meta)
TYPE(get_meta_reply)

#include "cls/fifo/cls_fifo_types.h"
using namespace rados::cls::fifo;
TYPE(data_params)
TYPE(objv)
TYPE(info)

#include "journal/Entry.h"
TYPE(journal::Entry)

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
TYPE(openc_response_t)

#include "messages/MClientRequest.h"
MESSAGE(MClientRequest)
TYPE(SnapPayload)
TYPE(MClientRequest::Release)

#include "messages/MClientRequestForward.h"
MESSAGE(MClientRequestForward)

#include "messages/MClientQuota.h"
MESSAGE(MClientQuota)

#include "messages/MClientSession.h"
MESSAGE(MClientSession)

#include "messages/MClientSnap.h"
MESSAGE(MClientSnap)

#include "messages/MCommand.h"
MESSAGE(MCommand)

#include "messages/MCommandReply.h"
MESSAGE(MCommandReply)

#include "messages/MConfig.h"
MESSAGE(MConfig)

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

#include "messages/MFSMap.h"
MESSAGE(MFSMap)

#include "messages/MFSMapUser.h"
MESSAGE(MFSMapUser)

#include "messages/MGatherCaps.h"
MESSAGE(MGatherCaps)

#include "messages/MGenericMessage.h"
MESSAGE(MGenericMessage)

#include "messages/MGetConfig.h"
MESSAGE(MGetConfig)

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

#include "messages/MMDSOpenIno.h"
MESSAGE(MMDSOpenIno)

#include "messages/MMDSOpenInoReply.h"
MESSAGE(MMDSOpenInoReply)

#include "messages/MMDSBeacon.h"
MESSAGE(MMDSBeacon)

#include "messages/MMDSCacheRejoin.h"
MESSAGE(MMDSCacheRejoin)
TYPE(MMDSCacheRejoin::dirfrag_strong)
TYPE(MMDSCacheRejoin::dn_strong)
TYPE(MMDSCacheRejoin::inode_strong)

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

#include "messages/MMgrBeacon.h"
MESSAGE(MMgrBeacon)

#include "messages/MMgrReport.h"
MESSAGE(MMgrReport)
TYPE(PerfCounterType)

#include "messages/MMDSResolve.h"
MESSAGE(MMDSResolve)

#include "messages/MMDSResolveAck.h"
MESSAGE(MMDSResolveAck)

#include "messages/MMDSPeerRequest.h"
MESSAGE(MMDSPeerRequest)

#include "messages/MMDSSnapUpdate.h"
MESSAGE(MMDSSnapUpdate)

#include "messages/MMDSTableRequest.h"
MESSAGE(MMDSTableRequest)

#include "messages/MMgrClose.h"
MESSAGE(MMgrClose)

#include "messages/MMgrConfigure.h"
MESSAGE(MMgrConfigure)

#include "messages/MMgrDigest.h"
MESSAGE(MMgrDigest)

#include "messages/MMgrMap.h"
MESSAGE(MMgrMap)

#include "messages/MMgrOpen.h"
MESSAGE(MMgrOpen)

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

#include "messages/MMonHealth.h"
MESSAGE(MMonHealth)

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

#include "messages/MOSDPGCreate2.h"
MESSAGE(MOSDPGCreate2)

#include "messages/MOSDPGInfo.h"
MESSAGE(MOSDPGInfo)

#include "messages/MOSDPGLog.h"
MESSAGE(MOSDPGLog)

#include "messages/MOSDPGNotify.h"
MESSAGE(MOSDPGNotify)

#include "messages/MOSDPGQuery.h"
MESSAGE(MOSDPGQuery)

#include "messages/MOSDPGRemove.h"
MESSAGE(MOSDPGRemove)

#include "messages/MOSDPGRecoveryDelete.h"
MESSAGE(MOSDPGRecoveryDelete)

#include "messages/MOSDPGRecoveryDeleteReply.h"
MESSAGE(MOSDPGRecoveryDeleteReply)

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

#include "messages/MOSDScrub2.h"
MESSAGE(MOSDScrub2)

#include "messages/MOSDForceRecovery.h"
MESSAGE(MOSDForceRecovery)

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

#include "messages/MServiceMap.h"
MESSAGE(MServiceMap)

#include "messages/MStatfs.h"
MESSAGE(MStatfs)

#include "messages/MStatfsReply.h"
MESSAGE(MStatfsReply)

#include "messages/MTimeCheck.h"
MESSAGE(MTimeCheck)

#include "messages/MTimeCheck2.h"
MESSAGE(MTimeCheck2)

#include "messages/MWatchNotify.h"
MESSAGE(MWatchNotify)

#include "messages/MMgrUpdate.h" 
MESSAGE(MMgrUpdate)

#include "messages/MOSDECSubOpRead.h"
MESSAGE(MOSDECSubOpRead)

#include "messages/MOSDECSubOpReadReply.h"
MESSAGE(MOSDECSubOpReadReply)

#include "messages/MOSDECSubOpWrite.h"
MESSAGE(MOSDECSubOpWrite)

#include "messages/MOSDECSubOpWriteReply.h"
MESSAGE(MOSDECSubOpWriteReply)

#include "messages/MOSDMarkMeDown.h"
MESSAGE(MOSDMarkMeDown)

#include "messages/MOSDPGCreated.h"
MESSAGE(MOSDPGCreated)

#include "messages/MOSDPGPush.h"
MESSAGE(MOSDPGPush)

#include "messages/MOSDPGPushReply.h"
MESSAGE(MOSDPGPushReply)

#include "messages/MOSDPGUpdateLogMissing.h"
MESSAGE(MOSDPGUpdateLogMissing)

#include "messages/MOSDPGUpdateLogMissingReply.h"
MESSAGE(MOSDPGUpdateLogMissingReply)

#include "messages/MOSDRepOp.h"
MESSAGE(MOSDRepOp)

#include "messages/MOSDRepOpReply.h"
MESSAGE(MOSDRepOpReply)

#include "messages/MRecoveryReserve.h"
MESSAGE(MRecoveryReserve)


#include "auth/cephx/CephxProtocol.h"
TYPE(CephXAuthenticate)
TYPE(CephXAuthorize)
TYPE(CephXAuthorizeChallenge)
TYPE(CephXAuthorizeReply)
TYPE(CephXChallengeBlob)
TYPE(CephXRequestHeader)
TYPE(CephXResponseHeader)
TYPE(CephXServerChallenge)
TYPE(CephXServiceTicket)
TYPE(CephXServiceTicketInfo)
TYPE(CephXServiceTicketRequest)
TYPE(CephXTicketBlob)

#include "auth/cephx/CephxKeyServer.h"
TYPE(KeyServerData)
TYPE(KeyServerData::Incremental)

#include "auth/Auth.h"
TYPE(RotatingSecrets)
TYPE(ExpiringCryptoKey)
TYPE(AuthCapsInfo)
TYPE(AuthTicket)
TYPE(EntityAuth)

#include "auth/Crypto.h"
TYPE(CryptoKey)

#include "common/ceph_json.h"
TYPE(JSONFormattable)
