// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifdef ENCODE_DUMP
# include <typeinfo>
# include <cxxabi.h>
#endif

#include <iostream>

#include "include/types.h"

#include "global/global_context.h"

#include "Message.h"

#include "messages/MPGStats.h"

#include "messages/MGenericMessage.h"

#include "messages/MPGStatsAck.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"


#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonPaxos.h"
#include "messages/MConfig.h"
#include "messages/MGetConfig.h"
#include "messages/MKVData.h"

#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonElection.h"
#include "messages/MMonSync.h"
#include "messages/MMonPing.h"
#include "messages/MMonScrub.h"

#include "messages/MLog.h"
#include "messages/MLogAck.h"

#include "messages/MPing.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"

#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDBeacon.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMarkMeDead.h"
#include "messages/MOSDFull.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"
#include "messages/MMonGetPurgedSnaps.h"
#include "messages/MMonGetPurgedSnapsReply.h"

#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGNotify2.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGQuery2.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGInfo2.h"
#include "messages/MOSDPGCreate2.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGLease.h"
#include "messages/MOSDPGLeaseAck.h"
#include "messages/MOSDScrub2.h"
#include "messages/MOSDScrubReserve.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDForceRecovery.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDBackoff.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"
#include "messages/MOSDPGReadyToMerge.h"

#include "messages/MRemoveSnaps.h"

#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MMonHealth.h"
#include "messages/MMonHealthChecks.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "messages/MMonGlobalID.h"
#include "messages/MMonUsedPendingKeys.h"
#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientReclaimReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"
#include "messages/MClientQuota.h"
#include "messages/MClientMetrics.h"

#include "messages/MMDSPeerRequest.h"
#include "messages/MMDSQuiesceDbListing.h"
#include "messages/MMDSQuiesceDbAck.h"

#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"
#include "messages/MFSMapUser.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSFindIno.h"
#include "messages/MMDSFindInoReply.h"
#include "messages/MMDSOpenIno.h"
#include "messages/MMDSOpenInoReply.h"
#include "messages/MMDSSnapUpdate.h"
#include "messages/MMDSScrub.h"
#include "messages/MMDSScrubStats.h"

#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

#include "messages/MMDSFragmentNotify.h"
#include "messages/MMDSFragmentNotifyAck.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"
#include "messages/MGatherCaps.h"


#include "messages/MDentryUnlink.h"
#include "messages/MDentryLink.h"

#include "messages/MHeartbeat.h"

#include "messages/MMDSTableRequest.h"
#include "messages/MMDSMetrics.h"
#include "messages/MMDSPing.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrDigest.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrUpdate.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMonMgrReport.h"
#include "messages/MMgrCommand.h"
#include "messages/MMgrCommandReply.h"
#include "messages/MServiceMap.h"

#include "messages/MLock.h"

#include "messages/MWatchNotify.h"
#include "messages/MTimeCheck.h"
#include "messages/MTimeCheck2.h"

#include "common/config.h"

#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGPull.h"

#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"

#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"

#ifdef WITH_BLKIN
#include "Messenger.h"
#endif

#define DEBUGLVL  10    // debug level of output

#define dout_subsys ceph_subsys_ms

void Message::encode(uint64_t features, int crcflags, bool skip_header_crc)
{
  // encode and copy out of *m
  if (empty_payload()) {
    ceph_assert(middle.length() == 0);
    encode_payload(features);

    if (byte_throttler) {
      byte_throttler->take(payload.length() + middle.length());
    }

    // if the encoder didn't specify past compatibility, we assume it
    // is incompatible.
    if (header.compat_version == 0)
      header.compat_version = header.version;
  }
  if (crcflags & MSG_CRC_HEADER)
    calc_front_crc();

  // update envelope
  header.front_len = get_payload().length();
  header.middle_len = get_middle().length();
  header.data_len = get_data().length();
  if (!skip_header_crc && (crcflags & MSG_CRC_HEADER))
    calc_header_crc();

  footer.flags = CEPH_MSG_FOOTER_COMPLETE;

  if (crcflags & MSG_CRC_DATA) {
    calc_data_crc();

#ifdef ENCODE_DUMP
    bufferlist bl;
    encode(get_header(), bl);

    // dump the old footer format
    ceph_msg_footer_old old_footer;
    old_footer.front_crc = footer.front_crc;
    old_footer.middle_crc = footer.middle_crc;
    old_footer.data_crc = footer.data_crc;
    old_footer.flags = footer.flags;
    encode(old_footer, bl);

    encode(get_payload(), bl);
    encode(get_middle(), bl);
    encode(get_data(), bl);

    // this is almost an exponential backoff, except because we count
    // bits we tend to sample things we encode later, which should be
    // more representative.
    static int i = 0;
    i++;
    int bits = 0;
    for (unsigned t = i; t; bits++)
      t &= t - 1;
    if (bits <= 2) {
      char fn[200];
      int status;
      snprintf(fn, sizeof(fn), ENCODE_STRINGIFY(ENCODE_DUMP) "/%s__%d.%x",
	       abi::__cxa_demangle(typeid(*this).name(), 0, 0, &status),
	       getpid(), i++);
      int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT|O_CLOEXEC|O_BINARY, 0644);
      if (fd >= 0) {
	bl.write_fd(fd);
	::close(fd);
      }
    }
#endif
  } else {
    footer.flags = (unsigned)footer.flags | CEPH_MSG_FOOTER_NOCRC;
  }
}

void Message::dump(ceph::Formatter *f) const
{
  std::stringstream ss;
  print(ss);
  f->dump_string("summary", ss.str());
}

Message *decode_message(CephContext *cct,
                        int crcflags,
                        ceph_msg_header& header,
                        ceph_msg_footer& footer,
                        ceph::bufferlist& front,
                        ceph::bufferlist& middle,
                        ceph::bufferlist& data,
                        Message::ConnectionRef conn)
{
#ifdef WITH_SEASTAR
  // In crimson, conn is independently maintained outside Message.
  ceph_assert(conn == nullptr);
#endif
  // verify crc
  if (crcflags & MSG_CRC_HEADER) {
    __u32 front_crc = front.crc32c(0);
    __u32 middle_crc = middle.crc32c(0);

    if (front_crc != footer.front_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in front " << front_crc << " != exp " << footer.front_crc
#ifndef WITH_SEASTAR
	              << " from " << conn->get_peer_addr()
#endif
	              << dendl;
	ldout(cct, 20) << " ";
	front.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }
    if (middle_crc != footer.middle_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in middle " << middle_crc << " != exp " << footer.middle_crc
#ifndef WITH_SEASTAR
	              << " from " << conn->get_peer_addr()
#endif
	              << dendl;
	ldout(cct, 20) << " ";
	middle.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }
  }
  if (crcflags & MSG_CRC_DATA) {
    if ((footer.flags & CEPH_MSG_FOOTER_NOCRC) == 0) {
      __u32 data_crc = data.crc32c(0);
      if (data_crc != footer.data_crc) {
	if (cct) {
	  ldout(cct, 0) << "bad crc in data " << data_crc << " != exp " << footer.data_crc
#ifndef WITH_SEASTAR
	                << " from " << conn->get_peer_addr()
#endif
	                << dendl;
	  ldout(cct, 20) << " ";
	  data.hexdump(*_dout);
	  *_dout << dendl;
	}
	return 0;
      }
    }
  }

  // make message
  ceph::ref_t<Message> m;
  int type = header.type;
  switch (type) {

    // -- with payload --

    using ceph::make_message;

  case MSG_PGSTATS:
    m = make_message<MPGStats>();
    break;
  case MSG_PGSTATSACK:
    m = make_message<MPGStatsAck>();
    break;

  case CEPH_MSG_STATFS:
    m = make_message<MStatfs>();
    break;
  case CEPH_MSG_STATFS_REPLY:
    m = make_message<MStatfsReply>();
    break;
  case MSG_GETPOOLSTATS:
    m = make_message<MGetPoolStats>();
    break;
  case MSG_GETPOOLSTATSREPLY:
    m = make_message<MGetPoolStatsReply>();
    break;
  case CEPH_MSG_POOLOP:
    m = make_message<MPoolOp>();
    break;
  case CEPH_MSG_POOLOP_REPLY:
    m = make_message<MPoolOpReply>();
    break;
  case MSG_MON_COMMAND:
    m = make_message<MMonCommand>();
    break;
  case MSG_MON_COMMAND_ACK:
    m = make_message<MMonCommandAck>();
    break;
  case MSG_MON_PAXOS:
    m = make_message<MMonPaxos>();
    break;
  case MSG_CONFIG:
    m = make_message<MConfig>();
    break;
  case MSG_GET_CONFIG:
    m = make_message<MGetConfig>();
    break;
  case MSG_KV_DATA:
    m = make_message<MKVData>();
    break;

  case MSG_MON_PROBE:
    m = make_message<MMonProbe>();
    break;
  case MSG_MON_JOIN:
    m = make_message<MMonJoin>();
    break;
  case MSG_MON_ELECTION:
    m = make_message<MMonElection>();
    break;
  case MSG_MON_SYNC:
    m = make_message<MMonSync>();
    break;
  case MSG_MON_PING:
    m = make_message<MMonPing>();
    break;
  case MSG_MON_SCRUB:
    m = make_message<MMonScrub>();
    break;

  case MSG_LOG:
    m = make_message<MLog>();
    break;
  case MSG_LOGACK:
    m = make_message<MLogAck>();
    break;

  case CEPH_MSG_PING:
    m = make_message<MPing>();
    break;
  case MSG_COMMAND:
    m = make_message<MCommand>();
    break;
  case MSG_COMMAND_REPLY:
    m = make_message<MCommandReply>();
    break;
  case MSG_OSD_BACKFILL_RESERVE:
    m = make_message<MBackfillReserve>();
    break;
  case MSG_OSD_RECOVERY_RESERVE:
    m = make_message<MRecoveryReserve>();
    break;
  case MSG_OSD_FORCE_RECOVERY:
    m = make_message<MOSDForceRecovery>();
    break;

  case MSG_ROUTE:
    m = make_message<MRoute>();
    break;
  case MSG_FORWARD:
    m = make_message<MForward>();
    break;
    
  case CEPH_MSG_MON_MAP:
    m = make_message<MMonMap>();
    break;
  case CEPH_MSG_MON_GET_MAP:
    m = make_message<MMonGetMap>();
    break;
  case CEPH_MSG_MON_GET_OSDMAP:
    m = make_message<MMonGetOSDMap>();
    break;
  case MSG_MON_GET_PURGED_SNAPS:
    m = make_message<MMonGetPurgedSnaps>();
    break;
  case MSG_MON_GET_PURGED_SNAPS_REPLY:
    m = make_message<MMonGetPurgedSnapsReply>();
    break;
  case CEPH_MSG_MON_GET_VERSION:
    m = make_message<MMonGetVersion>();
    break;
  case CEPH_MSG_MON_GET_VERSION_REPLY:
    m = make_message<MMonGetVersionReply>();
    break;

  case MSG_OSD_BOOT:
    m = make_message<MOSDBoot>();
    break;
  case MSG_OSD_ALIVE:
    m = make_message<MOSDAlive>();
    break;
  case MSG_OSD_BEACON:
    m = make_message<MOSDBeacon>();
    break;
  case MSG_OSD_PGTEMP:
    m = make_message<MOSDPGTemp>();
    break;
  case MSG_OSD_FAILURE:
    m = make_message<MOSDFailure>();
    break;
  case MSG_OSD_MARK_ME_DOWN:
    m = make_message<MOSDMarkMeDown>();
    break;
  case MSG_OSD_MARK_ME_DEAD:
    m = make_message<MOSDMarkMeDead>();
    break;
  case MSG_OSD_FULL:
    m = make_message<MOSDFull>();
    break;
  case MSG_OSD_PING:
    m = make_message<MOSDPing>();
    break;
  case CEPH_MSG_OSD_OP:
    m = make_message<MOSDOp>();
    break;
  case CEPH_MSG_OSD_OPREPLY:
    m = make_message<MOSDOpReply>();
    break;
  case MSG_OSD_REPOP:
    m = make_message<MOSDRepOp>();
    break;
  case MSG_OSD_REPOPREPLY:
    m = make_message<MOSDRepOpReply>();
    break;
  case MSG_OSD_PG_CREATED:
    m = make_message<MOSDPGCreated>();
    break;
  case MSG_OSD_PG_UPDATE_LOG_MISSING:
    m = make_message<MOSDPGUpdateLogMissing>();
    break;
  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    m = make_message<MOSDPGUpdateLogMissingReply>();
    break;
  case CEPH_MSG_OSD_BACKOFF:
    m = make_message<MOSDBackoff>();
    break;

  case CEPH_MSG_OSD_MAP:
    m = make_message<MOSDMap>();
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    m = make_message<MWatchNotify>();
    break;

  case MSG_OSD_PG_NOTIFY:
    m = make_message<MOSDPGNotify>();
    break;
  case MSG_OSD_PG_NOTIFY2:
    m = make_message<MOSDPGNotify2>();
    break;
  case MSG_OSD_PG_QUERY:
    m = make_message<MOSDPGQuery>();
    break;
  case MSG_OSD_PG_QUERY2:
    m = make_message<MOSDPGQuery2>();
    break;
  case MSG_OSD_PG_LOG:
    m = make_message<MOSDPGLog>();
    break;
  case MSG_OSD_PG_REMOVE:
    m = make_message<MOSDPGRemove>();
    break;
  case MSG_OSD_PG_INFO:
    m = make_message<MOSDPGInfo>();
    break;
  case MSG_OSD_PG_INFO2:
    m = make_message<MOSDPGInfo2>();
    break;
  case MSG_OSD_PG_CREATE2:
    m = make_message<MOSDPGCreate2>();
    break;
  case MSG_OSD_PG_TRIM:
    m = make_message<MOSDPGTrim>();
    break;
  case MSG_OSD_PG_LEASE:
    m = make_message<MOSDPGLease>();
    break;
  case MSG_OSD_PG_LEASE_ACK:
    m = make_message<MOSDPGLeaseAck>();
    break;

  case MSG_OSD_SCRUB2:
    m = make_message<MOSDScrub2>();
    break;
  case MSG_OSD_SCRUB_RESERVE:
    m = make_message<MOSDScrubReserve>();
    break;
  case MSG_REMOVE_SNAPS:
    m = make_message<MRemoveSnaps>();
    break;
  case MSG_OSD_REP_SCRUB:
    m = make_message<MOSDRepScrub>();
    break;
  case MSG_OSD_REP_SCRUBMAP:
    m = make_message<MOSDRepScrubMap>();
    break;
  case MSG_OSD_PG_SCAN:
    m = make_message<MOSDPGScan>();
    break;
  case MSG_OSD_PG_BACKFILL:
    m = make_message<MOSDPGBackfill>();
    break;
  case MSG_OSD_PG_BACKFILL_REMOVE:
    m = make_message<MOSDPGBackfillRemove>();
    break;
  case MSG_OSD_PG_PUSH:
    m = make_message<MOSDPGPush>();
    break;
  case MSG_OSD_PG_PULL:
    m = make_message<MOSDPGPull>();
    break;
  case MSG_OSD_PG_PUSH_REPLY:
    m = make_message<MOSDPGPushReply>();
    break;
  case MSG_OSD_PG_RECOVERY_DELETE:
    m = make_message<MOSDPGRecoveryDelete>();
    break;
  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    m = make_message<MOSDPGRecoveryDeleteReply>();
    break;
  case MSG_OSD_PG_READY_TO_MERGE:
    m = make_message<MOSDPGReadyToMerge>();
    break;
  case MSG_OSD_EC_WRITE:
    m = make_message<MOSDECSubOpWrite>();
    break;
  case MSG_OSD_EC_WRITE_REPLY:
    m = make_message<MOSDECSubOpWriteReply>();
    break;
  case MSG_OSD_EC_READ:
    m = make_message<MOSDECSubOpRead>();
    break;
  case MSG_OSD_EC_READ_REPLY:
    m = make_message<MOSDECSubOpReadReply>();
    break;
   // auth
  case CEPH_MSG_AUTH:
    m = make_message<MAuth>();
    break;
  case CEPH_MSG_AUTH_REPLY:
    m = make_message<MAuthReply>();
    break;

  case MSG_MON_GLOBAL_ID:
    m = make_message<MMonGlobalID>();
    break; 
  case MSG_MON_USED_PENDING_KEYS:
    m = make_message<MMonUsedPendingKeys>();
    break; 

    // clients
  case CEPH_MSG_MON_SUBSCRIBE:
    m = make_message<MMonSubscribe>();
    break;
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    m = make_message<MMonSubscribeAck>();
    break;
  case CEPH_MSG_CLIENT_SESSION:
    m = make_message<MClientSession>();
    break;
  case CEPH_MSG_CLIENT_RECONNECT:
    m = make_message<MClientReconnect>();
    break;
  case CEPH_MSG_CLIENT_REQUEST:
    m = make_message<MClientRequest>();
    break;
  case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    m = make_message<MClientRequestForward>();
    break;
  case CEPH_MSG_CLIENT_REPLY:
    m = make_message<MClientReply>();
    break;
  case CEPH_MSG_CLIENT_RECLAIM:
    m = make_message<MClientReclaim>();
    break;
  case CEPH_MSG_CLIENT_RECLAIM_REPLY:
    m = make_message<MClientReclaimReply>();
    break;
  case CEPH_MSG_CLIENT_CAPS:
    m = make_message<MClientCaps>();
    break;
  case CEPH_MSG_CLIENT_CAPRELEASE:
    m = make_message<MClientCapRelease>();
    break;
  case CEPH_MSG_CLIENT_LEASE:
    m = make_message<MClientLease>();
    break;
  case CEPH_MSG_CLIENT_SNAP:
    m = make_message<MClientSnap>();
    break;
  case CEPH_MSG_CLIENT_QUOTA:
    m = make_message<MClientQuota>();
    break;
  case CEPH_MSG_CLIENT_METRICS:
    m = make_message<MClientMetrics>();
    break;

    // mds
  case MSG_MDS_PEER_REQUEST:
    m = make_message<MMDSPeerRequest>();
    break;

  case CEPH_MSG_MDS_MAP:
    m = make_message<MMDSMap>();
    break;
  case CEPH_MSG_FS_MAP:
    m = make_message<MFSMap>();
    break;
  case CEPH_MSG_FS_MAP_USER:
    m = make_message<MFSMapUser>();
    break;
  case MSG_MDS_BEACON:
    m = make_message<MMDSBeacon>();
    break;
  case MSG_MDS_OFFLOAD_TARGETS:
    m = make_message<MMDSLoadTargets>();
    break;
  case MSG_MDS_RESOLVE:
    m = make_message<MMDSResolve>();
    break;
  case MSG_MDS_RESOLVEACK:
    m = make_message<MMDSResolveAck>();
    break;
  case MSG_MDS_CACHEREJOIN:
    m = make_message<MMDSCacheRejoin>();
	break;
  
  case MSG_MDS_DIRUPDATE:
    m = make_message<MDirUpdate>();
    break;

  case MSG_MDS_DISCOVER:
    m = make_message<MDiscover>();
    break;
  case MSG_MDS_DISCOVERREPLY:
    m = make_message<MDiscoverReply>();
    break;

  case MSG_MDS_FINDINO:
    m = make_message<MMDSFindIno>();
    break;
  case MSG_MDS_FINDINOREPLY:
    m = make_message<MMDSFindInoReply>();
    break;

  case MSG_MDS_OPENINO:
    m = make_message<MMDSOpenIno>();
    break;
  case MSG_MDS_OPENINOREPLY:
    m = make_message<MMDSOpenInoReply>();
    break;

  case MSG_MDS_SNAPUPDATE:
    m = make_message<MMDSSnapUpdate>();
    break;

  case MSG_MDS_FRAGMENTNOTIFY:
    m = make_message<MMDSFragmentNotify>();
    break;

  case MSG_MDS_FRAGMENTNOTIFYACK:
    m = make_message<MMDSFragmentNotifyAck>();
    break;

  case MSG_MDS_SCRUB:
    m = make_message<MMDSScrub>();
    break;

  case MSG_MDS_SCRUB_STATS:
    m = make_message<MMDSScrubStats>();
    break;

  case MSG_MDS_EXPORTDIRDISCOVER:
    m = make_message<MExportDirDiscover>();
    break;
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    m = make_message<MExportDirDiscoverAck>();
    break;
  case MSG_MDS_EXPORTDIRCANCEL:
    m = make_message<MExportDirCancel>();
    break;

  case MSG_MDS_EXPORTDIR:
    m = make_message<MExportDir>();
    break;
  case MSG_MDS_EXPORTDIRACK:
    m = make_message<MExportDirAck>();
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    m = make_message<MExportDirFinish>();
    break;

  case MSG_MDS_EXPORTDIRNOTIFY:
    m = make_message<MExportDirNotify>();
    break;

  case MSG_MDS_EXPORTDIRNOTIFYACK:
    m = make_message<MExportDirNotifyAck>();
    break;

  case MSG_MDS_EXPORTDIRPREP:
    m = make_message<MExportDirPrep>();
    break;

  case MSG_MDS_EXPORTDIRPREPACK:
    m = make_message<MExportDirPrepAck>();
    break;

  case MSG_MDS_EXPORTCAPS:
    m = make_message<MExportCaps>();
    break;
  case MSG_MDS_EXPORTCAPSACK:
    m = make_message<MExportCapsAck>();
    break;
  case MSG_MDS_GATHERCAPS:
    m = make_message<MGatherCaps>();
    break;


  case MSG_MDS_DENTRYUNLINK:
    m = make_message<MDentryUnlink>();
    break;
  case MSG_MDS_DENTRYLINK:
    m = make_message<MDentryLink>();
    break;

  case MSG_MDS_HEARTBEAT:
    m = make_message<MHeartbeat>();
    break;

  case MSG_MDS_CACHEEXPIRE:
    m = make_message<MCacheExpire>();
    break;

  case MSG_MDS_TABLE_REQUEST:
    m = make_message<MMDSTableRequest>();
    break;

  case MSG_MDS_QUIESCE_DB_LISTING:
    m = make_message<MMDSQuiesceDbListing>();
    break;

  case MSG_MDS_QUIESCE_DB_ACK:
    m = make_message<MMDSQuiesceDbAck>();
    break;

	/*  case MSG_MDS_INODEUPDATE:
    m = make_message<MInodeUpdate>();
    break;
	*/

  case MSG_MDS_INODEFILECAPS:
    m = make_message<MInodeFileCaps>();
    break;

  case MSG_MDS_LOCK:
    m = make_message<MLock>();
    break;

  case MSG_MDS_METRICS:
    m = make_message<MMDSMetrics>();
    break;

  case MSG_MDS_PING:
    m = make_message<MMDSPing>();
    break;

  case MSG_MGR_BEACON:
    m = make_message<MMgrBeacon>();
    break;

  case MSG_MON_MGR_REPORT:
    m = make_message<MMonMgrReport>();
    break;

  case MSG_SERVICE_MAP:
    m = make_message<MServiceMap>();
    break;

  case MSG_MGR_MAP:
    m = make_message<MMgrMap>();
    break;

  case MSG_MGR_DIGEST:
    m = make_message<MMgrDigest>();
    break;

  case MSG_MGR_COMMAND:
    m = make_message<MMgrCommand>();
    break;

  case MSG_MGR_COMMAND_REPLY:
    m = make_message<MMgrCommandReply>();
    break;

  case MSG_MGR_OPEN:
    m = make_message<MMgrOpen>();
    break;

  case MSG_MGR_UPDATE:
    m = make_message<MMgrUpdate>();
    break;

  case MSG_MGR_CLOSE:
    m = make_message<MMgrClose>();
    break;

  case MSG_MGR_REPORT:
    m = make_message<MMgrReport>();
    break;

  case MSG_MGR_CONFIGURE:
    m = make_message<MMgrConfigure>();
    break;

  case MSG_TIMECHECK:
    m = make_message<MTimeCheck>();
    break;
  case MSG_TIMECHECK2:
    m = make_message<MTimeCheck2>();
    break;

  case MSG_MON_HEALTH:
    m = make_message<MMonHealth>();
    break;

  case MSG_MON_HEALTH_CHECKS:
    m = make_message<MMonHealthChecks>();
    break;

    // -- simple messages without payload --

  case CEPH_MSG_SHUTDOWN:
    m = make_message<MGenericMessage>(type);
    break;

  default:
    if (cct) {
      ldout(cct, 0) << "can't decode unknown message type " << type << " MSG_AUTH=" << CEPH_MSG_AUTH << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	ceph_abort();
    }
    return 0;
  }

  m->set_cct(cct);

  // m->header.version, if non-zero, should be populated with the
  // newest version of the encoding the code supports.  If set, check
  // it against compat_version.
  if (m->get_header().version &&
      m->get_header().version < header.compat_version) {
    if (cct) {
      ldout(cct, 0) << "will not decode message of type " << type
		    << " version " << header.version
		    << " because compat_version " << header.compat_version
		    << " > supported version " << m->get_header().version << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	ceph_abort();
    }
    return 0;
  }

  m->set_connection(std::move(conn));
  m->set_header(header);
  m->set_footer(footer);
  m->set_payload(front);
  m->set_middle(middle);
  m->set_data(data);

  try {
    m->decode_payload();
  }
  catch (const ceph::buffer::error &e) {
    if (cct) {
      lderr(cct) << "failed to decode message of type " << type
		 << " v" << header.version
		 << ": " << e.what() << dendl;
      ldout(cct, ceph::dout::need_dynamic(
	cct->_conf->ms_dump_corrupt_message_level)) << "dump: \n";
      m->get_payload().hexdump(*_dout);
      *_dout << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	ceph_abort();
    }
    return 0;
  }

  // done!
  return m.detach();
}

void Message::encode_trace(ceph::bufferlist &bl, uint64_t features) const
{
  using ceph::encode;
  auto p = trace.get_info();
  static const blkin_trace_info empty = { 0, 0, 0 };
  if (!p) {
    p = &empty;
  }
  encode(*p, bl);
}

void Message::decode_trace(ceph::bufferlist::const_iterator &p, bool create)
{
  blkin_trace_info info = {};
  decode(info, p);

#ifdef WITH_BLKIN
  if (!connection)
    return;

  const auto msgr = connection->get_messenger();
  const auto endpoint = msgr->get_trace_endpoint();
  if (info.trace_id) {
    trace.init(get_type_name().data(), endpoint, &info, true);
    trace.event("decoded trace");
  } else if (create || (msgr->get_myname().is_osd() &&
                        msgr->cct->_conf->osd_blkin_trace_all)) {
    // create a trace even if we didn't get one on the wire
    trace.init(get_type_name().data(), endpoint);
    trace.event("created trace");
  }
  trace.keyval("tid", get_tid());
  trace.keyval("entity type", get_source().type_str());
  trace.keyval("entity num", get_source().num());
#endif
}


// This routine is not used for ordinary messages, but only when encapsulating a message
// for forwarding and routing.  It's also used in a backward compatibility test, which only
// effectively tests backward compability for those functions.  To avoid backward compatibility
// problems, we currently always encode and decode using the old footer format that doesn't
// allow for message authentication.  Eventually we should fix that.  PLR

void encode_message(Message *msg, uint64_t features, ceph::bufferlist& payload)
{
  ceph_msg_footer_old old_footer;
  msg->encode(features, MSG_CRC_ALL);
  encode(msg->get_header(), payload);

  // Here's where we switch to the old footer format.  PLR
  ceph_msg_footer footer = msg->get_footer();
  old_footer.front_crc = footer.front_crc;   
  old_footer.middle_crc = footer.middle_crc;   
  old_footer.data_crc = footer.data_crc;   
  old_footer.flags = footer.flags;   
  encode(old_footer, payload);

  using ceph::encode;
  encode(msg->get_payload(), payload);
  encode(msg->get_middle(), payload);
  encode(msg->get_data(), payload);
}

// See above for somewhat bogus use of the old message footer.  We switch to the current footer
// after decoding the old one so the other form of decode_message() doesn't have to change.
// We've slipped in a 0 signature at this point, so any signature checking after this will
// fail.  PLR

Message *decode_message(CephContext *cct, int crcflags, ceph::bufferlist::const_iterator& p)
{
  ceph_msg_header h;
  ceph_msg_footer_old fo;
  ceph_msg_footer f;
  ceph::bufferlist fr, mi, da;
  decode(h, p);
  decode(fo, p);
  f.front_crc = fo.front_crc;
  f.middle_crc = fo.middle_crc;
  f.data_crc = fo.data_crc;
  f.flags = fo.flags;
  f.sig = 0;
  using ceph::decode;
  decode(fr, p);
  decode(mi, p);
  decode(da, p);
  return decode_message(cct, crcflags, h, f, fr, mi, da, nullptr);
}
