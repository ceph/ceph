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

#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonElection.h"
#include "messages/MMonSync.h"
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
#include "messages/MOSDFull.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"

#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGCreate2.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDScrub.h"
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
#include "messages/MMonMetadata.h"
#include "messages/MDataPing.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "messages/MMonGlobalID.h"
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

#include "messages/MMDSSlaveRequest.h"

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

//#include "messages/MInodeUpdate.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrDigest.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMonMgrReport.h"
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

#define DEBUGLVL  10    // debug level of output

#define dout_subsys ceph_subsys_ms

void Message::encode(uint64_t features, int crcflags)
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
  if (crcflags & MSG_CRC_HEADER)
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
      int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT|O_CLOEXEC, 0644);
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

void Message::dump(Formatter *f) const
{
  stringstream ss;
  print(ss);
  f->dump_string("summary", ss.str());
}

Message *decode_message(CephContext *cct, int crcflags,
			ceph_msg_header& header,
			ceph_msg_footer& footer,
			bufferlist& front, bufferlist& middle,
			bufferlist& data, Connection* conn)
{
  // verify crc
  if (crcflags & MSG_CRC_HEADER) {
    __u32 front_crc = front.crc32c(0);
    __u32 middle_crc = middle.crc32c(0);

    if (front_crc != footer.front_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in front " << front_crc << " != exp " << footer.front_crc << dendl;
	ldout(cct, 20) << " ";
	front.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }
    if (middle_crc != footer.middle_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in middle " << middle_crc << " != exp " << footer.middle_crc << dendl;
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
	  ldout(cct, 0) << "bad crc in data " << data_crc << " != exp " << footer.data_crc << dendl;
	  ldout(cct, 20) << " ";
	  data.hexdump(*_dout);
	  *_dout << dendl;
	}
	return 0;
      }
    }
  }

  // make message
  Message::ref m;
  int type = header.type;
  switch (type) {

    // -- with payload --

  case MSG_PGSTATS:
    m = MPGStats::create();
    break;
  case MSG_PGSTATSACK:
    m = MPGStatsAck::create();
    break;

  case CEPH_MSG_STATFS:
    m = MStatfs::create();
    break;
  case CEPH_MSG_STATFS_REPLY:
    m = MStatfsReply::create();
    break;
  case MSG_GETPOOLSTATS:
    m = MGetPoolStats::create();
    break;
  case MSG_GETPOOLSTATSREPLY:
    m = MGetPoolStatsReply::create();
    break;
  case CEPH_MSG_POOLOP:
    m = MPoolOp::create();
    break;
  case CEPH_MSG_POOLOP_REPLY:
    m = MPoolOpReply::create();
    break;
  case MSG_MON_COMMAND:
    m = MMonCommand::create();
    break;
  case MSG_MON_COMMAND_ACK:
    m = MMonCommandAck::create();
    break;
  case MSG_MON_PAXOS:
    m = MMonPaxos::create();
    break;
  case MSG_CONFIG:
    m = MConfig::create();
    break;
  case MSG_GET_CONFIG:
    m = MGetConfig::create();
    break;

  case MSG_MON_PROBE:
    m = MMonProbe::create();
    break;
  case MSG_MON_JOIN:
    m = MMonJoin::create();
    break;
  case MSG_MON_ELECTION:
    m = MMonElection::create();
    break;
  case MSG_MON_SYNC:
    m = MMonSync::create();
    break;
  case MSG_MON_SCRUB:
    m = MMonScrub::create();
    break;

  case MSG_LOG:
    m = MLog::create();
    break;
  case MSG_LOGACK:
    m = MLogAck::create();
    break;

  case CEPH_MSG_PING:
    m = MPing::create();
    break;
  case MSG_COMMAND:
    m = MCommand::create();
    break;
  case MSG_COMMAND_REPLY:
    m = MCommandReply::create();
    break;
  case MSG_OSD_BACKFILL_RESERVE:
    m = MBackfillReserve::create();
    break;
  case MSG_OSD_RECOVERY_RESERVE:
    m = MRecoveryReserve::create();
    break;
  case MSG_OSD_FORCE_RECOVERY:
    m = MOSDForceRecovery::create();
    break;

  case MSG_ROUTE:
    m = MRoute::create();
    break;
  case MSG_FORWARD:
    m = MForward::create();
    break;
    
  case CEPH_MSG_MON_MAP:
    m = MMonMap::create();
    break;
  case CEPH_MSG_MON_GET_MAP:
    m = MMonGetMap::create();
    break;
  case CEPH_MSG_MON_GET_OSDMAP:
    m = MMonGetOSDMap::create();
    break;
  case CEPH_MSG_MON_GET_VERSION:
    m = MMonGetVersion::create();
    break;
  case CEPH_MSG_MON_GET_VERSION_REPLY:
    m = MMonGetVersionReply::create();
    break;
  case CEPH_MSG_MON_METADATA:
    m = MMonMetadata::create();
    break;

  case MSG_OSD_BOOT:
    m = MOSDBoot::create();
    break;
  case MSG_OSD_ALIVE:
    m = MOSDAlive::create();
    break;
  case MSG_OSD_BEACON:
    m = MOSDBeacon::create();
    break;
  case MSG_OSD_PGTEMP:
    m = MOSDPGTemp::create();
    break;
  case MSG_OSD_FAILURE:
    m = MOSDFailure::create();
    break;
  case MSG_OSD_MARK_ME_DOWN:
    m = MOSDMarkMeDown::create();
    break;
  case MSG_OSD_FULL:
    m = MOSDFull::create();
    break;
  case MSG_OSD_PING:
    m = MOSDPing::create();
    break;
  case CEPH_MSG_OSD_OP:
    m = MOSDOp::create();
    break;
  case CEPH_MSG_OSD_OPREPLY:
    m = MOSDOpReply::create();
    break;
  case MSG_OSD_REPOP:
    m = MOSDRepOp::create();
    break;
  case MSG_OSD_REPOPREPLY:
    m = MOSDRepOpReply::create();
    break;
  case MSG_OSD_PG_CREATED:
    m = MOSDPGCreated::create();
    break;
  case MSG_OSD_PG_UPDATE_LOG_MISSING:
    m = MOSDPGUpdateLogMissing::create();
    break;
  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    m = MOSDPGUpdateLogMissingReply::create();
    break;
  case CEPH_MSG_OSD_BACKOFF:
    m = MOSDBackoff::create();
    break;

  case CEPH_MSG_OSD_MAP:
    m = MOSDMap::create();
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    m = MWatchNotify::create();
    break;

  case MSG_OSD_PG_NOTIFY:
    m = MOSDPGNotify::create();
    break;
  case MSG_OSD_PG_QUERY:
    m = MOSDPGQuery::create();
    break;
  case MSG_OSD_PG_LOG:
    m = MOSDPGLog::create();
    break;
  case MSG_OSD_PG_REMOVE:
    m = MOSDPGRemove::create();
    break;
  case MSG_OSD_PG_INFO:
    m = MOSDPGInfo::create();
    break;
  case MSG_OSD_PG_CREATE:
    m = MOSDPGCreate::create();
    break;
  case MSG_OSD_PG_CREATE2:
    m = MOSDPGCreate2::create();
    break;
  case MSG_OSD_PG_TRIM:
    m = MOSDPGTrim::create();
    break;

  case MSG_OSD_SCRUB:
    m = MOSDScrub::create();
    break;
  case MSG_OSD_SCRUB2:
    m = MOSDScrub2::create();
    break;
  case MSG_OSD_SCRUB_RESERVE:
    m = MOSDScrubReserve::create();
    break;
  case MSG_REMOVE_SNAPS:
    m = MRemoveSnaps::create();
    break;
  case MSG_OSD_REP_SCRUB:
    m = MOSDRepScrub::create();
    break;
  case MSG_OSD_REP_SCRUBMAP:
    m = MOSDRepScrubMap::create();
    break;
  case MSG_OSD_PG_SCAN:
    m = MOSDPGScan::create();
    break;
  case MSG_OSD_PG_BACKFILL:
    m = MOSDPGBackfill::create();
    break;
  case MSG_OSD_PG_BACKFILL_REMOVE:
    m = MOSDPGBackfillRemove::create();
    break;
  case MSG_OSD_PG_PUSH:
    m = MOSDPGPush::create();
    break;
  case MSG_OSD_PG_PULL:
    m = MOSDPGPull::create();
    break;
  case MSG_OSD_PG_PUSH_REPLY:
    m = MOSDPGPushReply::create();
    break;
  case MSG_OSD_PG_RECOVERY_DELETE:
    m = MOSDPGRecoveryDelete::create();
    break;
  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    m = MOSDPGRecoveryDeleteReply::create();
    break;
  case MSG_OSD_PG_READY_TO_MERGE:
    m = MOSDPGReadyToMerge::create();
    break;
  case MSG_OSD_EC_WRITE:
    m = MOSDECSubOpWrite::create();
    break;
  case MSG_OSD_EC_WRITE_REPLY:
    m = MOSDECSubOpWriteReply::create();
    break;
  case MSG_OSD_EC_READ:
    m = MOSDECSubOpRead::create();
    break;
  case MSG_OSD_EC_READ_REPLY:
    m = MOSDECSubOpReadReply::create();
    break;
   // auth
  case CEPH_MSG_AUTH:
    m = MAuth::create();
    break;
  case CEPH_MSG_AUTH_REPLY:
    m = MAuthReply::create();
    break;

  case MSG_MON_GLOBAL_ID:
    m = MMonGlobalID::create();
    break; 

    // clients
  case CEPH_MSG_MON_SUBSCRIBE:
    m = MMonSubscribe::create();
    break;
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    m = MMonSubscribeAck::create();
    break;
  case CEPH_MSG_CLIENT_SESSION:
    m = MClientSession::create();
    break;
  case CEPH_MSG_CLIENT_RECONNECT:
    m = MClientReconnect::create();
    break;
  case CEPH_MSG_CLIENT_REQUEST:
    m = MClientRequest::create();
    break;
  case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    m = MClientRequestForward::create();
    break;
  case CEPH_MSG_CLIENT_REPLY:
    m = MClientReply::create();
    break;
  case CEPH_MSG_CLIENT_RECLAIM:
    m = MClientReclaim::create();
    break;
  case CEPH_MSG_CLIENT_RECLAIM_REPLY:
    m = MClientReclaimReply::create();
    break;
  case CEPH_MSG_CLIENT_CAPS:
    m = MClientCaps::create();
    break;
  case CEPH_MSG_CLIENT_CAPRELEASE:
    m = MClientCapRelease::create();
    break;
  case CEPH_MSG_CLIENT_LEASE:
    m = MClientLease::create();
    break;
  case CEPH_MSG_CLIENT_SNAP:
    m = MClientSnap::create();
    break;
  case CEPH_MSG_CLIENT_QUOTA:
    m = MClientQuota::create();
    break;

    // mds
  case MSG_MDS_SLAVE_REQUEST:
    m = MMDSSlaveRequest::create();
    break;

  case CEPH_MSG_MDS_MAP:
    m = MMDSMap::create();
    break;
  case CEPH_MSG_FS_MAP:
    m = MFSMap::create();
    break;
  case CEPH_MSG_FS_MAP_USER:
    m = MFSMapUser::create();
    break;
  case MSG_MDS_BEACON:
    m = MMDSBeacon::create();
    break;
  case MSG_MDS_OFFLOAD_TARGETS:
    m = MMDSLoadTargets::create();
    break;
  case MSG_MDS_RESOLVE:
    m = MMDSResolve::create();
    break;
  case MSG_MDS_RESOLVEACK:
    m = MMDSResolveAck::create();
    break;
  case MSG_MDS_CACHEREJOIN:
    m = MMDSCacheRejoin::create();
	break;
  
  case MSG_MDS_DIRUPDATE:
    m = MDirUpdate::create();
    break;

  case MSG_MDS_DISCOVER:
    m = MDiscover::create();
    break;
  case MSG_MDS_DISCOVERREPLY:
    m = MDiscoverReply::create();
    break;

  case MSG_MDS_FINDINO:
    m = MMDSFindIno::create();
    break;
  case MSG_MDS_FINDINOREPLY:
    m = MMDSFindInoReply::create();
    break;

  case MSG_MDS_OPENINO:
    m = MMDSOpenIno::create();
    break;
  case MSG_MDS_OPENINOREPLY:
    m = MMDSOpenInoReply::create();
    break;

  case MSG_MDS_SNAPUPDATE:
    m = MMDSSnapUpdate::create();
    break;

  case MSG_MDS_FRAGMENTNOTIFY:
    m = MMDSFragmentNotify::create();
    break;

  case MSG_MDS_FRAGMENTNOTIFYACK:
    m = MMDSFragmentNotifyAck::create();
    break;

  case MSG_MDS_EXPORTDIRDISCOVER:
    m = MExportDirDiscover::create();
    break;
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    m = MExportDirDiscoverAck::create();
    break;
  case MSG_MDS_EXPORTDIRCANCEL:
    m = MExportDirCancel::create();
    break;

  case MSG_MDS_EXPORTDIR:
    m = MExportDir::create();
    break;
  case MSG_MDS_EXPORTDIRACK:
    m = MExportDirAck::create();
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    m = MExportDirFinish::create();
    break;

  case MSG_MDS_EXPORTDIRNOTIFY:
    m = MExportDirNotify::create();
    break;

  case MSG_MDS_EXPORTDIRNOTIFYACK:
    m = MExportDirNotifyAck::create();
    break;

  case MSG_MDS_EXPORTDIRPREP:
    m = MExportDirPrep::create();
    break;

  case MSG_MDS_EXPORTDIRPREPACK:
    m = MExportDirPrepAck::create();
    break;

  case MSG_MDS_EXPORTCAPS:
    m = MExportCaps::create();
    break;
  case MSG_MDS_EXPORTCAPSACK:
    m = MExportCapsAck::create();
    break;
  case MSG_MDS_GATHERCAPS:
    m = MGatherCaps::create();
    break;


  case MSG_MDS_DENTRYUNLINK:
    m = MDentryUnlink::create();
    break;
  case MSG_MDS_DENTRYLINK:
    m = MDentryLink::create();
    break;

  case MSG_MDS_HEARTBEAT:
    m = MHeartbeat::create();
    break;

  case MSG_MDS_CACHEEXPIRE:
    m = MCacheExpire::create();
    break;

  case MSG_MDS_TABLE_REQUEST:
    m = MMDSTableRequest::create();
    break;

	/*  case MSG_MDS_INODEUPDATE:
    m = MInodeUpdate::create();
    break;
	*/

  case MSG_MDS_INODEFILECAPS:
    m = MInodeFileCaps::create();
    break;

  case MSG_MDS_LOCK:
    m = MLock::create();
    break;

  case MSG_MGR_BEACON:
    m = MMgrBeacon::create();
    break;

  case MSG_MON_MGR_REPORT:
    m = MMonMgrReport::create();
    break;

  case MSG_SERVICE_MAP:
    m = MServiceMap::create();
    break;

  case MSG_MGR_MAP:
    m = MMgrMap::create();
    break;

  case MSG_MGR_DIGEST:
    m = MMgrDigest::create();
    break;

  case MSG_MGR_OPEN:
    m = MMgrOpen::create();
    break;

  case MSG_MGR_CLOSE:
    m = MMgrClose::create();
    break;

  case MSG_MGR_REPORT:
    m = MMgrReport::create();
    break;

  case MSG_MGR_CONFIGURE:
    m = MMgrConfigure::create();
    break;

  case MSG_TIMECHECK:
    m = MTimeCheck::create();
    break;
  case MSG_TIMECHECK2:
    m = MTimeCheck2::create();
    break;

  case MSG_MON_HEALTH:
    m = MMonHealth::create();
    break;

  case MSG_MON_HEALTH_CHECKS:
    m = MMonHealthChecks::create();
    break;

#if defined(HAVE_XIO)
  case MSG_DATA_PING:
    m = MDataPing::create();
    break;
#endif
    // -- simple messages without payload --

  case CEPH_MSG_SHUTDOWN:
    m = MGenericMessage::create(type);
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

  m->set_connection(conn);
  m->set_header(header);
  m->set_footer(footer);
  m->set_payload(front);
  m->set_middle(middle);
  m->set_data(data);

  try {
    m->decode_payload();
  }
  catch (const buffer::error &e) {
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

void Message::encode_trace(bufferlist &bl, uint64_t features) const
{
  using ceph::encode;
  auto p = trace.get_info();
  static const blkin_trace_info empty = { 0, 0, 0 };
  if (!p) {
    p = &empty;
  }
  encode(*p, bl);
}

void Message::decode_trace(bufferlist::const_iterator &p, bool create)
{
  blkin_trace_info info = {};
  decode(info, p);

#ifdef WITH_BLKIN
  if (!connection)
    return;

  const auto msgr = connection->get_messenger();
  const auto endpoint = msgr->get_trace_endpoint();
  if (info.trace_id) {
    trace.init(get_type_name(), endpoint, &info, true);
    trace.event("decoded trace");
  } else if (create || (msgr->get_myname().is_osd() &&
                        msgr->cct->_conf->osd_blkin_trace_all)) {
    // create a trace even if we didn't get one on the wire
    trace.init(get_type_name(), endpoint);
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

void encode_message(Message *msg, uint64_t features, bufferlist& payload)
{
  bufferlist front, middle, data;
  ceph_msg_footer_old old_footer;
  ceph_msg_footer footer;
  msg->encode(features, MSG_CRC_ALL);
  encode(msg->get_header(), payload);

  // Here's where we switch to the old footer format.  PLR

  footer = msg->get_footer();
  old_footer.front_crc = footer.front_crc;   
  old_footer.middle_crc = footer.middle_crc;   
  old_footer.data_crc = footer.data_crc;   
  old_footer.flags = footer.flags;   
  encode(old_footer, payload);

  encode(msg->get_payload(), payload);
  encode(msg->get_middle(), payload);
  encode(msg->get_data(), payload);
}

// See above for somewhat bogus use of the old message footer.  We switch to the current footer
// after decoding the old one so the other form of decode_message() doesn't have to change.
// We've slipped in a 0 signature at this point, so any signature checking after this will
// fail.  PLR

Message *decode_message(CephContext *cct, int crcflags, bufferlist::const_iterator& p)
{
  ceph_msg_header h;
  ceph_msg_footer_old fo;
  ceph_msg_footer f;
  bufferlist fr, mi, da;
  decode(h, p);
  decode(fo, p);
  f.front_crc = fo.front_crc;
  f.middle_crc = fo.middle_crc;
  f.data_crc = fo.data_crc;
  f.flags = fo.flags;
  f.sig = 0;
  decode(fr, p);
  decode(mi, p);
  decode(da, p);
  return decode_message(cct, crcflags, h, f, fr, mi, da, nullptr);
}
