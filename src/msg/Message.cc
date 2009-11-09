// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <iostream>
using namespace std;

#include "include/types.h"

#include "Message.h"

#include "messages/MGenericMessage.h"

#include "messages/MPGStats.h"
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
#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"

#include "messages/MMonElection.h"
#include "messages/MLog.h"
#include "messages/MLogAck.h"

#include "messages/MPing.h"

#include "messages/MRoute.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDMap.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDScrub.h"

#include "messages/MRemoveSnaps.h"

#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"

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
#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSCacheRejoin.h"

#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

#include "messages/MMDSFragmentNotify.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDirWarning.h"
#include "messages/MExportDirWarningAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"


#include "messages/MDentryUnlink.h"

#include "messages/MHeartbeat.h"

#include "messages/MMDSTableRequest.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"

#include "messages/MClass.h"

#include "config.h"

#define DEBUGLVL  10    // debug level of output


void Message::encode()
{
  // encode and copy out of *m
  if (empty_payload())
    encode_payload();
  calc_front_crc();
  
  if (!g_conf.ms_nocrc)
    calc_data_crc();
  else
    footer.flags = (unsigned)footer.flags | CEPH_MSG_FOOTER_NOCRC;
}

Message *decode_message(ceph_msg_header& header, ceph_msg_footer& footer,
			bufferlist& front, bufferlist& middle, bufferlist& data)
{
  // verify crc
  if (!g_conf.ms_nocrc) {
    __u32 front_crc = front.crc32c(0);
    __u32 middle_crc = middle.crc32c(0);
    __u32 data_crc = data.crc32c(0);

    if (front_crc != footer.front_crc) {
      dout(0) << "bad crc in front " << front_crc << " != exp " << footer.front_crc << dendl;
      dout(20);
      front.hexdump(*_dout);
      *_dout << dendl;
      return 0;
    }
    if (middle_crc != footer.middle_crc) {
      dout(0) << "bad crc in middle " << middle_crc << " != exp " << footer.middle_crc << dendl;
      dout(20);
      middle.hexdump(*_dout);
      *_dout << dendl;
      return 0;
    }
    if (data_crc != footer.data_crc &&
        !(footer.flags & CEPH_MSG_FOOTER_NOCRC)) {
      dout(0) << "bad crc in data " << data_crc << " != exp " << footer.data_crc << dendl;
      dout(20);
      data.hexdump(*_dout);
      *_dout << dendl;
      return 0;
    }
  }

  // make message
  Message *m = 0;
  int type = header.type;
  switch (type) {

    // -- with payload --

  case MSG_PGSTATS:
    m = new MPGStats;
    break;
  case MSG_PGSTATSACK:
    m = new MPGStatsAck;
    break;

  case CEPH_MSG_STATFS:
    m = new MStatfs;
    break;
  case CEPH_MSG_STATFS_REPLY:
    m = new MStatfsReply;
    break;
  case MSG_GETPOOLSTATS:
    m = new MGetPoolStats;
    break;
  case MSG_GETPOOLSTATSREPLY:
    m = new MGetPoolStatsReply;
    break;
  case MSG_POOLOP:
    m = new MPoolOp;
    break;
  case MSG_POOLOPREPLY:
    m = new MPoolOpReply;
    break;
  case MSG_MON_COMMAND:
    m = new MMonCommand;
    break;
  case MSG_MON_COMMAND_ACK:
    m = new MMonCommandAck;
    break;
  case MSG_MON_PAXOS:
    m = new MMonPaxos;
    break;

  case MSG_MON_ELECTION:
    m = new MMonElection;
    break;

  case MSG_MON_OBSERVE:
    m = new MMonObserve;
    break;
  case MSG_MON_OBSERVE_NOTIFY:
    m = new MMonObserveNotify;
    break;
  case MSG_LOG:
    m = new MLog;
    break;
  case MSG_LOGACK:
    m = new MLogAck;
    break;

  case CEPH_MSG_PING:
    m = new MPing();
    break;
  case MSG_ROUTE:
    m = new MRoute;
    break;
    
  case CEPH_MSG_MON_MAP:
    m = new MMonMap;
    break;
  case CEPH_MSG_MON_GET_MAP:
    m = new MMonGetMap;
    break;

  case MSG_OSD_BOOT:
    m = new MOSDBoot();
    break;
  case MSG_OSD_ALIVE:
    m = new MOSDAlive();
    break;
  case MSG_OSD_PGTEMP:
    m = new MOSDPGTemp;
    break;
  case MSG_OSD_IN:
    m = new MOSDIn();
    break;
  case MSG_OSD_OUT:
    m = new MOSDOut();
    break;
  case MSG_OSD_FAILURE:
    m = new MOSDFailure();
    break;
  case MSG_OSD_PING:
    m = new MOSDPing();
    break;
  case CEPH_MSG_OSD_OP:
    m = new MOSDOp();
    break;
  case CEPH_MSG_OSD_OPREPLY:
    m = new MOSDOpReply();
    break;
  case MSG_OSD_SUBOP:
    m = new MOSDSubOp();
    break;
  case MSG_OSD_SUBOPREPLY:
    m = new MOSDSubOpReply();
    break;

  case CEPH_MSG_OSD_MAP:
    m = new MOSDMap;
    break;

  case MSG_OSD_PG_NOTIFY:
    m = new MOSDPGNotify;
    break;
  case MSG_OSD_PG_QUERY:
    m = new MOSDPGQuery;
    break;
  case MSG_OSD_PG_LOG:
    m = new MOSDPGLog;
    break;
  case MSG_OSD_PG_REMOVE:
    m = new MOSDPGRemove;
    break;
  case MSG_OSD_PG_INFO:
    m = new MOSDPGInfo;
    break;
  case MSG_OSD_PG_CREATE:
    m = new MOSDPGCreate;
    break;
  case MSG_OSD_PG_TRIM:
    m = new MOSDPGTrim;
    break;

  case MSG_OSD_SCRUB:
    m = new MOSDScrub;
    break;

  case MSG_REMOVE_SNAPS:
    m = new MRemoveSnaps;
    break;

   // auth
  case CEPH_MSG_AUTH:
    m = new MAuth;
    break;
  case CEPH_MSG_AUTH_REPLY:
    m = new MAuthReply;
    break;

  case MSG_MON_GLOBAL_ID:
    m = new MMonGlobalID;
    break; 

    // clients
  case CEPH_MSG_MON_SUBSCRIBE:
    m = new MMonSubscribe;
    break;
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    m = new MMonSubscribeAck;
    break;
  case CEPH_MSG_CLIENT_SESSION:
    m = new MClientSession;
    break;
  case CEPH_MSG_CLIENT_RECONNECT:
    m = new MClientReconnect;
    break;
  case CEPH_MSG_CLIENT_REQUEST:
    m = new MClientRequest;
    break;
  case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    m = new MClientRequestForward;
    break;
  case CEPH_MSG_CLIENT_REPLY:
    m = new MClientReply;
    break;
  case CEPH_MSG_CLIENT_CAPS:
    m = new MClientCaps;
    break;
  case CEPH_MSG_CLIENT_CAPRELEASE:
    m = new MClientCapRelease;
    break;
  case CEPH_MSG_CLIENT_LEASE:
    m = new MClientLease;
    break;
  case CEPH_MSG_CLIENT_SNAP:
    m = new MClientSnap;
    break;

    // mds
  case MSG_MDS_SLAVE_REQUEST:
    m = new MMDSSlaveRequest;
    break;

  case CEPH_MSG_MDS_MAP:
    m = new MMDSMap;
    break;
  case MSG_MDS_BEACON:
    m = new MMDSBeacon;
    break;
  case MSG_MDS_OFFLOAD_TARGETS:
    m = new MMDSLoadTargets;
    break;
  case MSG_MDS_RESOLVE:
    m = new MMDSResolve;
    break;
  case MSG_MDS_RESOLVEACK:
    m = new MMDSResolveAck;
    break;
  case MSG_MDS_CACHEREJOIN:
    m = new MMDSCacheRejoin;
	break;
	/*
  case MSG_MDS_CACHEREJOINACK:
	m = new MMDSCacheRejoinAck;
	break;
	*/

  case MSG_MDS_DIRUPDATE:
    m = new MDirUpdate();
    break;

  case MSG_MDS_DISCOVER:
    m = new MDiscover();
    break;
  case MSG_MDS_DISCOVERREPLY:
    m = new MDiscoverReply();
    break;

  case MSG_MDS_FRAGMENTNOTIFY:
    m = new MMDSFragmentNotify;
    break;

  case MSG_MDS_EXPORTDIRDISCOVER:
    m = new MExportDirDiscover();
    break;
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    m = new MExportDirDiscoverAck();
    break;
  case MSG_MDS_EXPORTDIRCANCEL:
    m = new MExportDirCancel();
    break;

  case MSG_MDS_EXPORTDIR:
    m = new MExportDir;
    break;
  case MSG_MDS_EXPORTDIRACK:
    m = new MExportDirAck;
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    m = new MExportDirFinish;
    break;

  case MSG_MDS_EXPORTDIRNOTIFY:
    m = new MExportDirNotify();
    break;

  case MSG_MDS_EXPORTDIRNOTIFYACK:
    m = new MExportDirNotifyAck();
    break;

  case MSG_MDS_EXPORTDIRPREP:
    m = new MExportDirPrep();
    break;

  case MSG_MDS_EXPORTDIRPREPACK:
    m = new MExportDirPrepAck();
    break;

  case MSG_MDS_EXPORTDIRWARNING:
    m = new MExportDirWarning;
    break;
  case MSG_MDS_EXPORTDIRWARNINGACK:
    m = new MExportDirWarningAck;
    break;

    
  case MSG_MDS_EXPORTCAPS:
    m = new MExportCaps;
    break;
  case MSG_MDS_EXPORTCAPSACK:
    m = new MExportCapsAck;
    break;


  case MSG_MDS_DENTRYUNLINK:
    m = new MDentryUnlink();
    break;

  case MSG_MDS_HEARTBEAT:
    m = new MHeartbeat();
    break;

  case MSG_MDS_CACHEEXPIRE:
    m = new MCacheExpire();
    break;

  case MSG_MDS_TABLE_REQUEST:
    m = new MMDSTableRequest;
    break;

	/*  case MSG_MDS_INODEUPDATE:
    m = new MInodeUpdate();
    break;
	*/

  case MSG_MDS_INODEFILECAPS:
    m = new MInodeFileCaps();
    break;

  case MSG_MDS_LOCK:
    m = new MLock();
    break;


    // -- simple messages without payload --

  case CEPH_MSG_SHUTDOWN:
    m = new MGenericMessage(type);
    break;

  case MSG_CLASS:
    m = new MClass();
    break;

  default:
    dout(0) << "can't decode unknown message type " << type << " MSG_AUTH=" << CEPH_MSG_AUTH << dendl;
    assert(0);
  }
  
  m->set_header(header);
  m->set_footer(footer);
  m->set_payload(front);
  m->set_middle(middle);
  m->set_data(data);

  try {
    m->decode_payload();
  }
  catch (buffer::error *e) {
    dout(0) << "failed to decode message of type " << type << ": " << *e << dendl;
    delete e;
    if (g_conf.ms_die_on_bad_msg)
      assert(0);
    return 0;
  }

  // done!
  return m;
}


void encode_message(Message *msg, bufferlist& payload)
{
  bufferlist front, middle, data;
  msg->encode();
  ::encode(msg->get_header(), payload);
  ::encode(msg->get_footer(), payload);
  ::encode(msg->get_payload(), payload);
  ::encode(msg->get_middle(), payload);
  ::encode(msg->get_data(), payload);
}

Message *decode_message(bufferlist::iterator& p)
{
  ceph_msg_header h;
  ceph_msg_footer f;
  bufferlist fr, mi, da;
  ::decode(h, p);
  ::decode(f, p);
  ::decode(fr, p);
  ::decode(mi, p);
  ::decode(da, p);
  return decode_message(h, f, fr, mi, da);
}
