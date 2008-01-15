// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <cassert>
#include <iostream>
using namespace std;

#include "include/types.h"

#include "Message.h"

#include "messages/MGenericMessage.h"

#include "messages/MPGStats.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonPaxos.h"

#include "messages/MMonElection.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGActivateSet.h"

#include "messages/MMonMap.h"

#include "messages/MClientMount.h"
#include "messages/MClientUnmount.h"
#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientFileCaps.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MMDSGetMap.h"
#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"
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

#include "messages/MAnchor.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"

#include "config.h"

#define dout(l)    if (l<=g_conf.debug) *_dout << dbeginl << g_clock.now() << " MESSENGER: "
#define DEBUGLVL  10    // debug level of output







Message *
decode_message(ceph_msg_header& env, bufferlist& front, bufferlist& data)
{
  // make message
  Message *m = 0;
  switch(env.type) {

    // -- with payload --

  case MSG_PGSTATS:
    m = new MPGStats;
    break;

  case CEPH_MSG_STATFS:
    m = new MStatfs;
    break;
  case CEPH_MSG_STATFS_REPLY:
    m = new MStatfsReply;
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

  case CEPH_MSG_PING:
    m = new MPing();
    break;
  case CEPH_MSG_PING_ACK:
    m = new MPingAck();
    break;
    
  case CEPH_MSG_MON_MAP:
    m = new MMonMap;
    break;

  case MSG_OSD_BOOT:
    m = new MOSDBoot();
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
    m = new MOSDMap();
    break;
  case CEPH_MSG_OSD_GETMAP:
    m = new MOSDGetMap();
    break;

  case MSG_OSD_PG_NOTIFY:
    m = new MOSDPGNotify();
    break;
  case MSG_OSD_PG_QUERY:
    m = new MOSDPGQuery();
    break;
  case MSG_OSD_PG_LOG:
    m = new MOSDPGLog();
    break;
  case MSG_OSD_PG_REMOVE:
    m = new MOSDPGRemove();
    break;
  case MSG_OSD_PG_ACTIVATE_SET:
    m = new MOSDPGActivateSet();
    break;


    // clients
  case CEPH_MSG_CLIENT_MOUNT:
    m = new MClientMount;
    break;
  case CEPH_MSG_CLIENT_UNMOUNT:
    m = new MClientUnmount;
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
  case CEPH_MSG_CLIENT_FILECAPS:
    m = new MClientFileCaps;
    break;

    // mds
  case MSG_MDS_SLAVE_REQUEST:
    m = new MMDSSlaveRequest;
    break;

  case CEPH_MSG_MDS_GETMAP:
	m = new MMDSGetMap();
	break;
  case CEPH_MSG_MDS_MAP:
	m = new MMDSMap();
	break;
  case MSG_MDS_BEACON:
	m = new MMDSBeacon;
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

  case MSG_MDS_ANCHOR:
    m = new MAnchor();
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
    m = new MGenericMessage(env.type);
    break;

  default:
    dout(0) << "can't decode unknown message type " << env.type << dendl;
    assert(0);
  }
  
  m->set_env(env);
  m->set_payload(front);
  m->set_data(data);

  m->decode_payload();

  // done!
  return m;
}


