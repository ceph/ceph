
#include <cassert>
#include <iostream>
using namespace std;

#include "include/types.h"

#include "Message.h"

#include "messages/MGenericMessage.h"

/*
#include "messages/MNSConnect.h"
#include "messages/MNSConnectAck.h"
#include "messages/MNSRegister.h"
#include "messages/MNSRegisterAck.h"
#include "messages/MNSLookup.h"
#include "messages/MNSLookupReply.h"
#include "messages/MNSFailure.h"
*/

#include "messages/MMonPaxos.h"

#include "messages/MMonElectionAck.h"
#include "messages/MMonElectionPropose.h"
#include "messages/MMonElectionVictory.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
//#include "messages/MFailure.h"
//#include "messages/MFailureAck.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDUpdate.h"
#include "messages/MOSDUpdateReply.h"

#include "messages/MClientBoot.h"
#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientAuthUser.h"
#include "messages/MClientAuthUserAck.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientFileCaps.h"
#include "messages/MClientUpdate.h"
#include "messages/MClientUpdateReply.h"
#include "messages/MClientRenewal.h"

#include "messages/MMDSGetMap.h"
#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSImportMap.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSCacheRejoinAck.h"

#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDirWarning.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MHashReaddir.h"
#include "messages/MHashReaddirReply.h"

#include "messages/MHashDirDiscover.h"
#include "messages/MHashDirDiscoverAck.h"
#include "messages/MHashDirPrep.h"
#include "messages/MHashDirPrepAck.h"
#include "messages/MHashDir.h"
#include "messages/MHashDirAck.h"
#include "messages/MHashDirNotify.h"

#include "messages/MUnhashDirPrep.h"
#include "messages/MUnhashDirPrepAck.h"
#include "messages/MUnhashDir.h"
#include "messages/MUnhashDirAck.h"
#include "messages/MUnhashDirNotify.h"
#include "messages/MUnhashDirNotifyAck.h"

#include "messages/MRenameWarning.h"
#include "messages/MRenameNotify.h"
#include "messages/MRenameNotifyAck.h"
#include "messages/MRename.h"
#include "messages/MRenamePrep.h"
#include "messages/MRenameReq.h"
#include "messages/MRenameAck.h"
#include "messages/MDentryUnlink.h"

#include "messages/MHeartbeat.h"

#include "messages/MAnchorRequest.h"
#include "messages/MAnchorReply.h"
#include "messages/MInodeLink.h"
#include "messages/MInodeLinkAck.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MInodeExpire.h"
#include "messages/MDirExpire.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"

#include "config.h"
#undef  dout
#define dout(l)    if (l<=g_conf.debug) cout << "messenger: "
#define DEBUGLVL  10    // debug level of output







Message *
decode_message(msg_envelope_t& env, bufferlist& payload)
{
  // make message
  Message *m = 0;
  switch(env.type) {

    // -- with payload --

	/*
  case MSG_NS_CONNECT:
    m = new MNSConnect();
    break;
  case MSG_NS_CONNECTACK:
    m = new MNSConnectAck();
    break;
  case MSG_NS_REGISTER:
    m = new MNSRegister();
    break;
  case MSG_NS_REGISTERACK:
    m = new MNSRegisterAck();
    break;
  case MSG_NS_LOOKUP:
    m = new MNSLookup();
    break;
  case MSG_NS_LOOKUPREPLY:
    m = new MNSLookupReply();
    break;
  case MSG_NS_FAILURE:
    m = new MNSFailure();
    break;
	*/

  case MSG_MON_PAXOS:
    m = new MMonPaxos;
    break;

  case MSG_MON_ELECTION_PROPOSE:
    m = new MMonElectionPropose;
    break;
  case MSG_MON_ELECTION_ACK:
    m = new MMonElectionAck;
    break;
  case MSG_MON_ELECTION_VICTORY:
    m = new MMonElectionVictory;
    break;

  case MSG_PING:
    m = new MPing();
    break;
  case MSG_PING_ACK:
    m = new MPingAck();
    break;
	/*
  case MSG_FAILURE:
    m = new MFailure();
    break;
  case MSG_FAILURE_ACK:
    m = new MFailureAck();
    break;
	*/

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
  case MSG_OSD_OP:
    m = new MOSDOp();
    break;
  case MSG_OSD_OPREPLY:
    m = new MOSDOpReply();
    break;

  case MSG_OSD_MAP:
    m = new MOSDMap();
    break;
  case MSG_OSD_GETMAP:
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
  case MSG_OSD_UPDATE:
    m = new MOSDUpdate();
    break;
  case MSG_OSD_UPDATE_REPLY:
    m = new MOSDUpdateReply();
    break;

    // clients
  case MSG_CLIENT_BOOT:
    m = new MClientBoot();
    break;
  case MSG_CLIENT_MOUNT:
    m = new MClientMount();
    break;
  case MSG_CLIENT_MOUNTACK:
    m = new MClientMountAck();
    break;
  case MSG_CLIENT_AUTH_USER:
    m = new MClientAuthUser();
    break;
  case MSG_CLIENT_AUTH_USER_ACK:
    m = new MClientAuthUserAck();
    break;
  case MSG_CLIENT_REQUEST:
    m = new MClientRequest();
    break;
  case MSG_CLIENT_REPLY:
    m = new MClientReply();
    break;
  case MSG_CLIENT_FILECAPS:
    m = new MClientFileCaps();
    break;
  case MSG_CLIENT_UPDATE:
    m = new MClientUpdate();
    break;
  case MSG_CLIENT_UPDATE_REPLY:
    m = new MClientUpdateReply();
    break;
  case MSG_CLIENT_RENEWAL:
    m = new MClientRenewal();
    break;

    // mds
  case MSG_MDS_GETMAP:
	m = new MMDSGetMap();
	break;
  case MSG_MDS_MAP:
	m = new MMDSMap();
	break;
  case MSG_MDS_BEACON:
	m = new MMDSBeacon;
	break;
  case MSG_MDS_IMPORTMAP:
	m = new MMDSImportMap;
	break;
  case MSG_MDS_CACHEREJOIN:
	m = new MMDSCacheRejoin;
	break;
  case MSG_MDS_CACHEREJOINACK:
	m = new MMDSCacheRejoinAck;
	break;

  case MSG_MDS_DIRUPDATE:
    m = new MDirUpdate();
    break;

  case MSG_MDS_DISCOVER:
    m = new MDiscover();
    break;
  case MSG_MDS_DISCOVERREPLY:
    m = new MDiscoverReply();
    break;

  case MSG_MDS_EXPORTDIRDISCOVER:
    m = new MExportDirDiscover();
    break;
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    m = new MExportDirDiscoverAck();
    break;

  case MSG_MDS_EXPORTDIR:
    m = new MExportDir();
    break;

  case MSG_MDS_EXPORTDIRFINISH:
    m = new MExportDirFinish();
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
    m = new MExportDirWarning();
    break;


  case MSG_MDS_HASHREADDIR:
    m = new MHashReaddir();
    break;
  case MSG_MDS_HASHREADDIRREPLY:
    m = new MHashReaddirReply();
    break;
    
  case MSG_MDS_HASHDIRDISCOVER:
    m = new MHashDirDiscover();
    break;
  case MSG_MDS_HASHDIRDISCOVERACK:
    m = new MHashDirDiscoverAck();
    break;
  case MSG_MDS_HASHDIRPREP:
    m = new MHashDirPrep();
    break;
  case MSG_MDS_HASHDIRPREPACK:
    m = new MHashDirPrepAck();
    break;
  case MSG_MDS_HASHDIR:
    m = new MHashDir();
    break;
  case MSG_MDS_HASHDIRACK:
    m = new MHashDirAck();
    break;
  case MSG_MDS_HASHDIRNOTIFY:
    m = new MHashDirNotify();
    break;

  case MSG_MDS_UNHASHDIRPREP:
    m = new MUnhashDirPrep();
    break;
  case MSG_MDS_UNHASHDIRPREPACK:
    m = new MUnhashDirPrepAck();
    break;
  case MSG_MDS_UNHASHDIR:
    m = new MUnhashDir();
    break;
  case MSG_MDS_UNHASHDIRACK:
    m = new MUnhashDirAck();
    break;
  case MSG_MDS_UNHASHDIRNOTIFY:
    m = new MUnhashDirNotify();
    break;
  case MSG_MDS_UNHASHDIRNOTIFYACK:
    m = new MUnhashDirNotifyAck();
    break;

  case MSG_MDS_RENAMEWARNING:
    m = new MRenameWarning();
    break;
  case MSG_MDS_RENAMENOTIFY:
    m = new MRenameNotify();
    break;
  case MSG_MDS_RENAMENOTIFYACK:
    m = new MRenameNotifyAck();
    break;
  case MSG_MDS_RENAME:
    m = new MRename();
    break;
  case MSG_MDS_RENAMEPREP:
    m = new MRenamePrep();
    break;
  case MSG_MDS_RENAMEREQ:
    m = new MRenameReq();
    break;
  case MSG_MDS_RENAMEACK:
    m = new MRenameAck();
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

  case MSG_MDS_ANCHORREQUEST:
    m = new MAnchorRequest();
    break;
  case MSG_MDS_ANCHORREPLY:
    m = new MAnchorReply();
    break;

  case MSG_MDS_INODELINK:
    m = new MInodeLink();
    break;
  case MSG_MDS_INODELINKACK:
    m = new MInodeLinkAck();
    break;

	/*  case MSG_MDS_INODEUPDATE:
    m = new MInodeUpdate();
    break;
	*/

  case MSG_MDS_INODEEXPIRE:
    m = new MInodeExpire();
    break;

  case MSG_MDS_INODEFILECAPS:
    m = new MInodeFileCaps();
    break;

  case MSG_MDS_DIREXPIRE:
    m = new MDirExpire();
    break;

  case MSG_MDS_LOCK:
    m = new MLock();
    break;


    // -- simple messages without payload --

  case MSG_CLOSE:
  case MSG_NS_STARTED:
  case MSG_NS_UNREGISTER:
  case MSG_SHUTDOWN:
  case MSG_MDS_SHUTDOWNSTART:
  case MSG_MDS_SHUTDOWNFINISH:
  case MSG_CLIENT_UNMOUNT:
  case MSG_OSD_MKFS_ACK:
    m = new MGenericMessage(env.type);
    break;

  default:
    dout(1) << "can't decode unknown message type " << env.type << endl;
    assert(0);
  }
  
  // env
  m->set_envelope(env);

  // decode
  m->set_payload(payload);
  m->decode_payload();

  // done!
  return m;
}


