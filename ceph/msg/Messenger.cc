
#include <ext/rope>
#include "include/types.h"
#include "include/config.h"

#include "Message.h"
#include "Messenger.h"
#include "messages/MGenericMessage.h"

#include <cassert>
#include <iostream>
using namespace std;



#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"

#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDGetClusterAck.h"

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientFileCaps.h"
#include "messages/MClientInodeAuthUpdate.h"

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

#include "messages/MInodeUpdate.h"
#include "messages/MInodeExpire.h"
#include "messages/MDirExpire.h"
#include "messages/MCacheExpire.h"

#include "messages/MLock.h"


ostream& operator<<(ostream& out, Message& m)
{
  // some messages define << themselves
  if (m.get_type() == MSG_CLIENT_REQUEST) return out << *((MClientRequest*)&m);

  // generic
  return out << "message(type=" << m.get_type() << ")";
}



Message *
decode_message(msg_envelope_t& env, bufferlist& payload)
{
  int type;

  // make message
  Message *m = 0;
  switch(env.type) {

	// -- with payload --

  case MSG_PING:
	m = new MPing();
	break;
  case MSG_PING_ACK:
	m = new MPingAck();
	break;
  case MSG_FAILURE:
	m = new MFailure();
	break;
  case MSG_FAILURE_ACK:
	m = new MFailureAck();
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
  case MSG_OSD_GETCLUSTERACK:
	m = new MOSDGetClusterAck();
	break;

	// clients
  case MSG_CLIENT_MOUNT:
	m = new MClientMount();
	break;
  case MSG_CLIENT_MOUNTACK:
	m = new MClientMountAck();
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
  case MSG_CLIENT_INODEAUTHUPDATE:
	m = new MClientInodeAuthUpdate();
	break;

	// mds
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

  case MSG_MDS_INODEUPDATE:
	m = new MInodeUpdate();
	break;

  case MSG_MDS_INODEEXPIRE:
	m = new MInodeExpire();
	break;

  case MSG_MDS_DIREXPIRE:
	m = new MDirExpire();
	break;

  case MSG_MDS_LOCK:
	m = new MLock();
	break;


	// -- simple messages without payload --

  case MSG_MDS_SHUTDOWNSTART:
  case MSG_MDS_SHUTDOWNFINISH:
  case MSG_SHUTDOWN:
  case MSG_CLIENT_UNMOUNT:
  case MSG_OSD_GETCLUSTER:
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


