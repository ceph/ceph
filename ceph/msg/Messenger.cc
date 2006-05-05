// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include <ext/rope>
#include "include/types.h"

#include "Message.h"
#include "Messenger.h"
#include "messages/MGenericMessage.h"

#include <cassert>
#include <iostream>
using namespace std;



#include "messages/MNSConnect.h"
#include "messages/MNSConnectAck.h"
#include "messages/MNSRegister.h"
#include "messages/MNSRegisterAck.h"
#include "messages/MNSLookup.h"
#include "messages/MNSLookupReply.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"

#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGSummary.h"
//#include "messages/MOSDPGUpdate.h"

//#include "messages/MOSDPGQuery.h"
//#include "messages/MOSDPGQueryReply.h"


#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientFileCaps.h"

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

#include "messages/MInodeUpdate.h"
#include "messages/MInodeExpire.h"
#include "messages/MDirExpire.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "messenger: "
#define DEBUGLVL  10    // debug level of output


ostream& operator<<(ostream& out, Message& m)
{
  // some messages define << themselves
  if (m.get_type() == MSG_CLIENT_REQUEST) return out << *((MClientRequest*)&m);

  // generic
  return out << "message(type=" << m.get_type() << ")";
}


// -------- 
// callbacks

Mutex                msgr_callback_lock;
list<Context*>       msgr_callback_queue;
//Context*             msgr_callback_kicker = 0;

void Messenger::queue_callback(Context *c) {
  msgr_callback_lock.Lock();
  msgr_callback_queue.push_back(c);
  msgr_callback_lock.Unlock();

  callback_kick();
}
void Messenger::queue_callbacks(list<Context*>& ls) {
  msgr_callback_lock.Lock();
  msgr_callback_queue.splice(msgr_callback_queue.end(), ls);
  msgr_callback_lock.Unlock();

  callback_kick();
}

void Messenger::do_callbacks() {
  // take list
  msgr_callback_lock.Lock();
  list<Context*> ls;
  ls.splice(ls.begin(), msgr_callback_queue);
  msgr_callback_lock.Unlock();

  // do them
  for (list<Context*>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	dout(10) << "--- doing callback " << *it << endl;
	(*it)->finish(0);
	delete *it;
  }
}

// ---------
// incoming messages


void Messenger::dispatch(Message *m) 
{
  assert(dispatcher);
  
  bump_lamport(m->get_lamport_send_stamp());
  m->set_lamport_recv_stamp(peek_lamport());

  long pcid = m->get_pcid();

  _lock.Lock();

  // was i expecting it?
  if (call_cond.count(pcid)) {
	// yes, this is a reply to a pending call.
	dout(DEBUGLVL) << "dispatch got reply for " << pcid << " " << m << endl;
	call_reply[pcid] = m;     // set reply
	call_cond[pcid]->Signal();

	// wait for sendrecv to wake up
	while (call_cond.count(pcid))
	  call_reply_finish_cond.Wait(_lock);

	_lock.Unlock();
  } else {
	// no, this is an unsolicited message.
	_lock.Unlock();	
	dout(DEBUGLVL) << "dispatch got unsolicited message pcid " << pcid << " m " << m << endl;
	dispatcher->dispatch(m);
  }
}



// ---------
// outgoing messages

Message *Messenger::sendrecv(Message *m, msg_addr_t dest, int port)
{
  int fromport = 0;

  Cond cond;

  // make up a pcid that is unique (to me!)
  /* NOTE: since request+replies are matched up on pcid's alone, it means that
	 two nodes using this mechanism can't do calls of each other or else their
	 pcid's might overlap.  
	 This should be fine. only the Client uses this so far (not MDS).
	 If OSDs want to use this, though, this must be made smarter!!!
  */
  long pcid = ++_last_pcid;
  m->set_pcid(pcid);

  _lock.Lock();

  dout(DEBUGLVL) << "sendrecv sending " << m << " on pcid " << pcid << endl;

  // add call records
  assert(call_cond.count(pcid) == 0);  // pcid should be UNIQUE
  call_cond[pcid] = &cond;
  call_reply[pcid] = 0;   // no reply yet

  // send.  drop locks in case send_message is bad and blocks
  _lock.Unlock();
  send_message(m, dest, port, fromport); 
  _lock.Lock();

  // wait?
  while (call_reply[pcid] == 0) {
	dout(DEBUGLVL) << "sendrecv waiting for reply on pcid " << pcid << endl;
	cond.Wait(_lock);
  } 

  // pick up reply
  Message *reply = call_reply[pcid];
  assert(reply);
  call_reply.erase(pcid);   // remove from call map
  call_cond.erase(pcid);

  dout(DEBUGLVL) << "sendrecv got reply " << reply << " on pcid " << pcid << endl;
  //delete sem;

  call_reply_finish_cond.Signal();
  _lock.Unlock();
  
  return reply;
}






Message *
decode_message(msg_envelope_t& env, bufferlist& payload)
{
  // make message
  Message *m = 0;
  switch(env.type) {

	// -- with payload --

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
  case MSG_OSD_MAP:
	m = new MOSDMap();
	break;
  case MSG_OSD_PG_NOTIFY:
	m = new MOSDPGNotify();
	break;
  case MSG_OSD_PG_QUERY:
	m = new MOSDPGQuery();
	break;
  case MSG_OSD_PG_SUMMARY:
	m = new MOSDPGSummary();
	break;
	/*
  case MSG_OSD_PG_QUERY:
	m = new MOSDPGQuery();
	break;
  case MSG_OSD_PG_QUERYREPLY:
	m = new MOSDPGQueryReply();
	break;
	*/
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

  case MSG_MDS_INODEUPDATE:
	m = new MInodeUpdate();
	break;

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

  case MSG_NS_STARTED:
  case MSG_NS_UNREGISTER:
  case MSG_SHUTDOWN:
  case MSG_MDS_SHUTDOWNSTART:
  case MSG_MDS_SHUTDOWNFINISH:
  case MSG_CLIENT_UNMOUNT:
  case MSG_OSD_GETMAP:
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


