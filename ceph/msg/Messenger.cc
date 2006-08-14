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
#include "messages/MNSFailure.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"

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


