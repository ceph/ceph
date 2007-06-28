// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Migrator.h"
#include "MDBalancer.h"
#include "AnchorClient.h"

#include "msg/Messenger.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MLock.h"

#include "messages/MDentryUnlink.h"

#include "events/EString.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"

#include "include/filepath.h"
#include "common/Timer.h"
#include "common/Logger.h"
#include "common/LogType.h"

#include <errno.h>
#include <fcntl.h>

#include <list>
#include <iostream>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".server "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".server "


void Server::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case MSG_CLIENT_RECONNECT:
    handle_client_reconnect((MClientReconnect*)m);
    return;
  }

  // active?
  if (!mds->is_active()) {
    dout(3) << "not active yet, waiting" << endl;
    mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
    return;
  }

  switch (m->get_type()) {
  case MSG_CLIENT_SESSION:
    handle_client_session((MClientSession*)m);
    return;
  case MSG_CLIENT_REQUEST:
    handle_client_request((MClientRequest*)m);
    return;
  case MSG_MDS_SLAVE_REQUEST:
    handle_slave_request((MMDSSlaveRequest*)m);
    return;
  }

  dout(1) << "server unknown message " << m->get_type() << endl;
  assert(0);
}



// ----------------------------------------------------------
// SESSION management


class C_MDS_session_finish : public Context {
  MDS *mds;
  entity_inst_t client_inst;
  bool open;
  version_t cmapv;
public:
  C_MDS_session_finish(MDS *m, entity_inst_t ci, bool s, version_t mv) :
    mds(m), client_inst(ci), open(s), cmapv(mv) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_session_logged(client_inst, open, cmapv);
  }
};


void Server::handle_client_session(MClientSession *m)
{
  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << endl;
  int from = m->get_source().num();
  bool open = m->op == MClientSession::OP_OPEN;

  if (open) {
    if (mds->clientmap.is_opening(from)) {
      dout(10) << "already opening, dropping this req" << endl;
      delete m;
      return;
    }
    mds->clientmap.add_opening(from);
  } else {
    if (mds->clientmap.is_closing(from)) {
      dout(10) << "already closing, dropping this req" << endl;
      delete m;
      return;
    }
    mds->clientmap.add_closing(from);
  }

  // journal it
  version_t cmapv = mds->clientmap.inc_projected();
  mdlog->submit_entry(new ESession(m->get_source_inst(), open, cmapv),
		      new C_MDS_session_finish(mds, m->get_source_inst(), open, cmapv));
  delete m;
}

void Server::_session_logged(entity_inst_t client_inst, bool open, version_t cmapv)
{
  dout(10) << "_session_logged " << client_inst << " " << (open ? "open":"close")
	   << " " << cmapv 
	   << endl;

  // apply
  int from = client_inst.name.num();
  if (open) {
    assert(mds->clientmap.is_opening(from));
    mds->clientmap.open_session(client_inst);
  } else {
    assert(mds->clientmap.is_closing(from));
    mds->clientmap.close_session(from);
    
    // purge completed requests from clientmap
    mds->clientmap.trim_completed_requests(from, 0);
  }
  
  assert(cmapv == mds->clientmap.get_version());
  
  // reply
  if (open) 
    mds->messenger->send_message(new MClientSession(MClientSession::OP_OPEN_ACK), client_inst);
  else
    mds->messenger->send_message(new MClientSession(MClientSession::OP_CLOSE_ACK), client_inst);
}


void Server::terminate_sessions()
{
  dout(2) << "terminate_sessions" << endl;

  // kill them off.  clients will retry etc.
  for (set<int>::const_iterator p = mds->clientmap.get_session_set().begin();
       p != mds->clientmap.get_session_set().end();
       ++p) {
    if (mds->clientmap.is_closing(*p)) 
      continue;
    mds->clientmap.add_closing(*p);
    version_t cmapv = mds->clientmap.inc_projected();
    mdlog->submit_entry(new ESession(mds->clientmap.get_inst(*p), false, cmapv),
			new C_MDS_session_finish(mds, mds->clientmap.get_inst(*p), false, cmapv));
  }
}


void Server::reconnect_clients()
{
  // reconnect with clients
  if (mds->clientmap.get_session_set().empty()) {
    dout(7) << "reconnect_clients -- no sessions, doing nothing." << endl;
    reconnect_finish();
    return;
  }
  
  dout(7) << "reconnect_clients -- sending mdsmap to clients with sessions" << endl;
  mds->set_want_state(MDSMap::STATE_RECONNECT);     // just fyi.
  
  // send mdsmap to all mounted clients
  mds->bcast_mds_map();

  // init gather list
  reconnect_start = g_clock.now();
  client_reconnect_gather = mds->clientmap.get_session_set();
}

void Server::handle_client_reconnect(MClientReconnect *m)
{
  dout(7) << "handle_client_reconnect " << m->get_source() << endl;
  int from = m->get_source().num();

  if (m->closed) {
    dout(7) << " client had no session, removing from clientmap" << endl;

    mds->clientmap.add_closing(from);
    version_t cmapv = mds->clientmap.inc_projected();
    mdlog->submit_entry(new ESession(mds->clientmap.get_inst(from), false, cmapv),
			new C_MDS_session_finish(mds, mds->clientmap.get_inst(from), false, cmapv));

  } else {

    // caps
    for (map<inodeno_t, MClientReconnect::inode_caps_t>::iterator p = m->inode_caps.begin();
	 p != m->inode_caps.end();
	 ++p) {
      CInode *in = mdcache->get_inode(p->first);
      if (!in) {
	dout(0) << "missing " << p->first << ", fetching via " << m->inode_path[p->first] << endl;
	assert(0);
	continue;
      }
      
      dout(10) << " client cap " << cap_string(p->second.wanted)
	       << " seq " << p->second.seq 
	       << " on " << *in << endl;
      Capability cap(p->second.wanted, p->second.seq);
      in->add_client_cap(from, cap);
      in->inode.size = MAX(in->inode.size, p->second.size);
      in->inode.mtime = MAX(in->inode.mtime, p->second.mtime);
      in->inode.atime = MAX(in->inode.atime, p->second.atime);
      
      reconnected_open_files.insert(in);
    }
  }

  // remove from gather set
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty()) reconnect_finish();

  delete m;
}

void Server::client_reconnect_failure(int from) 
{
  dout(5) << "client_reconnect_failure on client" << from << endl;
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty()) 
    reconnect_finish();
}

void Server::reconnect_finish()
{
  dout(7) << "reconnect_finish" << endl;

  // adjust filelock state appropriately
  for (set<CInode*>::iterator p = reconnected_open_files.begin();
       p != reconnected_open_files.end();
       ++p) {
    CInode *in = *p;
    int issued = in->get_caps_issued();
    if (in->is_auth()) {
      // wr?
      if (issued & (CAP_FILE_WR|CAP_FILE_WRBUFFER)) {
	if (issued & (CAP_FILE_RDCACHE|CAP_FILE_WRBUFFER)) {
	  in->filelock.set_state(LOCK_LONER);
	} else {
	  in->filelock.set_state(LOCK_MIXED);
	}
      }
    } else {
      // note that client should perform stale/reap cleanup during reconnect.
      assert(issued & (CAP_FILE_WR|CAP_FILE_WRBUFFER) == 0);   // ????
      if (in->filelock.is_xlocked())
	in->filelock.set_state(LOCK_LOCK);
      else
	in->filelock.set_state(LOCK_SYNC);  // might have been lock, previously
    }
    dout(10) << " issued " << cap_string(issued)
	     << " chose " << in->filelock
	     << " on " << *in << endl;
  }
  reconnected_open_files.clear();  // clean up

  // done
  if (mds->mdsmap->get_num_in_mds() == 1) 
    mds->set_want_state(MDSMap::STATE_ACTIVE);    // go active
  else
    mds->set_want_state(MDSMap::STATE_REJOIN);    // move to rejoin state
}



/*******
 * some generic stuff for finishing off requests
 */


/*
 * send generic response (just and error code)
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei)
{
  reply_request(mdr, new MClientReply(mdr->client_request, r), tracei);
}


/*
 * send given reply
 * include a trace to tracei
 */
void Server::reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei) 
{
  MClientRequest *req = mdr->client_request;
  
  dout(10) << "reply_request " << reply->get_result() 
	   << " (" << strerror(-reply->get_result())
	   << ") " << *req << endl;

  // note result code in clientmap?
  if (!req->is_idempotent())
    mds->clientmap.add_completed_request(mdr->reqid);

  // include trace
  if (tracei) {
    reply->set_trace_dist( tracei, mds->get_nodeid() );
  }
  
  // send reply
  messenger->send_message(reply, req->get_client_inst());
  
  // finish request
  mdcache->request_finish(mdr);
}





/***
 * process a client request
 */
void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << endl;
  int client = req->get_client();

  if (!mds->is_active()) {
    dout(5) << " not active, discarding client request." << endl;
    delete req;
    return;
  }
  
  if (!mdcache->get_root()) {
    dout(5) << "need to open root" << endl;
    mdcache->open_root(new C_MDS_RetryMessage(mds, req));
    return;
  }

  // active session?
  if (!mds->clientmap.have_session(client)) {
    dout(1) << "no session for client" << client << ", dropping" << endl;
    delete req;
    return;
  }


  // okay, i want
  CInode *ref = 0;

  // retry?
  if (req->get_retry_attempt()) {
    if (mds->clientmap.have_completed_request(req->get_reqid())) {
      dout(5) << "already completed " << req->get_reqid() << endl;
      mds->messenger->send_message(new MClientReply(req, 0),
				   req->get_client_inst());
      delete req;
      return;
    }
  }
  // trim completed_request list
  if (req->get_oldest_client_tid() > 0)
    mds->clientmap.trim_completed_requests(client,
					   req->get_oldest_client_tid());


  // -----
  // some ops are on ino's
  switch (req->get_op()) {
  case MDS_OP_FSTAT:
    ref = mdcache->get_inode(req->args.fstat.ino);
    assert(ref);
    break;
    
  case MDS_OP_TRUNCATE:
    if (!req->args.truncate.ino) 
      break;   // can be called w/ either fh OR path
    ref = mdcache->get_inode(req->args.truncate.ino);
    assert(ref);
    break;
    
  case MDS_OP_FSYNC:
    ref = mdcache->get_inode(req->args.fsync.ino);   // fixme someday no ino needed?
    assert(ref);
    break;
  }

  // register + dispatch
  MDRequest *mdr = mdcache->request_start(req);

  if (ref) {
    dout(10) << "inode op on ref " << *ref << endl;
    mdr->ref = ref;
    mdr->pin(ref);
  }

  dispatch_client_request(mdr);
  return;
}


void Server::dispatch_client_request(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  if (mdr->ref) {
    dout(7) << "dispatch_client_request " << *req << " ref " << *mdr->ref << endl;
  } else {
    dout(7) << "dispatch_client_request " << *req << endl;
  }

  // we shouldn't be waiting on anyone.
  assert(mdr->waiting_on_slave.empty());
  
  switch (req->get_op()) {

    // inodes ops.
  case MDS_OP_STAT:
  case MDS_OP_LSTAT:
    handle_client_stat(mdr);
    break;
  case MDS_OP_UTIME:
    handle_client_utime(mdr);
    break;
  case MDS_OP_CHMOD:
    handle_client_chmod(mdr);
    break;
  case MDS_OP_CHOWN:
    handle_client_chown(mdr);
    break;
  case MDS_OP_TRUNCATE:
    handle_client_truncate(mdr);
    break;
  case MDS_OP_READDIR:
    handle_client_readdir(mdr);
    break;
  case MDS_OP_FSYNC:
    //handle_client_fsync(req, ref);
    break;

    // funky.
  case MDS_OP_OPEN:
    if ((req->args.open.flags & O_CREAT) &&
	!mdr->ref) 
      handle_client_openc(mdr);
    else 
      handle_client_open(mdr);
    break;

    // namespace.
    // no prior locks.
  case MDS_OP_MKNOD:
    handle_client_mknod(mdr);
    break;
  case MDS_OP_LINK:
    handle_client_link(mdr);
    break;
  case MDS_OP_UNLINK:
  case MDS_OP_RMDIR:
    handle_client_unlink(mdr);
    break;
  case MDS_OP_RENAME:
    handle_client_rename(mdr);
    break;
  case MDS_OP_MKDIR:
    handle_client_mkdir(mdr);
    break;
  case MDS_OP_SYMLINK:
    handle_client_symlink(mdr);
    break;


  default:
    dout(1) << " unknown client op " << req->get_op() << endl;
    assert(0);
  }
}


// ---------------------------------------
// SLAVE REQUESTS

void Server::handle_slave_request(MMDSSlaveRequest *m)
{
  dout(4) << "handle_slave_request " << m->get_reqid() << " from " << m->get_source() << endl;
  int from = m->get_source().num();

  // reply?
  if (m->is_reply()) {

    switch (m->get_op()) {
    case MMDSSlaveRequest::OP_XLOCKACK:
      {
	// identify lock, master request
	SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
						 m->get_object_info());
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	mdr->slaves.insert(from);
	dout(10) << "got remote xlock on " << *lock << " on " << *lock->get_parent() << endl;
	mdr->xlocks.insert(lock);
	mdr->locks.insert(lock);
	lock->get_xlock(mdr);
	lock->finish_waiters(SimpleLock::WAIT_REMOTEXLOCK);
      }
      break;

    case MMDSSlaveRequest::OP_AUTHPINACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_auth_pin_ack(mdr, m);
      }
      break;

    case MMDSSlaveRequest::OP_LINKPREPACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_link_prep_ack(mdr, m);
      }
      break;

    case MMDSSlaveRequest::OP_RENAMEPREPACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_rename_prep_ack(mdr, m);
      }
      break;

    case MMDSSlaveRequest::OP_RENAMEGETINODEACK:
      {
	MDRequest *mdr = mdcache->request_get(m->get_reqid());
	handle_slave_rename_get_inode_ack(mdr, m);
      }
      break;

    default:
      assert(0);
    }

    // done with reply.
    delete m;
    return;

  } else {
    // am i a new slave?
    MDRequest *mdr;
    if (mdcache->have_request(m->get_reqid())) {
      // existing?
      mdr = mdcache->request_get(m->get_reqid());
      if (mdr->slave_to_mds != from) {   // may not even be a slave! (e.g. forward race)
	dout(10) << "local request " << *mdr << " not slave to mds" << from
		 << ", ignoring " << *m << endl;
	delete m;
	return;
      }
    } else {
      // new?
      if (m->get_op() == MMDSSlaveRequest::OP_FINISH) {
	dout(10) << "missing slave request for " << m->get_reqid() 
		 << " OP_FINISH, must have lost race with a forward" << endl;
	delete m;
	return;
      }
      mdr = mdcache->request_start_slave(m->get_reqid(), m->get_source().num());
    }
    assert(mdr->slave_request == 0);     // only one at a time, please!  
    mdr->slave_request = m;
    
    dispatch_slave_request(mdr);
  }
}

void Server::dispatch_slave_request(MDRequest *mdr)
{
  dout(7) << "dispatch_slave_request " << *mdr << " " << *mdr->slave_request << endl;

  if (mdr->aborted) {
    dout(7) << " abort flag set, finishing" << endl;
    mdcache->request_finish(mdr);
    return;
  }

  switch (mdr->slave_request->get_op()) {
  case MMDSSlaveRequest::OP_XLOCK:
    {
      // identify object
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());

      if (lock && lock->get_parent()->is_auth()) {
	// xlock.
	// use acquire_locks so that we get auth_pinning.
	set<SimpleLock*> rdlocks;
	set<SimpleLock*> wrlocks;
	set<SimpleLock*> xlocks = mdr->xlocks;
	xlocks.insert(lock);
	
	if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	  return;
	
	// ack
	MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_XLOCKACK);
	r->set_lock_type(lock->get_type());
	lock->get_parent()->set_object_info(r->get_object_info());
	mds->send_message_mds(r, mdr->slave_request->get_source().num(), MDS_PORT_SERVER);
      } else {
	if (lock) {
	  dout(10) << "not auth for remote xlock attempt, dropping on " 
		   << *lock << " on " << *lock->get_parent() << endl;
	} else {
	  dout(10) << "don't have object, dropping" << endl;
	  assert(0); // can this happen, if we auth pinned properly.
	}
      }

      // done.
      delete mdr->slave_request;
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_UNXLOCK:
    {  
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());
      assert(lock);
      mds->locker->xlock_finish(lock, mdr);
      
      // done.  no ack necessary.
      delete mdr->slave_request;
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_AUTHPIN:
    handle_slave_auth_pin(mdr);
    break;

  case MMDSSlaveRequest::OP_LINKPREP:
  case MMDSSlaveRequest::OP_UNLINKPREP:
    handle_slave_link_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREP:
    handle_slave_rename_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RENAMEGETINODE:
    handle_slave_rename_get_inode(mdr);
    break;

  case MMDSSlaveRequest::OP_FINISH:
    // finish off request.
    mdcache->request_finish(mdr);
    break;

  default: 
    assert(0);
  }
}


void Server::handle_slave_auth_pin(MDRequest *mdr)
{
  dout(10) << "handle_slave_auth_pin " << *mdr << endl;

  // build list of objects
  list<MDSCacheObject*> objects;
  bool fail = false;

  for (list<MDSCacheObjectInfo>::iterator p = mdr->slave_request->get_authpins().begin();
       p != mdr->slave_request->get_authpins().end();
       ++p) {
    MDSCacheObject *object = mdcache->get_object(*p);
    if (!object) {
      dout(10) << " don't have " << *p << endl;
      fail = true;
      break;
    }

    objects.push_back(object);
  }
  
  // can we auth pin them?
  if (!fail) {
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      if (!(*p)->is_auth()) {
	dout(10) << " not auth for " << **p << endl;
	fail = true;
	break;
      }
      if (!mdr->is_auth_pinned(*p) &&
	  !(*p)->can_auth_pin()) {
	// wait
	dout(10) << " waiting for authpinnable on " << **p << endl;
	(*p)->add_waiter(CDir::WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mdcache, mdr));
	mdr->drop_local_auth_pins();
	return;
      }
    }
  }

  // auth pin!
  if (fail) {
    mdr->drop_local_auth_pins();  // just in case
  } else {
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      dout(10) << "auth_pinning " << **p << endl;
      mdr->auth_pin(*p);
    }
  }

  // ack!
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_AUTHPINACK);
  
  // return list of my auth_pins (if any)
  for (set<MDSCacheObject*>::iterator p = mdr->auth_pins.begin();
       p != mdr->auth_pins.end();
       ++p) {
    MDSCacheObjectInfo info;
    (*p)->set_object_info(info);
    reply->get_authpins().push_back(info);
  }

  mds->send_message_mds(reply, mdr->slave_to_mds, MDS_PORT_SERVER);
  
  // clean up this request
  delete mdr->slave_request;
  mdr->slave_request = 0;
  return;
}

void Server::handle_slave_auth_pin_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_auth_pin_ack on " << *mdr << " " << *ack << endl;
  int from = ack->get_source().num();

  // added auth pins?
  set<MDSCacheObject*> pinned;
  for (list<MDSCacheObjectInfo>::iterator p = ack->get_authpins().begin();
       p != ack->get_authpins().end();
       ++p) {
    MDSCacheObject *object = mdcache->get_object(*p);
    assert(object);  // we pinned it
    dout(10) << " remote has pinned " << *object << endl;
    if (!mdr->is_auth_pinned(object))
      mdr->auth_pins.insert(object);
    pinned.insert(object);
  }

  // removed auth pins?
  set<MDSCacheObject*>::iterator p = mdr->auth_pins.begin();
  while (p != mdr->auth_pins.end()) {
    if ((*p)->authority().first == from &&
	pinned.count(*p) == 0) {
      dout(10) << " remote has unpinned " << **p << endl;
      set<MDSCacheObject*>::iterator o = p;
      ++p;
      mdr->auth_pins.erase(o);
    } else {
      ++p;
    }
  }
  
  // note slave
  mdr->slaves.insert(from);

  // clear from waiting list
  assert(mdr->waiting_on_slave.count(from));
  mdr->waiting_on_slave.erase(from);

  // go again?
  if (mdr->waiting_on_slave.empty())
    dispatch_client_request(mdr);
  else 
    dout(10) << "still waiting on slaves " << mdr->waiting_on_slave << endl;
}


// ---------------------------------------
// HELPERS


/** validate_dentry_dir
 *
 * verify that the dir exists and would own the dname.
 * do not check if the dentry exists.
 */
CDir *Server::validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname)
{
  // make sure parent is a dir?
  if (!diri->is_dir()) {
    dout(7) << "validate_dentry_dir: not a dir" << endl;
    reply_request(mdr, -ENOTDIR);
    return false;
  }

  // which dirfrag?
  frag_t fg = diri->pick_dirfrag(dname);

  CDir *dir = try_open_auth_dir(diri, fg, mdr);
  if (!dir)
    return 0;

  // frozen?
  if (dir->is_frozen()) {
    dout(7) << "dir is frozen " << *dir << endl;
    dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  
  return dir;
}


/** prepare_null_dentry
 * prepare a null (or existing) dentry in given dir. 
 * wait for any dn lock.
 */
CDentry* Server::prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist)
{
  dout(10) << "prepare_null_dentry " << dname << " in " << *dir << endl;
  assert(dir->is_auth());
  
  // does it already exist?
  CDentry *dn = dir->lookup(dname);
  if (dn) {
    if (!dn->lock.can_rdlock(mdr)) {
      dout(10) << "waiting on (existing!) unreadable dentry " << *dn << endl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }

    if (!dn->is_null()) {
      // name already exists
      dout(10) << "dentry " << dname << " exists in " << *dir << endl;
      if (!okexist) {
        reply_request(mdr, -EEXIST);
        return 0;
      }
    }

    return dn;
  }

  // make sure dir is complete
  if (!dir->is_complete()) {
    dout(7) << " incomplete dir contents for " << *dir << ", fetching" << endl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }
  
  // create
  dn = dir->add_dentry(dname, 0);
  dout(10) << "prepare_null_dentry added " << *dn << endl;

  return dn;
}


/** prepare_new_inode
 *
 * create a new inode.  set c/m/atime.  hit dir pop.
 */
CInode* Server::prepare_new_inode(MClientRequest *req, CDir *dir) 
{
  CInode *in = mdcache->create_inode();
  in->inode.uid = req->get_caller_uid();
  in->inode.gid = req->get_caller_gid();
  in->inode.ctime = in->inode.mtime = in->inode.atime = g_clock.real_now();   // now
  dout(10) << "prepare_new_inode " << *in << endl;

  // bump modify pop
  mds->balancer->hit_dir(dir, META_POP_DWR);

  return in;
}



CDir *Server::traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath)
{
  // figure parent dir vs dname
  if (refpath.depth() == 0) {
    dout(7) << "can't do that to root" << endl;
    reply_request(mdr, -EINVAL);
    return 0;
  }
  string dname = refpath.last_dentry();
  refpath.pop_dentry();
  
  dout(10) << "traverse_to_auth_dir dirpath " << refpath << " dname " << dname << endl;

  // traverse to parent dir
  int r = mdcache->path_traverse(mdr, mdr->client_request,
				 0, refpath, trace, true,
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0; // delayed
  if (r < 0) {
    reply_request(mdr, r);
    return 0;
  }

  // open inode
  CInode *diri;
  if (trace.empty())
    diri = mdcache->get_root();
  else
    diri = mdcache->get_dentry_inode(trace[trace.size()-1], mdr);
  if (!diri) 
    return 0; // opening inode.

  // is it an auth dir?
  CDir *dir = validate_dentry_dir(mdr, diri, dname);
  if (!dir)
    return 0; // forwarded or waiting for freeze

  dout(10) << "traverse_to_auth_dir " << *dir << endl;
  return dir;
}



CInode* Server::rdlock_path_pin_ref(MDRequest *mdr, bool want_auth)
{
  // already got ref?
  if (mdr->ref) 
    return mdr->ref;

  MClientRequest *req = mdr->client_request;

  // traverse
  filepath refpath = req->get_filepath();
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, req,
				 0, refpath, 
				 trace, req->follow_trailing_symlink(),
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return false; // delayed
  if (r < 0) {  // error
    reply_request(mdr, r);
    return 0;
  }

  // open ref inode
  CInode *ref = 0;
  if (trace.empty())
    ref = mdcache->get_root();
  else {
    CDentry *dn = trace[trace.size()-1];

    // if no inode, fw to dentry auth?
    if (want_auth && 
	dn->is_remote() &&
	!dn->inode && 
	!dn->is_auth()) {
      if (dn->is_ambiguous_auth()) {
	dout(10) << "waiting for single auth on " << *dn << endl;
	dn->dir->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryMessage(mds, req));
      } else {
	dout(10) << "fw to auth for " << *dn << endl;
	mds->forward_message_mds(req, dn->authority().first, MDS_PORT_SERVER);
      }
    }

    // open ref inode
    ref = mdcache->get_dentry_inode(dn, mdr);
    if (!ref) return 0;
  }
  dout(10) << "ref is " << *ref << endl;

  // fw to inode auth?
  if (want_auth && !ref->is_auth()) {
    if (ref->is_ambiguous_auth()) {
      dout(10) << "waiting for single auth on " << *ref << endl;
      ref->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryMessage(mds, req));
    } else {
      dout(10) << "fw to auth for " << *ref << endl;
      mds->forward_message_mds(req, ref->authority().first, MDS_PORT_SERVER);
    }
  }

  // auth_pin?
  if (want_auth) {
    if (ref->is_frozen()) {
      dout(7) << "waiting for !frozen/authpinnable on " << *ref << endl;
      ref->add_waiter(CInode::WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    mdr->auth_pin(ref);
  }

  // lock the path
  set<SimpleLock*> rdlocks, empty;

  for (int i=0; i<(int)trace.size(); i++) 
    rdlocks.insert(&trace[i]->lock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, empty, empty))
    return 0;
  
  // set and pin ref
  mdr->pin(ref);
  mdr->ref = ref;

  // save the locked trace.
  mdr->trace.swap(trace);
  
  return ref;
}


/** rdlock_path_xlock_dentry
 * traverse path to the directory that could/would contain dentry.
 * make sure i am auth for that dentry, forward as necessary.
 * create null dentry in place (or use existing if okexist).
 * get rdlocks on traversed dentries, xlock on new dentry.
 */
CDentry* Server::rdlock_path_xlock_dentry(MDRequest *mdr, bool okexist, bool mustexist)
{
  MClientRequest *req = mdr->client_request;

  vector<CDentry*> trace;
  CDir *dir = traverse_to_auth_dir(mdr, trace, req->get_filepath());
  if (!dir) return 0;
  dout(10) << "rdlock_path_xlock_dentry dir " << *dir << endl;

  // make sure we can auth_pin (or have already authpinned) dir
  if (dir->is_frozen()) {
    dout(7) << "waiting for !frozen/authpinnable on " << *dir << endl;
    dir->add_waiter(CInode::WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }

  // make a null dentry?
  const string &dname = req->get_filepath().last_dentry();
  CDentry *dn;
  if (mustexist) {
    dn = dir->lookup(dname);

    // make sure dir is complete
    if (!dn && !dir->is_complete()) {
      dout(7) << " incomplete dir contents for " << *dir << ", fetching" << endl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }

    // readable?
    if (dn && !dn->lock.can_rdlock(mdr)) {
      dout(10) << "waiting on (existing!) unreadable dentry " << *dn << endl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
      
    // exists?
    if (!dn || dn->is_null()) {
      dout(7) << "dentry " << dname << " dne in " << *dir << endl;
      reply_request(mdr, -ENOENT);
      return 0;
    }    
  } else {
    dn = prepare_null_dentry(mdr, dir, dname, okexist);
    if (!dn) 
      return 0;
  }

  // -- lock --
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)trace.size(); i++) 
    rdlocks.insert(&trace[i]->lock);
  if (dn->is_null()) {
    xlocks.insert(&dn->lock);                 // new dn, xlock
    wrlocks.insert(&dn->dir->inode->dirlock); // also, wrlock on dir mtime
  } else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return 0;

  // save the locked trace.
  mdr->trace.swap(trace);

  return dn;
}





CDir* Server::try_open_auth_dir(CInode *diri, frag_t fg, MDRequest *mdr)
{
  CDir *dir = diri->get_dirfrag(fg);

  // not open and inode not mine?
  if (!dir && !diri->is_auth()) {
    int inauth = diri->authority().first;
    dout(7) << "try_open_auth_dir: not open, not inode auth, fw to mds" << inauth << endl;
    mdcache->request_forward(mdr, inauth);
    return 0;
  }

  // not open and inode frozen?
  if (!dir && diri->is_frozen_dir()) {
    dout(10) << "try_open_auth_dir: dir inode is frozen, waiting " << *diri << endl;
    assert(diri->get_parent_dir());
    diri->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }

  // invent?
  if (!dir) {
    assert(diri->is_auth());
    dir = diri->get_or_open_dirfrag(mds->mdcache, fg);
  }
  assert(dir);
 
  // am i auth for the dirfrag?
  if (!dir->is_auth()) {
    int auth = dir->authority().first;
    dout(7) << "try_open_auth_dir: not auth for " << *dir
	    << ", fw to mds" << auth << endl;
    mdcache->request_forward(mdr, auth);
    return 0;
  }

  return dir;
}

/*
CDir* Server::try_open_dir(CInode *diri, frag_t fg, MDRequest *mdr)
{
  CDir *dir = diri->get_dirfrag(fg);
  if (dir) 
    return dir;

  if (diri->is_auth()) {
    // auth
    // not open and inode frozen?
    if (!dir && diri->is_frozen_dir()) {
      dout(10) << "try_open_dir: dir inode is auth+frozen, waiting " << *diri << endl;
      assert(diri->get_parent_dir());
      diri->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    
    // invent?
    if (!dir) {
      assert(diri->is_auth());
      dir = diri->get_or_open_dirfrag(mds->mdcache, fg);
    }
    assert(dir);
    return dir;
  } else {
    // not auth
    mdcache->open_remote_dir(diri, fg,
			     new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }
}
*/


/** predirty_dn_diri
 * predirty the directory inode for a new dentry, if it is auth (and not root)
 * BUG: root inode doesn't get dirtied properly, currently.  blech.
 */
version_t Server::predirty_dn_diri(MDRequest *mdr, CDentry *dn, EMetaBlob *blob, utime_t mtime)
{
  version_t dirpv = 0;
  CInode *diri = dn->dir->inode;
  
  if (diri->is_auth() && !diri->is_root() &&
      mdr->wrlocks.count(&diri->dirlock)) {            // only if we've wrlocked it.
    dirpv = diri->pre_dirty();
    inode_t *pi = blob->add_primary_dentry(diri->get_parent_dn(), true);
    pi->version = dirpv;
    pi->ctime = pi->mtime = mtime;
    dout(10) << "predirty_dn_diri ctime/mtime " << mtime << " pv " << dirpv << " on " << *diri << endl;
  }
  
  return dirpv;
}

/** dirty_dn_diri
 * follow-up with actual dirty of inode after journal entry commits.
 */
void Server::dirty_dn_diri(CDentry *dn, version_t dirpv, utime_t mtime)
{
  CInode *diri = dn->dir->inode;
  
  // make the udpate
  diri->inode.ctime = diri->inode.mtime = mtime;

  if (dirpv) {
    assert(diri->is_auth() && !diri->is_root());

    // we were before, too.
    diri->mark_dirty(dirpv);
    dout(10) << "dirty_dn_diri ctime/mtime " << mtime << " v " << diri->inode.version << " on " << *diri << endl;

    /* any writebehind should be handled by the lock gather probably?
    } else {
      // write-behind.
      if (!diri->is_dirty())
	dirty_diri_mtime_writebehind(diri, mtime);
      // otherwise, if it's dirty, we know the mtime is journaled by another local update.
      // (something after the import, or the import itself)
    }
    */
  } else {
    // we're not auth.  dirlock scatterlock will propagate the update.
  }
}


class C_MDS_DirtyDiriMtimeWB : public Context {
  Server *server;
  CInode *diri;
  version_t dirpv;
public:
  C_MDS_DirtyDiriMtimeWB(Server *s, CInode *i, version_t v) :
    server(s), diri(i), dirpv(v) {}
  void finish(int r) {
    diri->mark_dirty(dirpv);
    diri->auth_unpin();
  }
};

void Server::dirty_diri_mtime_writebehind(CInode *diri, utime_t mtime)
{
  if (!diri->can_auth_pin())
    return;  // oh well!  hrm.

  diri->auth_pin();
  
  // we're newly auth.  write-behind.
  EUpdate *le = new EUpdate("dir.mtime writebehind");
  le->metablob.add_dir_context(diri->get_parent_dn()->get_dir());
  inode_t *pi = le->metablob.add_primary_dentry(diri->get_parent_dn(), true);
  pi->version = diri->pre_dirty();
  
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_sync(new C_MDS_DirtyDiriMtimeWB(this, diri, pi->version));
}






// ===============================================================================
// STAT

void Server::handle_client_stat(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *ref = rdlock_path_pin_ref(mdr, false);
  if (!ref) return;

  // which inode locks do I want?
  /* note: this works because we include existing locks in our lists,
     and because all new locks are on inodes and sort to the right of
     the dentry rdlocks previous acquired by rdlock_path_pin_ref(). */
  set<SimpleLock*> rdlocks = mdr->rdlocks;
  set<SimpleLock*> wrlocks = mdr->wrlocks;
  set<SimpleLock*> xlocks = mdr->xlocks;
  
  int mask = req->args.stat.mask;
  if (mask & INODE_MASK_LINK) rdlocks.insert(&ref->linklock);
  if (mask & INODE_MASK_AUTH) rdlocks.insert(&ref->authlock);
  if (ref->is_file() && 
      mask & INODE_MASK_FILE) rdlocks.insert(&ref->filelock);
  if (ref->is_dir() &&
      mask & INODE_MASK_MTIME) rdlocks.insert(&ref->dirlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // reply
  dout(10) << "reply to stat on " << *req << endl;
  MClientReply *reply = new MClientReply(req);
  reply_request(mdr, reply, ref);
}




// ===============================================================================
// INODE UPDATES


/* 
 * finisher: do a inode_file_write_finish and reply.
 */
class C_MDS_utime_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  utime_t mtime, atime;
public:
  C_MDS_utime_finish(MDS *m, MDRequest *r, CInode *i, version_t pdv, utime_t mt, utime_t at) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    mtime(mt), atime(at) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->inode.mtime = mtime;
    in->inode.atime = atime;
    in->mark_dirty(pv);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


// utime

void Server::handle_client_utime(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  // write
  if (!mds->locker->xlock_start(&cur->filelock, mdr))
    return;

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // prepare
  version_t pdv = cur->pre_dirty();
  utime_t mtime = req->args.utime.mtime;
  utime_t atime = req->args.utime.atime;
  C_MDS_utime_finish *fin = new C_MDS_utime_finish(mds, mdr, cur, pdv, 
						   mtime, atime);

  // log + wait
  EUpdate *le = new EUpdate("utime");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(cur->get_parent_dir());
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mtime = mtime;
  pi->atime = mtime;
  pi->ctime = g_clock.real_now();
  pi->version = pdv;
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


// --------------

/* 
 * finisher: do a inode_hard_xlock_finish and reply.
 */
class C_MDS_chmod_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  int mode;
public:
  C_MDS_chmod_finish(MDS *m, MDRequest *r, CInode *i, version_t pdv, int mo) :
    mds(m), mdr(r), in(i), pv(pdv), mode(mo) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->inode.mode &= ~04777;
    in->inode.mode |= (mode & 04777);
    in->mark_dirty(pv);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


// chmod

void Server::handle_client_chmod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  // write
  if (!mds->locker->xlock_start(&cur->authlock, mdr))
    return;

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // prepare
  version_t pdv = cur->pre_dirty();
  int mode = req->args.chmod.mode;
  C_MDS_chmod_finish *fin = new C_MDS_chmod_finish(mds, mdr, cur, pdv,
						   mode);

  // log + wait
  EUpdate *le = new EUpdate("chmod");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(cur->get_parent_dir());
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mode = mode;
  pi->version = pdv;
  pi->ctime = g_clock.real_now();
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


// chown

class C_MDS_chown_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  int uid, gid;
public:
  C_MDS_chown_finish(MDS *m, MDRequest *r, CInode *i, version_t pdv, int u, int g) :
    mds(m), mdr(r), in(i), pv(pdv), uid(u), gid(g) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    if (uid >= 0) in->inode.uid = uid;
    if (gid >= 0) in->inode.gid = gid;
    in->mark_dirty(pv);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


void Server::handle_client_chown(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  // write
  if (!mds->locker->xlock_start(&cur->authlock, mdr))
    return;

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // prepare
  version_t pdv = cur->pre_dirty();
  int uid = req->args.chown.uid;
  int gid = req->args.chown.gid;
  C_MDS_chown_finish *fin = new C_MDS_chown_finish(mds, mdr, cur, pdv,
						   uid, gid);

  // log + wait
  EUpdate *le = new EUpdate("chown");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(cur->get_parent_dir());
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  if (uid >= 0) pi->uid = uid;
  if (gid >= 0) pi->gid = gid;
  pi->version = pdv;
  pi->ctime = g_clock.real_now();
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}




// =================================================================
// DIRECTORY and NAMESPACE OPS

// READDIR

int Server::encode_dir_contents(CDir *dir, 
				list<InodeStat*>& inls,
				list<string>& dnls)
{
  int numfiles = 0;

  for (CDir_map_t::iterator it = dir->begin(); 
       it != dir->end(); 
       it++) {
    CDentry *dn = it->second;
    
    if (dn->is_null()) continue;

    CInode *in = dn->inode;
    if (!in) 
      continue;  // hmm, fixme!, what about REMOTE links?  
    
    dout(12) << "including inode " << *in << endl;

    // add this item
    // note: InodeStat makes note of whether inode data is readable.
    dnls.push_back( it->first );
    inls.push_back( new InodeStat(in, mds->get_nodeid()) );
    numfiles++;
  }
  return numfiles;
}


void Server::handle_client_readdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *diri = rdlock_path_pin_ref(mdr, false);
  if (!diri) return;

  // it's a directory, right?
  if (!diri->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << endl;
    reply_request(mdr, -ENOTDIR);
    return;
  }

  // which frag?
  frag_t fg = req->args.readdir.frag;

  // does it exist?
  if (diri->dirfragtree[fg] != fg) {
    dout(10) << "frag " << fg << " doesn't appear in fragtree " << diri->dirfragtree << endl;
    reply_request(mdr, -EAGAIN);
    return;
  }
  
  CDir *dir = try_open_auth_dir(diri, fg, mdr);
  if (!dir) return;

  // ok!
  assert(dir->is_auth());

  // check perm
  /*
  if (!mds->locker->inode_hard_rdlock_start(diri, mdr))
    return;
  mds->locker->inode_hard_rdlock_finish(diri, mdr);
  */

  if (!dir->is_complete()) {
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << endl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  // build dir contents
  list<InodeStat*> inls;
  list<string> dnls;
  int numfiles = encode_dir_contents(dir, inls, dnls);
  
  // . too
  dnls.push_back(".");
  inls.push_back(new InodeStat(diri, mds->get_nodeid()));
  ++numfiles;
  
  // yay, reply
  MClientReply *reply = new MClientReply(req);
  reply->take_dir_items(inls, dnls, numfiles);
  
  dout(10) << "reply to " << *req << " readdir " << numfiles << " files" << endl;
  reply->set_result(fg);
  
  //balancer->hit_dir(diri->dir);
  
  // reply
  reply_request(mdr, reply, diri);
}



// ------------------------------------------------

// MKNOD

class C_MDS_mknod_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
  version_t pv;
  version_t dirpv;
public:
  C_MDS_mknod_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni, version_t dirpv_) :
    mds(m), mdr(r), dn(d), newi(ni),
    pv(d->get_projected_version()), dirpv(dirpv_) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_inode(dn, newi);
    
    // dirty inode, dn, dir
    newi->mark_dirty(pv);

    // dir inode's mtime
    mds->server->dirty_dn_diri(dn, dirpv, newi->inode.ctime);
    
    // hit pop
    mds->balancer->hit_inode(newi, META_POP_IWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, newi);
  }
};



void Server::handle_client_mknod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  CInode *newi = prepare_new_inode(req, dn->dir);
  assert(newi);

  // it's a file.
  dn->pre_dirty();
  newi->inode.mode = req->args.mknod.mode;
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_FILE;
  
  // prepare finisher
  EUpdate *le = new EUpdate("mknod");
  le->metablob.add_client_req(req->get_reqid());

  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, newi->inode.ctime);  // dir mtime too

  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  
  // log + wait
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, mdr, dn, newi, dirpv);
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}



// MKDIR

void Server::handle_client_mkdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  // new inode
  CInode *newi = prepare_new_inode(req, dn->dir);  
  assert(newi);

  // it's a directory.
  dn->pre_dirty();
  newi->inode.mode = req->args.mkdir.mode;
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_DIR;
  newi->inode.layout = g_OSD_MDDirLayout;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dirfrag(mds->mdcache, frag_t());
  newdir->mark_complete();
  newdir->mark_dirty(newdir->pre_dirty());

  // prepare finisher
  EUpdate *le = new EUpdate("mkdir");
  le->metablob.add_client_req(req->get_reqid());
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, newi->inode.ctime);  // dir mtime too
  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  le->metablob.add_dir(newdir, true);
  
  // log + wait
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, mdr, dn, newi, dirpv);
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);


  /* old export heuristic.  pbly need to reimplement this at some point.    
  if (
      diri->dir->is_auth() &&
      diri->dir->is_rep() &&
      newdir->is_auth() &&
      !newdir->is_hashing()) {
    int dest = rand() % mds->mdsmap->get_num_mds();
    if (dest != whoami) {
      dout(10) << "exporting new dir " << *newdir << " in replicated parent " << *diri->dir << endl;
      mdcache->migrator->export_dir(newdir, dest);
    }
  }
  */
}


// SYMLINK

void Server::handle_client_symlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  
  CDentry *dn = rdlock_path_xlock_dentry(mdr, false, false);
  if (!dn) return;

  CInode *newi = prepare_new_inode(req, dn->dir);
  assert(newi);

  // it's a symlink
  dn->pre_dirty();
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_SYMLINK;
  newi->symlink = req->get_sarg();

  // prepare finisher
  EUpdate *le = new EUpdate("symlink");
  le->metablob.add_client_req(req->get_reqid());
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, newi->inode.ctime);  // dir mtime too
  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  
  // log + wait
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, mdr, dn, newi, dirpv);
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}





// LINK

void Server::handle_client_link(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "handle_client_link " << req->get_filepath()
	  << " to " << req->get_sarg()
	  << endl;

  // traverse to dest dir, make sure it's ours.
  const filepath &linkpath = req->get_filepath();
  const string &dname = linkpath.last_dentry();
  vector<CDentry*> linktrace;
  CDir *dir = traverse_to_auth_dir(mdr, linktrace, linkpath);
  if (!dir) return;
  dout(7) << "handle_client_link link " << dname << " in " << *dir << endl;
  
  // traverse to link target
  filepath targetpath = req->get_sarg();
  dout(7) << "handle_client_link discovering target " << targetpath << endl;
  vector<CDentry*> targettrace;
  int r = mdcache->path_traverse(mdr, req,
				 0, targetpath, targettrace, false,
				 MDS_TRAVERSE_DISCOVER);
  if (r > 0) return; // wait
  if (targettrace.empty()) r = -EINVAL; 
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  
  // identify target inode
  CInode *targeti = targettrace[targettrace.size()-1]->inode;
  assert(targeti);

  // dir?
  dout(7) << "target is " << *targeti << endl;
  if (targeti->is_dir()) {
    dout(7) << "target is a dir, failing..." << endl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  // get/make null link dentry
  CDentry *dn = prepare_null_dentry(mdr, dir, dname, false);
  if (!dn) return;

  // create lock lists
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)linktrace.size(); i++)
    rdlocks.insert(&linktrace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->dir->inode->dirlock);
  for (int i=0; i<(int)targettrace.size(); i++)
    rdlocks.insert(&targettrace[i]->lock);
  xlocks.insert(&targeti->linklock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mdr->done_locking = true;  // avoid wrlock moving target issues.
  
  // pick mtime
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // does the target need an anchor?
  if (targeti->is_auth()) {
    /*if (targeti->get_parent_dir() == dn->dir) {
      dout(7) << "target is in the same dirfrag, sweet" << endl;
    } 
    else 
      */
    if (targeti->is_anchored() && !targeti->is_unanchoring()) {
      dout(7) << "target anchored already (nlink=" << targeti->inode.nlink << "), sweet" << endl;
    } 
    else {
      dout(7) << "target needs anchor, nlink=" << targeti->inode.nlink << ", creating anchor" << endl;
      
      mdcache->anchor_create(mdr, targeti,
			     new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // go!

  // local or remote?
  if (targeti->is_auth()) 
    _link_local(mdr, dn, targeti);
  else 
    _link_remote(mdr, dn, targeti);
}


class C_MDS_link_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *targeti;
  version_t dpv;
  version_t tpv;
  version_t dirpv;
public:
  C_MDS_link_local_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti, version_t dirpv_) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dpv(d->get_projected_version()),
    tpv(targeti->get_parent_dn()->get_projected_version()),
    dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_local_finish(mdr, dn, targeti, dpv, tpv, dirpv);
  }
};


void Server::_link_local(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_local " << *dn << " to " << *targeti << endl;

  // ok, let's do it.
  // prepare log entry
  EUpdate *le = new EUpdate("link_local");
  le->metablob.add_client_req(mdr->reqid);

  // predirty
  dn->pre_dirty();
  version_t tpdv = targeti->pre_dirty();
  
  // add to event
  utime_t now = g_clock.real_now();
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, mdr->now);   // dir inode's mtime
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_remote_dentry(dn, true, targeti->ino());  // new remote
  le->metablob.add_dir_context(targeti->get_parent_dir());
  inode_t *pi = le->metablob.add_primary_dentry(targeti->parent, true, targeti);  // update old primary

  // update journaled target inode
  pi->nlink++;
  pi->ctime = mdr->now;
  pi->version = tpdv;

  // finisher
  C_MDS_link_local_finish *fin = new C_MDS_link_local_finish(mds, mdr, dn, targeti, dirpv);
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}

void Server::_link_local_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				version_t dpv, version_t tpv, version_t dirpv)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << endl;

  // link and unlock the new dentry
  dn->dir->link_inode(dn, targeti->ino());
  dn->set_version(dpv);
  dn->mark_dirty(dpv);

  // update the target
  targeti->inode.nlink++;
  targeti->inode.ctime = mdr->now;
  targeti->mark_dirty(tpv);

  // dir inode's mtime
  dirty_dn_diri(dn, dirpv, mdr->now);
  
  // bump target popularity
  mds->balancer->hit_inode(targeti, META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->get_dir()->get_inode());  // FIXME: imprecise ref
}


// remote

class C_MDS_link_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *targeti;
  version_t dpv;
  version_t dirpv;
public:
  C_MDS_link_remote_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti, version_t dirpv_) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dpv(d->get_projected_version()),
    dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_remote_finish(mdr, dn, targeti, dpv, dirpv);
  }
};

void Server::_link_remote(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote " << *dn << " to " << *targeti << endl;
    
  // 1. send LinkPrepare to dest (journal nlink++ prepare)
  int linkauth = targeti->authority().first;
  if (mdr->witnessed.count(linkauth) == 0) {
    dout(10) << " targeti auth must prepare nlink++" << endl;

    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_LINKPREP);
    targeti->set_object_info(req->get_object_info());
    req->now = mdr->now;
    mds->send_message_mds(req, linkauth, MDS_PORT_SERVER);

    assert(mdr->waiting_on_slave.count(linkauth) == 0);
    mdr->waiting_on_slave.insert(linkauth);
    return;
  }
  dout(10) << " targeti auth has prepared nlink++" << endl;

  // 2. create+journal new dentry, as with link_local.
  // prepare log entry
  EUpdate *le = new EUpdate("link_remote");
  le->metablob.add_client_req(mdr->reqid);
  
  // predirty
  dn->pre_dirty();
  
  // add to event
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, mdr->now);   // dir inode's mtime
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_remote_dentry(dn, true, targeti->ino());  // new remote

  // finisher
  C_MDS_link_remote_finish *fin = new C_MDS_link_remote_finish(mds, mdr, dn, targeti, dirpv);
  
  // mark committing (needed for proper recovery)
  mdr->committing = true;

  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}

void Server::_link_remote_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				 version_t dpv, version_t dirpv)
{
  dout(10) << "_link_remote_finish " << *dn << " to " << *targeti << endl;

  // link the new dentry
  dn->dir->link_inode(dn, targeti->ino());
  dn->set_version(dpv);
  dn->mark_dirty(dpv);

  // dir inode's mtime
  dirty_dn_diri(dn, dirpv, mdr->now);
  
  // bump target popularity
  mds->balancer->hit_inode(targeti, META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->get_dir()->get_inode());  // FIXME: imprecise ref
}


// remote linking/unlinking

class C_MDS_SlaveLinkPrep : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
  version_t tpv;
  bool inc;
public:
  C_MDS_SlaveLinkPrep(Server *s, MDRequest *r, CInode *t, version_t v, bool in) :
    server(s), mdr(r), targeti(t), tpv(v), inc(in) { }
  void finish(int r) {
    assert(r == 0);
    server->_logged_slave_link(mdr, targeti, tpv, inc);
  }
};

void Server::handle_slave_link_prep(MDRequest *mdr)
{
  dout(10) << "handle_slave_link_prep " << *mdr 
	   << " on " << mdr->slave_request->get_object_info() 
	   << endl;

  CInode *targeti = mdcache->get_inode(mdr->slave_request->get_object_info().ino);
  assert(targeti);
  dout(10) << "targeti " << *targeti << endl;
  CDentry *dn = targeti->get_parent_dn();
  assert(dn->is_primary());

  mdr->now = mdr->slave_request->now;

  // anchor?
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    if (targeti->is_anchored() && !targeti->is_unanchoring()) {
      dout(7) << "target anchored already (nlink=" << targeti->inode.nlink << "), sweet" << endl;
    } 
    else {
      dout(7) << "target needs anchor, nlink=" << targeti->inode.nlink << ", creating anchor" << endl;
      mdcache->anchor_create(mdr, targeti,
			     new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate("slave_link_prep", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_PREPARE);

  version_t tpv = targeti->pre_dirty();

  // add to event
  le->metablob.add_dir_context(targeti->get_parent_dir());
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, targeti);  // update old primary

  // update journaled target inode
  bool inc;
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    inc = true;
    pi->nlink++;
  } else {
    inc = false;
    pi->nlink--;
  }
  pi->ctime = mdr->now;
  pi->version = tpv;

  mds->mdlog->submit_entry(le, new C_MDS_SlaveLinkPrep(this, mdr, targeti, tpv, inc));
}

class C_MDS_SlaveLinkCommit : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
  version_t tpv;
  bool inc;
public:
  C_MDS_SlaveLinkCommit(Server *s, MDRequest *r, CInode *t, version_t v, bool in) :
    server(s), mdr(r), targeti(t), tpv(v), inc(in) { }
  void finish(int r) {
    server->_commit_slave_link(mdr, r, targeti, tpv, inc);
  }
};

void Server::_logged_slave_link(MDRequest *mdr, CInode *targeti, version_t tpv, bool inc) 
{
  dout(10) << "_logged_slave_link " << *mdr
	   << " inc=" << inc
	   << " " << *targeti << endl;

  // ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_LINKPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds, MDS_PORT_SERVER);
  
  // set up commit waiter
  mdr->slave_commit = new C_MDS_SlaveLinkCommit(this, mdr, targeti, tpv, inc);

  // done.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}

void Server::_commit_slave_link(MDRequest *mdr, int r, CInode *targeti, version_t tpv, bool inc)
{  
  dout(10) << "_commit_slave_link " << *mdr
	   << " r=" << r
	   << " inc=" << inc
	   << " " << *targeti << endl;

  ESlaveUpdate *le;
  
  if (r == 0) {
    // commit.

    // update the target
    if (inc)
      targeti->inode.nlink++;
    else
      targeti->inode.nlink--;
    targeti->inode.ctime = mdr->now;
    targeti->mark_dirty(tpv);
    
    // write a commit to the journal
    le = new ESlaveUpdate("slave_link_commit", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT);
  } else {
    // abort
    le = new ESlaveUpdate("slave_link_abort", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_ABORT);
  }

  mds->mdlog->submit_entry(le);
}




void Server::handle_slave_link_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m)
{
  dout(10) << "handle_slave_link_prep_ack " << *mdr 
	   << " " << *m << endl;
  int from = m->get_source().num();

  // note slave
  mdr->slaves.insert(from);
  
  // witnessed!
  assert(mdr->witnessed.count(from) == 0);
  mdr->witnessed.insert(from);
  
  // remove from waiting list
  assert(mdr->waiting_on_slave.count(from));
  mdr->waiting_on_slave.erase(from);

  assert(mdr->waiting_on_slave.empty());

  dispatch_client_request(mdr);  // go again!
}





// UNLINK

void Server::handle_client_unlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  // traverse to path
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, req, 
				 0, req->get_filepath(), trace, false,
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return;
  if (trace.empty()) r = -EINVAL;   // can't unlink root
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }

  CDentry *dn = trace[trace.size()-1];
  assert(dn);
  
  // is it my dentry?
  if (!dn->is_auth()) {
    // fw to auth
    mdcache->request_forward(mdr, dn->authority().first);
    return;
  }

  // rmdir or unlink?
  bool rmdir = false;
  if (req->get_op() == MDS_OP_RMDIR) rmdir = true;
  
  if (rmdir) {
    dout(7) << "handle_client_rmdir on " << *dn << endl;
  } else {
    dout(7) << "handle_client_unlink on " << *dn << endl;
  }

  // readable?
  if (!dn->lock.can_rdlock(mdr)) {
    dout(10) << "waiting on unreadable dentry " << *dn << endl;
    dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  // dn looks ok.

  // get/open inode.
  mdr->trace.swap(trace);
  CInode *in = mdcache->get_dentry_inode(dn, mdr);
  if (!in) return;
  dout(7) << "dn links to " << *in << endl;

  // rmdir vs is_dir 
  if (in->is_dir()) {
    if (rmdir) {
      // do empty directory checks
      if (!_verify_rmdir(mdr, in))
	return;
    } else {
      dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << endl;
      reply_request(mdr, -EISDIR);
      return;
    }
  } else {
    if (rmdir) {
      // unlink
      dout(7) << "handle_client_rmdir on non-dir " << *in << ", returning error" << endl;
      reply_request(mdr, -ENOTDIR);
      return;
    }
  }

  // lock
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (int i=0; i<(int)trace.size()-1; i++)
    rdlocks.insert(&trace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->dir->inode->dirlock);
  xlocks.insert(&in->linklock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // yay!
  mdr->done_locking = true;  // avoid wrlock racing
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // get stray dn ready?
  CDentry *straydn = 0;
  if (dn->is_primary()) {
    straydn = mdcache->get_or_create_stray_dentry(dn->inode);
    dout(10) << " straydn is " << *straydn << endl;

    if (!mdr->dst_reanchor_atid &&
	dn->inode->is_anchored()) {
      dout(10) << "reanchoring to stray " << *dn->inode << endl;
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, dn->inode);
      mds->anchorclient->prepare_update(dn->inode->ino(), trace, &mdr->dst_reanchor_atid, 
					new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // ok!
  if (dn->is_remote() && !dn->inode->is_auth()) 
    _unlink_remote(mdr, dn);
  else
    _unlink_local(mdr, dn, straydn);
}



class C_MDS_unlink_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CDentry *straydn;
  version_t ipv;  // referred inode
  version_t dnpv;  // deleted dentry
  version_t dirpv;
public:
  C_MDS_unlink_local_finish(MDS *m, MDRequest *r, CDentry *d, CDentry *sd,
			    version_t v, version_t dirpv_) :
    mds(m), mdr(r), dn(d), straydn(sd),
    ipv(v), 
    dnpv(d->get_projected_version()), dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_local_finish(mdr, dn, straydn, ipv, dnpv, dirpv);
  }
};


void Server::_unlink_local(MDRequest *mdr, CDentry *dn, CDentry *straydn)
{
  dout(10) << "_unlink_local " << *dn << endl;

  // ok, let's do it.
  // prepare log entry
  EUpdate *le = new EUpdate("unlink_local");
  le->metablob.add_client_req(mdr->reqid);

  version_t ipv = 0;  // dirty inode version
  inode_t *pi = 0;    // the inode

  if (dn->is_primary()) {
    // primary link.  add stray dentry.
    assert(straydn);
    ipv = straydn->pre_dirty(dn->inode->inode.version);
    le->metablob.add_dir_context(straydn->dir);
    pi = le->metablob.add_primary_dentry(straydn, true, dn->inode);
  } else {
    // remote link.  update remote inode.
    ipv = dn->inode->pre_dirty();
    le->metablob.add_dir_context(dn->inode->get_parent_dir());
    pi = le->metablob.add_primary_dentry(dn->inode->parent, true, dn->inode);  // update primary
  }
  
  // the unlinked dentry
  dn->pre_dirty();
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, mdr->now);
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_null_dentry(dn, true);

  // update journaled target inode
  pi->nlink--;
  pi->ctime = mdr->now;
  pi->version = ipv;

  if (mdr->dst_reanchor_atid)
    le->metablob.add_anchor_transaction(mdr->dst_reanchor_atid);

  // finisher
  C_MDS_unlink_local_finish *fin = new C_MDS_unlink_local_finish(mds, mdr, dn, straydn, 
								 ipv, dirpv);
  
  journal_opens();  // journal pending opens, just in case
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
  
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);
}

void Server::_unlink_local_finish(MDRequest *mdr, 
				  CDentry *dn, CDentry *straydn,
				  version_t ipv, version_t dnpv, version_t dirpv) 
{
  dout(10) << "_unlink_local_finish " << *dn << endl;

  // unlink main dentry
  CInode *in = dn->inode;
  dn->dir->unlink_inode(dn);

  // relink as stray?  (i.e. was primary link?)
  if (straydn) straydn->dir->link_inode(straydn, in);  

  // nlink--
  in->inode.ctime = mdr->now;
  in->inode.nlink--;
  in->mark_dirty(ipv);  // dirty inode
  dn->mark_dirty(dnpv);  // dirty old dentry

  // dir inode's mtime
  dirty_dn_diri(dn, dirpv, mdr->now);
  
  // bump target popularity
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);

  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    dout(7) << "_unlink_local_finish sending MDentryUnlink to mds" << it->first << endl;
    MDentryUnlink *unlink = new MDentryUnlink(dn->dir->dirfrag(), dn->name);
    if (straydn) {
      unlink->strayin = straydn->dir->inode->replicate_to(it->first);
      unlink->straydir = straydn->dir->replicate_to(it->first);
      unlink->straydn = straydn->replicate_to(it->first);
    }
    mds->send_message_mds(unlink, it->first, MDS_PORT_CACHE);
  }
  
  // commit anchor update?
  if (mdr->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->dst_reanchor_atid);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->dir->get_inode());  // FIXME: imprecise ref
  
  // clean up?
  if (straydn)
    mdcache->eval_stray(straydn);
}


class C_MDS_unlink_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  version_t dnpv;  // deleted dentry
  version_t dirpv;
public:
  C_MDS_unlink_remote_finish(MDS *m, MDRequest *r, CDentry *d, 
			     version_t dirpv_) :
    mds(m), mdr(r), dn(d), 
    dnpv(d->get_projected_version()), dirpv(dirpv_) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_remote_finish(mdr, dn, dnpv, dirpv);
  }
};

void Server::_unlink_remote(MDRequest *mdr, CDentry *dn) 
{
  dout(10) << "_unlink_remote " << *dn << " " << *dn->inode << endl;

  // 1. send LinkPrepare to dest (journal nlink-- prepare)
  int inauth = dn->inode->authority().first;
  if (mdr->witnessed.count(inauth) == 0) {
    dout(10) << " inode auth must prepare nlink--" << endl;

    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_UNLINKPREP);
    dn->inode->set_object_info(req->get_object_info());
    req->now = mdr->now;
    mds->send_message_mds(req, inauth, MDS_PORT_SERVER);

    assert(mdr->waiting_on_slave.count(inauth) == 0);
    mdr->waiting_on_slave.insert(inauth);
    return;
  }
  dout(10) << " inode auth has prepared nlink--" << endl;

  // ok, let's do it.
  // prepare log entry
  EUpdate *le = new EUpdate("unlink_remote");
  le->metablob.add_client_req(mdr->reqid);

  // the unlinked dentry
  dn->pre_dirty();
  version_t dirpv = predirty_dn_diri(mdr, dn, &le->metablob, mdr->now);
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_null_dentry(dn, true);

  if (mdr->dst_reanchor_atid)
    le->metablob.add_anchor_transaction(mdr->dst_reanchor_atid);

  // finisher
  C_MDS_unlink_remote_finish *fin = new C_MDS_unlink_remote_finish(mds, mdr, dn, dirpv);
  
  journal_opens();  // journal pending opens, just in case
  
  // mark committing (needed for proper recovery)
  mdr->committing = true;

  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
  
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);
}

void Server::_unlink_remote_finish(MDRequest *mdr, 
				   CDentry *dn, 
				   version_t dnpv, version_t dirpv) 
{
  dout(10) << "_unlink_remote_finish " << *dn << endl;

  // unlink main dentry
  dn->dir->unlink_inode(dn);
  dn->mark_dirty(dnpv);  // dirty old dentry

  // dir inode's mtime
  dirty_dn_diri(dn, dirpv, mdr->now);
    
  // bump target popularity
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);

  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    dout(7) << "_unlink_remote_finish sending MDentryUnlink to mds" << it->first << endl;
    MDentryUnlink *unlink = new MDentryUnlink(dn->dir->dirfrag(), dn->name);
    mds->send_message_mds(unlink, it->first, MDS_PORT_CACHE);
  }

  // commit anchor update?
  if (mdr->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->dst_reanchor_atid);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, dn->dir->get_inode());  // FIXME: imprecise ref
}




/** _verify_rmdir
 *
 * verify that a directory is empty (i.e. we can rmdir it),
 * and make sure it is part of the same subtree (i.e. local)
 * so that rmdir will occur locally.
 *
 * @param in is the inode being rmdir'd.
 */
bool Server::_verify_rmdir(MDRequest *mdr, CInode *in)
{
  dout(10) << "_verify_rmdir " << *in << endl;
  assert(in->is_auth());

  list<frag_t> frags;
  in->dirfragtree.get_leaves(frags);

  for (list<frag_t>::iterator p = frags.begin();
       p != frags.end();
       ++p) {
    CDir *dir = in->get_dirfrag(*p);
    if (!dir) 
      dir = in->get_or_open_dirfrag(mdcache, *p);
    assert(dir);

    // dir looks empty but incomplete?
    if (dir->is_auth() &&
	dir->get_size() == 0 && 
	!dir->is_complete()) {
      dout(7) << "_verify_rmdir fetching incomplete dir " << *dir << endl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }
    
    // does the frag _look_ empty?
    if (dir->get_size()) {
      dout(10) << "_verify_rmdir still " << dir->get_size() << " items in frag " << *dir << endl;
      reply_request(mdr, -ENOTEMPTY);
      return false;
    }
    
    // not dir auth?
    if (!dir->is_auth()) {
      dout(10) << "_verify_rmdir not auth for " << *dir << ", FIXME BUG" << endl;
      reply_request(mdr, -ENOTEMPTY);
      return false;
    }
  }

  return true;
}
/*
      // export sanity check
      if (!in->is_auth()) {
        // i should be exporting this now/soon, since the dir is empty.
        dout(7) << "handle_client_rmdir dir is auth, but not inode." << endl;
	mdcache->migrator->export_empty_import(in->dir);          
        in->dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mds, req, diri));
        return;
      }
*/




// ======================================================


class C_MDS_rename_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *srcdn;
  CDentry *destdn;
  CDentry *straydn;
public:
  C_MDS_rename_finish(MDS *m, MDRequest *r,
		      CDentry *sdn, CDentry *ddn, CDentry *stdn) :
    mds(m), mdr(r),
    srcdn(sdn), destdn(ddn), straydn(stdn) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_rename_finish(mdr, srcdn, destdn, straydn);
  }
};


/** handle_client_rename
 *
 */
void Server::handle_client_rename(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  dout(7) << "handle_client_rename " << *req << endl;

  // traverse to dest dir (not dest)
  //  we do this FIRST, because the rename should occur on the 
  //  destdn's auth.
  const filepath &destpath = req->get_sarg();
  const string &destname = destpath.last_dentry();
  vector<CDentry*> desttrace;
  CDir *destdir = traverse_to_auth_dir(mdr, desttrace, destpath);
  if (!destdir) return;  // fw or error out
  dout(10) << "dest will be " << destname << " in " << *destdir << endl;
  assert(destdir->is_auth());

  // traverse to src
  filepath srcpath = req->get_filepath();
  vector<CDentry*> srctrace;
  int r = mdcache->path_traverse(mdr, req,
				 0, srcpath, srctrace, false,
				 MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  if (srctrace.empty()) r = -EINVAL;  // can't rename root
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  CDentry *srcdn = srctrace[srctrace.size()-1];
  dout(10) << " srcdn " << *srcdn << endl;
  CInode *srci = mdcache->get_dentry_inode(srcdn, mdr);
  dout(10) << " srci " << *srci << endl;

  // -- some sanity checks --
  // src == dest?
  if (srcdn->get_dir() == destdir && srcdn->name == destname) {
    dout(7) << "rename src=dest, noop" << endl;
    reply_request(mdr, 0);
    return;
  }

  // dest a child of src?
  // e.g. mv /usr /usr/foo
  CDentry *pdn = destdir->inode->parent;
  while (pdn) {
    if (pdn == srcdn) {
      dout(7) << "cannot rename item to be a child of itself" << endl;
      reply_request(mdr, -EINVAL);
      return;
    }
    pdn = pdn->dir->inode->parent;
  }


  // identify/create dest dentry
  CDentry *destdn = destdir->lookup(destname);
  if (destdn && !destdn->lock.can_rdlock(mdr)) {
    destdn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return;
  }

  CInode *oldin = 0;
  if (destdn && !destdn->is_null()) {
    //dout(10) << "dest dn exists " << *destdn << endl;
    oldin = mdcache->get_dentry_inode(destdn, mdr);
    if (!oldin) return;
    dout(10) << " oldin " << *oldin << endl;
    
    // mv /some/thing /to/some/existing_other_thing
    if (oldin->is_dir() && !srci->is_dir()) {
      reply_request(mdr, -EISDIR);
      return;
    }
    if (!oldin->is_dir() && srci->is_dir()) {
      reply_request(mdr, -ENOTDIR);
      return;
    }

    // non-empty dir?
    if (oldin->is_dir() && !_verify_rmdir(mdr, oldin))      
      return;
  }
  if (!destdn) {
    // mv /some/thing /to/some/non_existent_name
    destdn = prepare_null_dentry(mdr, destdir, destname);
    if (!destdn) return;
  }

  dout(10) << " destdn " << *destdn << endl;


  // -- locks --
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // rdlock sourcedir path, xlock src dentry
  for (int i=0; i<(int)srctrace.size()-1; i++) 
    rdlocks.insert(&srctrace[i]->lock);
  xlocks.insert(&srcdn->lock);
  wrlocks.insert(&srcdn->dir->inode->dirlock);

  // rdlock destdir path, xlock dest dentry
  for (int i=0; i<(int)desttrace.size(); i++)
    rdlocks.insert(&desttrace[i]->lock);
  xlocks.insert(&destdn->lock);
  wrlocks.insert(&destdn->dir->inode->dirlock);

  // xlock oldin (for nlink--)
  if (oldin) xlocks.insert(&oldin->linklock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;
  
  // set done_locking flag, to avoid problems with wrlock moving auth target
  mdr->done_locking = true;

  // -- open all srcdn inode frags, if any --
  // we need these open so that auth can properly delegate from inode to dirfrags
  // after the inode is _ours_.
  if (srcdn->is_primary() && 
      !srcdn->is_auth() && 
      srci->is_dir()) {
    dout(10) << "srci is remote dir, opening all frags" << endl;
    list<frag_t> frags;
    srci->dirfragtree.get_leaves(frags);
    for (list<frag_t>::iterator p = frags.begin();
	 p != frags.end();
	 ++p) {
      CDir *dir = srci->get_dirfrag(*p);
      if (dir) {
	dout(10) << " opened " << *dir << endl;
	mdr->pin(dir);
      } else {
	mdcache->open_remote_dir(srci, *p, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }
  }

  // -- declare now --
  if (mdr->now == utime_t())
    mdr->now = g_clock.real_now();

  // -- create stray dentry? --
  CDentry *straydn = 0;
  if (destdn->is_primary()) {
    straydn = mdcache->get_or_create_stray_dentry(destdn->inode);
    dout(10) << "straydn is " << *straydn << endl;
  }

  // -- prepare witnesses --
  set<int> witnesses = mdr->extra_witnesses;
  if (srcdn->is_auth())
    srcdn->list_replicas(witnesses);
  else
    witnesses.insert(srcdn->authority().first);
  destdn->list_replicas(witnesses);

  for (set<int>::iterator p = witnesses.begin();
       p != witnesses.end();
       ++p) {
    if (mdr->witnessed.count(*p)) {
      dout(10) << " already witnessed by mds" << *p << endl;
    } else {
      dout(10) << " not yet witnessed by mds" << *p << ", sending prepare" << endl;
      MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEPREP);
      srcdn->make_path(req->srcdnpath);
      destdn->make_path(req->destdnpath);
      req->now = mdr->now;

      if (straydn) {
	CInodeDiscover *indis = straydn->dir->inode->replicate_to(*p);
	CDirDiscover *dirdis = straydn->dir->replicate_to(*p);
	CDentryDiscover *dndis = straydn->replicate_to(*p);
	indis->_encode(req->stray);
	dirdis->_encode(req->stray);
	dndis->_encode(req->stray);
	delete indis;
	delete dirdis;
	delete dndis;
      }

      mds->send_message_mds(req, *p, MDS_PORT_SERVER);

      assert(mdr->waiting_on_slave.count(*p) == 0);
      mdr->waiting_on_slave.insert(*p);
    }
  }
  if (!mdr->waiting_on_slave.empty())
    return;  // we're waiting for a witness.

  // -- inode migration? --
  if (!srcdn->is_auth() &&
      srcdn->is_primary()) {
    if (mdr->inode_import.length() == 0) {
      // get inode
      int auth = srcdn->authority().first;
      dout(10) << " requesting inode export from srcdn auth mds" << auth << endl;
      MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEGETINODE);
      srcdn->make_path(req->srcdnpath);
      mds->send_message_mds(req, auth, MDS_PORT_SERVER);

      assert(mdr->waiting_on_slave.count(auth) == 0);
      mdr->waiting_on_slave.insert(auth);
      return;
    }
    dout(10) << " already (just!) got inode export from srcdn auth" << endl;
  }
  
  // -- prepare anchor updates -- 
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  if (!linkmerge) {
    C_Gather *anchorgather = 0;

    if (srcdn->is_primary() && srcdn->inode->is_anchored() &&
	srcdn->dir != destdn->dir &&
	!mdr->src_reanchor_atid) {
      dout(10) << "reanchoring src->dst " << *srcdn->inode << endl;
      vector<Anchor> trace;
      destdn->make_anchor_trace(trace, srcdn->inode);
      
      anchorgather = new C_Gather(new C_MDS_RetryRequest(mdcache, mdr));
      mds->anchorclient->prepare_update(srcdn->inode->ino(), trace, &mdr->src_reanchor_atid, 
					anchorgather->new_sub());
    }
    if (destdn->is_primary() &&
	destdn->inode->is_anchored() &&
	!mdr->dst_reanchor_atid) {
      dout(10) << "reanchoring dst->stray " << *destdn->inode << endl;

      assert(straydn);
      vector<Anchor> trace;
      straydn->make_anchor_trace(trace, destdn->inode);
      
      if (!anchorgather)
	anchorgather = new C_Gather(new C_MDS_RetryRequest(mdcache, mdr));
      mds->anchorclient->prepare_update(destdn->inode->ino(), trace, &mdr->dst_reanchor_atid, 
					anchorgather->new_sub());
    }

    if (anchorgather) 
      return;  // waiting for anchor prepares
  }

  // -- prepare journal entry --
  EUpdate *le = new EUpdate("rename");
  le->metablob.add_client_req(mdr->reqid);
  
  _rename_prepare(mdr, &le->metablob, srcdn, destdn, straydn);

  // -- commit locally --
  C_MDS_rename_finish *fin = new C_MDS_rename_finish(mds, mdr, srcdn, destdn, straydn);

  // and apply!
  // we do this now because we may also be importing an inode, and the locker is currently
  // depending on a this happening quickly.
  _rename_apply(mdr, srcdn, destdn, straydn);

  journal_opens();  // journal pending opens, just in case

  // mark committing (needed for proper recovery)
  mdr->committing = true;
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


void Server::_rename_finish(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_finish " << *mdr << endl;

  // apply
  //_rename_apply(mdr, srcdn, destdn, straydn);
  
  // commit anchor updates?
  if (mdr->src_reanchor_atid) mds->anchorclient->commit(mdr->src_reanchor_atid);
  if (mdr->dst_reanchor_atid) mds->anchorclient->commit(mdr->dst_reanchor_atid);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply, destdn->dir->get_inode());  // FIXME: imprecise ref
  
  // clean up?
  if (straydn) 
    mdcache->eval_stray(straydn);
}



// helpers

void Server::_rename_prepare(MDRequest *mdr,
			     EMetaBlob *metablob, 
			     CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_prepare " << *mdr << " " << *srcdn << " " << *destdn << endl;

  // primary+remote link merge?
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  inode_t *pi = 0; // inode getting nlink--
  version_t ipv;   // it's version
  
  if (linkmerge) {
    dout(10) << "will merge remote+primary links" << endl;

    // destdn -> primary
    metablob->add_dir_context(destdn->dir);
    if (destdn->is_auth())
      ipv = mdr->pvmap[destdn] = destdn->pre_dirty(destdn->inode->inode.version);
    pi = metablob->add_primary_dentry(destdn, true, destdn->inode); 
    
    // do src dentry
    metablob->add_dir_context(srcdn->dir);
    if (srcdn->is_auth())
      mdr->pvmap[srcdn] = srcdn->pre_dirty();
    metablob->add_null_dentry(srcdn, true);

  } else {
    // move to stray?
    if (destdn->is_primary()) {
      // primary.  we'll move inode to stray dir.
      assert(straydn);

      // link-- inode, move to stray dir.
      metablob->add_dir_context(straydn->dir);
      if (straydn->is_auth())
	ipv = mdr->pvmap[straydn] = straydn->pre_dirty(destdn->inode->inode.version);
      pi = metablob->add_primary_dentry(straydn, true, destdn->inode);
    } 
    else if (destdn->is_remote()) {
      // remote.
      // nlink-- targeti
      metablob->add_dir_context(destdn->inode->get_parent_dir());
      if (destdn->inode->is_auth())
	ipv = mdr->pvmap[destdn->inode] = destdn->inode->pre_dirty();
      pi = metablob->add_primary_dentry(destdn->inode->parent, true, destdn->inode);  // update primary
      dout(10) << "remote targeti (nlink--) is " << *destdn->inode << endl;
    }
    else {
      assert(destdn->is_null());
    }

    // add dest dentry
    metablob->add_dir_context(destdn->dir);
    if (srcdn->is_primary()) {
      dout(10) << "src is a primary dentry" << endl;
      if (destdn->is_auth()) {
	mdr->pvmap[destdn] = destdn->pre_dirty(MAX(srcdn->inode->inode.version,
						   mdr->inode_import_v));
      }
      metablob->add_primary_dentry(destdn, true, srcdn->inode); 

    } else {
      assert(srcdn->is_remote());
      dout(10) << "src is a remote dentry" << endl;
      if (destdn->is_auth())
	mdr->pvmap[destdn] = destdn->pre_dirty();
      metablob->add_remote_dentry(destdn, true, srcdn->get_remote_ino()); 
    }
    
    // remove src dentry
    metablob->add_dir_context(srcdn->dir);
    if (srcdn->is_auth())
      mdr->pvmap[srcdn] = srcdn->pre_dirty();
    metablob->add_null_dentry(srcdn, true);
  }

  if (pi) {
    // update journaled target inode
    pi->nlink--;
    pi->ctime = mdr->now;
    pi->version = ipv;
  }

  // anchor updates?
  if (mdr->src_reanchor_atid)
    metablob->add_anchor_transaction(mdr->src_reanchor_atid);
  if (mdr->dst_reanchor_atid)
    metablob->add_anchor_transaction(mdr->dst_reanchor_atid);
}


void Server::_rename_apply(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_rename_apply " << *mdr << " " << *srcdn << " " << *destdn << endl;
  dout(10) << " pvs " << mdr->pvmap << endl;

  CInode *oldin = destdn->inode;
  
  // primary+remote link merge?
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  // dir mtimes
  /*
  dirty_dn_diri(destdn, ddirpv, ictime);
  if (destdn->dir != srcdn->dir)
    dirty_dn_diri(srcdn, sdirpv, ictime);
  */

  if (linkmerge) {
    if (destdn->is_primary()) {
      dout(10) << "merging remote onto primary link" << endl;

      // nlink-- in place
      destdn->inode->inode.nlink--;
      destdn->inode->inode.ctime = mdr->now;
      if (destdn->inode->is_auth())
	destdn->inode->mark_dirty(mdr->pvmap[destdn]);

      // unlink srcdn
      srcdn->dir->unlink_inode(srcdn);
      if (srcdn->is_auth())
	srcdn->mark_dirty(mdr->pvmap[srcdn]);
    } else {
      dout(10) << "merging primary onto remote link" << endl;
      assert(srcdn->is_primary());
      
      // move inode to dest
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->unlink_inode(destdn);
      destdn->dir->link_inode(destdn, oldin);
      
      // nlink--
      destdn->inode->inode.nlink--;
      destdn->inode->inode.ctime = mdr->now;
      if (destdn->inode->is_auth())
	destdn->inode->mark_dirty(mdr->pvmap[destdn]);
      
      // mark src dirty
      if (srcdn->is_auth())
	srcdn->mark_dirty(mdr->pvmap[srcdn]);
    }
  } 
  else {
    // unlink destdn?
    if (!destdn->is_null())
      destdn->dir->unlink_inode(destdn);

    if (straydn) {
      dout(10) << "straydn is " << *straydn << endl;

      // relink oldin to stray dir.  destdn was primary.
      assert(oldin);
      straydn->dir->link_inode(straydn, oldin);
      //assert(straypv == ipv);

      // nlink-- in stray dir.
      oldin->inode.nlink--;
      oldin->inode.ctime = mdr->now;
      if (oldin->is_auth())
	oldin->mark_dirty(mdr->pvmap[straydn]);
    }
    else if (oldin) {
      // nlink-- remote.  destdn was remote.
      oldin->inode.nlink--;
      oldin->inode.ctime = mdr->now;
      if (oldin->is_auth())
	oldin->mark_dirty(mdr->pvmap[oldin]);
    }
    
    CInode *in = srcdn->inode;
    assert(in);
    if (srcdn->is_remote()) {
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->link_inode(destdn, in->ino());    
    } else {
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->link_inode(destdn, in);
    }
    if (destdn->is_auth())
      destdn->mark_dirty(mdr->pvmap[destdn]);
    if (srcdn->is_auth())
      srcdn->mark_dirty(mdr->pvmap[srcdn]);

    // srcdn inode import?
    if (!srcdn->is_auth() && destdn->is_primary() && destdn->is_auth()) {
      assert(mdr->inode_import.length() > 0);
      int off = 0;
      mdcache->migrator->decode_import_inode(destdn, mdr->inode_import, off, 
					     srcdn->authority().first);
    }
  }

  // update subtree map?
  if (destdn->is_primary() && destdn->inode->is_dir()) 
    mdcache->adjust_subtree_after_rename(destdn->inode, srcdn->dir);
}





// ------------
// SLAVE

class C_MDS_SlaveRenamePrep : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *srcdn, *destdn, *straydn;
public:
  C_MDS_SlaveRenamePrep(Server *s, MDRequest *m, CDentry *sr, CDentry *de, CDentry *st) :
    server(s), mdr(m), srcdn(sr), destdn(de), straydn(st) {}
  void finish(int r) {
    server->_logged_slave_rename(mdr, srcdn, destdn, straydn);
  }
};

class C_MDS_SlaveRenameCommit : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *srcdn, *destdn, *straydn;
public:
  C_MDS_SlaveRenameCommit(Server *s, MDRequest *m, CDentry *sr, CDentry *de, CDentry *st) :
    server(s), mdr(m), srcdn(sr), destdn(de), straydn(st) {}
  void finish(int r) {
    server->_commit_slave_rename(mdr, r, srcdn, destdn, straydn);
  }
};

void Server::handle_slave_rename_prep(MDRequest *mdr)
{
  dout(10) << "handle_slave_rename_prep " << *mdr 
	   << " " << mdr->slave_request->srcdnpath 
	   << " to " << mdr->slave_request->destdnpath
	   << endl;

  // discover destdn
  filepath destpath(mdr->slave_request->destdnpath);
  dout(10) << " dest " << destpath << endl;
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, mdr->slave_request, 
				 0, destpath, trace, false,
				 MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);  // we shouldn't get an error here!
      
  CDentry *destdn = trace[trace.size()-1];
  dout(10) << " destdn " << *destdn << endl;
  mdr->pin(destdn);
  
      
  // discover srcdn
  filepath srcpath(mdr->slave_request->srcdnpath);
  dout(10) << " src " << srcpath << endl;
  r = mdcache->path_traverse(mdr, mdr->slave_request,
			     0, srcpath, trace, false,  
			     MDS_TRAVERSE_DISCOVERXLOCK);
  if (r > 0) return;
  assert(r == 0);  // we shouldn't get an error here!
      
  CDentry *srcdn = trace[trace.size()-1];
  dout(10) << " srcdn " << *srcdn << endl;
  mdr->pin(srcdn);

  // stray?
  CDentry *straydn = 0;
  if (destdn->is_primary()) {
    assert(mdr->slave_request->stray.length() > 0);
    straydn = mdcache->add_replica_stray(mdr->slave_request->stray, 
					 destdn->inode, mdr->slave_to_mds);
    assert(straydn);
    mdr->pin(straydn);
  }

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate("slave_rename_prep", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_PREPARE);
  
  mdr->now = mdr->slave_request->now;
  _rename_prepare(mdr, &le->metablob, srcdn, destdn, straydn);

  mds->mdlog->submit_entry(le, new C_MDS_SlaveRenamePrep(this, mdr, srcdn, destdn, straydn));
}

void Server::_logged_slave_rename(MDRequest *mdr, 
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_logged_slave_rename " << *mdr << endl;

  // ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEPREPACK);
  if (srcdn->is_auth()) {
    // share the replica list, so that they can all witness the rename.
    srcdn->list_replicas(reply->srcdn_replicas);

    // note srcdn, we'll get asked for inode momentarily
    mdr->srcdn = srcdn;
  }  

  mds->send_message_mds(reply, mdr->slave_to_mds, MDS_PORT_SERVER);
  
  // set up commit waiter
  mdr->slave_commit = new C_MDS_SlaveRenameCommit(this, mdr, srcdn, destdn, straydn);

  // done.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}

void Server::_commit_slave_rename(MDRequest *mdr, int r,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn)
{
  dout(10) << "_commit_slave_rename " << *mdr << " r=" << r << endl;

  ESlaveUpdate *le;
  if (r == 0) {
    // commit
    _rename_apply(mdr, srcdn, destdn, straydn);
    
    // write a commit to the journal
    le = new ESlaveUpdate("slave_rename_commit", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_COMMIT);
  } else {
    // abort
    le = new ESlaveUpdate("slave_rename_abort", mdr->reqid, mdr->slave_to_mds, ESlaveUpdate::OP_ABORT);
  }
  mds->mdlog->submit_entry(le);
}

void Server::handle_slave_rename_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m)
{
  dout(10) << "handle_slave_rename_prep_ack " << *mdr 
	   << " witnessed by " << m->get_source()
	   << " " << *m << endl;
  int from = m->get_source().num();

  // note slave
  mdr->slaves.insert(from);

  // witnessed!
  assert(mdr->witnessed.count(from) == 0);
  mdr->witnessed.insert(from);


  // add extra witnesses?
  if (!m->srcdn_replicas.empty()) {
    dout(10) << " extra witnesses (srcdn replicas) are " << m->srcdn_replicas << endl;
    mdr->extra_witnesses = m->srcdn_replicas;
    mdr->extra_witnesses.erase(mds->get_nodeid());  // not me!
  }

  if (m->inode_export.length()) {
    dout(10) << " got inode export, saving in " << *mdr << endl;
    mdr->inode_import.claim(m->inode_export);
    mdr->inode_import_v = m->inode_export_v;
  }

  // remove from waiting list
  assert(mdr->waiting_on_slave.count(from));
  mdr->waiting_on_slave.erase(from);

  if (mdr->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->waiting_on_slave << endl;
}



void Server::handle_slave_rename_get_inode(MDRequest *mdr)
{
  dout(10) << "handle_slave_rename_get_inode " << *mdr << endl;

  assert(mdr->srcdn);
  assert(mdr->srcdn->is_auth());
  assert(mdr->srcdn->is_primary());

  // reply
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_RENAMEGETINODEACK);
  dout(10) << " replying with inode export info" << endl;
  mdcache->migrator->encode_export_inode(mdr->srcdn->inode, reply->inode_export, mdr->slave_to_mds);
  reply->inode_export_v = mdr->srcdn->inode->inode.version;

  mdr->inode_import = reply->inode_export;   // keep a copy locally, in case we have to rollback
  
  mds->send_message_mds(reply, mdr->slave_to_mds, MDS_PORT_SERVER);

  // clean up.
  delete mdr->slave_request;
  mdr->slave_request = 0;
}

void Server::handle_slave_rename_get_inode_ack(MDRequest *mdr, MMDSSlaveRequest *m)
{
  dout(10) << "handle_slave_rename_get_inode_ack " << *mdr 
	   << " " << *m << endl;
  int from = m->get_source().num();

  assert(m->inode_export.length());
  dout(10) << " got inode export, saving in " << *mdr << endl;
  mdr->inode_import.claim(m->inode_export);
  mdr->inode_import_v = m->inode_export_v;

  assert(mdr->waiting_on_slave.count(from));
  mdr->waiting_on_slave.erase(from);

  if (mdr->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->waiting_on_slave << endl;
}






// ===================================
// TRUNCATE, FSYNC

class C_MDS_truncate_purged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  off_t size;
  utime_t ctime;
public:
  C_MDS_truncate_purged(MDS *m, MDRequest *r, CInode *i, version_t pdv, off_t sz, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    size(sz), ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // apply to cache
    in->inode.size = size;
    in->inode.ctime = ctime;
    in->inode.mtime = ctime;
    in->mark_dirty(pv);

    // hit pop
    mds->balancer->hit_inode(in, META_POP_IWR);   

    // reply
    mds->server->reply_request(mdr, 0);
  }
};

class C_MDS_truncate_logged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  off_t size;
  utime_t ctime;
public:
  C_MDS_truncate_logged(MDS *m, MDRequest *r, CInode *i, version_t pdv, off_t sz, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    size(sz), ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // purge
    mds->mdcache->purge_inode(&in->inode, size);
    mds->mdcache->wait_for_purge(in->inode.ino, size, 
				 new C_MDS_truncate_purged(mds, mdr, in, pv, size, ctime));
  }
};

void Server::handle_client_truncate(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  CInode *cur = rdlock_path_pin_ref(mdr, true);
  if (!cur) return;

  // check permissions?  

  // xlock inode
  if (!mds->locker->xlock_start(&cur->filelock, mdr))
    return;  // fw or (wait for) lock
  
  // already small enough?
  if (cur->inode.size >= req->args.truncate.length) {
    reply_request(mdr, 0);
    return;
  }

  // prepare
  version_t pdv = cur->pre_dirty();
  utime_t ctime = g_clock.real_now();
  Context *fin = new C_MDS_truncate_logged(mds, mdr, cur, 
					   pdv, req->args.truncate.length, ctime);
  
  // log + wait
  EUpdate *le = new EUpdate("truncate");
  le->metablob.add_client_req(mdr->reqid);
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_inode_truncate(cur->inode, req->args.truncate.length);
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mtime = ctime;
  pi->ctime = ctime;
  pi->version = pdv;
  pi->size = req->args.truncate.length;
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


// ===========================
// open, openc, close

void Server::handle_client_open(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  int flags = req->args.open.flags;
  int cmode = req->get_open_file_mode();
  bool need_auth = ((cmode != FILE_MODE_R && cmode != FILE_MODE_LAZY) ||
		    (flags & O_TRUNC));
  dout(10) << "open flags = " << flags
	   << ", filemode = " << cmode
	   << ", need_auth = " << need_auth
	   << endl;

  CInode *cur = rdlock_path_pin_ref(mdr, need_auth);
  if (!cur) return;
  
  // regular file?
  if ((cur->inode.mode & INODE_TYPE_MASK) != INODE_MODE_FILE) {
    dout(7) << "not a regular file " << *cur << endl;
    reply_request(mdr, -EINVAL);                 // FIXME what error do we want?
    return;
  }

  // hmm, check permissions or something.


  // O_TRUNC
  if (flags & O_TRUNC) {
    assert(cur->is_auth());

    // xlock file size
    if (!mds->locker->xlock_start(&cur->filelock, mdr))
      return;
    
    if (cur->inode.size > 0) {
      handle_client_opent(mdr);
      return;
    }
  }
  
  // do it
  _do_open(mdr, cur);
}

void Server::_do_open(MDRequest *mdr, CInode *cur)
{
  MClientRequest *req = mdr->client_request;
  int cmode = req->get_open_file_mode();

  // can we issue the caps they want?
  version_t fdv = mds->locker->issue_file_data_version(cur);
  Capability *cap = mds->locker->issue_new_caps(cur, cmode, req);
  if (!cap) return; // can't issue (yet), so wait!
  
  dout(12) << "_do_open issuing caps " << cap_string(cap->pending())
	   << " for " << req->get_source()
	   << " on " << *cur << endl;
  
  // hit pop
  if (cmode == FILE_MODE_RW ||
      cmode == FILE_MODE_W) 
    mds->balancer->hit_inode(cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(cur, META_POP_IRD);

  // reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_file_caps(cap->pending());
  reply->set_file_caps_seq(cap->get_last_seq());
  reply->set_file_data_version(fdv);
  reply_request(mdr, reply, cur);

  // journal?
  if (cur->last_open_journaled == 0) {
    queue_journal_open(cur);
    maybe_journal_opens();
  }

}

void Server::queue_journal_open(CInode *in)
{
  dout(10) << "queue_journal_open on " << *in << endl;

  if (journal_open_queue.count(in) == 0) {
    // pin so our pointer stays valid
    in->get(CInode::PIN_BATCHOPENJOURNAL);
    
    // queue it up for a bit
    journal_open_queue.insert(in);
  }
}


void Server::journal_opens()
{
  dout(10) << "journal_opens " << journal_open_queue.size() << " inodes" << endl;
  if (journal_open_queue.empty()) return;

  EOpen *le = 0;

  // check queued inodes
  for (set<CInode*>::iterator p = journal_open_queue.begin();
       p != journal_open_queue.end();
       ++p) {
    (*p)->put(CInode::PIN_BATCHOPENJOURNAL);
    if ((*p)->is_any_caps()) {
      if (!le) le = new EOpen;
      le->add_inode(*p);
      (*p)->last_open_journaled = mds->mdlog->get_write_pos();
    }
  }
  journal_open_queue.clear();
  
  if (le) {
    // journal
    mds->mdlog->submit_entry(le);
  
    // add waiters to journal entry
    for (list<Context*>::iterator p = journal_open_waiters.begin();
	 p != journal_open_waiters.end();
	 ++p) 
      mds->mdlog->wait_for_sync(*p);
    journal_open_waiters.clear();
  } else {
    // nothing worth journaling here, just kick the waiters.
    mds->queue_waiters(journal_open_waiters);
  }
}




class C_MDS_open_truncate_purged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  utime_t ctime;
public:
  C_MDS_open_truncate_purged(MDS *m, MDRequest *r, CInode *i, version_t pdv, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // apply to cache
    in->inode.size = 0;
    in->inode.ctime = ctime;
    in->inode.mtime = ctime;
    in->mark_dirty(pv);
    
    // hit pop
    mds->balancer->hit_inode(in, META_POP_IWR);   
    
    // do the open
    mds->server->_do_open(mdr, in);
  }
};

class C_MDS_open_truncate_logged : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  version_t pv;
  utime_t ctime;
public:
  C_MDS_open_truncate_logged(MDS *m, MDRequest *r, CInode *i, version_t pdv, utime_t ct) :
    mds(m), mdr(r), in(i), 
    pv(pdv),
    ctime(ct) { }
  void finish(int r) {
    assert(r == 0);

    // purge also...
    mds->mdcache->purge_inode(&in->inode, 0);
    mds->mdcache->wait_for_purge(in->inode.ino, 0,
				 new C_MDS_open_truncate_purged(mds, mdr, in, pv, ctime));
  }
};


void Server::handle_client_opent(MDRequest *mdr)
{
  CInode *cur = mdr->ref;
  assert(cur);

  // prepare
  version_t pdv = cur->pre_dirty();
  utime_t ctime = g_clock.real_now();
  Context *fin = new C_MDS_open_truncate_logged(mds, mdr, cur, 
						pdv, ctime);
  
  // log + wait
  EUpdate *le = new EUpdate("open_truncate");
  le->metablob.add_client_req(mdr->reqid);
  le->metablob.add_dir_context(cur->get_parent_dir());
  le->metablob.add_inode_truncate(cur->inode, 0);
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mtime = ctime;
  pi->ctime = ctime;
  pi->version = pdv;
  pi->size = 0;
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}



class C_MDS_openc_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *newi;
  version_t pv;
public:
  C_MDS_openc_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni) :
    mds(m), mdr(r), dn(d), newi(ni),
    pv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_inode(dn, newi);

    // dirty inode, dn, dir
    newi->mark_dirty(pv);

    // downgrade xlock to rdlock
    //mds->locker->dentry_xlock_downgrade_to_rdlock(dn, mdr);

    // set/pin ref inode for open()
    mdr->ref = newi;
    mdr->pin(newi);
    
    // hit pop
    mds->balancer->hit_inode(newi, META_POP_IWR);

    // ok, do the open.
    mds->server->handle_client_open(mdr);
  }
};


void Server::handle_client_openc(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << endl;
  
  bool excl = (req->args.open.flags & O_EXCL);
  CDentry *dn = rdlock_path_xlock_dentry(mdr, !excl, false);
  if (!dn) return;

  if (!dn->is_null()) {
    // it existed.  
    if (req->args.open.flags & O_EXCL) {
      dout(10) << "O_EXCL, target exists, failing with -EEXIST" << endl;
      reply_request(mdr, -EEXIST, dn->get_dir()->get_inode());
      return;
    } 
    
    // pass to regular open handler.
    handle_client_open(mdr);
    return;
  }

  // created null dn.
    
  // create inode.
  CInode *in = prepare_new_inode(req, dn->dir);
  assert(in);
  
  // it's a file.
  dn->pre_dirty();
  in->inode.mode = req->args.open.mode;
  in->inode.mode |= INODE_MODE_FILE;
  
  // prepare finisher
  C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, mdr, dn, in);
  EUpdate *le = new EUpdate("openc");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, in);
  pi->version = dn->get_projected_version();
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
  
  /*
    FIXME. this needs to be rewritten when the write capability stuff starts
    getting journaled.  
  */
}














