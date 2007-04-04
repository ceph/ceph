// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MLock.h"

#include "messages/MDentryUnlink.h"
#include "messages/MInodeLink.h"

#include "events/EString.h"
#include "events/EUpdate.h"
#include "events/EMount.h"
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
  // active?
  if (!mds->is_active()) {
    dout(3) << "not active yet, waiting" << endl;
    mds->queue_waitfor_active(new C_MDS_RetryMessage(mds, m));
    return;
  }

  switch (m->get_type()) {
  case MSG_CLIENT_MOUNT:
    handle_client_mount((MClientMount*)m);
    return;
  case MSG_CLIENT_UNMOUNT:
    handle_client_unmount(m);
    return;
  case MSG_CLIENT_REQUEST:
    handle_client_request((MClientRequest*)m);
    return;

  }

  dout(1) << " main unknown message " << m->get_type() << endl;
  assert(0);
}



// ----------------------------------------------------------
// MOUNT and UNMOUNT


class C_MDS_mount_finish : public Context {
  MDS *mds;
  Message *m;
  bool mount;
  version_t cmapv;
public:
  C_MDS_mount_finish(MDS *m, Message *msg, bool mnt, version_t mv) :
    mds(m), m(msg), mount(mnt), cmapv(mv) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    if (mount)
      mds->clientmap.add_mount(m->get_source_inst());
    else
      mds->clientmap.rem_mount(m->get_source().num());
    
    assert(cmapv == mds->clientmap.get_version());
    
    // reply
    if (mount) {
      // mounted
      mds->messenger->send_message(new MClientMountAck((MClientMount*)m, mds->mdsmap, mds->osdmap), 
				   m->get_source_inst());
      delete m;
    } else {
      // ack by sending back to client
      mds->messenger->send_message(m, m->get_source_inst());

      // unmounted
      if (g_conf.mds_shutdown_on_last_unmount &&
	  mds->clientmap.get_mount_set().empty()) {
	dout(3) << "all clients done, initiating shutdown" << endl;
	mds->shutdown_start();
      }
    }
  }
};


void Server::handle_client_mount(MClientMount *m)
{
  dout(3) << "mount by " << m->get_source() << " oldv " << mds->clientmap.get_version() << endl;

  // journal it
  version_t cmapv = mds->clientmap.inc_projected();
  mdlog->submit_entry(new EMount(m->get_source_inst(), true, cmapv),
		      new C_MDS_mount_finish(mds, m, true, cmapv));
}

void Server::handle_client_unmount(Message *m)
{
  dout(3) << "unmount by " << m->get_source() << " oldv " << mds->clientmap.get_version() << endl;

  // purge completed requests from clientmap
  mds->clientmap.trim_completed_requests(m->get_source().num(), 0);
  
  // journal it
  version_t cmapv = mds->clientmap.inc_projected();
  mdlog->submit_entry(new EMount(m->get_source_inst(), false, cmapv),
		      new C_MDS_mount_finish(mds, m, false, cmapv));
}




/*******
 * some generic stuff for finishing off requests
 */


/*
 * send generic response (just and error code)
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei)
{
  MClientRequest *req = mdr->client_request();
  reply_request(mdr, new MClientReply(req, r), tracei);
}


/*
 * send given reply
 * include a trace to tracei
 */
void Server::reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei) 
{
  MClientRequest *req = mdr->client_request();
  
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

  // okay, i want
  CInode           *ref = 0;

  // retry?
  if (req->get_retry_attempt()) {
    if (mds->clientmap.have_completed_request(req->get_reqid())) {
      dout(5) << "already completed " << req->get_reqid() << endl;
      mds->messenger->send_message(new MClientReply(req, 0),
				   req->get_source_inst());
      delete req;
      return;
    }
  }
  // trim completed_request list
  if (req->get_oldest_client_tid() > 0)
    mds->clientmap.trim_completed_requests(req->get_source().num(),
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

  dispatch_request(mdr);
  return;
}


void Server::dispatch_request(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();

  if (mdr->ref) {
    dout(7) << "dispatch_request " << *req << " ref " << *mdr->ref << endl;
  } else {
    dout(7) << "dispatch_request " << *req << endl;
  }

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
  in->inode.ctime = in->inode.mtime = in->inode.atime = g_clock.now();   // now
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
  Context *ondelay = new C_MDS_RetryRequest(mdcache, mdr);
  int r = mdcache->path_traverse(mdr,
				 0,
				 refpath, trace, true,
				 mdr->request, ondelay,
				 MDS_TRAVERSE_FORWARD,
				 true); // is MClientRequest
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

  MClientRequest *req = mdr->client_request();

  // traverse
  filepath refpath = req->get_filepath();
  Context *ondelay = new C_MDS_RetryRequest(mdcache, mdr);
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(mdr, 0,
				 refpath, trace, req->follow_trailing_symlink(),
				 req, ondelay,
				 MDS_TRAVERSE_FORWARD,
				 true); // is MClientRequest
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
    if (!ref->can_auth_pin()) {
      dout(7) << "waiting for authpinnable on " << *ref << endl;
      ref->add_waiter(CInode::WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    mdr->auth_pin(ref);
  }

  // lock the path
  set<SimpleLock*> rdlocks;
  set<SimpleLock*> xlocks;

  for (unsigned i=0; i<trace.size(); i++) 
    rdlocks.insert(&trace[i]->lock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, xlocks))
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
  MClientRequest *req = mdr->client_request();

  vector<CDentry*> trace;
  CDir *dir = traverse_to_auth_dir(mdr, trace, req->get_filepath());
  if (!dir) return 0;
  dout(10) << "rdlock_path_xlock_dentry dir " << *dir << endl;

  // make sure we can auth_pin dir
  if (!dir->can_auth_pin()) {
    dout(7) << "waiting for authpinnable on " << *dir << endl;
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
  set<SimpleLock*> rdlocks;
  set<SimpleLock*> xlocks;

  for (unsigned i=0; i<trace.size(); i++) 
    rdlocks.insert(&trace[i]->lock);
  if (dn->is_null())
    xlocks.insert(&dn->lock);   // new dn, xlock
  else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock

  if (!mds->locker->acquire_locks(mdr, rdlocks, xlocks))
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

// ===============================================================================
// STAT

void Server::handle_client_stat(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
  CInode *ref = rdlock_path_pin_ref(mdr, false);
  if (!ref) return;

  // FIXME: this is really not the way to handle the statlite mask.

  // do I need file info?
  int mask = req->args.stat.mask;
  if (mask & (INODE_MASK_SIZE|INODE_MASK_MTIME)) {
    // yes.  do a full stat.
    if (!mds->locker->rdlock_start(&ref->filelock, mdr))
      return;  // syncing
    mds->locker->rdlock_finish(&ref->filelock, mdr);
  } else {
    // nope!  easy peasy.
  }
  
  mds->balancer->hit_inode(ref, META_POP_IRD);   
  
  // reply
  //dout(10) << "reply to " << *req << " stat " << ref->inode.mtime << endl;
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
    MClientReply *reply = new MClientReply(mdr->client_request(), 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


// utime

void Server::handle_client_utime(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
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
  pi->ctime = g_clock.now();
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
    MClientReply *reply = new MClientReply(mdr->client_request(), 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


// chmod

void Server::handle_client_chmod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
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
  pi->ctime = g_clock.now();
  
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
    MClientReply *reply = new MClientReply(mdr->client_request(), 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, in);
  }
};


void Server::handle_client_chown(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
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
  pi->ctime = g_clock.now();
  
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
  MClientRequest *req = mdr->client_request();
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
public:
  C_MDS_mknod_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ni) :
    mds(m), mdr(r), dn(d), newi(ni),
    pv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_inode(dn, newi);

    // dirty inode, dn, dir
    newi->mark_dirty(pv);

    // hit pop
    mds->balancer->hit_inode(newi, META_POP_IWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request(), 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply, newi);
  }
};

void Server::handle_client_mknod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
  
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
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, mdr, dn, newi);
  EUpdate *le = new EUpdate("mknod");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}



// MKDIR

void Server::handle_client_mkdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
  
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
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, mdr, dn, newi);
  EUpdate *le = new EUpdate("mkdir");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  le->metablob.add_dir(newdir, true);
  
  // log + wait
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
  MClientRequest *req = mdr->client_request();
  
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
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, mdr, dn, newi);
  EUpdate *le = new EUpdate("symlink");
  le->metablob.add_client_req(req->get_reqid());
  le->metablob.add_dir_context(dn->dir);
  inode_t *pi = le->metablob.add_primary_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}





// LINK

void Server::handle_client_link(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();

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
  Context *ondelay = new C_MDS_RetryRequest(mdcache, mdr);
  vector<CDentry*> targettrace;
  int r = mdcache->path_traverse(mdr, 0,
				 targetpath, targettrace, false,
				 req, ondelay,
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
      
      mdcache->anchor_create(targeti,
			     new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  // can we create the dentry?
  CDentry *dn = 0;
  
  // make null link dentry
  dn = prepare_null_dentry(mdr, dir, dname, false);
  if (!dn) return;

  // create lock lists
  set<SimpleLock*> rdlocks;
  set<SimpleLock*> xlocks;

  for (unsigned i=0; i<linktrace.size(); i++)
    rdlocks.insert(&linktrace[i]->lock);
  xlocks.insert(&dn->lock);
  for (unsigned i=0; i<targettrace.size(); i++)
    rdlocks.insert(&targettrace[i]->lock);
  xlocks.insert(&targeti->linklock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, xlocks))
    return;

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
  utime_t tctime;
  version_t tpv;
public:
  C_MDS_link_local_finish(MDS *m, MDRequest *r, CDentry *d, CInode *ti, utime_t ct) :
    mds(m), mdr(r), dn(d), targeti(ti),
    dpv(d->get_projected_version()),
    tctime(ct), 
    tpv(targeti->get_parent_dn()->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_local_finish(mdr, dn, targeti, dpv, tctime, tpv);
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
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_remote_dentry(dn, true, targeti->ino());  // new remote
  le->metablob.add_dir_context(targeti->get_parent_dir());
  inode_t *pi = le->metablob.add_primary_dentry(targeti->parent, true, targeti);  // update old primary

  // update journaled target inode
  pi->nlink++;
  pi->ctime = g_clock.now();
  pi->version = tpdv;

  // finisher
  C_MDS_link_local_finish *fin = new C_MDS_link_local_finish(mds, mdr, dn, targeti, pi->ctime);
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}

void Server::_link_local_finish(MDRequest *mdr, CDentry *dn, CInode *targeti,
				version_t dpv, utime_t tctime, version_t tpv)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << endl;

  // link and unlock the new dentry
  dn->dir->link_inode(dn, targeti->ino());
  dn->set_version(dpv);
  dn->mark_dirty(dpv);

  // update the target
  targeti->inode.nlink++;
  targeti->inode.ctime = tctime;
  targeti->mark_dirty(tpv);

  // bump target popularity
  mds->balancer->hit_inode(targeti, META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request(), 0);
  reply_request(mdr, reply, dn->get_dir()->get_inode());  // FIXME: imprecise ref
}



void Server::_link_remote(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote " << *dn << " to " << *targeti << endl;
  
  // 1. send LinkPrepare to dest (journal nlink++ prepare)
  // 2. create+journal new dentry, as with link_local.
  // 3. send LinkCommit to dest (journals commit)  

  // IMPLEMENT ME
  reply_request(mdr, -EXDEV);
}


/*
void Server::handle_client_link_finish(MClientRequest *req, CInode *ref,
				       CDentry *dn, CInode *targeti)
{
  // create remote link
  dn->dir->link_inode(dn, targeti->ino());
  dn->link_remote( targeti );   // since we have it
  dn->_mark_dirty(); // fixme
  
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);

  // done!
  commit_request(req, new MClientReply(req, 0), ref,
                 0);          // FIXME i should log something
}
*/

/*
class C_MDS_RemoteLink : public Context {
  Server *server;
  MClientRequest *req;
  CInode *ref;
  CDentry *dn;
  CInode *targeti;
public:
  C_MDS_RemoteLink(Server *server, MClientRequest *req, CInode *ref, CDentry *dn, CInode *targeti) {
    this->server = server;
    this->req = req;
    this->ref = ref;
    this->dn = dn;
    this->targeti = targeti;
  }
  void finish(int r) {
    if (r > 0) { // success
      // yay
      server->handle_client_link_finish(req, ref, dn, targeti);
    } 
    else if (r == 0) {
      // huh?  retry!
      assert(0);
      server->dispatch_request(req, ref);      
    } else {
      // link failed
      server->reply_request(req, r);
    }
  }
};


  } else {
    // remote: send nlink++ request, wait
    dout(7) << "target is remote, sending InodeLink" << endl;
    mds->send_message_mds(new MInodeLink(targeti->ino(), mds->get_nodeid()), targeti->authority().first, MDS_PORT_CACHE);
    
    // wait
    targeti->add_waiter(CInode::WAIT_LINK, new C_MDS_RemoteLink(this, req, diri, dn, targeti));
    return;
  }

*/





// UNLINK

void Server::handle_client_unlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();

  // traverse to path
  vector<CDentry*> trace;
  Context *ondelay = new C_MDS_RetryRequest(mdcache, mdr);
  int r = mdcache->path_traverse(mdr, 0,
				 req->get_filepath(), trace, false,
				 req, ondelay,
				 MDS_TRAVERSE_FORWARD);
  if (r > 0) return;
  if (trace.empty()) r = -EINVAL;   // can't unlink root
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }

  CDentry *dn = trace[trace.size()-1];
  assert(dn);

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
  set<SimpleLock*> rdlocks;
  set<SimpleLock*> xlocks;

  for (unsigned i=0; i<trace.size()-1; i++)
    rdlocks.insert(&trace[i]->lock);
  xlocks.insert(&dn->lock);
  xlocks.insert(&in->linklock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, xlocks))
    return;

  // ok!
  if (dn->is_remote() && !dn->inode->is_auth()) 
    _unlink_remote(mdr, dn);
  else
    _unlink_local(mdr, dn);
}



class C_MDS_unlink_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CDentry *straydn;
  version_t ipv;  // referred inode
  utime_t ictime;
  version_t dpv;  // deleted dentry
public:
  C_MDS_unlink_local_finish(MDS *m, MDRequest *r, CDentry *d, CDentry *sd,
			    version_t v, utime_t ct) :
    mds(m), mdr(r), dn(d), straydn(sd),
    ipv(v), ictime(ct),
    dpv(d->get_projected_version()) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_local_finish(mdr, dn, straydn, ipv, ictime, dpv);
  }
};


void Server::_unlink_local(MDRequest *mdr, CDentry *dn)
{
  dout(10) << "_unlink_local " << *dn << endl;

  // get stray dn ready?
  CDentry *straydn = 0;
  if (dn->is_primary()) {
    string straydname;
    dn->inode->name_stray_dentry(straydname);
    frag_t fg = mdcache->get_stray()->pick_dirfrag(straydname);
    CDir *straydir = mdcache->get_stray()->get_or_open_dirfrag(mdcache, fg);
    straydn = straydir->add_dentry(straydname, 0);
    dout(10) << "_unlink_local straydn is " << *straydn << endl;
  }

  
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
  le->metablob.add_dir_context(dn->get_dir());
  le->metablob.add_null_dentry(dn, true);

  // update journaled target inode
  pi->nlink--;
  pi->ctime = g_clock.now();
  pi->version = ipv;
  
  // finisher
  C_MDS_unlink_local_finish *fin = new C_MDS_unlink_local_finish(mds, mdr, dn, straydn, 
								 ipv, pi->ctime);
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
  
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);
}

void Server::_unlink_local_finish(MDRequest *mdr, 
				  CDentry *dn, CDentry *straydn,
				  version_t ipv, utime_t ictime, version_t dpv) 
{
  dout(10) << "_unlink_local " << *dn << endl;

  // unlink main dentry
  CInode *in = dn->inode;
  dn->dir->unlink_inode(dn);

  // relink as stray?  (i.e. was primary link?)
  if (straydn) straydn->dir->link_inode(straydn, in);  

  // nlink--
  in->inode.ctime = ictime;
  in->inode.nlink--;
  in->mark_dirty(ipv);  // dirty inode
  dn->mark_dirty(dpv);  // dirty old dentry

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

  // bump target popularity
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request(), 0);
  reply_request(mdr, reply, dn->dir->get_inode());  // FIXME: imprecise ref

  if (straydn)
    mdcache->eval_stray(straydn);
}



void Server::_unlink_remote(MDRequest *mdr, CDentry *dn) 
{
  // IMPLEMENT ME
  reply_request(mdr, -EXDEV);
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





// RENAME

class C_MDS_RenameTraverseDst : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *srci;
  CDir *srcdir;
  CDentry *srcdn;
  filepath destpath;
public:
  vector<CDentry*> trace;
  
  C_MDS_RenameTraverseDst(Server *server,
                          MDRequest *r,
                          CDentry *srcdn,
                          filepath& destpath) {
    this->server = server;
    this->mdr = r;
    this->srcdn = srcdn;
    this->destpath = destpath;
  }
  void finish(int r) {
    server->handle_client_rename_2(mdr,
				   srcdn, destpath,
				   trace, r);
  }
};


/** handle_client_rename
 *
 * NOTE: caller did not path_pin the ref (srcdir) inode, as it normally does.
 *  

  weirdness iwith rename:
    - ref inode is what was originally srcdiri, but that may change by the time
      the rename actually happens.  for all practical purpose, ref is useless except
      for C_MDS_RetryRequest

 */

bool Server::_rename_open_dn(CDir *dir, CDentry *dn, bool mustexist, MDRequest *mdr)
{
  // xlocked?
  if (dn && !dn->lock.can_rdlock(mdr)) {
    dout(10) << "_rename_open_dn waiting on " << *dn << endl;
    dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  
  if (mustexist && 
      ((dn && dn->is_null()) ||
       (!dn && dir->is_complete()))) {
    dout(10) << "_rename_open_dn dn dne in " << *dir << endl;
    reply_request(mdr, -ENOENT);
    return false;
  }
  
  if (!dn && !dir->is_complete()) {
    dout(10) << "_rename_open_dn readding incomplete dir" << endl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
  assert(dn && !dn->is_null());
  
  dout(10) << "_rename_open_dn dn is " << *dn << endl;
  CInode *in = mdcache->get_dentry_inode(dn, mdr);
  if (!in) return false;
  dout(10) << "_rename_open_dn inode is " << *in << endl;
  
  return true;
}

void Server::handle_client_rename(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request();
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
  Context *ondelay = new C_MDS_RetryRequest(mdcache, mdr);
  int r = mdcache->path_traverse(mdr, 0,
				 srcpath, srctrace, false,
				 req, ondelay,
				 MDS_TRAVERSE_DISCOVER);
  if (r > 0) return;
  if (srctrace.empty()) r = -EINVAL;  // can't rename root
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  CDentry *srcdn = srctrace[srctrace.size()-1];
  dout(10) << "srcdn is " << *srcdn << endl;
  CInode *srci = mdcache->get_dentry_inode(srcdn, mdr);
  dout(10) << "srci is " << *srci << endl;

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
    dout(10) << "dest dn exists " << *destdn << endl;
    oldin = mdcache->get_dentry_inode(destdn, mdr);
    if (!oldin) return;
    dout(10) << "oldin " << *oldin << endl;
    
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

  dout(10) << "destdn " << *destdn << endl;


  // -- locks --
  set<SimpleLock*> rdlocks;
  set<SimpleLock*> xlocks;

  // rdlock sourcedir path, xlock src dentry
  for (unsigned i=0; i<srctrace.size()-1; i++) 
    rdlocks.insert(&srctrace[i]->lock);
  xlocks.insert(&srcdn->lock);

  // rdlock destdir path, xlock dest dentry
  for (unsigned i=0; i<desttrace.size(); i++)
    rdlocks.insert(&desttrace[i]->lock);
  xlocks.insert(&destdn->lock);

  // xlock oldin
  if (oldin) xlocks.insert(&oldin->linklock);
  
  if (!mds->locker->acquire_locks(mdr, rdlocks, xlocks))
    return;

  
  // ok go!
  if (srcdn->is_auth() && destdn->is_auth()) 
    _rename_local(mdr, srcdn, destdn);
  else {
    //   _rename_remote(mdr, srcdn, destdn);
    reply_request(mdr, -EXDEV);
    return;
  }
}




class C_MDS_rename_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *srcdn;
  CDentry *destdn;
  CDentry *straydn;
  version_t ipv;
  version_t straypv;
  version_t destpv;
  version_t srcpv;
  utime_t ictime;
public:
  version_t atid1;
  version_t atid2;
  C_MDS_rename_local_finish(MDS *m, MDRequest *r,
			    CDentry *sdn, CDentry *ddn, CDentry *stdn,
			    version_t v, utime_t ct) :
    mds(m), mdr(r),
    srcdn(sdn), destdn(ddn), straydn(stdn),
    ipv(v), 
    straypv(straydn ? straydn->get_projected_version():0),
    destpv(destdn->get_projected_version()),
    srcpv(srcdn->get_projected_version()),
    ictime(ct),
    atid1(0), atid2(0) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_rename_local_finish(mdr, srcdn, destdn, straydn,
				      srcpv, destpv, straypv, ipv, ictime, 
				      atid1, atid2);
  }
};

class C_MDS_rename_local_anchor : public Context {
  Server *server;
public:
  LogEvent *le;
  C_MDS_rename_local_finish *fin;
  version_t atid1;
  version_t atid2;
  
  C_MDS_rename_local_anchor(Server *s) : server(s), le(0), fin(0), atid1(0), atid2(0) { }
  void finish(int r) {
    server->_rename_local_reanchored(le, fin, atid1, atid2);
  }
};

void Server::_rename_local(MDRequest *mdr,
			   CDentry *srcdn,
			   CDentry *destdn)
{
  dout(10) << "_rename_local " << *srcdn << " to " << *destdn << endl;

  // let's go.
  EUpdate *le = new EUpdate("rename_local");
  le->metablob.add_client_req(mdr->reqid);

  CDentry *straydn = 0;
  inode_t *pi = 0;
  version_t ipv = 0;
  
  C_MDS_rename_local_anchor *anchorfin = 0;
  C_Gather *anchorgather = 0;

  // primary+remote link merge?
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));
  if (linkmerge) {
    dout(10) << "will merge remote+primary links" << endl;
    
    // destdn -> primary
    le->metablob.add_dir_context(destdn->dir);
    ipv = destdn->pre_dirty(destdn->inode->inode.version);
    pi = le->metablob.add_primary_dentry(destdn, true, destdn->inode); 
    
    // do src dentry
    le->metablob.add_dir_context(srcdn->dir);
    srcdn->pre_dirty();
    le->metablob.add_null_dentry(srcdn, true);

    // anchor update?
    if (srcdn->is_primary() && srcdn->inode->is_anchored() &&
	srcdn->dir != destdn->dir) {
      dout(10) << "reanchoring src->dst " << *srcdn->inode << endl;
      vector<Anchor> trace;
      destdn->make_anchor_trace(trace, srcdn->inode);
      anchorfin = new C_MDS_rename_local_anchor(this);
      mds->anchorclient->prepare_update(srcdn->inode->ino(), trace, &anchorfin->atid1, anchorfin);
    }

  } else {
    // move to stray?
    if (destdn->is_primary()) {
      // primary.
      // move inode to stray dir.
      string straydname;
      destdn->inode->name_stray_dentry(straydname);
      frag_t fg = mdcache->get_stray()->pick_dirfrag(straydname);
      CDir *straydir = mdcache->get_stray()->get_or_open_dirfrag(mdcache, fg);
      straydn = straydir->add_dentry(straydname, 0);
      dout(10) << "straydn is " << *straydn << endl;

      // renanchor?
      if (destdn->inode->is_anchored()) {
	dout(10) << "reanchoring dst->stray " << *destdn->inode << endl;
	vector<Anchor> trace;
	straydn->make_anchor_trace(trace, destdn->inode);
	anchorfin = new C_MDS_rename_local_anchor(this);
	anchorgather = new C_Gather(anchorfin);
	mds->anchorclient->prepare_update(destdn->inode->ino(), trace, &anchorfin->atid1, 
					  anchorgather->new_sub());
      }

      // link-- inode, move to stray dir.
      le->metablob.add_dir_context(straydn->dir);
      ipv = straydn->pre_dirty(destdn->inode->inode.version);
      pi = le->metablob.add_primary_dentry(straydn, true, destdn->inode);
    } 
    else if (destdn->is_remote()) {
      // remote.
      // nlink-- targeti
      le->metablob.add_dir_context(destdn->inode->get_parent_dir());
      ipv = destdn->inode->pre_dirty();
      pi = le->metablob.add_primary_dentry(destdn->inode->parent, true, destdn->inode);  // update primary
      dout(10) << "remote targeti (nlink--) is " << *destdn->inode << endl;
    }
    else {
      assert(destdn->is_null());
    }

    // add dest dentry
    le->metablob.add_dir_context(destdn->dir);
    if (srcdn->is_primary()) {
      dout(10) << "src is a primary dentry" << endl;
      destdn->pre_dirty(srcdn->inode->inode.version);
      le->metablob.add_primary_dentry(destdn, true, srcdn->inode); 

      if (srcdn->inode->is_anchored()) {
	dout(10) << "reanchoring src->dst " << *srcdn->inode << endl;
	vector<Anchor> trace;
	destdn->make_anchor_trace(trace, srcdn->inode);
	if (!anchorfin) anchorfin = new C_MDS_rename_local_anchor(this);
	if (!anchorgather) anchorgather = new C_Gather(anchorfin);
	mds->anchorclient->prepare_update(srcdn->inode->ino(), trace, &anchorfin->atid2, 
					  anchorgather->new_sub());
	
      }
    } else {
      assert(srcdn->is_remote());
      dout(10) << "src is a remote dentry" << endl;
      destdn->pre_dirty();
      le->metablob.add_remote_dentry(destdn, true, srcdn->get_remote_ino()); 
    }
    
    // remove src dentry
    le->metablob.add_dir_context(srcdn->dir);
    srcdn->pre_dirty();
    le->metablob.add_null_dentry(srcdn, true);
  }

  if (pi) {
    // update journaled target inode
    pi->nlink--;
    pi->ctime = g_clock.now();
    pi->version = ipv;
  }

  C_MDS_rename_local_finish *fin = new C_MDS_rename_local_finish(mds, mdr, 
								 srcdn, destdn, straydn,
								 ipv, pi ? pi->ctime:utime_t());
  
  if (anchorfin) {
    // doing anchor update prepare first
    anchorfin->fin = fin;
    anchorfin->le = le;
  } else {
    // log + wait
    mdlog->submit_entry(le);
    mdlog->wait_for_sync(fin);
  }
}


void Server::_rename_local_reanchored(LogEvent *le, C_MDS_rename_local_finish *fin, 
				      version_t atid1, version_t atid2)
{
  dout(10) << "_rename_local_reanchored, logging " << *le << endl;
  
  // note anchor transaction ids
  fin->atid1 = atid1;
  fin->atid2 = atid2;

  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


void Server::_rename_local_finish(MDRequest *mdr,
				  CDentry *srcdn, CDentry *destdn, CDentry *straydn,
				  version_t srcpv, version_t destpv, version_t straypv, version_t ipv,
				  utime_t ictime,
				  version_t atid1, version_t atid2)
{
  MClientRequest *req = mdr->client_request();
  dout(10) << "_rename_local_finish " << *req << endl;

  CInode *oldin = destdn->inode;
  
  // primary+remote link merge?
  bool linkmerge = (srcdn->inode == destdn->inode &&
		    (srcdn->is_primary() || destdn->is_primary()));

  if (linkmerge) {
    assert(ipv);
    if (destdn->is_primary()) {
      dout(10) << "merging remote onto primary link" << endl;

      // nlink-- in place
      destdn->inode->inode.nlink--;
      destdn->inode->inode.ctime = ictime;
      destdn->inode->mark_dirty(destpv);

      // unlink srcdn
      srcdn->dir->unlink_inode(srcdn);
      srcdn->mark_dirty(srcpv);
    } else {
      dout(10) << "merging primary onto remote link" << endl;
      assert(srcdn->is_primary());
      
      // move inode to dest
      srcdn->dir->unlink_inode(srcdn);
      destdn->dir->unlink_inode(destdn);
      destdn->dir->link_inode(destdn, oldin);
      
      // nlink--
      destdn->inode->inode.nlink--;
      destdn->inode->inode.ctime = ictime;
      destdn->inode->mark_dirty(destpv);
      
      // mark src dirty
      srcdn->mark_dirty(srcpv);
    }
  } 
  else {
    // unlink destdn?
    if (!destdn->is_null())
      destdn->dir->unlink_inode(destdn);
    
    if (straydn) {
      // relink oldin to stray dir
      assert(oldin);
      straydn->dir->link_inode(straydn, oldin);
      assert(straypv == ipv);
    }
    
    if (oldin) {
      // nlink--
      oldin->inode.nlink--;
      oldin->inode.ctime = ictime;
      oldin->mark_dirty(ipv);
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
    destdn->mark_dirty(destpv);
    srcdn->mark_dirty(srcpv);
  }

  // commit anchor updates?
  if (atid1) mds->anchorclient->commit(atid1);
  if (atid2) mds->anchorclient->commit(atid2);

  // update subtree map?
  if (destdn->inode->is_dir()) 
    mdcache->adjust_subtree_after_rename(destdn->inode, srcdn->dir);

  // share news with replicas
  // ***

  // reply
  MClientReply *reply = new MClientReply(req, 0);
  reply_request(mdr, reply, destdn->dir->get_inode());  // FIXME: imprecise ref

  // clean up?
  if (straydn) 
    mdcache->eval_stray(straydn);
}




/*
void Server::handle_client_rename_local(MClientRequest *req,
					CInode *ref,
					const string& srcpath,
					CInode *srcdiri,
					CDentry *srcdn,
					const string& destpath,
					CDir *destdir,
					CDentry *destdn,
					const string& destname)
{
*/
  //bool everybody = false;
  //if (true || srcdn->inode->is_dir()) {
    /* overkill warning: lock w/ everyone for simplicity.  FIXME someday!  along with the foreign rename crap!
       i could limit this to cases where something beneath me is exported.
       could possibly limit the list.    (maybe.)
       Underlying constraint is that, regardless of the order i do the xlocks, and whatever
       imports/exports might happen in the process, the destdir _must_ exist on any node
       importing something beneath me when rename finishes, or else mayhem ensues when
       their import is dangling in the cache.
     */
    /*
      having made a proper mess of this on the first pass, here is my plan:
      
      - xlocks of src, dest are done in lex order
      - xlock is optional.. if you have the dentry, lock it, if not, don't.
      - if you discover an xlocked dentry, you get the xlock.

      possible trouble:
      - you have an import beneath the source, and don't have the dest dir.
        - when the actual rename happens, you discover the dest
        - actually, do this on any open dir, so we don't detach whole swaths
          of our cache.
      
      notes:
      - xlocks are initiated from authority, as are discover_replies, so replicas are 
        guaranteed to either not have dentry, or to have it xlocked. 
      - 
      - foreign xlocks are eventually unraveled by the initiator on success or failure.

      todo to make this work:
      - hose bool everybody param crap
      /- make handle_lock_dn not discover, clean up cases
      /- put dest path in MRenameNotify
      /- make rename_notify discover if its a dir
      /  - this will catch nested imports too, obviously
      /- notify goes to merged list on local rename
      /- notify goes to everybody on a foreign rename 
      /- handle_notify needs to gracefully ignore spurious notifies
    */
  //dout(7) << "handle_client_rename_local: overkill?  doing xlocks with _all_ nodes" << endl;
  //everybody = true;
  //}
/*
  bool srclocal = srcdn->dir->dentry_authority(srcdn->name).first == mds->get_nodeid();
  bool destlocal = destdir->dentry_authority(destname).first == mds->get_nodeid();

  dout(7) << "handle_client_rename_local: src local=" << srclocal << " " << *srcdn << endl;
  if (destdn) {
    dout(7) << "handle_client_rename_local: dest local=" << destlocal << " " << *destdn << endl;
  } else {
    dout(7) << "handle_client_rename_local: dest local=" << destlocal << " dn dne yet" << endl;
  }

  // lock source and dest dentries, in lexicographic order.
  bool dosrc = srcpath < destpath;
  for (int i=0; i<2; i++) {
    if (dosrc) {

      // src
      if (srclocal) {
        if (!srcdn->is_xlockedbyme(req) &&
            !mds->locker->dentry_xlock_start(srcdn, req, ref))
          return;  
      } else {
        if (!srcdn || srcdn->xlockedby != req) {
          mds->locker->dentry_xlock_request(srcdn->dir, srcdn->name, false, req, new C_MDS_RetryRequest(mds, req, ref));
          return;
        }
      }
      dout(7) << "handle_client_rename_local: srcdn is xlock " << *srcdn << endl;
      
    } else {

      if (destlocal) {
        // dest
        if (!destdn) destdn = destdir->add_dentry(destname);
        if (!destdn->is_xlockedbyme(req) &&
            !mds->locker->dentry_xlock_start(destdn, req, ref)) {
          if (destdn->is_clean() && destdn->is_null() && destdn->is_sync()) destdir->remove_dentry(destdn);
          return;
        }
      } else {
        if (!destdn || destdn->xlockedby != req) {
          // NOTE: require that my xlocked item be a leaf/file, NOT a dir.  in case
          // my traverse and determination of dest vs dest/srcfilename was out of date.
          mds->locker->dentry_xlock_request(destdir, destname, true, req, new C_MDS_RetryRequest(mds, req, ref));
          return;
        }
      }
      dout(7) << "handle_client_rename_local: destdn is xlock " << *destdn << endl;

    }
    
    dosrc = !dosrc;
  }

  
  // final check: verify if dest exists that src is a file

  // FIXME: is this necessary?

  if (destdn->inode) {
    if (destdn->inode->is_dir()) {
      dout(7) << "handle_client_rename_local failing, dest exists and is a dir: " << *destdn->inode << endl;
      assert(0);
      reply_request(req, -EINVAL);  
      return; 
    }
    if (srcdn->inode->is_dir()) {
      dout(7) << "handle_client_rename_local failing, dest exists and src is a dir: " << *destdn->inode << endl;
      assert(0);
      reply_request(req, -EINVAL);  
      return; 
    }
  } else {
    // if destdn->inode is null, then we know it's a non-existent dest,
    // why?  because if it's local, it dne.  and if it's remote, we xlocked with 
    // REQXLOCKC, which will only allow you to lock a file.
    // so we know dest is a file, or non-existent
    if (!destlocal) {
      if (srcdn->inode->is_dir()) { 
        // help: maybe the dest exists and is a file?   ..... FIXME
      } else {
        // we're fine, src is file, dest is file|dne
      }
    }
  }
  
  mds->balancer->hit_dir(srcdn->dir, META_POP_DWR);
  mds->balancer->hit_dir(destdn->dir, META_POP_DWR);

  // we're golden.
  // everything is xlocked by us, we rule, etc.
  MClientReply *reply = new MClientReply(req, 0);
  mdcache->renamer->file_rename( srcdn, destdn,
				 new C_MDS_CommitRequest(this, req, reply, srcdn->inode,
							 new EString("file rename fixme")) );
}



*/







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
  MClientRequest *req = mdr->client_request();
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
  utime_t ctime = g_clock.now();
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
  MClientRequest *req = mdr->client_request();

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
  MClientRequest *req = mdr->client_request();
  int cmode = req->get_open_file_mode();

  // can we issue the caps they want?
  version_t fdv = mds->locker->issue_file_data_version(cur);
  Capability *cap = mds->locker->issue_new_caps(cur, cmode, req);
  if (!cap) return; // can't issue (yet), so wait!
  
  dout(12) << "_do_open issuing caps " << cap_string(cap->pending())
	   << " for " << req->get_source()
	   << " on " << *cur << endl;
  
  // hit pop
  mds->balancer->hit_inode(cur, META_POP_IRD);

  // reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_file_caps(cap->pending());
  reply->set_file_caps_seq(cap->get_last_seq());
  reply->set_file_data_version(fdv);
  reply_request(mdr, reply, cur);

  // journal?
  if (cur->last_open_journaled == 0) {
    cur->last_open_journaled = mdlog->get_write_pos();
    mdlog->submit_entry(new EOpen(cur));
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
  utime_t ctime = g_clock.now();
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
  MClientRequest *req = mdr->client_request();

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














