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
#include "Renamer.h"
#include "MDStore.h"

#include "UserBatch.h"

#include "msg/Messenger.h"

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MHashReaddir.h"
#include "messages/MHashReaddirReply.h"

#include "messages/MLock.h"

#include "messages/MInodeLink.h"

#include "events/EString.h"
#include "events/EUpdate.h"

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
  }
 

  switch (m->get_type()) {
  case MSG_CLIENT_REQUEST:
    handle_client_request((MClientRequest*)m);
    return;
  case MSG_CLIENT_UPDATE:
    handle_client_update((MClientUpdate*)m);
    return;
  case MSG_CLIENT_RENEWAL:
    handle_client_renewal((MClientRenewal*)m);
    return;

  case MSG_MDS_HASHREADDIR:
    handle_hash_readdir((MHashReaddir*)m);
    return;
  case MSG_MDS_HASHREADDIRREPLY:
    handle_hash_readdir_reply((MHashReaddirReply*)m);
    return;
    
  }

  dout(1) << " main unknown message " << m->get_type() << endl;
  assert(0);
}





void Server::handle_client_mount(MClientMount *m)
{
  int n = m->get_source().num();
  dout(3) << "mount by client" << n << endl;
  mds->clientmap.add_mount(n, m->get_source_inst());

  assert(mds->get_nodeid() == 0);  // mds0 mounts/unmounts

  // ack
  messenger->send_message(new MClientMountAck(m, mds->mdsmap, mds->osdmap), 
                          m->get_source_inst());
  delete m;
}

void Server::handle_client_unmount(Message *m)
{
  int n = m->get_source().num();
  dout(3) << "unmount by client" << n << endl;

  assert(mds->get_nodeid() == 0);  // mds0 mounts/unmounts

  mds->clientmap.rem_mount(n);

  if (g_conf.mds_shutdown_on_last_unmount &&
      mds->clientmap.get_mount_set().empty()) {
    dout(3) << "all clients done, initiating shutdown" << endl;
    mds->shutdown_start();
  }

  // ack by sending back to client
  messenger->send_message(m, m->get_source_inst());
}



/*******
 * some generic stuff for finishing off requests
 */

/** C_MDS_CommitRequest
 */

class C_MDS_CommitRequest : public Context {
  Server *server;
  MClientRequest *req;
  MClientReply *reply;
  CInode *tracei;    // inode to include a trace for
  LogEvent *event;

public:
  C_MDS_CommitRequest(Server *server,
                      MClientRequest *req, MClientReply *reply, CInode *tracei, 
                      LogEvent *event=0) {
    this->server = server;
    this->req = req;
    this->tracei = tracei;
    this->reply = reply;
    this->event = event;
  }
  void finish(int r) {
    if (r != 0) {
      // failure.  set failure code and reply.
      reply->set_result(r);
    }
    if (event) {
      server->commit_request(req, reply, tracei, event);
    } else {
      // reply.
      server->reply_request(req, reply, tracei);
    }
  }
};


/*
 * send generic response (just and error code)
 */
void Server::reply_request(MClientRequest *req, int r, CInode *tracei)
{
  reply_request(req, new MClientReply(req, r), tracei);
}


/*
 * send given reply
 * include a trace to tracei
 */
void Server::reply_request(MClientRequest *req, MClientReply *reply, CInode *tracei) {
  dout(10) << "reply_request r=" << reply->get_result() << " " << *req << endl;

  // include trace
  if (tracei) {
    reply->set_trace_dist( tracei, mds->get_nodeid() );
  }
  
  // send reply
  messenger->send_message(reply,
                          req->get_client_inst());

  // discard request
  mdcache->request_finish(req);

  // stupid stats crap (FIXME)
  stat_ops++;
}


void Server::submit_update(MClientRequest *req,
			   CInode *wrlockedi,
			   LogEvent *event,
			   Context *oncommit)
{
  // log
  mdlog->submit_entry(event);

  // pin
  mdcache->request_pin_inode(req, wrlockedi);

  // wait
  mdlog->wait_for_sync(oncommit);
}


/* 
 * commit event(s) to the metadata journal, then reply.
 * or, be sloppy and do it concurrently (see g_conf.mds_log_before_reply)
 *
 * NOTE: this is old and bad (write-behind!)
 */
void Server::commit_request(MClientRequest *req,
                         MClientReply *reply,
                         CInode *tracei,
                         LogEvent *event,
                         LogEvent *event2) 
{      
  // log
  if (event) mdlog->submit_entry(event);
  if (event2) mdlog->submit_entry(event2);
  
  if (g_conf.mds_log_before_reply && g_conf.mds_log && event) {
    // SAFE mode!

    // pin inode so it doesn't go away!
    if (tracei) mdcache->request_pin_inode(req, tracei);

    // wait for log sync
    mdlog->wait_for_sync(new C_MDS_CommitRequest(this, req, reply, tracei)); 
    return;
  }
  else {
    // just reply
    reply_request(req, reply, tracei);
  }
}

// update group operations
void Server::handle_client_update(MClientUpdate *m)
{
  hash_t my_hash = m->get_user_hash();
  dout(3) << "handle_client_update for " << my_hash << endl;

  MClientUpdateReply *reply;
  // its a file group
  if (g_conf.mds_group == 3) {
    reply = new MClientUpdateReply(my_hash, mds->unix_groups_byhash[my_hash].get_inode_list());
    reply->set_sig(mds->unix_groups_byhash[my_hash].get_sig());
  }
  // its a user group
  else {
    reply = new MClientUpdateReply(my_hash,
				   mds->unix_groups_byhash[my_hash].get_list(),
				   mds->unix_groups_byhash[my_hash].get_sig());
  }

  messenger->send_message(reply, m->get_source_inst());
}

// capability renewal request
// automatically renews all recently requested caps
// plus all open caps
void Server::handle_client_renewal(MClientRenewal *m)
{
  cout << "Got a renewal request" << endl;
  bool new_caps = false;
  set<cap_id_t> requested_caps = m->get_cap_set();  

  // any new caps requested?
  for (set<cap_id_t>::iterator si = requested_caps.begin();
       si != requested_caps.end();
       si++) {
    // new cap renewal request
    if (mds->recent_caps.count(*si) == 0) {
      new_caps = true;
      mds->recent_caps.insert(*si);
    }
  }
  if (new_caps || mds->token.num_renewed_caps() == 0) {
    // re-make extension
    mds->token = Renewal(mds->recent_caps);
    //mds->token.add_set(mds->recent_caps);

    // re-sign
    mds->token.sign_renewal(mds->getPrvKey());
    cout << "Made a new token" << endl;
  }
  else 
    cout << "Cached token was good" << endl;

  // if no new caps && cached extension, return cached extension
  // create extension for entire recent_cap set
  // cache the extension
}

/***
 * process a client request
 */

void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "req " << *req << endl;

  // note original client addr
  if (req->get_source().is_client()) {
    req->set_client_inst( req->get_source_inst() );
    req->clear_payload();
  }

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
  vector<CDentry*> trace;      // might be blank, for fh guys

  bool follow_trailing_symlink = false;

  // operations on fh's or other non-files
  switch (req->get_op()) {
    /*
  case MDS_OP_FSTAT:
    reply = handle_client_fstat(req, cur);
    break; ****** fiX ME ***
    */
    
  case MDS_OP_TRUNCATE:
    if (!req->get_ino()) break;   // can be called w/ either fh OR path
    
  case MDS_OP_RELEASE:
  case MDS_OP_FSYNC:
    ref = mdcache->get_inode(req->get_ino());   // fixme someday no ino needed?

    if (!ref) {
      int next = mds->get_nodeid() + 1;
      if (next >= mds->mdsmap->get_num_mds()) next = 0;
      dout(10) << "got request on ino we don't have, passing buck to " << next << endl;
      mds->send_message_mds(req, next, MDS_PORT_SERVER);
      return;
    }
  }

  // file specific operations
  if (!ref) {
    // we need to traverse a path
    filepath refpath = req->get_filepath();
    
    // ops on non-existing files --> directory paths
    switch (req->get_op()) {
    case MDS_OP_OPEN:
      if (!(req->get_iarg() & O_CREAT)) break;
      
    case MDS_OP_MKNOD:
    case MDS_OP_MKDIR:
    case MDS_OP_SYMLINK:
    case MDS_OP_LINK:
    case MDS_OP_UNLINK:   // also wrt parent dir, NOT the unlinked inode!!
    case MDS_OP_RMDIR:
    case MDS_OP_RENAME:
      // remove last bit of path
      refpath = refpath.prefixpath(refpath.depth()-1);
      break;
    }
    dout(10) << "refpath = " << refpath << endl;
    
    Context *ondelay = new C_MDS_RetryMessage(mds, req);
    
    if (req->get_op() == MDS_OP_LSTAT) {
      follow_trailing_symlink = false;
    }

    // do trace
    int r = mdcache->path_traverse(refpath, trace, follow_trailing_symlink,
                                   req, ondelay,
                                   MDS_TRAVERSE_FORWARD,
                                   0,
                                   true); // is MClientRequest
    
    if (r > 0) return; // delayed
    if (r == -ENOENT ||
        r == -ENOTDIR ||
        r == -EISDIR) {
      // error! 
      dout(10) << " path traverse error " << r << ", replying" << endl;
      
      // send error
      messenger->send_message(new MClientReply(req, r),
                              req->get_client_inst());

      // <HACK>
      // is this a special debug command?
      if (refpath.depth() - 1 == trace.size() &&
	  refpath.last_bit().find(".ceph.") == 0) {
	CDir *dir = 0;
	if (trace.empty())
	  dir = mdcache->get_root()->dir;
	else
	  dir = trace[trace.size()-1]->get_inode()->dir;

	dout(1) << "** POSSIBLE CEPH DEBUG COMMAND '" << refpath.last_bit() << "' in " << *dir << endl;

	if (refpath.last_bit() == ".ceph.hash" &&
	    refpath.depth() > 1) {
	  dout(1) << "got explicit hash command " << refpath << endl;
	  CDir *dir = trace[trace.size()-1]->get_inode()->dir;
	  if (!dir->is_hashed() &&
	      !dir->is_hashing() &&
	      dir->is_auth())
	    mdcache->migrator->hash_dir(dir);
	}
	else if (refpath.last_bit() == ".ceph.commit") {
	  dout(1) << "got explicit commit command on  " << *dir << endl;
	  mds->mdstore->commit_dir(dir, 0);
	}
      }
      // </HACK>


      delete req;
      return;
    }

    // if not an error return from path_traverse
    if (trace.size()) 
      ref = trace[trace.size()-1]->inode;
    else
      ref = mdcache->get_root();
  }
  
  dout(10) << "ref is " << *ref << endl;
  
  // rename doesn't pin src path (initially)
  if (req->get_op() == MDS_OP_RENAME) trace.clear();

  // register
  if (!mdcache->request_start(req, ref, trace))
    return;
  
  // process
  dispatch_request(req, ref);
}



void Server::dispatch_request(Message *m, CInode *ref)
{
  MClientRequest *req = 0;

  // MLock or MClientRequest?
  /* this is a little weird.
     client requests and mlocks both initial dentry xlocks, path pins, etc.,
     and thus both make use of the context C_MDS_RetryRequest.
  */
  switch (m->get_type()) {
  case MSG_CLIENT_REQUEST:
    req = (MClientRequest*)m;
    break; // continue below!

  case MSG_MDS_LOCK:
    mds->locker->handle_lock_dn((MLock*)m);
    return; // done

  default:
    assert(0);  // shouldn't get here
  }

  // MClientRequest.

  switch(req->get_op()) {
    
    // files
  case MDS_OP_OPEN:
    if (req->get_iarg() & O_CREAT) 
      handle_client_openc(req, ref);
    else {
      handle_client_open(req, ref);
    }
    break;
  case MDS_OP_TRUNCATE:
    handle_client_truncate(req, ref);
    break;
    /*
  case MDS_OP_FSYNC:
    handle_client_fsync(req, ref);
    break;
    */
    /*
  case MDS_OP_RELEASE:
    handle_client_release(req, ref);
    break;
    */

    // inodes
  case MDS_OP_STAT:
  case MDS_OP_LSTAT:
    handle_client_stat(req, ref);
    break;
  case MDS_OP_UTIME:
    handle_client_utime(req, ref);
    break;
  case MDS_OP_CHMOD:
    handle_client_chmod(req, ref);
    break;
  case MDS_OP_CHOWN:
    handle_client_chown(req, ref);
    break;

    // namespace
  case MDS_OP_READDIR:
    handle_client_readdir(req, ref);
    break;
  case MDS_OP_MKNOD:
    handle_client_mknod(req, ref);
    break;
  case MDS_OP_LINK:
    handle_client_link(req, ref);
    break;
  case MDS_OP_UNLINK:
    handle_client_unlink(req, ref);
    break;
  case MDS_OP_RENAME:
    handle_client_rename(req, ref);
    break;
  case MDS_OP_RMDIR:
    handle_client_unlink(req, ref);
    break;
  case MDS_OP_MKDIR:
    handle_client_mkdir(req, ref);
    break;
  case MDS_OP_SYMLINK:
    handle_client_symlink(req, ref);
    break;



  default:
    dout(1) << " unknown client op " << req->get_op() << endl;
    assert(0);
  }

  return;
}


// FIXME: this probably should go somewhere else.

bool Server::try_open_dir(CInode *in, MClientRequest *req)
{
  if (!in->dir && in->is_frozen_dir()) {
    // doh!
    dout(10) << " dir inode is frozen, can't open dir, waiting " << *in << endl;
    assert(in->get_parent_dir());
    in->get_parent_dir()->add_waiter(CDIR_WAIT_UNFREEZE,
                                     new C_MDS_RetryRequest(mds, req, in));
    return false;
  }

  in->get_or_open_dir(mds->mdcache);
  return true;
}





// ===============================================================================
// STAT

void Server::handle_client_stat(MClientRequest *req,
				CInode *ref)
{
  // FIXME: this is really not the way to handle the statlite mask.

  // do I need file info?
  int mask = req->get_iarg();
  if (mask & (INODE_MASK_SIZE|INODE_MASK_MTIME)) {
    // yes.  do a full stat.
    if (!mds->locker->inode_file_read_start(ref, req))
      return;  // syncing
    mds->locker->inode_file_read_finish(ref);
  } else {
    // nope!  easy peasy.
  }
  
  mds->balancer->hit_inode(ref, META_POP_IRD);   
  
  // reply
  //dout(10) << "reply to " << *req << " stat " << ref->inode.mtime << endl;
  MClientReply *reply = new MClientReply(req);
  reply_request(req, reply, ref);
}




// ===============================================================================
// INODE UPDATES


/* 
 * finisher: do a inode_file_write_finish and reply.
 */
class C_MDS_utime_finish : public Context {
  MDS *mds;
  MClientRequest *req;
  CInode *in;
  version_t pv;
  time_t mtime, atime;
public:
  C_MDS_utime_finish(MDS *m, MClientRequest *r, CInode *i, version_t pdv, time_t mt, time_t at) :
    mds(m), req(r), in(i), 
    pv(pdv),
    mtime(mt), atime(at) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->inode.mtime = mtime;
    in->inode.atime = atime;
    in->mark_dirty(pv);

    // unlock
    mds->locker->inode_file_write_finish(in);

    // reply
    MClientReply *reply = new MClientReply(req, 0);
    reply->set_result(0);
    mds->server->reply_request(req, reply, in);
  }
};


// utime

void Server::handle_client_utime(MClientRequest *req,
				 CInode *cur)
{
  // write
  if (!mds->locker->inode_file_write_start(cur, req))
    return;  // fw or (wait for) sync

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // prepare
  version_t pdv = cur->pre_dirty();
  time_t mtime = req->get_targ();
  time_t atime = req->get_targ2();
  C_MDS_utime_finish *fin = new C_MDS_utime_finish(mds, req, cur, pdv, 
						   mtime, atime);

  // log + wait
  EUpdate *le = new EUpdate("utime");
  le->metablob.add_dir_context(cur->get_parent_dir());
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mtime = mtime;
  pi->atime = mtime;
  pi->version = pdv;
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


// --------------

/* 
 * finisher: do a inode_hard_write_finish and reply.
 */
class C_MDS_chmod_finish : public Context {
  MDS *mds;
  MClientRequest *req;
  CInode *in;
  version_t pv;
  int mode;
public:
  C_MDS_chmod_finish(MDS *m, MClientRequest *r, CInode *i, version_t pdv, int mo) :
    mds(m), req(r), in(i), pv(pdv), mode(mo) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->inode.mode &= ~04777;
    in->inode.mode |= (mode & 04777);
    in->mark_dirty(pv);

    // unlock
    mds->locker->inode_hard_write_finish(in);

    // reply
    MClientReply *reply = new MClientReply(req, 0);
    reply->set_result(0);
    mds->server->reply_request(req, reply, in);
  }
};


// chmod

void Server::handle_client_chmod(MClientRequest *req,
				 CInode *cur)
{
  // write
  if (!mds->locker->inode_hard_write_start(cur, req))
    return;  // fw or (wait for) lock

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // prepare
  version_t pdv = cur->pre_dirty();
  int mode = req->get_iarg();
  C_MDS_chmod_finish *fin = new C_MDS_chmod_finish(mds, req, cur, pdv,
						   mode);

  // log + wait
  EUpdate *le = new EUpdate("chmod");
  le->metablob.add_dir_context(cur->get_parent_dir());
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  pi->mode = mode;
  pi->version = pdv;
  
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}


// chown

class C_MDS_chown_finish : public Context {
  MDS *mds;
  MClientRequest *req;
  CInode *in;
  version_t pv;
  int uid, gid;
public:
  C_MDS_chown_finish(MDS *m, MClientRequest *r, CInode *i, version_t pdv, int u, int g) :
    mds(m), req(r), in(i), pv(pdv), uid(u), gid(g) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    if (uid >= 0) in->inode.uid = uid;
    if (gid >= 0) in->inode.gid = gid;
    in->mark_dirty(pv);

    // unlock
    mds->locker->inode_hard_write_finish(in);

    // reply
    MClientReply *reply = new MClientReply(req, 0);
    reply->set_result(0);
    mds->server->reply_request(req, reply, in);
  }
};


void Server::handle_client_chown(MClientRequest *req,
				 CInode *cur)
{
  // write
  if (!mds->locker->inode_hard_write_start(cur, req))
    return;  // fw or (wait for) lock

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // prepare
  version_t pdv = cur->pre_dirty();
  int uid = req->get_iarg();
  int gid = req->get_iarg2();
  C_MDS_chown_finish *fin = new C_MDS_chown_finish(mds, req, cur, pdv,
						   uid, gid);

  // log + wait
  EUpdate *le = new EUpdate("chown");
  le->metablob.add_dir_context(cur->get_parent_dir());
  inode_t *pi = le->metablob.add_dentry(cur->parent, true);
  if (uid >= 0) pi->uid = uid;
  if (gid >= 0) pi->gid = gid;
  pi->version = pdv;
  
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
    
    // hashed?
    if (dir->is_hashed() &&
        mds->get_nodeid() != mds->mdcache->hash_dentry( dir->ino(), it->first ))
      continue;

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


/*
 * note: this is pretty sloppy, but should work just fine i think...
 */
void Server::handle_hash_readdir(MHashReaddir *m)
{
  CInode *cur = mdcache->get_inode(m->get_ino());
  assert(cur);

  if (!cur->dir ||
      !cur->dir->is_hashed()) {
    assert(0);
    dout(7) << "handle_hash_readdir don't have dir open, or not hashed.  giving up!" << endl;
    delete m;
    return;    
  }
  CDir *dir = cur->dir;
  assert(dir);
  assert(dir->is_hashed());

  // complete?
  if (!dir->is_complete()) {
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << endl;
    mds->mdstore->fetch_dir(dir, new C_MDS_RetryMessage(mds, m));
    return;
  }  
  
  // get content
  list<InodeStat*> inls;
  list<string> dnls;
  int num = encode_dir_contents(dir, inls, dnls);
  
  // sent it back!
  messenger->send_message(new MHashReaddirReply(dir->ino(), inls, dnls, num),
                          m->get_source_inst(), MDS_PORT_CACHE);
}


void Server::handle_hash_readdir_reply(MHashReaddirReply *m)
{
  CInode *cur = mdcache->get_inode(m->get_ino());
  assert(cur);

  if (!cur->dir ||
      !cur->dir->is_hashed()) {
    assert(0);
    dout(7) << "handle_hash_readdir don't have dir open, or not hashed.  giving up!" << endl;
    delete m;
    return;    
  }
  CDir *dir = cur->dir;
  assert(dir);
  assert(dir->is_hashed());
  
  // move items to hashed_readdir gather
  int from = m->get_source().num();
  assert(dir->hashed_readdir.count(from) == 0);
  dir->hashed_readdir[from].first.splice(dir->hashed_readdir[from].first.begin(),
                                         m->get_in());
  dir->hashed_readdir[from].second.splice(dir->hashed_readdir[from].second.begin(),
                                          m->get_dn());
  delete m;

  // gather finished?
  if (dir->hashed_readdir.size() < (unsigned)mds->mdsmap->get_num_mds()) {
    dout(7) << "still waiting for more hashed readdir bits" << endl;
    return;
  }
  
  dout(7) << "got last bit!  finishing waiters" << endl;
  
  // do these finishers.  they'll copy the results.
  list<Context*> finished;
  dir->take_waiting(CDIR_WAIT_THISHASHEDREADDIR, finished);
  finish_contexts(finished);
  
  // now discard these results
  for (map<int, pair< list<InodeStat*>, list<string> > >::iterator it = dir->hashed_readdir.begin();
       it != dir->hashed_readdir.end();
       it++) {
    for (list<InodeStat*>::iterator ci = it->second.first.begin();
         ci != it->second.first.end();
         ci++) 
      delete *ci;
  }
  dir->hashed_readdir.clear();
  
  // unpin dir (we're done!)
  dir->auth_unpin();
  
  // trigger any waiters for next hashed readdir cycle
  dir->take_waiting(CDIR_WAIT_NEXTHASHEDREADDIR, mds->finished_queue);
}


class C_MDS_HashReaddir : public Context {
  Server *server;
  MClientRequest *req;
  CDir *dir;
public:
  C_MDS_HashReaddir(Server *server, MClientRequest *req, CDir *dir) {
    this->server = server;
    this->req = req;
    this->dir = dir;
  }
  void finish(int r) {
    server->finish_hash_readdir(req, dir);
  }
};

void Server::finish_hash_readdir(MClientRequest *req, CDir *dir) 
{
  dout(7) << "finish_hash_readdir on " << *dir << endl;

  assert(dir->is_hashed());
  assert(dir->hashed_readdir.size() == (unsigned)mds->mdsmap->get_num_mds());

  // reply!
  MClientReply *reply = new MClientReply(req);
  reply->set_result(0);

  for (int i=0; i<mds->mdsmap->get_num_mds(); i++) {
    reply->copy_dir_items(dir->hashed_readdir[i].first,
                          dir->hashed_readdir[i].second);
  }

  // ok!
  reply_request(req, reply, dir->inode);
}


void Server::handle_client_readdir(MClientRequest *req,
                                CInode *cur)
{
  // it's a directory, right?
  if (!cur->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << endl;
    reply_request(req, -ENOTDIR);
    return;
  }

  // auth?
  if (!cur->dir_is_auth()) {
    int dirauth = cur->authority();
    if (cur->dir)
      dirauth = cur->dir->authority();
    assert(dirauth >= 0);
    assert(dirauth != mds->get_nodeid());
    
    // forward to authority
    dout(10) << " forwarding readdir to authority " << dirauth << endl;
    mdcache->request_forward(req, dirauth);
    return;
  }
  
  if (!try_open_dir(cur, req))
    return;
  assert(cur->dir->is_auth());

  // unhashing?  wait!
  if (cur->dir->is_hashed() &&
      cur->dir->is_unhashing()) {
    dout(10) << "unhashing, waiting" << endl;
    cur->dir->add_waiter(CDIR_WAIT_UNFREEZE,
                         new C_MDS_RetryRequest(mds, req, cur));
    return;
  }

  // check perm
  if (!mds->locker->inode_hard_read_start(cur,req))
    return;
  mds->locker->inode_hard_read_finish(cur);

  CDir *dir = cur->dir;
  assert(dir);

  if (!dir->is_complete()) {
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *cur->dir << ", fetching" << endl;
    mds->mdstore->fetch_dir(dir, new C_MDS_RetryRequest(mds, req, cur));
    return;
  }

  if (dir->is_hashed()) {
    // HASHED
    dout(7) << "hashed dir" << endl;
    if (!dir->can_auth_pin()) {
      dout(7) << "can't auth_pin dir " << *dir << " waiting" << endl;
      dir->add_waiter(CDIR_WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mds, req, cur));
      return;
    }

    if (!dir->hashed_readdir.empty()) {
      dout(7) << "another readdir gather in progres, waiting" << endl;
      dir->add_waiter(CDIR_WAIT_NEXTHASHEDREADDIR, new C_MDS_RetryRequest(mds, req, cur));
      return;
    }

    // start new readdir gather
    dout(7) << "staring new hashed readdir gather" << endl;

    // pin auth for process!
    dir->auth_pin();
    
    // get local bits
    encode_dir_contents(cur->dir, 
                        dir->hashed_readdir[mds->get_nodeid()].first,
                        dir->hashed_readdir[mds->get_nodeid()].second);
    
    // request other bits
    for (int i=0; i<mds->mdsmap->get_num_mds(); i++) {
      if (i == mds->get_nodeid()) continue;
      mds->send_message_mds(new MHashReaddir(dir->ino()), i, MDS_PORT_SERVER);
    }

    // wait
    dir->add_waiter(CDIR_WAIT_THISHASHEDREADDIR, 
                    new C_MDS_HashReaddir(this, req, dir));
  } else {
    // NON-HASHED
    // build dir contents
    list<InodeStat*> inls;
    list<string> dnls;
    int numfiles = encode_dir_contents(cur->dir, inls, dnls);
    
    // . too
    dnls.push_back(".");
    inls.push_back(new InodeStat(cur, mds->get_nodeid()));
    ++numfiles;

    // yay, reply
    MClientReply *reply = new MClientReply(req);
    reply->take_dir_items(inls, dnls, numfiles);
    
    dout(10) << "reply to " << *req << " readdir " << numfiles << " files" << endl;
    reply->set_result(0);
    
    //balancer->hit_dir(cur->dir);
    
    // reply
    reply_request(req, reply, cur);
  }
}



// ------------------------------------------------

// MKNOD

class C_MDS_mknod_finish : public Context {
  MDS *mds;
  MClientRequest *req;
  CDentry *dn;
  CInode *newi;
  version_t pv;
public:
  C_MDS_mknod_finish(MDS *m, MClientRequest *r, CDentry *d, CInode *ni) :
    mds(m), req(r), dn(d), newi(ni),
    pv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_inode(dn, newi);

    // dirty inode, dn, dir
    newi->mark_dirty(pv);

    // unlock
    mds->locker->dentry_xlock_finish(dn);

    // hit pop
    mds->balancer->hit_inode(newi, META_POP_IWR);

    // reply
    MClientReply *reply = new MClientReply(req, 0);
    reply->set_result(0);
    mds->server->reply_request(req, reply, newi);
  }
};

void Server::handle_client_mknod(MClientRequest *req, CInode *diri)
{
  CInode *newi = 0;
  CDentry *dn = 0;

  // make dentry and inode, xlock dentry.
  if (!prepare_mknod(req, diri, &newi, &dn)) 
    return;
  assert(newi);
  assert(dn);

  // it's a file.
  newi->inode.mode = req->get_iarg();
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_FILE;
  
  // prepare finisher
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, req, dn, newi);
  EUpdate *le = new EUpdate("mknod");
  le->metablob.add_dir_context(diri->dir);
  inode_t *pi = le->metablob.add_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}



/*
 * verify that the dir exists and would own the dname.
 * do not check if the dentry exists.
 */
CDir *Server::validate_new_dentry_dir(MClientRequest *req, CInode *diri, string& name)
{
  // make sure parent is a dir?
  if (!diri->is_dir()) {
    dout(7) << "validate_new_dentry_dir: not a dir" << endl;
    reply_request(req, -ENOTDIR);
    return false;
  }

  // am i not open, not auth?
  if (!diri->dir && !diri->is_auth()) {
    int dirauth = diri->authority();
    dout(7) << "validate_new_dentry_dir: don't know dir auth, not open, auth is i think mds" << dirauth << endl;
    mdcache->request_forward(req, dirauth);
    return false;
  }
  
  if (!try_open_dir(diri, req)) 
    return false;
  CDir *dir = diri->dir;
  
  // make sure it's my dentry
  int dnauth = dir->dentry_authority(name);  
  if (dnauth != mds->get_nodeid()) {
    // fw
    dout(7) << "mknod on " << req->get_path() << ", dentry " << *dir
	    << " dn " << name
	    << " not mine, fw to " << dnauth << endl;
    mdcache->request_forward(req, dnauth);
    return false;
  }

  // dir auth pinnable?
  if (!dir->can_auth_pin()) {
    dout(7) << "validate_new_dentry_dir: dir " << *dir << " not pinnable, waiting" << endl;
    dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
		    new C_MDS_RetryRequest(mds, req, diri));
    return false;
  }

  // frozen?
  if (dir->is_frozen()) {
    dout(7) << "dir is frozen " << *dir << endl;
    dir->add_waiter(CDIR_WAIT_UNFREEZE,
                    new C_MDS_RetryRequest(mds, req, diri));
    return false;
  }

  return dir;
}

/*
 * prepare a mknod-type operation (mknod, mkdir, symlink, open+create).
 * create the inode and dentry, but do not link them.
 * pre_dirty the dentry+dir.
 * xlock the dentry.
 *
 * return val
 *  0 - wait for something
 *  1 - created
 *  2 - already exists (only if okexist=true)
 */
int Server::prepare_mknod(MClientRequest *req, CInode *diri, 
			  CInode **pin, CDentry **pdn, 
			  bool okexist) 
{
  dout(10) << "prepare_mknod " << req->get_filepath() << " in " << *diri << endl;
  
  // get containing directory (without last bit)
  filepath dirpath = req->get_filepath().prefixpath(req->get_filepath().depth() - 1);
  string name = req->get_filepath().last_bit();
  
  CDir *dir = validate_new_dentry_dir(req, diri, name);
  if (!dir) return 0;

  // make sure name doesn't already exist
  *pdn = dir->lookup(name);
  if (*pdn) {
    if (!(*pdn)->can_read(req)) {
      dout(10) << "waiting on (existing!) dentry " << **pdn << endl;
      dir->add_waiter(CDIR_WAIT_DNREAD, name, new C_MDS_RetryRequest(mds, req, diri));
      return 0;
    }

    if (!(*pdn)->is_null()) {
      // name already exists
      if (okexist) {
        dout(10) << "dentry " << name << " exists in " << *dir << endl;
	*pin = (*pdn)->inode;
        return 2;
      } else {
        dout(10) << "dentry " << name << " exists in " << *dir << endl;
        reply_request(req, -EEXIST);
        return 0;
      }
    }
  }

  // make sure dir is complete
  if (!dir->is_complete()) {
    dout(7) << " incomplete dir contents for " << *dir << ", fetching" << endl;
    mds->mdstore->fetch_dir(dir, new C_MDS_RetryRequest(mds, req, diri));
    return 0;
  }

  // make sure dir is pinnable
  

  *pin = mdcache->create_inode();
  (*pin)->inode.uid = req->get_caller_uid();
  (*pin)->inode.gid = req->get_caller_gid();
  (*pin)->inode.ctime = (*pin)->inode.mtime = (*pin)->inode.atime = g_clock.gettime();   // now

  // note: inode.version will get set by finisher's mark_dirty.

  // create dentry
  if (!*pdn) 
    *pdn = dir->add_dentry(name, 0);

  (*pdn)->pre_dirty();

  // xlock dentry
  bool res = mds->locker->dentry_xlock_start(*pdn, req, diri);
  assert(res == true);
  
  // bump modify pop
  mds->balancer->hit_dir(dir, META_POP_DWR);

  return 1;
}





// MKDIR

void Server::handle_client_mkdir(MClientRequest *req, CInode *diri)
{
  CInode *newi = 0;
  CDentry *dn = 0;
  
  // make dentry and inode, xlock dentry.
  if (!prepare_mknod(req, diri, &newi, &dn)) 
    return;
  assert(newi);
  assert(dn);

  // it's a directory.
  newi->inode.mode = req->get_iarg();
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_DIR;
  newi->inode.layout = g_OSD_MDDirLayout;

  // ...and that new dir is empty.
  CDir *newdir = newi->get_or_open_dir(mds->mdcache);
  newdir->mark_complete();
  newdir->mark_dirty(newdir->pre_dirty());

  // prepare finisher
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, req, dn, newi);
  EUpdate *le = new EUpdate("mkdir");
  le->metablob.add_dir_context(diri->dir);
  inode_t *pi = le->metablob.add_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  le->metablob.add_dir(newi->dir, true);
  
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

void Server::handle_client_symlink(MClientRequest *req, CInode *diri)
{
  CInode *newi = 0;
  CDentry *dn = 0;

  // make dentry and inode, xlock dentry.
  if (!prepare_mknod(req, diri, &newi, &dn)) 
    return;
  assert(newi);
  assert(dn);

  // it's a symlink
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_SYMLINK;
  newi->symlink = req->get_sarg();

  // prepare finisher
  C_MDS_mknod_finish *fin = new C_MDS_mknod_finish(mds, req, dn, newi);
  EUpdate *le = new EUpdate("symlink");
  le->metablob.add_dir_context(diri->dir);
  inode_t *pi = le->metablob.add_dentry(dn, true, newi);
  pi->version = dn->get_projected_version();
  
  // log + wait
  mdlog->submit_entry(le);
  mdlog->wait_for_sync(fin);
}





// LINK

class C_MDS_LinkTraverse : public Context {
  Server *server;
  MClientRequest *req;
  CInode *ref;
public:
  vector<CDentry*> trace;
  C_MDS_LinkTraverse(Server *server, MClientRequest *req, CInode *ref) {
    this->server = server;
    this->req = req;
    this->ref = ref;
  }
  void finish(int r) {
    server->handle_client_link_2(r, req, ref, trace);
  }
};

void Server::handle_client_link(MClientRequest *req, CInode *ref)
{
  // figure out name
  string dname = req->get_filepath().last_bit();
  dout(7) << "handle_client_link dname is " << dname << endl;
  
  // validate dir
  CDir *dir = validate_new_dentry_dir(req, ref, dname);
  if (!dir) return;

  // dentry exists?
  CDentry *dn = dir->lookup(dname);
  if (dn && (!dn->is_null() || dn->is_xlockedbyother(req))) {
    dout(7) << "handle_client_link dn exists " << *dn << endl;
    reply_request(req, -EEXIST);
    return;
  }

  // xlock dentry
  if (!dn->is_xlockedbyme(req)) {
    if (!mds->locker->dentry_xlock_start(dn, req, ref)) 
      return;
  }

  // discover link target
  filepath target = req->get_sarg();
  dout(7) << "handle_client_link discovering target " << target << endl;
  C_MDS_LinkTraverse *onfinish = new C_MDS_LinkTraverse(this, req, ref);
  Context *ondelay = new C_MDS_RetryRequest(mds, req, ref);
  
  mdcache->path_traverse(target, onfinish->trace, false,
                         req, ondelay,
                         MDS_TRAVERSE_DISCOVER,  //XLOCK, 
                         onfinish);
}


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

void Server::handle_client_link_2(int r, MClientRequest *req, CInode *diri, vector<CDentry*>& trace)
{
  // target dne?
  if (r < 0) {
    dout(7) << "target " << req->get_sarg() << " dne" << endl;
    reply_request(req, r);
    return;
  }
  assert(r == 0);

  CInode *targeti = mdcache->get_root();
  if (trace.size()) targeti = trace[trace.size()-1]->inode;
  assert(targeti);

  // dir?
  dout(7) << "target is " << *targeti << endl;
  if (targeti->is_dir()) {
    dout(7) << "target is a dir, failing" << endl;
    reply_request(req, -EINVAL);
    return;
  }
  
  // what was the new dentry again?
  CDir *dir = diri->dir;
  assert(dir);
  string dname = req->get_filepath().last_bit();
  CDentry *dn = dir->lookup(dname);
  assert(dn);
  assert(dn->is_xlockedbyme(req));


  // ok!
  if (targeti->is_auth()) {
    // mine

    // same dir?
    if (targeti->get_parent_dir() == dn->get_dir()) {
      dout(7) << "target is in the same dir, sweet" << endl;
    } 
    else if (targeti->is_anchored()) {
      dout(7) << "target anchored already (nlink=" << targeti->inode.nlink << "), sweet" << endl;
    } else {
      assert(targeti->inode.nlink == 1);
      dout(7) << "target needs anchor, nlink=" << targeti->inode.nlink << ", creating anchor" << endl;
      
      mdcache->anchor_inode(targeti,
                            new C_MDS_RetryRequest(mds, req, diri));
      return;
    }

    // ok, inc link!
    targeti->inode.nlink++;
    dout(7) << "nlink++, now " << targeti->inode.nlink << " on " << *targeti << endl;
    targeti->_mark_dirty(); // fixme
    
  } else {
    // remote: send nlink++ request, wait
    dout(7) << "target is remote, sending InodeLink" << endl;
    mds->send_message_mds(new MInodeLink(targeti->ino(), mds->get_nodeid()), targeti->authority(), MDS_PORT_CACHE);
    
    // wait
    targeti->add_waiter(CINODE_WAIT_LINK,
                        new C_MDS_RemoteLink(this, req, diri, dn, targeti));
    return;
  }

  handle_client_link_finish(req, diri, dn, targeti);
}

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


// UNLINK

void Server::handle_client_unlink(MClientRequest *req, 
                               CInode *diri)
{
  // rmdir or unlink
  bool rmdir = false;
  if (req->get_op() == MDS_OP_RMDIR) rmdir = true;
  
  // find it
  if (req->get_filepath().depth() == 0) {
    dout(7) << "can't rmdir root" << endl;
    reply_request(req, -EINVAL);
    return;
  }
  string name = req->get_filepath().last_bit();
  
  // make sure parent is a dir?
  if (!diri->is_dir()) {
    dout(7) << "not a dir" << endl;
    reply_request(req, -ENOTDIR);
    return;
  }

  // am i not open, not auth?
  if (!diri->dir && !diri->is_auth()) {
    int dirauth = diri->authority();
    dout(7) << "don't know dir auth, not open, auth is i think " << dirauth << endl;
    mdcache->request_forward(req, dirauth);
    return;
  }
  
  if (!try_open_dir(diri, req)) return;
  CDir *dir = diri->dir;
  int dnauth = dir->dentry_authority(name);  

  // does it exist?
  CDentry *dn = dir->lookup(name);
  if (!dn) {
    if (dnauth == mds->get_nodeid()) {
      dout(7) << "handle_client_rmdir/unlink dne " << name << " in " << *dir << endl;
      reply_request(req, -ENOENT);
    } else {
      // send to authority!
      dout(7) << "handle_client_rmdir/unlink fw, don't have " << name << " in " << *dir << endl;
      mdcache->request_forward(req, dnauth);
    }
    return;
  }

  // have it.  locked?
  if (!dn->can_read(req)) {
    dout(10) << " waiting on " << *dn << endl;
    dir->add_waiter(CDIR_WAIT_DNREAD,
                    name,
                    new C_MDS_RetryRequest(mds, req, diri));
    return;
  }

  // null?
  if (dn->is_null()) {
    dout(10) << "unlink on null dn " << *dn << endl;
    reply_request(req, -ENOENT);
    return;
  }

  // ok!
  CInode *in = dn->inode;
  assert(in);
  if (rmdir) {
    dout(7) << "handle_client_rmdir on dir " << *in << endl;
  } else {
    dout(7) << "handle_client_unlink on non-dir " << *in << endl;
  }

  // dir stuff 
  if (in->is_dir()) {
    if (rmdir) {
      // rmdir
      
      // open dir?
      if (in->is_auth() && !in->dir) {
        if (!try_open_dir(in, req)) return;
      }

      // not dir auth?  (or not open, which implies the same!)
      if (!in->dir) {
        dout(7) << "handle_client_rmdir dir not open for " << *in << ", sending to dn auth " << dnauth << endl;
        mdcache->request_forward(req, dnauth);
        return;
      }
      if (!in->dir->is_auth()) {
        int dirauth = in->dir->authority();
        dout(7) << "handle_client_rmdir not auth for dir " << *in->dir << ", sending to dir auth " << dnauth << endl;
        mdcache->request_forward(req, dirauth);
        return;
      }

      assert(in->dir);
      assert(in->dir->is_auth());

      // dir size check on dir auth (but not necessarily dentry auth)?

      // should be empty
      if (in->dir->get_size() == 0 && !in->dir->is_complete()) {
        dout(7) << "handle_client_rmdir on dir " << *in->dir << ", empty but not complete, fetching" << endl;
        mds->mdstore->fetch_dir(in->dir, 
				new C_MDS_RetryRequest(mds, req, diri));
        return;
      }
      if (in->dir->get_size() > 0) {
        dout(7) << "handle_client_rmdir on dir " << *in->dir << ", not empty" << endl;
        reply_request(req, -ENOTEMPTY);
        return;
      }
        
      dout(7) << "handle_client_rmdir dir is empty!" << endl;

      // export sanity check
      if (!in->is_auth()) {
        // i should be exporting this now/soon, since the dir is empty.
        dout(7) << "handle_client_rmdir dir is auth, but not inode." << endl;
        if (!in->dir->is_freezing() && in->dir->is_frozen()) {
          assert(in->dir->is_import());
          mdcache->migrator->export_empty_import(in->dir);          
        } else {
          dout(7) << "apparently already exporting" << endl;
        }
        in->dir->add_waiter(CDIR_WAIT_UNFREEZE,
                            new C_MDS_RetryRequest(mds, req, diri));
        return;
      }

    } else {
      // unlink
      dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << endl;
      reply_request(req, -EISDIR);
      return;
    }
  } else {
    if (rmdir) {
      // unlink
      dout(7) << "handle_client_rmdir on non-dir " << *in << ", returning error" << endl;
      reply_request(req, -ENOTDIR);
      return;
    }
  }

  // am i dentry auth?
  if (dnauth != mds->get_nodeid()) {
    // not auth; forward!
    dout(7) << "handle_client_unlink not auth for " << *dir << " dn " << dn->name << ", fwd to " << dnauth << endl;
    mdcache->request_forward(req, dnauth);
    return;
  }
    
  dout(7) << "handle_client_unlink/rmdir on " << *in << endl;
  
  // xlock dentry
  if (!mds->locker->dentry_xlock_start(dn, req, diri))
    return;

  // is this a remote link?
  if (dn->is_remote() && !dn->inode) {
    CInode *in = mdcache->get_inode(dn->get_remote_ino());
    if (in) {
      dn->link_remote(in);
    } else {
      // open inode
      dout(7) << "opening target inode first, ino is " << dn->get_remote_ino() << endl;
      mdcache->open_remote_ino(dn->get_remote_ino(), req, 
                               new C_MDS_RetryRequest(mds, req, diri));
      return;
    }
  }

    
  mds->balancer->hit_dir(dn->dir, META_POP_DWR);

  // it's locked, unlink!
  MClientReply *reply = new MClientReply(req,0);
  mdcache->dentry_unlink(dn,
                         new C_MDS_CommitRequest(this, req, reply, diri,
                                                 new EString("unlink fixme")));
  return;
}






// RENAME

class C_MDS_RenameTraverseDst : public Context {
  Server *server;
  MClientRequest *req;
  CInode *ref;
  CInode *srcdiri;
  CDir *srcdir;
  CDentry *srcdn;
  filepath destpath;
public:
  vector<CDentry*> trace;
  
  C_MDS_RenameTraverseDst(Server *server,
                          MClientRequest *req, 
                          CInode *ref,
                          CInode *srcdiri,
                          CDir *srcdir,
                          CDentry *srcdn,
                          filepath& destpath) {
    this->server = server;
    this->req = req;
    this->ref = ref;
    this->srcdiri = srcdiri;
    this->srcdir = srcdir;
    this->srcdn = srcdn;
    this->destpath = destpath;
  }
  void finish(int r) {
    server->handle_client_rename_2(req, ref,
				   srcdiri, srcdir, srcdn, destpath,
				   trace, r);
  }
};


/*
  
  weirdness iwith rename:
    - ref inode is what was originally srcdiri, but that may change by the tiem
      the rename actually happens.  for all practical purpose, ref is useless except
      for C_MDS_RetryRequest

 */
void Server::handle_client_rename(MClientRequest *req,
                               CInode *ref)
{
  dout(7) << "handle_client_rename on " << *req << endl;

  // sanity checks
  if (req->get_filepath().depth() == 0) {
    dout(7) << "can't rename root" << endl;
    reply_request(req, -EINVAL);
    return;
  }
  // mv a/b a/b/c  -- meaningless
  if (req->get_sarg().compare( 0, req->get_path().length(), req->get_path()) == 0 &&
      req->get_sarg().c_str()[ req->get_path().length() ] == '/') {
    dout(7) << "can't rename to underneath myself" << endl;
    reply_request(req, -EINVAL);
    return;
  }

  // mv blah blah  -- also meaningless
  if (req->get_sarg() == req->get_path()) {
    dout(7) << "can't rename something to itself (or into itself)" << endl;
    reply_request(req, -EINVAL);
    return;
  }
  
  // traverse to source
  /*
    this is abnoraml, just for rename.  since we don't pin source path 
    (because we don't want to screw up the lock ordering) the ref inode 
    (normally/initially srcdiri) may move, and this may fail.
 -> so, re-traverse path.  and make sure we request_finish in the case of a forward!
   */
  filepath refpath = req->get_filepath();
  string srcname = refpath.last_bit();
  refpath = refpath.prefixpath(refpath.depth()-1);

  dout(7) << "handle_client_rename src traversing to srcdir " << refpath << endl;
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(refpath, trace, true,
                                 req, new C_MDS_RetryRequest(mds, req, ref),
                                 MDS_TRAVERSE_FORWARD);
  if (r == 2) {
    dout(7) << "path traverse forwarded, ending request, doing manual request_cleanup" << endl;
    dout(7) << "(pseudo) request_forward to 9999 req " << *req << endl;
    mdcache->request_cleanup(req);  // not _finish (deletes) or _forward (path_traverse did that)
    return;
  }
  if (r > 0) return;
  if (r < 0) {   // dne or something.  got renamed out from under us, probably!
    dout(7) << "traverse r=" << r << endl;
    reply_request(req, r);
    return;
  }
  
  CInode *srcdiri;
  if (trace.size()) 
    srcdiri = trace[trace.size()-1]->inode;
  else
    srcdiri = mdcache->get_root();

  dout(7) << "handle_client_rename srcdiri is " << *srcdiri << endl;

  dout(7) << "handle_client_rename srcname is " << srcname << endl;

  // make sure parent is a dir?
  if (!srcdiri->is_dir()) {
    dout(7) << "srcdiri not a dir " << *srcdiri << endl;
    reply_request(req, -EINVAL);
    return;
  }

  // am i not open, not auth?
  if (!srcdiri->dir && !srcdiri->is_auth()) {
    int dirauth = srcdiri->authority();
    dout(7) << "don't know dir auth, not open, srcdir auth is probably " << dirauth << endl;
    mdcache->request_forward(req, dirauth);
    return;
  }
  
  if (!try_open_dir(srcdiri, req)) return;
  CDir *srcdir = srcdiri->dir;
  dout(7) << "handle_client_rename srcdir is " << *srcdir << endl;
  
  // make sure it's my dentry
  int srcauth = srcdir->dentry_authority(srcname);  
  if (srcauth != mds->get_nodeid()) {
    // fw
    dout(7) << "rename on " << req->get_path() << ", dentry " << *srcdir << " dn " << srcname << " not mine, fw to " << srcauth << endl;
    mdcache->request_forward(req, srcauth);
    return;
  }
  // ok, done passing buck.

  // src dentry
  CDentry *srcdn = srcdir->lookup(srcname);

  // xlocked?
  if (srcdn && !srcdn->can_read(req)) {
    dout(10) << " waiting on " << *srcdn << endl;
    srcdir->add_waiter(CDIR_WAIT_DNREAD,
                       srcname,
                       new C_MDS_RetryRequest(mds, req, srcdiri));
    return;
  }
  
  if ((srcdn && !srcdn->inode) ||
      (!srcdn && srcdir->is_complete())) {
    dout(10) << "handle_client_rename src dne " << endl;
    reply_request(req, -EEXIST);
    return;
  }
  
  if (!srcdn && !srcdir->is_complete()) {
    dout(10) << "readding incomplete dir" << endl;
    mds->mdstore->fetch_dir(srcdir,
			    new C_MDS_RetryRequest(mds, req, srcdiri));
    return;
  }
  assert(srcdn && srcdn->inode);


  dout(10) << "handle_client_rename srcdn is " << *srcdn << endl;
  dout(10) << "handle_client_rename srci is " << *srcdn->inode << endl;

  // pin src in cache (so it won't expire)
  mdcache->request_pin_inode(req, srcdn->inode);
  
  // find the destination, normalize
  // discover, etc. on the way... just get it on the local node.
  filepath destpath = req->get_sarg();   

  C_MDS_RenameTraverseDst *onfinish = new C_MDS_RenameTraverseDst(this, req, ref, srcdiri, srcdir, srcdn, destpath);
  Context *ondelay = new C_MDS_RetryRequest(mds, req, ref);
  
  /*
   * use DISCOVERXLOCK mode:
   *   the dest may not exist, and may be xlocked from a remote host
   *   we want to succeed if we find the xlocked dentry
   * ??
   */
  mdcache->path_traverse(destpath, onfinish->trace, false,
                         req, ondelay,
                         MDS_TRAVERSE_DISCOVER,  //XLOCK, 
                         onfinish);
}

void Server::handle_client_rename_2(MClientRequest *req,
                                 CInode *ref,
                                 CInode *srcdiri,
                                 CDir *srcdir,
                                 CDentry *srcdn,
                                 filepath& destpath,
                                 vector<CDentry*>& trace,
                                 int r)
{
  dout(7) << "handle_client_rename_2 on " << *req << endl;
  dout(12) << " r = " << r << " trace depth " << trace.size() << "  destpath depth " << destpath.depth() << endl;

  CInode *srci = srcdn->inode;
  assert(srci);
  CDir*  destdir = 0;
  string destname;
  
  // what is the dest?  (dir or file or complete filename)
  // note: trace includes root, destpath doesn't (include leading /)
  if (trace.size() && trace[trace.size()-1]->inode == 0) {
    dout(10) << "dropping null dentry from tail of trace" << endl;
    trace.pop_back();    // drop it!
  }
  
  CInode *d;
  if (trace.size()) 
    d = trace[trace.size()-1]->inode;
  else
    d = mdcache->get_root();
  assert(d);
  dout(10) << "handle_client_rename_2 traced to " << *d << ", trace size = " << trace.size() << ", destpath = " << destpath.depth() << endl;
  
  // make sure i can open the dir?
  if (d->is_dir() && !d->dir_is_auth() && !d->dir) {
    // discover it
    mdcache->open_remote_dir(d,
                             new C_MDS_RetryRequest(mds, req, ref));
    return;
  }

  if (trace.size() == destpath.depth()) {
    if (d->is_dir()) {
      // mv /some/thing /to/some/dir 
      if (!try_open_dir(d, req)) return;
      destdir = d->dir;                           // /to/some/dir
      destname = req->get_filepath().last_bit();  // thing
      destpath.add_dentry(destname);
    } else {
      // mv /some/thing /to/some/existing_filename
      destdir = trace[trace.size()-1]->dir;       // /to/some
      destname = destpath.last_bit();             // existing_filename
    }
  }
  else if (trace.size() == destpath.depth()-1) {
    if (d->is_dir()) {
      // mv /some/thing /to/some/place_that_maybe_dne     (we might be replica)
      if (!try_open_dir(d, req)) return;
      destdir = d->dir;                  // /to/some
      destname = destpath.last_bit();    // place_that_MAYBE_dne
    } else {
      dout(7) << "dest dne" << endl;
      reply_request(req, -EINVAL);
      return;
    }
  }
  else {
    assert(trace.size() < destpath.depth()-1);
    // check traverse return value
    if (r > 0) {
      return;  // discover, readdir, etc.
    }

    // ??
    assert(r < 0 || trace.size() == 0);  // musta been an error

    // error out
    dout(7) << " rename dest " << destpath << " dne" << endl;
    reply_request(req, -EINVAL);
    return;
  }

  string srcpath = req->get_path();
  dout(10) << "handle_client_rename_2 srcpath " << srcpath << endl;
  dout(10) << "handle_client_rename_2 destpath " << destpath << endl;

  // src == dest?
  if (srcdn->get_dir() == destdir && srcdn->name == destname) {
    dout(7) << "rename src=dest, same file " << endl;
    reply_request(req, -EINVAL);
    return;
  }

  // does destination exist?  (is this an overwrite?)
  CDentry *destdn = destdir->lookup(destname);
  CInode  *oldin = 0;
  if (destdn) {
    oldin = destdn->get_inode();
    
    if (oldin) {
      // make sure it's also a file!
      // this can happen, e.g. "mv /some/thing /a/dir" where /a/dir/thing exists and is a dir.
      if (oldin->is_dir()) {
        // fail!
        dout(7) << "dest exists and is dir" << endl;
        reply_request(req, -EISDIR);
        return;
      }

      if (srcdn->inode->is_dir() &&
          !oldin->is_dir()) {
        dout(7) << "cannot overwrite non-directory with directory" << endl;
        reply_request(req, -EISDIR);
        return;
      }
    }

    dout(7) << "dest exists " << *destdn << endl;
    if (destdn->get_inode()) {
      dout(7) << "destino is " << *destdn->get_inode() << endl;
    } else {
      dout(7) << "dest dn is a NULL stub" << endl;
    }
  } else {
    dout(7) << "dest dn dne (yet)" << endl;
  }
  

  // local or remote?
  int srcauth = srcdir->dentry_authority(srcdn->name);
  int destauth = destdir->dentry_authority(destname);
  dout(7) << "handle_client_rename_2 destname " << destname << " destdir " << *destdir << " auth " << destauth << endl;
  
  // 
  if (srcauth != mds->get_nodeid() || 
      destauth != mds->get_nodeid()) {
    dout(7) << "rename has remote dest " << destauth << endl;
    dout(7) << "FOREIGN RENAME" << endl;
    
    // punt?
    if (false && srcdn->inode->is_dir()) {
      reply_request(req, -EINVAL);  
      return; 
    }

  } else {
    dout(7) << "rename is local" << endl;
  }

  handle_client_rename_local(req, ref,
                             srcpath, srcdiri, srcdn, 
                             destpath.get_path(), destdir, destdn, destname);
  return;
}




void Server::handle_client_rename_local(MClientRequest *req,
                                     CInode *ref,
                                     string& srcpath,
                                     CInode *srcdiri,
                                     CDentry *srcdn,
                                     string& destpath,
                                     CDir *destdir,
                                     CDentry *destdn,
                                     string& destname)
{
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

  bool srclocal = srcdn->dir->dentry_authority(srcdn->name) == mds->get_nodeid();
  bool destlocal = destdir->dentry_authority(destname) == mds->get_nodeid();

  dout(7) << "handle_client_rename_local: src local=" << srclocal << " " << *srcdn << endl;
  if (destdn) {
    dout(7) << "handle_client_rename_local: dest local=" << destlocal << " " << *destdn << endl;
  } else {
    dout(7) << "handle_client_rename_local: dest local=" << destlocal << " dn dne yet" << endl;
  }

  /* lock source and dest dentries, in lexicographic order.
   */
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
          /* NOTE: require that my xlocked item be a leaf/file, NOT a dir.  in case
           * my traverse and determination of dest vs dest/srcfilename was out of date.
           */
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











// ===================================
// TRUNCATE, FSYNC

/*
 * FIXME: this truncate implemention is WRONG WRONG WRONG
 */

void Server::handle_client_truncate(MClientRequest *req, CInode *cur)
{
  // write
  if (!mds->locker->inode_file_write_start(cur, req))
    return;  // fw or (wait for) lock

  // check permissions
  
  // do update
  cur->inode.size = req->get_sizearg();
  cur->_mark_dirty(); // fixme

  mds->locker->inode_file_write_finish(cur);

  mds->balancer->hit_inode(cur, META_POP_IWR);   

  // start reply
  MClientReply *reply = new MClientReply(req, 0);

  // commit
  commit_request(req, reply, cur,
                 new EString("truncate fixme"));
}

set<inodeno_t> Server::parse_predictions(string pred_string) {
  // get number of predictions
  int first_index = pred_string.find_first_of(",");
  int num_preds = atoi(pred_string.substr(0, first_index).c_str());
  cout << "Number of predictions to make " << num_preds << endl;
  
  string str_to_parse = pred_string.substr(first_index+1, pred_string.size()-1);
  cout << "Remaining string " << str_to_parse << ":" << endl;
  
  set<inodeno_t> successors;
  string pred;
  inodeno_t place_holder_inode;
  int comma_index;
  for (int parser = 0; parser < num_preds; parser++) {

    comma_index = str_to_parse.find_first_of(",");
    pred = str_to_parse.substr(0, comma_index);
    cout << "Got token " << pred << ":" << endl;
    if (pred == ";")
      break;

    memset((byte*)&place_holder_inode, 0x00, sizeof(place_holder_inode));
    memcpy((byte*)&place_holder_inode, pred.c_str(), pred.size());
    successors.insert(place_holder_inode);
    cout << "Just inserted place_holder_inode " << place_holder_inode << endl;
    str_to_parse = str_to_parse.substr(comma_index+1, str_to_parse.size()-1);
  }

  return successors;
}

int Server::get_bl_ss(bufferlist& bl)
{
  string dir = "/home/aleung/ssrc/ceph/branches/aleung/security1/ceph";
  /*
  char fn[200];
  if (b) {
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    sprintf(fn, "%s/%s", dir.c_str(), a);
  }
  */
  char fn[200];
  sprintf(fn, "predictions");
  
  int fd = ::open(fn, O_RDONLY);
  if (!fd) {
    /*
    if (b) {
      dout(15) << "get_bl " << a << "/" << b << " DNE" << endl;
    } else {
      dout(15) << "get_bl " << a << " DNE" << endl;
    }
    */
    dout(15) << "get_bl predictions DNE" << endl;
    return 0;
  }

  // get size
  struct stat st;
  int rc = ::fstat(fd, &st);
  assert(rc == 0);
  __int32_t len = st.st_size;
 
  // read buffer
  bl.clear();
  bufferptr bp(len);
  int off = 0;
  while (off < len) {
    dout(20) << "reading at off " << off << " of " << len << endl;
    int r = ::read(fd, bp.c_str()+off, len-off);
    if (r < 0) derr(0) << "errno on read " << strerror(errno) << endl;
    assert(r>0);
    off += r;
  }
  bl.append(bp);
  ::close(fd);

  /*
  if (b) {
    dout(15) << "get_bl " << a << "/" << b << " = " << bl.length() << " bytes" << endl;
  } else {
    dout(15) << "get_bl " << a << " = " << bl.length() << " bytes" << endl;
  }
  */
  dout(15) << "get_bl predictions = " << bl.length() << " bytes" << endl;

  return len;
}

int Server::put_bl_ss(bufferlist& bl)
{
  string dir = "/home/aleung/ssrc/ceph/branches/aleung/security1/ceph";
  /*
  char fn[200];
  sprintf(fn, "%s/%s", dir.c_str(), a);
  if (b) {
    ::mkdir(fn, 0755);
    dout(15) << "put_bl " << a << "/" << b << " = " << bl.length() << " bytes" << endl;
    sprintf(fn, "%s/%s/%s", dir.c_str(), a, b);
  } else {
    dout(15) << "put_bl " << a << " = " << bl.length() << " bytes" << endl;
  }
  
  char tfn[200];
  sprintf(tfn, "%s.new", fn);  
  */
  char tfn[200];
  sprintf(tfn, "predictions");
  int fd = ::open(tfn, O_WRONLY|O_CREAT);
  assert(fd);
  
  // chmod
  ::fchmod(fd, 0644);

  // write data
  for (list<bufferptr>::const_iterator it = bl.buffers().begin();
       it != bl.buffers().end();
       it++)  {
    int r = ::write(fd, it->c_str(), it->length());
    if (r != (int)it->length())
      derr(0) << "put_bl_ss ::write() returned " << r << " not " << it->length() << endl;
    if (r < 0) 
      derr(0) << "put_bl_ss ::write() errored out, errno is " << strerror(errno) << endl;
  }

  ::fsync(fd);
  ::close(fd);
  //::rename(tfn, fn);

  return 0;
}

// ===========================
// open, openc, close

void Server::handle_client_open(MClientRequest *req,
				CInode *cur)
{

  utime_t start_time = g_clock.now();
  utime_t end_time;
  int flags = req->get_iarg();
  int mode = req->get_iarg2();

  dout(7) << "open " << flags << " on " << *cur << endl;
  dout(10) << "open flags = " << flags << "  mode = " << mode << endl;

  // is it a file?
  if (!(cur->inode.mode & INODE_MODE_FILE)) {
    dout(7) << "not a regular file" << endl;
    reply_request(req, -EINVAL);                 // FIXME what error do we want?
    return;
  }


  // check file prediction stuff
  if (g_conf.collect_predictions != 0) {
    // if this isnt the first access
    if (mds->last_access != inodeno_t() && mds->last_access != cur->ino()) {
      cout << "Observed " << cur->ino() << " followed "
	   << mds->last_access << endl;

      mds->rp_predicter.add_observation(mds->last_access, cur->ino());
      bufferlist bl;
      ::_encode(mds->rp_predicter.get_sequence(), bl);
      put_bl_ss(bl);
    }

    mds->last_access = cur->ino();
  }
  
  // auth for write access
  // redirect the write open to the auth?
  if (mode != FILE_MODE_R && mode != FILE_MODE_LAZY &&
      !cur->is_auth()) {
    int auth = cur->authority();
    assert(auth != mds->get_nodeid());
    dout(9) << "open writeable on replica for " << *cur << " fw to auth " << auth << endl;
    
    mdcache->request_forward(req, auth);
    return;
  }

  // hmm, check permissions or something.
  // permissions must be checked at the user/group/world level

  // can we issue the caps they want?
  version_t fdv = mds->locker->issue_file_data_version(cur);
  //Capability *cap = mds->locker->issue_new_caps(cur, mode, req);

  // create signed security capability
  // no security, just include a blank cap
  ExtCap *ext_cap;
  
  utime_t sec_time_start = g_clock.now();
  ext_cap = mds->locker->issue_new_extcaps(cur, mode, req);
  utime_t sec_time_end = g_clock.now();
  dout(2) << "Get security cap time " << sec_time_end - sec_time_start << endl;

  Capability *cap = mds->locker->issue_new_caps(cur, mode, req);
  if (!cap) return; // can't issue (yet), so wait!

  dout(12) << "open gets caps " << cap_string(cap->pending()) << " for " << req->get_source() << " on " << *cur << endl;

  mds->balancer->hit_inode(cur, META_POP_IRD);

  end_time = g_clock.now();
  dout(2) << "Open() request latency " << end_time - start_time << endl;
  if (mds->logger) {
    mds->logger->finc("lsum", (double) end_time - start_time);
    mds->logger->inc("lnum");
  }

  // reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_file_caps(cap->pending());
  reply->set_file_caps_seq(cap->get_last_seq());
  reply->set_file_data_version(fdv);
  // set security cap if security is on
  if (g_conf.secure_io)
    reply->set_ext_cap(ext_cap);
  
  reply_request(req, reply, cur);
}


class C_MDS_openc_finish : public Context {
  MDS *mds;
  MClientRequest *req;
  CDentry *dn;
  CInode *newi;
  version_t pv;
public:
  C_MDS_openc_finish(MDS *m, MClientRequest *r, CDentry *d, CInode *ni) :
    mds(m), req(r), dn(d), newi(ni),
    pv(d->get_projected_version()) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->get_dir()->link_inode(dn, newi);

    // dirty inode, dn, dir
    newi->mark_dirty(pv);

    // unlock
    mds->locker->dentry_xlock_finish(dn);

    // hit pop
    mds->balancer->hit_inode(newi, META_POP_IWR);

    // ok, do the open.
    if (g_conf.mds_group == 3) {
      uid_t user_id = req->get_caller_uid();
      utime_t open_req_time = g_clock.now();
      
      if (mds->user_batch[user_id]->should_batch(open_req_time)) {
	
	mds->user_batch[user_id]->update_batch_time(open_req_time);
	
	cout << "Passing inode " << newi->ino() << " to add_to_batch" << endl;
	mds->user_batch[user_id]->add_to_batch(req, newi);
	
	return;
      }
      else {
	mds->user_batch[user_id]->update_batch_time(open_req_time);
	mds->server->handle_client_open(req, newi);
      }
    }
    else
      mds->server->handle_client_open(req, newi);
  }
};


void Server::handle_client_openc(MClientRequest *req, CInode *diri)
{
  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << endl;

  CInode *in = 0;
  CDentry *dn = 0;
  
  // make dentry and inode, xlock dentry.
  bool excl = req->get_iarg() & O_EXCL;
  int r = prepare_mknod(req, diri, &in, &dn, !excl);

  if (!r) 
    return; // wait on something
  assert(in);
  assert(dn);

  if (r == 1) {
    // created.
    // it's a file.
    in->inode.mode = 0644;              // FIXME req should have a umask
    in->inode.mode |= INODE_MODE_FILE;

    if (g_conf.mds_group == 3) {
      uid_t my_user = req->get_caller_uid();
      // create and start user batching thread
      if (! mds->user_batch[my_user]) {
	mds->user_batch[my_user] = new UserBatch(this, mds, my_user);
	mds->user_batch[my_user]->update_batch_time(g_clock.now());
      }
    }

    // prepare finisher
    C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, req, dn, in);
    EUpdate *le = new EUpdate("openc");
    le->metablob.add_dir_context(diri->dir);
    inode_t *pi = le->metablob.add_dentry(dn, true, in);
    pi->version = dn->get_projected_version();
    
    // log + wait
    mdlog->submit_entry(le);
    mdlog->wait_for_sync(fin);

    /*
      FIXME. this needs to be rewritten when the write capability stuff starts
      getting journaled.  
    */
  } else {
    // exists!
    // FIXME: do i need to repin path based existant inode? hmm.
    if (g_conf.mds_group == 2) {

      utime_t open_req_time = g_clock.now();

      if (in->should_batch(open_req_time)) {

	in->update_buffer_time(open_req_time);

	in->add_to_buffer(req, this, mds);

	return;
      }
      else {
	in->update_buffer_time(open_req_time);
	handle_client_open(req, in);
      }
    }
    else if (g_conf.mds_group == 3) {
      uid_t user_id = req->get_caller_uid();
      utime_t open_req_time = g_clock.now();
      
      if (mds->user_batch[user_id]->should_batch(open_req_time)) {

	mds->user_batch[user_id]->update_batch_time(open_req_time);
	
	mds->user_batch[user_id]->add_to_batch(req, in);

	return;
      }
      else {
	mds->user_batch[user_id]->update_batch_time(open_req_time);
	handle_client_open(req, in);
      }

    }
    else
      handle_client_open(req, in);
  }
}














