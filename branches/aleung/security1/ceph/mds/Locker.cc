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
#include "MDCache.h"
#include "Locker.h"
#include "Server.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Migrator.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EString.h"
#include "events/EUpdate.h"
#include "events/EUnlink.h"

#include "msg/Messenger.h"

#include "messages/MGenericMessage.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

#include "messages/MDirUpdate.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MInodeLink.h"
#include "messages/MInodeLinkAck.h"
#include "messages/MInodeUnlink.h"
#include "messages/MInodeUnlinkAck.h"

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MClientRequest.h"
#include "messages/MClientFileCaps.h"

#include <errno.h>
#include <assert.h>

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".locker "



void Locker::dispatch(Message *m)
{
  switch (m->get_type()) {

    // locking
  case MSG_MDS_LOCK:
    handle_lock((MLock*)m);
    break;

    // cache fun
  case MSG_MDS_INODEFILECAPS:
    handle_inode_file_caps((MInodeFileCaps*)m);
    break;

  case MSG_CLIENT_FILECAPS:
    handle_client_file_caps((MClientFileCaps*)m);
    break;

    

  default:
    assert(0);
  }
}


void Locker::send_lock_message(CInode *in, int msg, int type)
{
  for (map<int,int>::iterator it = in->replicas_begin(); 
       it != in->replicas_end(); 
       it++) {
    MLock *m = new MLock(msg, mds->get_nodeid());
    m->set_ino(in->ino(), type);
    mds->send_message_mds(m, it->first, MDS_PORT_LOCKER);
  }
}


void Locker::send_lock_message(CInode *in, int msg, int type, bufferlist& data)
{
  for (map<int,int>::iterator it = in->replicas_begin(); 
       it != in->replicas_end(); 
       it++) {
    MLock *m = new MLock(msg, mds->get_nodeid());
    m->set_ino(in->ino(), type);
    m->set_data(data);
    mds->send_message_mds(m, it->first, MDS_PORT_LOCKER);
  }
}

void Locker::send_lock_message(CDentry *dn, int msg)
{
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    MLock *m = new MLock(msg, mds->get_nodeid());
    m->set_dn(dn->dir->ino(), dn->name);
    mds->send_message_mds(m, it->first, MDS_PORT_LOCKER);
  }
}



// file i/o -----------------------------------------

__uint64_t Locker::issue_file_data_version(CInode *in)
{
  dout(7) << "issue_file_data_version on " << *in << endl;
  return in->inode.file_data_version;
}


Capability* Locker::issue_new_caps(CInode *in,
                                    int mode,
                                    MClientRequest *req)
{
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << endl;
  
  // my needs
  int my_client = req->get_client();
  int my_want = 0;
  if (mode & FILE_MODE_R) my_want |= CAP_FILE_RDCACHE  | CAP_FILE_RD;
  if (mode & FILE_MODE_W) my_want |= CAP_FILE_WRBUFFER | CAP_FILE_WR;

  // register a capability
  // checks capabilities for the file indexed by client id
  // returns 0 if there is no cached capability
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
    // new cap
    // makes a cap which only contains the desired mode (my_want)
    Capability c(my_want);
    // caches this capability in the inode
    in->add_client_cap(my_client, c);
    // pull the cap back out, why? you have a pointer to it already
    // ah, because this is the variable used later
    cap = in->get_client_cap(my_client);
    
    // note client addr
    // increase reference count on this clientid
    mds->clientmap.add_open(my_client, req->get_client_inst());
    
  } else {
    // make sure it has sufficient caps
    // if the mode cached is not the mode were looking for
    if (cap->wanted() & ~my_want) {
      // augment wanted caps for this client
      // just changed  the cached capability to the mode we want
      cap->set_wanted( cap->wanted() | my_want );
    }
  }

  // suppress file cap messages for this guy for a few moments (we'll bundle with the open() reply)
  // is this so that issue_caps() does actually send caps to the client?
  cap->set_suppress(true);
  // checks the mode the client asked for previously
  int before = cap->pending();

  // if this MDS is the authority for the inode
  if (in->is_auth()) {
    // [auth] twiddle mode?
    inode_file_eval(in);
  } else {
    // [replica] tell auth about any new caps wanted
    request_inode_file_caps(in);
  }
    
  // issue caps (pot. incl new one)
  issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  // this doesn't actually seem to do anything
  cap->issue(cap->pending());
  
  // ok, stop suppressing.
  cap->set_suppress(false);

  // the mode of the most recently sent, should be the desired mode?
  int now = cap->pending();
  if (before != now &&
      (before & CAP_FILE_WR) == 0 &&
      (now & CAP_FILE_WR)) {
    // FIXME FIXME FIXME
  }
  
  // twiddle file_data_version?
  if ((before & CAP_FILE_WRBUFFER) == 0 &&
      (now & CAP_FILE_WRBUFFER)) {
    in->inode.file_data_version++;
    dout(7) << " incrementing file_data_version, now " << in->inode.file_data_version << " for " << *in << endl;
  }

  return cap;
}

/**********
 * This function returns a new extended capability
 * for the user for file
 * This function does nothing for synchronization
 **********/
ExtCap* Locker::issue_new_extcaps(CInode *in, int mode, MClientRequest *req) {
  dout(3) << "issue_new_extcaps for mode " << mode << " on " << *in << endl;

  if (g_conf.secure_io == 0)
    return 0;

  // get the uid
  uid_t my_user = req->get_caller_uid();
  gid_t my_group = req->get_caller_gid();
  //cout << "User " << my_user << " Group " << my_group << endl;

  // issue most generic cap (RW)
  int my_want = 0;
  my_want |= FILE_MODE_RW;

  utime_t test_time = g_clock.now();

  // check cache
  ExtCap *ext_cap;

  // unix groups
  if (g_conf.mds_group == 1) {
    if (my_user == in->get_uid())
      ext_cap = in->get_unix_user_cap();
    else if(my_group == in->get_gid())
      ext_cap = in->get_unix_group_cap();
    else
      ext_cap = in->get_unix_world_cap();
  }
  else if (g_conf.mds_group == 4) {
    if (mds->predict_cap_cache[in->ino()].count(my_user) == 0)
      ext_cap = 0;
    else
      ext_cap = &(mds->predict_cap_cache[in->ino()][my_user]);
  }
  // no grouping
  else 
    ext_cap = in->get_user_extcap(my_user);

  // make new capability
  if (!ext_cap) {

    // unix grouping
    // are using mds groups?
    if (g_conf.mds_group == 1) {

      // is this a new group? if so, create it
      if (mds->unix_groups_map.count(my_group) == 0) {

	CapGroup group(my_user);

	group.sign_list(mds->getPrvKey());

	// make hash group and unix pointer to it
	mds->unix_groups_byhash[group.get_root_hash()] = group;
	mds->unix_groups_map[my_group] = group.get_root_hash();

      }

      hash_t my_hash = mds->unix_groups_map[my_group];
      // is user in the group? if not, add them
      if (!(mds->unix_groups_byhash[my_hash].contains(my_user))) {

	// make a new group, equal to old group (keep old group around)
	CapGroup group = mds->unix_groups_byhash[my_hash];

	// add the user
	group.add_user(my_user);

	// re-compute the signature
	group.sign_list(mds->getPrvKey());	

	// get the new hash
	hash_t new_hash = group.get_root_hash();
	
	// put it into the list	
	mds->unix_groups_byhash[new_hash] = group;
	mds->unix_groups_map[my_group] = new_hash;

      }

      //get hash for gid
      hash_t gid_hash = mds->unix_groups_map[my_group];
      
      ext_cap  = new ExtCap(my_want, my_user, my_group, gid_hash, in->ino());
      
      ext_cap->set_type(1);
      
    }
    // do prediction
    else if (g_conf.mds_group == 4) {
      // can we make any predictions?
      if (mds->precompute_succ.count(in->ino()) != 0) {
	//cout << "Making a prediction in capability for " << in->ino() << endl;
	// add the hash
	hash_t inode_hash = mds->precompute_succ[in->ino()];
	ext_cap = new ExtCap(FILE_MODE_RW, my_user, my_group, inode_hash);
	ext_cap->set_type(USER_BATCH);
      }
      else {
	//cout << "Can't make predictions for this cap for " << in->ino() << endl;
	ext_cap = new ExtCap(my_want, my_user, in->ino());
	ext_cap->set_type(0);
      }

    }
    // default no grouping
    else {
      ext_cap = new ExtCap(my_want, my_user, in->ino());
      ext_cap->set_type(0);
    }
    
    // set capability id
    ext_cap->set_id(mds->cap_id_count, mds->get_nodeid());
    mds->cap_id_count++;

    dout(3) << "Made new " << my_want << " capability for uid: "
       << ext_cap->get_uid() << " for inode: " << ext_cap->get_ino()<< endl;
    
    ext_cap->sign_extcap(mds->getPrvKey());

    // caches this capability in the inode
    if (g_conf.mds_group == 1) {
      if (my_user == in->get_uid()) {
	in->set_unix_user_cap(ext_cap);
	in->set_unix_group_cap(ext_cap);
      }
      else if(my_group == in->get_gid())
	in->set_unix_group_cap(ext_cap);
      else
	in->set_unix_world_cap(ext_cap);
    }
    else if (g_conf.mds_group == 4) {
      // did we use a hash for the inodes?
      if (ext_cap->get_type() == USER_BATCH) {
	hash_t inode_hash = ext_cap->get_file_hash();
	list<inodeno_t> inode_list = mds->unix_groups_byhash[inode_hash].get_inode_list();
	// cache in every inode included in the cap
	for (list<inodeno_t>::iterator lii = inode_list.begin();
	     lii != inode_list.end();
	     lii++) {
	  mds->predict_cap_cache[*lii][my_user] = (*ext_cap);
	}
      }
      mds->predict_cap_cache[in->ino()][my_user] = (*ext_cap);
    }
    else
      in->add_user_extcap(my_user,ext_cap);

  }
  // we want to index based on mode, so we can cache more caps
  // does the cached cap have the write mode?
  else {
    dout(3) << "Got cached " << my_want << " capability for uid: "
	 << ext_cap->get_uid() << " for inode: " << ext_cap->get_ino() << endl;
    if (ext_cap->mode() < mode) {
      ext_cap->set_mode(mode);
      ext_cap->sign_extcap(mds->getPrvKey());
    }
  }
  // add capability as recently used for renewal
  mds->recent_caps.insert(ext_cap->get_id());

  return ext_cap;
}

bool Locker::issue_caps(CInode *in)
{
  // allowed caps are determined by the lock mode.
  int allowed = in->filelock.caps_allowed(in->is_auth());
  dout(7) << "issue_caps filelock allows=" << cap_string(allowed) 
          << " on " << *in << endl;

  // count conflicts with
  int nissued = 0;        

  // client caps
  for (map<int, Capability>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       it++) {
    if (it->second.issued() != (it->second.wanted() & allowed)) {
      // issue
      nissued++;

      int before = it->second.pending();
      long seq = it->second.issue(it->second.wanted() & allowed);
      int after = it->second.pending();

      // twiddle file_data_version?
      if (!(before & CAP_FILE_WRBUFFER) &&
          (after & CAP_FILE_WRBUFFER)) {
        dout(7) << "   incrementing file_data_version for " << *in << endl;
        in->inode.file_data_version++;
      }

      if (seq > 0 && 
          !it->second.is_suppress()) {
        dout(7) << "   sending MClientFileCaps to client" << it->first << " seq " << it->second.get_last_seq() << " new pending " << cap_string(it->second.pending()) << " was " << cap_string(before) << endl;
        mds->messenger->send_message(new MClientFileCaps(in->inode,
                                                         it->second.get_last_seq(),
                                                         it->second.pending(),
                                                         it->second.wanted()),
                                     mds->clientmap.get_inst(it->first), 
				     0, MDS_PORT_LOCKER);
      }
    }
  }

  return (nissued == 0);  // true if no re-issued, no callbacks
}



void Locker::request_inode_file_caps(CInode *in)
{
  int wanted = in->get_caps_wanted();
  if (wanted != in->replica_caps_wanted) {

    if (wanted == 0) {
      if (in->replica_caps_wanted_keep_until > g_clock.recent_now()) {
        // ok, release them finally!
        in->replica_caps_wanted_keep_until.sec_ref() = 0;
        dout(7) << "request_inode_file_caps " << cap_string(wanted)
                 << " was " << cap_string(in->replica_caps_wanted) 
                 << " no keeping anymore " 
                 << " on " << *in 
                 << endl;
      }
      else if (in->replica_caps_wanted_keep_until.sec() == 0) {
        in->replica_caps_wanted_keep_until = g_clock.recent_now();
        in->replica_caps_wanted_keep_until.sec_ref() += 2;
        
        dout(7) << "request_inode_file_caps " << cap_string(wanted)
                 << " was " << cap_string(in->replica_caps_wanted) 
                 << " keeping until " << in->replica_caps_wanted_keep_until
                 << " on " << *in 
                 << endl;
        return;
      } else {
        // wait longer
        return;
      }
    } else {
      in->replica_caps_wanted_keep_until.sec_ref() = 0;
    }
    assert(!in->is_auth());

    int auth = in->authority();
    dout(7) << "request_inode_file_caps " << cap_string(wanted)
            << " was " << cap_string(in->replica_caps_wanted) 
            << " on " << *in << " to mds" << auth << endl;
    assert(!in->is_auth());

    in->replica_caps_wanted = wanted;
    mds->send_message_mds(new MInodeFileCaps(in->ino(), mds->get_nodeid(),
					     in->replica_caps_wanted),
			  auth, MDS_PORT_LOCKER);
  } else {
    in->replica_caps_wanted_keep_until.sec_ref() = 0;
  }
}

void Locker::handle_inode_file_caps(MInodeFileCaps *m)
{
  CInode *in = mdcache->get_inode(m->get_ino());
  assert(in);
  assert(in->is_auth() || in->is_proxy());
  
  dout(7) << "handle_inode_file_caps replica mds" << m->get_from() << " wants caps " << cap_string(m->get_caps()) << " on " << *in << endl;

  if (in->is_proxy()) {
    dout(7) << "proxy, fw" << endl;
    mds->send_message_mds(m, in->authority(), MDS_PORT_LOCKER);
    return;
  }

  if (m->get_caps())
    in->mds_caps_wanted[m->get_from()] = m->get_caps();
  else
    in->mds_caps_wanted.erase(m->get_from());

  inode_file_eval(in);
  delete m;
}


/*
 * note: we only get these from the client if
 * - we are calling back previously issued caps (fewer than the client previously had)
 * - or if the client releases (any of) its caps on its own
 */
void Locker::handle_client_file_caps(MClientFileCaps *m)
{
  int client = m->get_source().num();
  CInode *in = mdcache->get_inode(m->get_ino());
  Capability *cap = 0;
  if (in) 
    cap = in->get_client_cap(client);

  if (!in || !cap) {
    if (!in) {
      dout(7) << "handle_client_file_caps on unknown ino " << m->get_ino() << ", dropping" << endl;
    } else {
      dout(7) << "handle_client_file_caps no cap for client" << client << " on " << *in << endl;
    }
    delete m;
    return;
  } 
  
  assert(cap);

  // filter wanted based on what we could ever give out (given auth/replica status)
  int wanted = m->get_wanted() & in->filelock.caps_allowed_ever(in->is_auth());
  
  dout(7) << "handle_client_file_caps seq " << m->get_seq() 
          << " confirms caps " << cap_string(m->get_caps()) 
          << " wants " << cap_string(wanted)
          << " from client" << client
          << " on " << *in 
          << endl;  
  
  // update wanted
  if (cap->wanted() != wanted)
    cap->set_wanted(wanted);

  // confirm caps
  int had = cap->confirm_receipt(m->get_seq(), m->get_caps());
  int has = cap->confirmed();
  if (cap->is_null()) {
    dout(7) << " cap for client" << client << " is now null, removing from " << *in << endl;
    in->remove_client_cap(client);
    if (!in->is_auth())
      request_inode_file_caps(in);

    // dec client addr counter
    mds->clientmap.dec_open(client);

    // tell client.
    MClientFileCaps *r = new MClientFileCaps(in->inode, 
                                             0, 0, 0,
                                             MClientFileCaps::FILECAP_RELEASE);
    mds->messenger->send_message(r, m->get_source_inst(), 0, MDS_PORT_LOCKER);
  }

  // merge in atime?
  if (m->get_inode().atime > in->inode.atime) {
      dout(7) << "  taking atime " << m->get_inode().atime << " > " 
              << in->inode.atime << " for " << *in << endl;
    in->inode.atime = m->get_inode().atime;
  }
  
  if ((has|had) & CAP_FILE_WR) {
    bool dirty = false;

    // mtime
    if (m->get_inode().mtime > in->inode.mtime) {
      dout(7) << "  taking mtime " << m->get_inode().mtime << " > " 
              << in->inode.mtime << " for " << *in << endl;
      in->inode.mtime = m->get_inode().mtime;
      dirty = true;
    }
    // size
    if (m->get_inode().size > in->inode.size) {
      dout(7) << "  taking size " << m->get_inode().size << " > " 
              << in->inode.size << " for " << *in << endl;
      in->inode.size = m->get_inode().size;
      dirty = true;
    }

    if (dirty) 
      mds->mdlog->submit_entry(new EString("cap inode update dirty fixme"));
  }  

  // reevaluate, waiters
  inode_file_eval(in);
  in->finish_waiting(CINODE_WAIT_CAPS, 0);

  delete m;
}










// locks ----------------------------------------------------------------

/*


INODES:

= two types of inode metadata:
   hard  - uid/gid, mode
   file  - mtime, size
 ? atime - atime  (*)       <-- we want a lazy update strategy?

= correspondingly, two types of inode locks:
   hardlock - hard metadata
   filelock - file metadata

   -> These locks are completely orthogonal! 

= metadata ops and how they affect inode metadata:
        sma=size mtime atime
   HARD FILE OP
  files:
    R   RRR stat
    RW      chmod/chown
    R    W  touch   ?ctime
    R       openr
          W read    atime
    R       openw
    Wc      openwc  ?ctime
        WW  write   size mtime
            close 

  dirs:
    R     W readdir atime 
        RRR  ( + implied stats on files)
    Rc  WW  mkdir         (ctime on new dir, size+mtime on parent dir)
    R   WW  link/unlink/rename/rmdir  (size+mtime on dir)

  

= relationship to client (writers):

  - ops in question are
    - stat ... need reasonable value for mtime (+ atime?)
      - maybe we want a "quicksync" type operation instead of full lock
    - truncate ... need to stop writers for the atomic truncate operation
      - need a full lock




= modes
  - SYNC
              Rauth  Rreplica  Wauth  Wreplica
        sync
        




ALSO:

  dirlock  - no dir changes (prior to unhashing)
  denlock  - dentry lock    (prior to unlink, rename)

     
*/


void Locker::handle_lock(MLock *m)
{
  switch (m->get_otype()) {
  case LOCK_OTYPE_IHARD:
    handle_lock_inode_hard(m);
    break;
    
  case LOCK_OTYPE_IFILE:
    handle_lock_inode_file(m);
    break;
    
  case LOCK_OTYPE_DIR:
    handle_lock_dir(m);
    break;
    
  case LOCK_OTYPE_DN:
    handle_lock_dn(m);
    break;

  default:
    dout(7) << "handle_lock got otype " << m->get_otype() << endl;
    assert(0);
    break;
  }
}
 


// ===============================
// hard inode metadata

bool Locker::inode_hard_read_try(CInode *in, Context *con)
{
  dout(7) << "inode_hard_read_try on " << *in << endl;  

  // can read?  grab ref.
  if (in->hardlock.can_read(in->is_auth())) 
    return true;
  
  assert(!in->is_auth());

  // wait!
  dout(7) << "inode_hard_read_try waiting on " << *in << endl;
  in->add_waiter(CINODE_WAIT_HARDR, con);
  return false;
}

bool Locker::inode_hard_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_hard_read_start  on " << *in << endl;  

  // can read?  grab ref.
  if (in->hardlock.can_read(in->is_auth())) {
    in->hardlock.get_read();
    return true;
  }
  
  // can't read, and replicated.
  assert(!in->is_auth());

  // wait!
  dout(7) << "inode_hard_read_start waiting on " << *in << endl;
  in->add_waiter(CINODE_WAIT_HARDR, new C_MDS_RetryRequest(mds, m, in));
  return false;
}


void Locker::inode_hard_read_finish(CInode *in)
{
  // drop ref
  assert(in->hardlock.can_read(in->is_auth()));
  in->hardlock.put_read();

  dout(7) << "inode_hard_read_finish on " << *in << endl;
  
  //if (in->hardlock.get_nread() == 0) in->finish_waiting(CINODE_WAIT_HARDNORD);
}


bool Locker::inode_hard_write_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_hard_write_start  on " << *in << endl;

  // if not replicated, i can twiddle lock at will
  if (in->is_auth() &&
      !in->is_replicated() &&
      in->hardlock.get_state() != LOCK_LOCK) 
    in->hardlock.set_state(LOCK_LOCK);
  
  // can write?  grab ref.
  if (in->hardlock.can_write(in->is_auth())) {
    assert(in->is_auth());
    if (!in->can_auth_pin()) {
      dout(7) << "inode_hard_write_start waiting for authpinnable on " << *in << endl;
      in->add_waiter(CINODE_WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mds, m, in));
      return false;
    }

    in->auth_pin();  // ugh, can't condition this on nwrite==0 bc we twiddle that in handle_lock_*
    in->hardlock.get_write(m);
    return true;
  }
  
  // can't write, replicated.
  if (in->is_auth()) {
    // auth
    if (in->hardlock.can_write_soon(in->is_auth())) {
      // just wait
    } else {
      // initiate lock
      inode_hard_lock(in);
    }
    
    dout(7) << "inode_hard_write_start waiting on " << *in << endl;
    in->add_waiter(CINODE_WAIT_HARDW, new C_MDS_RetryRequest(mds, m, in));

    return false;
  } else {
    // replica
    // fw to auth
    int auth = in->authority();
    dout(7) << "inode_hard_write_start " << *in << " on replica, fw to auth " << auth << endl;
    assert(auth != mds->get_nodeid());
    mdcache->request_forward(m, auth);
    return false;
  }
}


void Locker::inode_hard_write_finish(CInode *in)
{
  // drop ref
  //assert(in->hardlock.can_write(in->is_auth()));
  in->hardlock.put_write();
  in->auth_unpin();
  dout(7) << "inode_hard_write_finish on " << *in << endl;

  // others waiting?
  if (in->is_hardlock_write_wanted()) {
    // wake 'em up
    in->take_waiting(CINODE_WAIT_HARDW, mds->finished_queue);
  } else {
    // auto-sync if alone.
    if (in->is_auth() &&
        !in->is_replicated() &&
        in->hardlock.get_state() != LOCK_SYNC) 
      in->hardlock.set_state(LOCK_SYNC);
    
    inode_hard_eval(in);
  }
}


void Locker::inode_hard_eval(CInode *in)
{
  // finished gather?
  if (in->is_auth() &&
      !in->hardlock.is_stable() &&
      in->hardlock.gather_set.empty()) {
    dout(7) << "inode_hard_eval finished gather on " << *in << endl;
    switch (in->hardlock.get_state()) {
    case LOCK_GLOCKR:
      in->hardlock.set_state(LOCK_LOCK);
      
      // waiters
      //in->hardlock.get_write();
      in->finish_waiting(CINODE_WAIT_HARDRWB|CINODE_WAIT_HARDSTABLE);
      //in->hardlock.put_write();
      break;
      
    default:
      assert(0);
    }
  }
  if (!in->hardlock.is_stable()) return;
  
  if (in->is_auth()) {

    // sync?
    if (in->is_replicated() &&
        in->is_hardlock_write_wanted() &&
        in->hardlock.get_state() != LOCK_SYNC) {
      dout(7) << "inode_hard_eval stable, syncing " << *in << endl;
      inode_hard_sync(in);
    }

  } else {
    // replica
  }
}


// mid

void Locker::inode_hard_sync(CInode *in)
{
  dout(7) << "inode_hard_sync on " << *in << endl;
  assert(in->is_auth());
  
  // check state
  if (in->hardlock.get_state() == LOCK_SYNC)
    return; // already sync
  if (in->hardlock.get_state() == LOCK_GLOCKR) 
    assert(0); // um... hmm!
  assert(in->hardlock.get_state() == LOCK_LOCK);
  
  // hard data
  bufferlist harddata;
  in->encode_hard_state(harddata);
  
  // bcast to replicas
  send_lock_message(in, LOCK_AC_SYNC, LOCK_OTYPE_IHARD, harddata);
  
  // change lock
  in->hardlock.set_state(LOCK_SYNC);
  
  // waiters?
  in->finish_waiting(CINODE_WAIT_HARDSTABLE);
}

void Locker::inode_hard_lock(CInode *in)
{
  dout(7) << "inode_hard_lock on " << *in << " hardlock=" << in->hardlock << endl;  
  assert(in->is_auth());
  
  // check state
  if (in->hardlock.get_state() == LOCK_LOCK ||
      in->hardlock.get_state() == LOCK_GLOCKR) 
    return;  // already lock or locking
  assert(in->hardlock.get_state() == LOCK_SYNC);
  
  // bcast to replicas
  send_lock_message(in, LOCK_AC_LOCK, LOCK_OTYPE_IHARD);
  
  // change lock
  in->hardlock.set_state(LOCK_GLOCKR);
  in->hardlock.init_gather(in->get_replicas());
}





// messenger

void Locker::handle_lock_inode_hard(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IHARD);
  
  if (mds->logger) mds->logger->inc("lih");

  int from = m->get_asker();
  CInode *in = mdcache->get_inode(m->get_ino());
  
  if (LOCK_AC_FOR_AUTH(m->get_action())) {
    // auth
    assert(in);
    assert(in->is_auth() || in->is_proxy());
    dout(7) << "handle_lock_inode_hard " << *in << " hardlock=" << in->hardlock << endl;  

    if (in->is_proxy()) {
      // fw
      int newauth = in->authority();
      assert(newauth >= 0);
      if (from == newauth) {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, but from new auth, dropping" << endl;
        delete m;
      } else {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
        mds->send_message_mds(m, newauth, MDS_PORT_LOCKER);
      }
      return;
    }
  } else {
    // replica
    if (!in) {
      dout(7) << "handle_lock_inode_hard " << m->get_ino() << ": don't have it anymore" << endl;
      /* do NOT nak.. if we go that route we need to duplicate all the nonce funkiness
         to keep gather_set a proper/correct subset of cached_by.  better to use the existing
         cacheexpire mechanism instead!
      */
      delete m;
      return;
    }
    
    assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_hard a=" << m->get_action() << " from " << from << " " << *in << " hardlock=" << in->hardlock << endl;  
 
  CLock *lock = &in->hardlock;
  
  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK);
    
    { // assim data
      int off = 0;
      in->decode_hard_state(m->get_data(), off);
    }
    
    // update lock
    lock->set_state(LOCK_SYNC);
    
    // no need to reply
    
    // waiters
    in->finish_waiting(CINODE_WAIT_HARDR|CINODE_WAIT_HARDSTABLE);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC);
    //||           lock->get_state() == LOCK_GLOCKR);
    
    // wait for readers to finish?
    if (lock->get_nread() > 0) {
      dout(7) << "handle_lock_inode_hard readers, waiting before ack on " << *in << endl;
      lock->set_state(LOCK_GLOCKR);
      in->add_waiter(CINODE_WAIT_HARDNORD,
                     new C_MDS_RetryMessage(mds,m));
      assert(0);  // does this ever happen?  (if so, fix hard_read_finish, and CInodeExport.update_inode!)
      return;
     } else {

      // update lock and reply
      lock->set_state(LOCK_LOCK);
      
      {
        MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IHARD);
        mds->send_message_mds(reply, from, MDS_PORT_LOCKER);
      }
    }
    break;
    
    
    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->state == LOCK_GLOCKR);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);

    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_hard " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_hard " << *in << " from " << from << ", last one" << endl;
      inode_hard_eval(in);
    }
  }  
  delete m;
}




// =====================
// soft inode metadata


bool Locker::inode_file_read_start(CInode *in, MClientRequest *m)
{
  dout(7) << "inode_file_read_start " << *in << " filelock=" << in->filelock << endl;  

  // can read?  grab ref.
  if (in->filelock.can_read(in->is_auth())) {
    in->filelock.get_read();
    return true;
  }
  
  // can't read, and replicated.
  if (in->filelock.can_read_soon(in->is_auth())) {
    // wait
    dout(7) << "inode_file_read_start can_read_soon " << *in << endl;
  } else {    
    if (in->is_auth()) {
      // auth

      // FIXME or qsync?

      if (in->filelock.is_stable()) {
        inode_file_lock(in);     // lock, bc easiest to back off

        if (in->filelock.can_read(in->is_auth())) {
          in->filelock.get_read();
          
          //in->filelock.get_write();
          in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
          //in->filelock.put_write();
          return true;
        }
      } else {
        dout(7) << "inode_file_read_start waiting until stable on " << *in << ", filelock=" << in->filelock << endl;
        in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
        return false;
      }
    } else {
      // replica
      if (in->filelock.is_stable()) {

        // fw to auth
        int auth = in->authority();
        dout(7) << "inode_file_read_start " << *in << " on replica and async, fw to auth " << auth << endl;
        assert(auth != mds->get_nodeid());
        mdcache->request_forward(m, auth);
        return false;
        
      } else {
        // wait until stable
        dout(7) << "inode_file_read_start waiting until stable on " << *in << ", filelock=" << in->filelock << endl;
        in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
        return false;
      }
    }
  }

  // wait
  dout(7) << "inode_file_read_start waiting on " << *in << ", filelock=" << in->filelock << endl;
  in->add_waiter(CINODE_WAIT_FILER, new C_MDS_RetryRequest(mds, m, in));
        
  return false;
}


void Locker::inode_file_read_finish(CInode *in)
{
  // drop ref
  assert(in->filelock.can_read(in->is_auth()));
  in->filelock.put_read();

  dout(7) << "inode_file_read_finish on " << *in << ", filelock=" << in->filelock << endl;

  if (in->filelock.get_nread() == 0) {
    in->finish_waiting(CINODE_WAIT_FILENORD);
    inode_file_eval(in);
  }
}


bool Locker::inode_file_write_start(CInode *in, MClientRequest *m)
{
  // can't write?
  if (!in->filelock.can_write(in->is_auth())) {
  
    // can't write.
    if (in->is_auth()) {
      // auth
      if (!in->filelock.can_write_soon(in->is_auth())) {
	if (!in->filelock.is_stable()) {
	  dout(7) << "inode_file_write_start on auth, waiting for stable on " << *in << endl;
	  in->add_waiter(CINODE_WAIT_FILESTABLE, new C_MDS_RetryRequest(mds, m, in));
	  return false;
	}
	
	// initiate lock 
	inode_file_lock(in);

	// fall-thru to below.
      }
    } else {
      // replica
      // fw to auth
      int auth = in->authority();
      dout(7) << "inode_file_write_start " << *in << " on replica, fw to auth " << auth << endl;
      assert(auth != mds->get_nodeid());
      mdcache->request_forward(m, auth);
      return false;
    }
  } 
  
  // check again
  if (in->filelock.can_write(in->is_auth())) {
    // can i auth pin?
    assert(in->is_auth());
    if (!in->can_auth_pin()) {
      dout(7) << "inode_file_write_start waiting for authpinnable on " << *in << endl;
      in->add_waiter(CINODE_WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mds, m, in));
      return false;
    }
    
    in->auth_pin();
    in->filelock.get_write(m);
    return true;
  } else {
    dout(7) << "inode_file_write_start on auth, waiting for write on " << *in << endl;
    in->add_waiter(CINODE_WAIT_FILEW, new C_MDS_RetryRequest(mds, m, in));
    return false;
  }
}


void Locker::inode_file_write_finish(CInode *in)
{
  // drop ref
  assert(in->filelock.can_write(in->is_auth()));
  in->filelock.put_write();
  dout(7) << "inode_file_write_finish on " << *in << ", filelock=" << in->filelock << endl;
  
  // drop lock?
  if (!in->is_filelock_write_wanted()) {
    in->finish_waiting(CINODE_WAIT_FILENOWR);
    inode_file_eval(in);
  }
}


/*
 * ...
 *
 * also called after client caps are acked to us
 * - checks if we're in unstable sfot state and can now move on to next state
 * - checks if soft state should change (eg bc last writer closed)
 */

void Locker::inode_file_eval(CInode *in)
{
  int issued = in->get_caps_issued();

  // [auth] finished gather?
  if (in->is_auth() &&
      !in->filelock.is_stable() &&
      in->filelock.gather_set.size() == 0) {
    dout(7) << "inode_file_eval finished mds gather on " << *in << endl;

    switch (in->filelock.get_state()) {
      // to lock
    case LOCK_GLOCKR:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
      if (issued == 0) {
        in->filelock.set_state(LOCK_LOCK);
        
        // waiters
        in->filelock.get_read();
        //in->filelock.get_write();
        in->finish_waiting(CINODE_WAIT_FILERWB|CINODE_WAIT_FILESTABLE);
        in->filelock.put_read();
        //in->filelock.put_write();
      }
      break;
      
      // to mixed
    case LOCK_GMIXEDR:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        in->filelock.set_state(LOCK_MIXED);
        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;

    case LOCK_GMIXEDL:
      if ((issued & ~(CAP_FILE_WR)) == 0) {
        in->filelock.set_state(LOCK_MIXED);

        if (in->is_replicated()) {
          // data
          bufferlist softdata;
          in->encode_file_state(softdata);
          
          // bcast to replicas
	  send_lock_message(in, LOCK_AC_MIXED, LOCK_OTYPE_IFILE, softdata);
        }

        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;

      // to loner
    case LOCK_GLONERR:
      if (issued == 0) {
        in->filelock.set_state(LOCK_LONER);
        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;

    case LOCK_GLONERM:
      if ((issued & ~CAP_FILE_WR) == 0) {
        in->filelock.set_state(LOCK_LONER);
        in->finish_waiting(CINODE_WAIT_FILESTABLE);
      }
      break;
      
      // to sync
    case LOCK_GSYNCL:
    case LOCK_GSYNCM:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        in->filelock.set_state(LOCK_SYNC);
        
        { // bcast data to replicas
          bufferlist softdata;
          in->encode_file_state(softdata);
          
	  send_lock_message(in, LOCK_AC_SYNC, LOCK_OTYPE_IFILE, softdata);
        }
        
        // waiters
        in->filelock.get_read();
        in->finish_waiting(CINODE_WAIT_FILER|CINODE_WAIT_FILESTABLE);
        in->filelock.put_read();
      }
      break;
      
    default: 
      assert(0);
    }

    issue_caps(in);
  }
  
  // [replica] finished caps gather?
  if (!in->is_auth() &&
      !in->filelock.is_stable()) {
    switch (in->filelock.get_state()) {
    case LOCK_GMIXEDR:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        in->filelock.set_state(LOCK_MIXED);
        
        // ack
        MLock *reply = new MLock(LOCK_AC_MIXEDACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(reply, in->authority(), MDS_PORT_LOCKER);
      }
      break;

    case LOCK_GLOCKR:
      if (issued == 0) {
        in->filelock.set_state(LOCK_LOCK);
        
        // ack
        MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(reply, in->authority(), MDS_PORT_LOCKER);
      }
      break;

    default:
      assert(0);
    }
  }

  // !stable -> do nothing.
  if (!in->filelock.is_stable()) return; 


  // stable.
  assert(in->filelock.is_stable());

  if (in->is_auth()) {
    // [auth]
    int wanted = in->get_caps_wanted();
    bool loner = (in->client_caps.size() == 1) && in->mds_caps_wanted.empty();
    dout(7) << "inode_file_eval wanted=" << cap_string(wanted)
            << "  filelock=" << in->filelock 
            << "  loner=" << loner
            << endl;

    // * -> loner?
    if (in->filelock.get_nread() == 0 &&
        !in->is_filelock_write_wanted() &&
        (wanted & CAP_FILE_WR) &&
        loner &&
        in->filelock.get_state() != LOCK_LONER) {
      dout(7) << "inode_file_eval stable, bump to loner " << *in << ", filelock=" << in->filelock << endl;
      inode_file_loner(in);
    }

    // * -> mixed?
    else if (in->filelock.get_nread() == 0 &&
             !in->is_filelock_write_wanted() &&
             (wanted & CAP_FILE_RD) &&
             (wanted & CAP_FILE_WR) &&
             !(loner && in->filelock.get_state() == LOCK_LONER) &&
             in->filelock.get_state() != LOCK_MIXED) {
      dout(7) << "inode_file_eval stable, bump to mixed " << *in << ", filelock=" << in->filelock << endl;
      inode_file_mixed(in);
    }

    // * -> sync?
    else if (!in->is_filelock_write_wanted() &&
             !(wanted & CAP_FILE_WR) &&
             ((wanted & CAP_FILE_RD) || 
              in->is_replicated() || 
              (!loner && in->filelock.get_state() == LOCK_LONER)) &&
             in->filelock.get_state() != LOCK_SYNC) {
      dout(7) << "inode_file_eval stable, bump to sync " << *in << ", filelock=" << in->filelock << endl;
      inode_file_sync(in);
    }

    // * -> lock?  (if not replicated or open)
    else if (!in->is_replicated() &&
             wanted == 0 &&
             in->filelock.get_state() != LOCK_LOCK) {
      inode_file_lock(in);
    }
    
  } else {
    // replica
    // recall? check wiaters?  XXX
  }
}


// mid

bool Locker::inode_file_sync(CInode *in)
{
  dout(7) << "inode_file_sync " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());

  // check state
  if (in->filelock.get_state() == LOCK_SYNC ||
      in->filelock.get_state() == LOCK_GSYNCL ||
      in->filelock.get_state() == LOCK_GSYNCM)
    return true;

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  assert((in->get_caps_wanted() & CAP_FILE_WR) == 0);

  if (in->filelock.get_state() == LOCK_LOCK) {
    if (in->is_replicated()) {
      // soft data
      bufferlist softdata;
      in->encode_file_state(softdata);
      
      // bcast to replicas
      send_lock_message(in, LOCK_AC_SYNC, LOCK_OTYPE_IFILE, softdata);
    }

    // change lock
    in->filelock.set_state(LOCK_SYNC);

    // reissue caps
    issue_caps(in);
    return true;
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
    // writers?
    if (issued & CAP_FILE_WR) {
      // gather client write caps
      in->filelock.set_state(LOCK_GSYNCM);
      issue_caps(in);
    } else {
      // no writers, go straight to sync

      if (in->is_replicated()) {
        // bcast to replicas
	send_lock_message(in, LOCK_AC_SYNC, LOCK_OTYPE_IFILE);
      }
    
      // change lock
      in->filelock.set_state(LOCK_SYNC);
    }
    return false;
  }

  else if (in->filelock.get_state() == LOCK_LONER) {
    // writers?
    if (issued & CAP_FILE_WR) {
      // gather client write caps
      in->filelock.set_state(LOCK_GSYNCL);
      issue_caps(in);
    } else {
      // no writers, go straight to sync
      if (in->is_replicated()) {
        // bcast to replicas
	send_lock_message(in, LOCK_AC_SYNC, LOCK_OTYPE_IFILE);
      }

      // change lock
      in->filelock.set_state(LOCK_SYNC);
    }
    return false;
  }
  else 
    assert(0); // wtf.

  return false;
}



void Locker::inode_file_lock(CInode *in)
{
  dout(7) << "inode_file_lock " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->filelock.get_state() == LOCK_LOCK ||
      in->filelock.get_state() == LOCK_GLOCKR ||
      in->filelock.get_state() == LOCK_GLOCKM ||
      in->filelock.get_state() == LOCK_GLOCKL) 
    return;  // lock or locking

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  if (in->filelock.get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(in, LOCK_AC_LOCK, LOCK_OTYPE_IFILE);
      in->filelock.init_gather(in->get_replicas());
      
      // change lock
      in->filelock.set_state(LOCK_GLOCKR);

      // call back caps
      if (issued) 
        issue_caps(in);
    } else {
      if (issued) {
        // call back caps
        in->filelock.set_state(LOCK_GLOCKR);
        issue_caps(in);
      } else {
        in->filelock.set_state(LOCK_LOCK);
      }
    }
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(in, LOCK_AC_LOCK, LOCK_OTYPE_IFILE);
      in->filelock.init_gather(in->get_replicas());

      // change lock
      in->filelock.set_state(LOCK_GLOCKM);
      
      // call back caps
      issue_caps(in);
    } else {
      //assert(issued);  // ??? -sage 2/19/06
      if (issued) {
        // change lock
        in->filelock.set_state(LOCK_GLOCKM);
        
        // call back caps
        issue_caps(in);
      } else {
        in->filelock.set_state(LOCK_LOCK);
      }
    }
      
  }
  else if (in->filelock.get_state() == LOCK_LONER) {
    if (issued & CAP_FILE_WR) {
      // change lock
      in->filelock.set_state(LOCK_GLOCKL);
  
      // call back caps
      issue_caps(in);
    } else {
      in->filelock.set_state(LOCK_LOCK);
    }
  }
  else 
    assert(0); // wtf.
}


void Locker::inode_file_mixed(CInode *in)
{
  dout(7) << "inode_file_mixed " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());
  
  // check state
  if (in->filelock.get_state() == LOCK_GMIXEDR ||
      in->filelock.get_state() == LOCK_GMIXEDL)
    return;     // mixed or mixing

  assert(in->filelock.is_stable());

  int issued = in->get_caps_issued();

  if (in->filelock.get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(in, LOCK_AC_MIXED, LOCK_OTYPE_IFILE);
      in->filelock.init_gather(in->get_replicas());
    
      in->filelock.set_state(LOCK_GMIXEDR);
      issue_caps(in);
    } else {
      if (issued) {
        in->filelock.set_state(LOCK_GMIXEDR);
        issue_caps(in);
      } else {
        in->filelock.set_state(LOCK_MIXED);
      }
    }
  }

  else if (in->filelock.get_state() == LOCK_LOCK) {
    if (in->is_replicated()) {
      // data
      bufferlist softdata;
      in->encode_file_state(softdata);
      
      // bcast to replicas
      send_lock_message(in, LOCK_AC_MIXED, LOCK_OTYPE_IFILE, softdata);
    }

    // change lock
    in->filelock.set_state(LOCK_MIXED);
    issue_caps(in);
  }

  else if (in->filelock.get_state() == LOCK_LONER) {
    if (issued & CAP_FILE_WRBUFFER) {
      // gather up WRBUFFER caps
      in->filelock.set_state(LOCK_GMIXEDL);
      issue_caps(in);
    }
    else if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(in, LOCK_AC_MIXED, LOCK_OTYPE_IFILE);
      in->filelock.set_state(LOCK_MIXED);
      issue_caps(in);
    } else {
      in->filelock.set_state(LOCK_MIXED);
      issue_caps(in);
    }
  }

  else 
    assert(0); // wtf.
}


void Locker::inode_file_loner(CInode *in)
{
  dout(7) << "inode_file_loner " << *in << " filelock=" << in->filelock << endl;  

  assert(in->is_auth());

  // check state
  if (in->filelock.get_state() == LOCK_LONER ||
      in->filelock.get_state() == LOCK_GLONERR ||
      in->filelock.get_state() == LOCK_GLONERM)
    return; 

  assert(in->filelock.is_stable());
  assert((in->client_caps.size() == 1) && in->mds_caps_wanted.empty());
  
  if (in->filelock.get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(in, LOCK_AC_LOCK, LOCK_OTYPE_IFILE);
      in->filelock.init_gather(in->get_replicas());
      
      // change lock
      in->filelock.set_state(LOCK_GLONERR);
    } else {
      // only one guy with file open, who gets it all, so
      in->filelock.set_state(LOCK_LONER);
      issue_caps(in);
    }
  }

  else if (in->filelock.get_state() == LOCK_LOCK) {
    // change lock.  ignore replicas; they don't know about LONER.
    in->filelock.set_state(LOCK_LONER);
    issue_caps(in);
  }

  else if (in->filelock.get_state() == LOCK_MIXED) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(in, LOCK_AC_LOCK, LOCK_OTYPE_IFILE);
      in->filelock.init_gather(in->get_replicas());
      
      // change lock
      in->filelock.set_state(LOCK_GLONERM);
    } else {
      in->filelock.set_state(LOCK_LONER);
      issue_caps(in);
    }
  }

  else 
    assert(0);
}

// messenger

void Locker::handle_lock_inode_file(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_IFILE);
  
  if (mds->logger) mds->logger->inc("lif");

  CInode *in = mdcache->get_inode(m->get_ino());
  int from = m->get_asker();

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
    // auth
    assert(in);
    assert(in->is_auth() || in->is_proxy());
    dout(7) << "handle_lock_inode_file " << *in << " hardlock=" << in->hardlock << endl;  
        
    if (in->is_proxy()) {
      // fw
      int newauth = in->authority();
      assert(newauth >= 0);
      if (from == newauth) {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, but from new auth, dropping" << endl;
        delete m;
      } else {
        dout(7) << "handle_lock " << m->get_ino() << " from " << from << ": proxy, fw to " << newauth << endl;
        mds->send_message_mds(m, newauth, MDS_PORT_LOCKER);
      }
      return;
    }
  } else {
    // replica
    if (!in) {
      // drop it.  don't nak.
      dout(7) << "handle_lock " << m->get_ino() << ": don't have it anymore" << endl;
      delete m;
      return;
    }
    
    assert(!in->is_auth());
  }

  dout(7) << "handle_lock_inode_file a=" << m->get_action() << " from " << from << " " << *in << " filelock=" << in->filelock << endl;  
  
  CLock *lock = &in->filelock;
  int issued = in->get_caps_issued();

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK ||
           lock->get_state() == LOCK_MIXED);
    
    { // assim data
      int off = 0;
      in->decode_file_state(m->get_data(), off);
    }
    
    // update lock
    lock->set_state(LOCK_SYNC);
    
    // no need to reply.
    
    // waiters
    in->filelock.get_read();
    in->finish_waiting(CINODE_WAIT_FILER|CINODE_WAIT_FILESTABLE);
    in->filelock.put_read();
    inode_file_eval(in);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_MIXED);
    
    // call back caps?
    if (issued & CAP_FILE_RD) {
      dout(7) << "handle_lock_inode_file client readers, gathering caps on " << *in << endl;
      issue_caps(in);
    }
    if (lock->get_nread() > 0) {
      dout(7) << "handle_lock_inode_file readers, waiting before ack on " << *in << endl;
      in->add_waiter(CINODE_WAIT_FILENORD,
                     new C_MDS_RetryMessage(mds,m));
      lock->set_state(LOCK_GLOCKR);
      assert(0);// i am broken.. why retry message when state captures all the info i need?
      return;
    } 
    if (issued & CAP_FILE_RD) {
      lock->set_state(LOCK_GLOCKR);
      break;
    }

    // nothing to wait for, lock and ack.
    {
      lock->set_state(LOCK_LOCK);

      MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
      reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
      mds->send_message_mds(reply, from, MDS_PORT_LOCKER);
    }
    break;
    
  case LOCK_AC_MIXED:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_LOCK);
    
    if (lock->get_state() == LOCK_SYNC) {
      // MIXED
      if (issued & CAP_FILE_RD) {
        // call back client caps
        lock->set_state(LOCK_GMIXEDR);
        issue_caps(in);
        break;
      } else {
        // no clients, go straight to mixed
        lock->set_state(LOCK_MIXED);

        // ack
        MLock *reply = new MLock(LOCK_AC_MIXEDACK, mds->get_nodeid());
        reply->set_ino(in->ino(), LOCK_OTYPE_IFILE);
        mds->send_message_mds(reply, from, MDS_PORT_LOCKER);
      }
    } else {
      // LOCK
      lock->set_state(LOCK_MIXED);
      
      // no ack needed.
    }

    issue_caps(in);
    
    // waiters
    //in->filelock.get_write();
    in->finish_waiting(CINODE_WAIT_FILEW|CINODE_WAIT_FILESTABLE);
    //in->filelock.put_write();
    inode_file_eval(in);
    break;

 
    

    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->state == LOCK_GLOCKR ||
           lock->state == LOCK_GLOCKM ||
           lock->state == LOCK_GLONERM ||
           lock->state == LOCK_GLONERR);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);

    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
      inode_file_eval(in);
    }
    break;
    
  case LOCK_AC_SYNCACK:
    assert(lock->state == LOCK_GSYNCM);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);
    
    /* not used currently
    {
      // merge data  (keep largest size, mtime, etc.)
      int off = 0;
      in->decode_merge_file_state(m->get_data(), off);
    }
    */

    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
      inode_file_eval(in);
    }
    break;

  case LOCK_AC_MIXEDACK:
    assert(lock->state == LOCK_GMIXEDR);
    assert(lock->gather_set.count(from));
    lock->gather_set.erase(from);
    
    if (lock->gather_set.size()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", still gathering " << lock->gather_set << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from << ", last one" << endl;
      inode_file_eval(in);
    }
    break;


  default:
    assert(0);
  }  
  
  delete m;
}














void Locker::handle_lock_dir(MLock *m) 
{

}



// DENTRY

bool Locker::dentry_xlock_start(CDentry *dn, Message *m, CInode *ref)
{
  dout(7) << "dentry_xlock_start on " << *dn << endl;

  // locked?
  if (dn->lockstate == DN_LOCK_XLOCK) {
    if (dn->xlockedby == m) return true;  // locked by me!

    // not by me, wait
    dout(7) << "dentry " << *dn << " xlock by someone else" << endl;
    dn->dir->add_waiter(CDIR_WAIT_DNREAD, dn->name,
                        new C_MDS_RetryRequest(mds,m,ref));
    return false;
  }

  // prelock?
  if (dn->lockstate == DN_LOCK_PREXLOCK) {
    if (dn->xlockedby == m) {
      dout(7) << "dentry " << *dn << " prexlock by me" << endl;
      dn->dir->add_waiter(CDIR_WAIT_DNLOCK, dn->name,
                          new C_MDS_RetryRequest(mds,m,ref));
    } else {
      dout(7) << "dentry " << *dn << " prexlock by someone else" << endl;
      dn->dir->add_waiter(CDIR_WAIT_DNREAD, dn->name,
                          new C_MDS_RetryRequest(mds,m,ref));
    }
    return false;
  }


  // lockable!
  assert(dn->lockstate == DN_LOCK_SYNC ||
         dn->lockstate == DN_LOCK_UNPINNING);
  
  // dir auth pinnable?
  if (!dn->dir->can_auth_pin()) {
    dout(7) << "dentry " << *dn << " dir not pinnable, waiting" << endl;
    dn->dir->add_waiter(CDIR_WAIT_AUTHPINNABLE,
                        new C_MDS_RetryRequest(mds,m,ref));
    return false;
  }

  // is dentry path pinned?
  if (dn->is_pinned()) {
    dout(7) << "dentry " << *dn << " pinned, waiting" << endl;
    dn->lockstate = DN_LOCK_UNPINNING;
    dn->dir->add_waiter(CDIR_WAIT_DNUNPINNED,
                        dn->name,
                        new C_MDS_RetryRequest(mds,m,ref));
    return false;
  }

  // pin path up to dentry!            (if success, point of no return)
  CDentry *pdn = dn->dir->inode->get_parent_dn();
  if (pdn) {
    if (mdcache->active_requests[m].traces.count(pdn)) {
      dout(7) << "already path pinned parent dentry " << *pdn << endl;
    } else {
      dout(7) << "pinning parent dentry " << *pdn << endl;
      vector<CDentry*> trace;
      mdcache->make_trace(trace, pdn->inode);
      assert(trace.size());

      if (!mdcache->path_pin(trace, m, new C_MDS_RetryRequest(mds, m, ref))) return false;
      
      mdcache->active_requests[m].traces[trace[trace.size()-1]] = trace;
    }
  }

  // pin dir!
  dn->dir->auth_pin();
  
  // mine!
  dn->xlockedby = m;

  if (dn->is_replicated()) {
    dn->lockstate = DN_LOCK_PREXLOCK;
    
    // xlock with whom?
    set<int> who;
    for (map<int,int>::iterator p = dn->replicas_begin();
	 p != dn->replicas_end();
	 ++p)
      who.insert(p->first);
    dn->gather_set = who;

    // make path
    string path;
    dn->make_path(path);
    dout(10) << "path is " << path << " for " << *dn << endl;

    for (set<int>::iterator it = who.begin();
         it != who.end();
         it++) {
      MLock *m = new MLock(LOCK_AC_LOCK, mds->get_nodeid());
      m->set_dn(dn->dir->ino(), dn->name);
      m->set_path(path);
      mds->send_message_mds(m, *it, MDS_PORT_LOCKER);
    }

    // wait
    dout(7) << "dentry_xlock_start locking, waiting for replicas " << endl;
    dn->dir->add_waiter(CDIR_WAIT_DNLOCK, dn->name,
                        new C_MDS_RetryRequest(mds, m, ref));
    return false;
  } else {
    dn->lockstate = DN_LOCK_XLOCK;
    mdcache->active_requests[dn->xlockedby].xlocks.insert(dn);
    return true;
  }
}

void Locker::dentry_xlock_finish(CDentry *dn, bool quiet)
{
  dout(7) << "dentry_xlock_finish on " << *dn << endl;
  
  assert(dn->xlockedby);
  if (dn->xlockedby == DN_XLOCK_FOREIGN) {
    dout(7) << "this was a foreign xlock" << endl;
  } else {
    // remove from request record
    assert(mdcache->active_requests[dn->xlockedby].xlocks.count(dn) == 1);
    mdcache->active_requests[dn->xlockedby].xlocks.erase(dn);
  }

  dn->xlockedby = 0;
  dn->lockstate = DN_LOCK_SYNC;

  // unpin parent dir?
  // -> no?  because we might have xlocked 2 things in this dir.
  //         instead, we let request_finish clean up the mess.
    
  // tell replicas?
  if (!quiet) {
    // tell even if dn is null.
    if (dn->is_replicated()) {
      send_lock_message(dn, LOCK_AC_SYNC);
    }
  }
  
  // unpin dir
  dn->dir->auth_unpin();

  // kick waiters
  list<Context*> finished;
  dn->dir->take_waiting(CDIR_WAIT_DNREAD, finished);
  mds->queue_finished(finished);
}


/*
 * onfinish->finish() will be called with 
 * 0 on successful xlock,
 * -1 on failure
 */

class C_MDC_XlockRequest : public Context {
  Locker *mdc;
  CDir *dir;
  string dname;
  Message *req;
  Context *finisher;
public:
  C_MDC_XlockRequest(Locker *mdc, 
                     CDir *dir, string& dname, 
                     Message *req,
                     Context *finisher) {
    this->mdc = mdc;
    this->dir = dir;
    this->dname = dname;
    this->req = req;
    this->finisher = finisher;
  }

  void finish(int r) {
    mdc->dentry_xlock_request_finish(r, dir, dname, req, finisher);
  }
};

void Locker::dentry_xlock_request_finish(int r, 
					  CDir *dir, string& dname, 
					  Message *req,
					  Context *finisher) 
{
  dout(10) << "dentry_xlock_request_finish r = " << r << endl;
  if (r == 1) {  // 1 for xlock request success
    CDentry *dn = dir->lookup(dname);
    if (dn && dn->xlockedby == 0) {
      // success
      dn->xlockedby = req;   // our request was the winner
      dout(10) << "xlock request success, now xlocked by req " << req << " dn " << *dn << endl;
      
      // remember!
      mdcache->active_requests[req].foreign_xlocks.insert(dn);
    }        
  }
  
  // retry request (or whatever)
  finisher->finish(0);
  delete finisher;
}

void Locker::dentry_xlock_request(CDir *dir, string& dname, bool create,
                                   Message *req, Context *onfinish)
{
  dout(10) << "dentry_xlock_request on dn " << dname << " create=" << create << " in " << *dir << endl; 
  // send request
  int dauth = dir->dentry_authority(dname);
  MLock *m = new MLock(create ? LOCK_AC_REQXLOCKC:LOCK_AC_REQXLOCK, mds->get_nodeid());
  m->set_dn(dir->ino(), dname);
  mds->send_message_mds(m, dauth, MDS_PORT_LOCKER);
  
  // add waiter
  dir->add_waiter(CDIR_WAIT_DNREQXLOCK, dname, 
                  new C_MDC_XlockRequest(this, 
                                         dir, dname, req,
                                         onfinish));
}




void Locker::handle_lock_dn(MLock *m)
{
  assert(m->get_otype() == LOCK_OTYPE_DN);
  
  CInode *diri = mdcache->get_inode(m->get_ino());  // may be null 
  CDir *dir = 0;
  if (diri) dir = diri->dir;           // may be null
  string dname = m->get_dn();
  int from = m->get_asker();
  CDentry *dn = 0;

  if (LOCK_AC_FOR_AUTH(m->get_action())) {
    // auth

    // normally we have it always
    if (diri && dir) {
      int dauth = dir->dentry_authority(dname);
      assert(dauth == mds->get_nodeid() || dir->is_proxy() ||  // mine or proxy,
             m->get_action() == LOCK_AC_REQXLOCKACK ||         // or we did a REQXLOCK and this is our ack/nak
             m->get_action() == LOCK_AC_REQXLOCKNAK);
      
      if (dir->is_proxy()) {

        assert(dauth >= 0);

        if (dauth == m->get_asker() && 
            (m->get_action() == LOCK_AC_REQXLOCK ||
             m->get_action() == LOCK_AC_REQXLOCKC)) {
          dout(7) << "handle_lock_dn got reqxlock from " << dauth << " and they are auth.. dropping on floor (their import will have woken them up)" << endl;
          if (mdcache->active_requests.count(m)) 
            mdcache->request_finish(m);
          else
            delete m;
          return;
        }

        dout(7) << "handle_lock_dn " << m << " " << m->get_ino() << " dname " << dname << " from " << from << ": proxy, fw to " << dauth << endl;

        // forward
        if (mdcache->active_requests.count(m)) {
          // xlock requests are requests, use request_* functions!
          assert(m->get_action() == LOCK_AC_REQXLOCK ||
                 m->get_action() == LOCK_AC_REQXLOCKC);
          // forward as a request
          mdcache->request_forward(m, dauth, MDS_PORT_LOCKER);
        } else {
          // not an xlock req, or it is and we just didn't register the request yet
          // forward normally
          mds->send_message_mds(m, dauth, MDS_PORT_LOCKER);
        }
        return;
      }
      
      dn = dir->lookup(dname);
    }

    // except with.. an xlock request?
    if (!dn) {
      assert(dir);  // we should still have the dir, though!  the requester has the dir open.
      switch (m->get_action()) {

      case LOCK_AC_LOCK:
        dout(7) << "handle_lock_dn xlock on " << dname << ", adding (null)" << endl;
        dn = dir->add_dentry(dname);
        break;

      case LOCK_AC_REQXLOCK:
        // send nak
        if (dir->state_test(CDIR_STATE_DELETED)) {
          dout(7) << "handle_lock_dn reqxlock on deleted dir " << *dir << ", nak" << endl;
        } else {
          dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << " dne, nak" << endl;
        }
        {
          MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
          reply->set_dn(dir->ino(), dname);
          reply->set_path(m->get_path());
          mds->send_message_mds(reply, m->get_asker(), MDS_PORT_LOCKER);
        }
         
        // finish request (if we got that far)
        if (mdcache->active_requests.count(m)) 
	  mdcache->request_finish(m);

        delete m;
        return;

      case LOCK_AC_REQXLOCKC:
        dout(7) << "handle_lock_dn reqxlockc on " << dname << " in " << *dir << " dne (yet!)" << endl;
        break;

      default:
        assert(0);
      }
    }
  } else {
    // replica
    if (dir) dn = dir->lookup(dname);
    if (!dn) {
      dout(7) << "handle_lock_dn " << m << " don't have " << m->get_ino() << " dname " << dname << endl;
      
      if (m->get_action() == LOCK_AC_REQXLOCKACK ||
          m->get_action() == LOCK_AC_REQXLOCKNAK) {
        dout(7) << "handle_lock_dn got reqxlockack/nak, but don't have dn " << m->get_path() << ", discovering" << endl;
        //assert(0);  // how can this happen?  tell me now!
        
        vector<CDentry*> trace;
        filepath path = m->get_path();
        int r = mdcache->path_traverse(path, trace, true,
				       m, new C_MDS_RetryMessage(mds,m), 
				       MDS_TRAVERSE_DISCOVER);
        assert(r>0);
        return;
      } 

      if (m->get_action() == LOCK_AC_LOCK) {
        if (0) { // not anymore
          dout(7) << "handle_lock_dn don't have " << m->get_path() << ", discovering" << endl;
          
          vector<CDentry*> trace;
          filepath path = m->get_path();
          int r = mdcache->path_traverse(path, trace, true,
					 m, new C_MDS_RetryMessage(mds,m), 
					 MDS_TRAVERSE_DISCOVER);
          assert(r>0);
        }
        if (1) {
          // NAK
          MLock *reply = new MLock(LOCK_AC_LOCKNAK, mds->get_nodeid());
          reply->set_dn(m->get_ino(), dname);
          mds->send_message_mds(reply, m->get_asker(), MDS_PORT_LOCKER);
        }
      } else {
        dout(7) << "safely ignoring." << endl;
        delete m;
      }
      return;
    }

    assert(dn);
  }

  if (dn) {
    dout(7) << "handle_lock_dn a=" << m->get_action() << " from " << from << " " << *dn << endl;
  } else {
    dout(7) << "handle_lock_dn a=" << m->get_action() << " from " << from << " " << dname << " in " << *dir << endl;
  }
  
  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_LOCK:
    assert(dn->lockstate == DN_LOCK_SYNC ||
           dn->lockstate == DN_LOCK_UNPINNING ||
           dn->lockstate == DN_LOCK_XLOCK);   // <-- bc the handle_lock_dn did the discover!

    if (dn->is_pinned()) {
      dn->lockstate = DN_LOCK_UNPINNING;

      // wait
      dout(7) << "dn pinned, waiting " << *dn << endl;
      dn->dir->add_waiter(CDIR_WAIT_DNUNPINNED,
                          dn->name,
                          new C_MDS_RetryMessage(mds, m));
      return;
    } else {
      dn->lockstate = DN_LOCK_XLOCK;
      dn->xlockedby = 0;

      // ack now
      MLock *reply = new MLock(LOCK_AC_LOCKACK, mds->get_nodeid());
      reply->set_dn(diri->ino(), dname);
      mds->send_message_mds(reply, from, MDS_PORT_LOCKER);
    }

    // wake up waiters
    dir->finish_waiting(CDIR_WAIT_DNLOCK, dname);   // ? will this happen on replica ? 
    break;

  case LOCK_AC_SYNC:
    assert(dn->lockstate == DN_LOCK_XLOCK);
    dn->lockstate = DN_LOCK_SYNC;
    dn->xlockedby = 0;

    // null?  hose it.
    if (dn->is_null()) {
      dout(7) << "hosing null (and now sync) dentry " << *dn << endl;
      dir->remove_dentry(dn);
    }

    // wake up waiters
    dir->finish_waiting(CDIR_WAIT_DNREAD, dname);   // will this happen either?  YES: if a rename lock backs out
    break;

  case LOCK_AC_REQXLOCKACK:
  case LOCK_AC_REQXLOCKNAK:
    {
      dout(10) << "handle_lock_dn got ack/nak on a reqxlock for " << *dn << endl;
      list<Context*> finished;
      dir->take_waiting(CDIR_WAIT_DNREQXLOCK, m->get_dn(), finished, 1);  // TAKE ONE ONLY!
      finish_contexts(finished, 
                      (m->get_action() == LOCK_AC_REQXLOCKACK) ? 1:-1);
    }
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
  case LOCK_AC_LOCKNAK:
    assert(dn->gather_set.count(from) == 1);
    dn->gather_set.erase(from);
    if (dn->gather_set.size() == 0) {
      dout(7) << "handle_lock_dn finish gather, now xlock on " << *dn << endl;
      dn->lockstate = DN_LOCK_XLOCK;
      mdcache->active_requests[dn->xlockedby].xlocks.insert(dn);
      dir->finish_waiting(CDIR_WAIT_DNLOCK, dname);
    }
    break;


  case LOCK_AC_REQXLOCKC:
    // make sure it's a _file_, if it exists.
    if (dn && dn->inode && dn->inode->is_dir()) {
      dout(7) << "handle_lock_dn failing, reqxlockc on dir " << *dn->inode << endl;
      
      // nak
      string path;
      dn->make_path(path);

      MLock *reply = new MLock(LOCK_AC_REQXLOCKNAK, mds->get_nodeid());
      reply->set_dn(dir->ino(), dname);
      reply->set_path(path);
      mds->send_message_mds(reply, m->get_asker(), MDS_PORT_LOCKER);
      
      // done
      if (mdcache->active_requests.count(m)) 
        mdcache->request_finish(m);
      else
        delete m;
      return;
    }

  case LOCK_AC_REQXLOCK:
    if (dn) {
      dout(7) << "handle_lock_dn reqxlock on " << *dn << endl;
    } else {
      dout(7) << "handle_lock_dn reqxlock on " << dname << " in " << *dir << endl;      
    }
    

    // start request?
    if (!mdcache->active_requests.count(m)) {
      vector<CDentry*> trace;
      if (!mdcache->request_start(m, dir->inode, trace))
        return;  // waiting for pin
    }
    
    // try to xlock!
    if (!dn) {
      assert(m->get_action() == LOCK_AC_REQXLOCKC);
      dn = dir->add_dentry(dname);
    }

    if (dn->xlockedby != m) {
      if (!dentry_xlock_start(dn, m, dir->inode)) {
        // hose null dn if we're waiting on something
        if (dn->is_clean() && dn->is_null() && dn->is_sync()) dir->remove_dentry(dn);
        return;    // waiting for xlock
      }
    } else {
      // successfully xlocked!  on behalf of requestor.
      string path;
      dn->make_path(path);

      dout(7) << "handle_lock_dn reqxlock success for " << m->get_asker() << " on " << *dn << ", acking" << endl;
      
      // ACK xlock request
      MLock *reply = new MLock(LOCK_AC_REQXLOCKACK, mds->get_nodeid());
      reply->set_dn(dir->ino(), dname);
      reply->set_path(path);
      mds->send_message_mds(reply, m->get_asker(), MDS_PORT_LOCKER);

      // note: keep request around in memory (to hold the xlock/pins on behalf of requester)
      return;
    }
    break;

  case LOCK_AC_UNXLOCK:
    dout(7) << "handle_lock_dn unxlock on " << *dn << endl;
    {
      string dname = dn->name;
      Message *m = dn->xlockedby;

      // finish request
      mdcache->request_finish(m);  // this will drop the locks (and unpin paths!)
      return;
    }
    break;

  default:
    assert(0);
  }

  delete m;
}







