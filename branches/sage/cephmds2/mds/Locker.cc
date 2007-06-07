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
#include "MDCache.h"
#include "Locker.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EString.h"
#include "events/EUpdate.h"

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


void Locker::send_lock_message(SimpleLock *lock, int msg)
{
  for (map<int,int>::iterator it = lock->get_parent()->replicas_begin(); 
       it != lock->get_parent()->replicas_end(); 
       it++) {
    if (mds->mdsmap->get_state(it->first) < MDSMap::STATE_REJOIN) 
      continue;
    MLock *m = new MLock(lock, msg, mds->get_nodeid());
    mds->send_message_mds(m, it->first, MDS_PORT_LOCKER);
  }
}

void Locker::send_lock_message(SimpleLock *lock, int msg, const bufferlist &data)
{
  for (map<int,int>::iterator it = lock->get_parent()->replicas_begin(); 
       it != lock->get_parent()->replicas_end(); 
       it++) {
    if (mds->mdsmap->get_state(it->first) < MDSMap::STATE_REJOIN) 
      continue;
    MLock *m = new MLock(lock, msg, mds->get_nodeid());
    m->set_data(data);
    mds->send_message_mds(m, it->first, MDS_PORT_LOCKER);
  }
}











bool Locker::acquire_locks(MDRequest *mdr,
			   set<SimpleLock*> &rdlocks,
			   set<SimpleLock*> &wrlocks,
			   set<SimpleLock*> &xlocks)
{
  dout(10) << "acquire_locks " << *mdr << endl;

  // sort everything we will lock
  set<SimpleLock*, SimpleLock::ptr_lt> sorted;

  // (local) AUTH PINS

  // make list of items to authpin
  set<SimpleLock*> mustpin = xlocks;
  for (set<SimpleLock*>::iterator p = wrlocks.begin(); p != wrlocks.end(); ++p)
    mustpin.insert(*p);
  
  // can i auth pin them all now?
  for (set<SimpleLock*>::iterator p = mustpin.begin();
       p != mustpin.end();
       ++p) {
    dout(10) << "must authpin " << **p << " " << *(*p)->get_parent() << endl;

    // sort in
    sorted.insert(*p);

    if ((*p)->get_type() == LOCK_OTYPE_DN) {
      CDir *dir = ((CDentry*)(*p)->get_parent())->dir;
      dout(10) << "might auth_pin " << *dir << endl;
      
      if (!dir->is_auth()) continue;
      if (!mdr->is_auth_pinned(dir) &&
	  !dir->can_auth_pin()) {
	// wait
	dir->add_waiter(CDir::WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mdcache, mdr));
	mdcache->request_drop_locks(mdr);
	mdr->drop_auth_pins();
	return false;
      }
    } else {
      CInode *in = (CInode*)(*p)->get_parent();
      if (!in->is_auth()) continue;
      if (!mdr->is_auth_pinned(in) &&
	  !in->can_auth_pin()) {
	in->add_waiter(CInode::WAIT_AUTHPINNABLE, new C_MDS_RetryRequest(mdcache, mdr));
	mdcache->request_drop_locks(mdr);
	mdr->drop_auth_pins();
	return false;
      }
    }
  }

  // ok, grab the auth pins
  for (set<SimpleLock*>::iterator p = mustpin.begin();
       p != mustpin.end();
       ++p) {
    if ((*p)->get_type() == LOCK_OTYPE_DN) {
      CDir *dir = ((CDentry*)(*p)->get_parent())->dir;
      if (!dir->is_auth()) continue;
      dout(10) << "auth_pinning " << *dir << endl;
      mdr->auth_pin(dir);
    } else {
      CInode *in = (CInode*)(*p)->get_parent();
      if (!in->is_auth()) continue;
      dout(10) << "auth_pinning " << *in << endl;
      mdr->auth_pin(in);
    }
  }

  // sort in rdlocks too
  for (set<SimpleLock*>::iterator p = rdlocks.begin();
	 p != rdlocks.end();
       ++p) {
    dout(10) << "will rdlock " << **p << " " << *(*p)->get_parent() << endl;
    sorted.insert(*p);
  }

  // acquire locks.
  // make sure they match currently acquired locks.
  set<SimpleLock*, SimpleLock::ptr_lt>::iterator existing = mdr->locks.begin();
  for (set<SimpleLock*, SimpleLock::ptr_lt>::iterator p = sorted.begin();
       p != sorted.end();
       ++p) {

    // already locked?
    if (existing != mdr->locks.end() && *existing == *p) {
      // right kind?
      SimpleLock *had = *existing;
      if (xlocks.count(*p) == mdr->xlocks.count(*p) &&
	  wrlocks.count(*p) == mdr->wrlocks.count(*p) &&
	  rdlocks.count(*p) == mdr->rdlocks.count(*p)) {
	dout(10) << "acquire_locks already locked " << *had << " " << *had->get_parent() << endl;
	existing++;
	continue;
      }
    }
    
    // hose any stray locks
    while (existing != mdr->locks.end()) {
      SimpleLock *had = *existing;
      existing++;
      dout(10) << "acquire_locks unlocking out-of-order " << **existing
	       << " " << *(*existing)->get_parent() << endl;
      if (mdr->xlocks.count(had)) 
	xlock_finish(had, mdr);
      else if (mdr->wrlocks.count(had))
	wrlock_finish(had, mdr);
      else
	rdlock_finish(had, mdr);
    }
      
    // lock
    if (xlocks.count(*p)) {
      if (!xlock_start(*p, mdr)) 
	return false;
      dout(10) << "acquire_locks got xlock on " << **p << " " << *(*p)->get_parent() << endl;
    } else if (wrlocks.count(*p)) {
      if (!wrlock_start(*p, mdr)) 
	return false;
      dout(10) << "acquire_locks got wrlock on " << **p << " " << *(*p)->get_parent() << endl;
    } else {
      if (!rdlock_start(*p, mdr)) 
	return false;
      dout(10) << "acquire_locks got rdlock on " << **p << " " << *(*p)->get_parent() << endl;
    }
  }
    
  // any extra unneeded locks?
  while (existing != mdr->locks.end()) {
    dout(10) << "acquire_locks unlocking " << *existing
	     << " " << *(*existing)->get_parent() << endl;
    if (mdr->xlocks.count(*existing))
      xlock_finish(*existing, mdr);
    else if (mdr->wrlocks.count(*existing))
      wrlock_finish(*existing, mdr);
    else
      rdlock_finish(*existing, mdr);
  }

  return true;
}



// generics

bool Locker::rdlock_start(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IFILE:
    return file_rdlock_start((FileLock*)lock, mdr);
  case LOCK_OTYPE_IDIR:
    return scatter_rdlock_start((ScatterLock*)lock, mdr);
  default:
    return simple_rdlock_start(lock, mdr);
  }
}

void Locker::rdlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IFILE:
    return file_rdlock_finish((FileLock*)lock, mdr);
  case LOCK_OTYPE_IDIR:
    return scatter_rdlock_finish((ScatterLock*)lock, mdr);
  default:
    return simple_rdlock_finish(lock, mdr);
  }
}

bool Locker::wrlock_start(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IDIR:
    return scatter_wrlock_start((ScatterLock*)lock, mdr);
  default:
    assert(0);
  }
}

void Locker::wrlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IDIR:
    return scatter_wrlock_finish((ScatterLock*)lock, mdr);
  default:
    assert(0);
  }
}

bool Locker::xlock_start(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IFILE:
    return file_xlock_start((FileLock*)lock, mdr);
  case LOCK_OTYPE_IDIR:
    assert(0);
  default:
    return simple_xlock_start(lock, mdr);
  }
}

void Locker::xlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IFILE:
    return file_xlock_finish((FileLock*)lock, mdr);
  case LOCK_OTYPE_IDIR:
    assert(0);
  default:
    return simple_xlock_finish(lock, mdr);
  }
}






// file i/o -----------------------------------------

version_t Locker::issue_file_data_version(CInode *in)
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
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
    // new cap
    Capability c(my_want);
    in->add_client_cap(my_client, c);
    cap = in->get_client_cap(my_client);

    // suppress file cap messages for new cap (we'll bundle with the open() reply)
    cap->set_suppress(true);
  } else {
    // make sure it has sufficient caps
    if (cap->wanted() & ~my_want) {
      // augment wanted caps for this client
      cap->set_wanted( cap->wanted() | my_want );
    }
  }

  int before = cap->pending();

  if (in->is_auth()) {
    // [auth] twiddle mode?
    file_eval(&in->filelock);
  } else {
    // [replica] tell auth about any new caps wanted
    request_inode_file_caps(in);
  }
    
  // issue caps (pot. incl new one)
  issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  cap->issue(cap->pending());
  
  // ok, stop suppressing.
  cap->set_suppress(false);

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



bool Locker::issue_caps(CInode *in)
{
  // allowed caps are determined by the lock mode.
  int allowed = in->filelock.caps_allowed();
  dout(7) << "issue_caps filelock allows=" << cap_string(allowed) 
          << " on " << *in << endl;

  // count conflicts with
  int nissued = 0;        

  // client caps
  for (map<int, Capability>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       it++) {
    if (it->second.pending() != (it->second.wanted() & allowed)) {
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

    int auth = in->authority().first;
    dout(7) << "request_inode_file_caps " << cap_string(wanted)
            << " was " << cap_string(in->replica_caps_wanted) 
            << " on " << *in << " to mds" << auth << endl;
    assert(!in->is_auth());

    in->replica_caps_wanted = wanted;

    if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN)
      mds->send_message_mds(new MInodeFileCaps(in->ino(), mds->get_nodeid(),
					       in->replica_caps_wanted),
			    auth, MDS_PORT_LOCKER);
  } else {
    in->replica_caps_wanted_keep_until.sec_ref() = 0;
  }
}

void Locker::handle_inode_file_caps(MInodeFileCaps *m)
{
  // nobody should be talking to us during recovery.
  assert(mds->is_rejoin() || mds->is_active() || mds->is_stopping());

  // ok
  CInode *in = mdcache->get_inode(m->get_ino());
  assert(in);
  assert(in->is_auth());

  if (mds->is_rejoin() &&
      in->is_rejoining()) {
    dout(7) << "handle_inode_file_caps still rejoining " << *in << ", dropping " << *m << endl;
    delete m;
    return;
  }

  
  dout(7) << "handle_inode_file_caps replica mds" << m->get_from() << " wants caps " << cap_string(m->get_caps()) << " on " << *in << endl;

  if (m->get_caps())
    in->mds_caps_wanted[m->get_from()] = m->get_caps();
  else
    in->mds_caps_wanted.erase(m->get_from());

  file_eval(&in->filelock);
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
  int wanted = m->get_wanted() & in->filelock.caps_allowed_ever();
  
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

    // tell client.
    MClientFileCaps *r = new MClientFileCaps(in->inode, 
                                             0, 0, 0,
                                             MClientFileCaps::OP_RELEASE);
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
  file_eval(&in->filelock);
  in->finish_waiting(CInode::WAIT_CAPS, 0);

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
  // nobody should be talking to us during recovery.
  assert(mds->is_rejoin() || mds->is_active() || mds->is_stopping());

  switch (m->get_otype()) {
  case LOCK_OTYPE_DN:
    {
      CDir *dir = mdcache->get_dirfrag(m->get_dirfrag());
      CDentry *dn = 0;
      if (dir) 
	dn = dir->lookup(m->get_dn());
      if (!dn) {
	dout(7) << "dont' have dn " << m->get_dirfrag() << " " << m->get_dn() << endl;
	delete m;
	return;
      }
    
      handle_simple_lock(&dn->lock, m);
    }
    break;

  case LOCK_OTYPE_IAUTH:
  case LOCK_OTYPE_ILINK:
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IFILE:
    {
      CInode *in = mdcache->get_inode(m->get_ino());
      if (!in) {
	dout(7) << "dont' have ino " << m->get_ino() << endl;
	delete m;
	return;
      }
      switch (m->get_otype()) {
      case LOCK_OTYPE_IAUTH:
	handle_simple_lock(&in->authlock, m);
	break;
      case LOCK_OTYPE_ILINK:
	handle_simple_lock(&in->linklock, m);
	break;
      case LOCK_OTYPE_IDIRFRAGTREE:
	handle_simple_lock(&in->dirfragtreelock, m);
	break;
      case LOCK_OTYPE_IFILE:
	handle_file_lock(&in->filelock, m);
	break;
      }
    }
    break;

  case LOCK_OTYPE_IDIR:
    {
      CInode *in = mdcache->get_inode(m->get_ino());
      if (!in) {
	dout(7) << "dont' have ino " << m->get_ino() << endl;
	delete m;
	return;
      }
      handle_scatter_lock(&in->dirlock, m);
    }
    break;

  default:
    dout(7) << "handle_lock got otype " << m->get_otype() << endl;
    assert(0);
    break;
  }
}
 




// ==========================================================================
// simple lock

void Locker::handle_simple_lock(SimpleLock *lock, MLock *m)
{
  int from = m->get_asker();
  
  if (mds->is_rejoin()) {
    if (lock->get_parent()->is_rejoining()) {
      dout(7) << "handle_simple_lock still rejoining " << *lock->get_parent()
	      << ", dropping " << *m << endl;
      delete m;
      return;
    }
  }

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK);
    lock->decode_locked_state(m->get_data());
    lock->set_state(LOCK_SYNC);
    lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC);
    //||           lock->get_state() == LOCK_GLOCKR);
    
    // wait for readers to finish?
    if (lock->is_rdlocked()) {
      dout(7) << "handle_simple_lock has reader, waiting before ack on " << *lock
	      << " on " << *lock->get_parent() << endl;
      lock->set_state(LOCK_GLOCKR);
    } else {
      // update lock and reply
      lock->set_state(LOCK_LOCK);
      mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid()), 
			    from, MDS_PORT_LOCKER);
    }
    break;


  case LOCK_AC_REQXLOCKACK:
    dout(7) << "handle_simple_lock got remote xlock on "
	    << *lock << " " << *lock->get_parent() << endl;
    {
      MDRequest *mdr = mdcache->request_get(m->get_reqid());
      mdr->xlocks.insert(lock);
      mdr->locks.insert(lock);
      lock->set_state(LOCK_REMOTEXLOCK);
      lock->finish_waiters(SimpleLock::WAIT_REMOTEXLOCK);
    }
    break;

    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->get_state() == LOCK_GLOCKR);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->is_gathering()) {
      dout(7) << "handle_simple_lock " << *lock << " on " << *lock->get_parent() << " from " << from
	      << ", still gathering " << lock->get_gather_set() << endl;
    } else {
      dout(7) << "handle_simple_lock " << *lock << " on " << *lock->get_parent() << " from " << from
	      << ", last one" << endl;
      simple_eval(lock);
    }
    break;

  case LOCK_AC_REQXLOCK:
    assert(lock->get_parent()->is_auth());
    {
      // register request
      MDRequest *mdr = mdcache->request_start(m);

      dout(7) << "handle_simple_lock " << m->get_source() << " " << *mdr << " requesting xlock "
	      << *lock << " on " << *lock->get_parent()
	      << endl;

      if (!simple_xlock_start(lock, mdr))
	return;

      // ack
      MLock *m = new MLock(lock, LOCK_AC_REQXLOCKACK, mds->get_nodeid());
      mds->send_message_mds(m, mdr->request->get_source().num(), MDS_PORT_LOCKER);
    }
    return;

  case LOCK_AC_UNXLOCK:
    assert(lock->get_parent()->is_auth());
    {
      // get request
      MDRequest *mdr = mdcache->request_get(m->get_reqid());

      dout(7) << "handle_simple_lock " << m->get_source() << " " << *mdr << " dropping xlock "
	      << *lock << " on " << *lock->get_parent()
	
	      << endl;

      simple_xlock_finish(lock, mdr);

      if (mdr->locks.empty()) 
	mdcache->request_finish(mdr);
      
    }
    return;

  }

  delete m;
}


class C_Locker_SimpleEval : public Context {
  Locker *locker;
  SimpleLock *lock;
public:
  C_Locker_SimpleEval(Locker *l, SimpleLock *lk) : locker(l), lock(lk) {}
  void finish(int r) {
    locker->simple_eval(lock);
  }
};

void Locker::simple_eval(SimpleLock *lock)
{
  // unstable and ambiguous auth?
  if (!lock->is_stable() &&
      lock->get_parent()->is_ambiguous_auth()) {
    dout(7) << "simple_eval not stable and ambiguous auth, waiting on " << *lock->get_parent() << endl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_Locker_SimpleEval(this, lock));
    return;
  }

  // finished remote xlock?
  if (lock->get_state() == LOCK_REMOTEXLOCK &&
      !lock->is_xlocked()) {
    // tell auth
    assert(!lock->get_parent()->is_auth()); // should be auth_pinned on the auth
    dout(7) << "simple_eval releasing remote xlock on " << *lock->get_parent()  << endl;
    int auth = lock->get_parent()->authority().first;
    if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN)
      mds->send_message_mds(new MLock(lock, LOCK_AC_UNXLOCK, mds->get_nodeid()),
			    auth, MDS_PORT_LOCKER);
    lock->set_state(LOCK_LOCK);
  }

  // finished gathering?
  if (lock->get_state() == LOCK_GLOCKR &&
      !lock->is_gathering() &&
      !lock->is_rdlocked()) {
    dout(7) << "simple_eval finished gather on " << *lock << " on " << *lock->get_parent() << endl;

    // replica: tell auth
    if (!lock->get_parent()->is_auth()) {
      int auth = lock->get_parent()->authority().first;
      if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) 
	mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid()), 
			      lock->get_parent()->authority().first, MDS_PORT_LOCKER);
    }
    
    lock->set_state(LOCK_LOCK);
    lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR);
  }

  // stable -> sync?
  if (lock->get_parent()->is_auth() &&
      lock->is_stable() &&
      lock->get_state() != LOCK_SYNC &&
      !lock->is_waiter_for(SimpleLock::WAIT_WR)) {
    dout(7) << "simple_eval stable, syncing " << *lock 
	    << " on " << *lock->get_parent() << endl;
    simple_sync(lock);
  }
  
}


// mid

void Locker::simple_sync(SimpleLock *lock)
{
  dout(7) << "simple_sync on " << *lock << " on " << *lock->get_parent() << endl;
  assert(lock->get_parent()->is_auth());
  
  // check state
  if (lock->get_state() == LOCK_SYNC)
    return; // already sync
  if (lock->get_state() == LOCK_GLOCKR) 
    assert(0); // um... hmm!
  assert(lock->get_state() == LOCK_LOCK);

  // sync.
  if (lock->get_parent()->is_replicated()) {
    // hard data
    bufferlist data;
    lock->encode_locked_state(data);
    
    // bcast to replicas
    send_lock_message(lock, LOCK_AC_SYNC, data);
  }
  
  // change lock
  lock->set_state(LOCK_SYNC);
  
  // waiters?
  lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
}

void Locker::simple_lock(SimpleLock *lock)
{
  dout(7) << "simple_lock on " << *lock << " on " << *lock->get_parent() << endl;
  assert(lock->get_parent()->is_auth());
  
  // check state
  if (lock->get_state() == LOCK_LOCK ||
      lock->get_state() == LOCK_GLOCKR) 
    return;  // already lock or locking
  assert(lock->get_state() == LOCK_SYNC);
  
  if (lock->get_parent()->is_replicated()) {
    // bcast to replicas
    send_lock_message(lock, LOCK_AC_LOCK);
    
    // change lock
    lock->set_state(LOCK_GLOCKR);
    lock->init_gather();
  } else {
    lock->set_state(LOCK_LOCK);
  }
}


// top

bool Locker::simple_rdlock_try(SimpleLock *lock, Context *con)
{
  dout(7) << "simple_rdlock_try on " << *lock << " on " << *lock->get_parent() << endl;  

  // can read?  grab ref.
  if (lock->can_rdlock(0)) 
    return true;
  
  assert(!lock->get_parent()->is_auth());

  // wait!
  dout(7) << "simple_rdlock_try waiting on " << *lock << " on " << *lock->get_parent() << endl;
  lock->add_waiter(SimpleLock::WAIT_RD, con);
  return false;
}

bool Locker::simple_rdlock_start(SimpleLock *lock, MDRequest *mdr)
{
  dout(7) << "simple_rdlock_start  on " << *lock << " on " << *lock->get_parent() << endl;  

  // can read?  grab ref.
  if (lock->can_rdlock(mdr)) {
    lock->get_rdlock();
    mdr->rdlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  }
  
  // wait!
  dout(7) << "simple_rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << endl;
  lock->add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
  return false;
}

void Locker::simple_rdlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  // drop ref
  lock->put_rdlock();
  if (mdr) {
    mdr->rdlocks.erase(lock);
    mdr->locks.erase(lock);
  }

  dout(7) << "simple_rdlock_finish on " << *lock << " on " << *lock->get_parent() << endl;
  
  // last one?
  if (!lock->is_rdlocked())
    simple_eval(lock);
}

bool Locker::simple_xlock_start(SimpleLock *lock, MDRequest *mdr)
{
  dout(7) << "simple_xlock_start  on " << *lock << " on " << *lock->get_parent() << endl;

  // xlock by me?
  if (lock->is_xlocked() &&
      lock->get_xlocked_by() == mdr) 
    return true;

  // auth?
  if (lock->get_parent()->is_auth()) {
    // auth

    // lock.
    if (lock->get_state() == LOCK_SYNC) 
      simple_lock(lock);

    // already locked?
    if (lock->get_state() == LOCK_LOCK) {
      if (lock->is_xlocked()) {
	// by someone else.
	lock->add_waiter(SimpleLock::WAIT_WR, new C_MDS_RetryRequest(mdcache, mdr));
	return false;
      }

      // xlock.
      lock->get_xlock(mdr);
      mdr->xlocks.insert(lock);
      mdr->locks.insert(lock);
      return true;
    } else {
      // wait for lock
      lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }
  } else {
    // replica
    
    // wait for single auth
    if (lock->get_parent()->is_ambiguous_auth()) {
      lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
				     new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }
    int auth = lock->get_parent()->authority().first;

    // wait for sync.
    // (???????????)
    if (lock->get_state() != LOCK_SYNC) {
      lock->add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }

    // send lock request
    MLock *m = new MLock(lock, LOCK_AC_REQXLOCK, mds->get_nodeid());
    mds->send_message_mds(m, auth, MDS_PORT_LOCKER);
  
    // wait
    lock->add_waiter(SimpleLock::WAIT_REMOTEXLOCK, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
}


void Locker::simple_xlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  // drop ref
  assert(lock->can_xlock(mdr));
  lock->put_xlock();
  assert(mdr);
  mdr->xlocks.erase(lock);
  mdr->locks.erase(lock);
  dout(7) << "simple_xlock_finish on " << *lock << " on " << *lock->get_parent() << endl;

  // others waiting?
  lock->finish_waiters(SimpleLock::WAIT_WR, 0); 

  // eval
  simple_eval(lock);
}



// dentry specific helpers

// trace helpers

/** dentry_can_rdlock_trace
 * see if we can _anonymously_ rdlock an entire trace.  
 * if not, and req is specified, wait and retry that message.
 */
bool Locker::dentry_can_rdlock_trace(vector<CDentry*>& trace, MClientRequest *req) 
{
  // verify dentries are rdlockable.
  // we do this because
  // - we're being less aggressive about locks acquisition, and
  // - we're not acquiring the locks in order!
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) {
    CDentry *dn = *it;
    if (!dn->lock.can_rdlock(0)) {
      if (req) {
	dout(10) << "can_rdlock_trace can't rdlock " << *dn << ", waiting" << endl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryMessage(mds, req));
      } else {
	dout(10) << "can_rdlock_trace can't rdlock " << *dn << endl;
      }
      return false;
    }
  }
  return true;
}

void Locker::dentry_anon_rdlock_trace_start(vector<CDentry*>& trace)
{
  // grab dentry rdlocks
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++)
    (*it)->lock.get_rdlock();
}


void Locker::dentry_anon_rdlock_trace_finish(vector<CDentry*>& trace)
{
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++) 
    simple_rdlock_finish(&(*it)->lock, 0);
}



// ==========================================================================
// scatter lock

bool Locker::scatter_rdlock_start(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_rdlock_start  on " << *lock
	  << " on " << *lock->get_parent() << endl;  

  // pre-twiddle?
  if (lock->get_state() == LOCK_SCATTER &&
      lock->get_parent()->is_auth() &&
      !lock->get_parent()->is_replicated() &&
      !lock->is_wrlocked()) 
    scatter_sync(lock);

  // can rdlock?
  if (lock->can_rdlock(mdr)) {
    lock->get_rdlock();
    mdr->rdlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  }

  // wait for read.
  lock->add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));

  // initiate sync?
  if (lock->get_state() == LOCK_SCATTER &&
      lock->get_parent()->is_auth())
    scatter_sync(lock);

  return false;
}

void Locker::scatter_rdlock_finish(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_rdlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << endl;  
  lock->put_rdlock();
  if (mdr) {
    mdr->rdlocks.erase(lock);
    mdr->locks.erase(lock);
  }
  
  scatter_eval(lock);
}


bool Locker::scatter_wrlock_start(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_wrlock_start  on " << *lock
	  << " on " << *lock->get_parent() << endl;  
  
  // pre-twiddle?
  if (lock->get_state() == LOCK_SYNC &&
      lock->get_parent()->is_auth() &&
      !lock->get_parent()->is_replicated() &&
      !lock->is_rdlocked()) 
    scatter_scatter(lock);

  // can wrlock?
  if (lock->can_wrlock()) {
    lock->get_wrlock();
    mdr->wrlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  }

  // wait for write.
  lock->add_waiter(SimpleLock::WAIT_WR, new C_MDS_RetryRequest(mdcache, mdr));

  // initiate scatter?
  if (lock->get_state() == LOCK_SYNC &&
      lock->get_parent()->is_auth())
    scatter_scatter(lock);

  return false;
}

void Locker::scatter_wrlock_finish(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << endl;  
  lock->put_wrlock();
  if (mdr) {
    mdr->wrlocks.erase(lock);
    mdr->locks.erase(lock);
  }
  
  scatter_eval(lock);
}


class C_Locker_ScatterEval : public Context {
  Locker *locker;
  ScatterLock *lock;
public:
  C_Locker_ScatterEval(Locker *l, ScatterLock *lk) : locker(l), lock(lk) {}
  void finish(int r) {
    locker->scatter_eval(lock);
  }
};

void Locker::scatter_eval(ScatterLock *lock)
{
  // unstable and ambiguous auth?
  if (!lock->is_stable() &&
      lock->get_parent()->is_ambiguous_auth()) {
    dout(7) << "scatter_eval not stable and ambiguous auth, waiting on " << *lock->get_parent() << endl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_Locker_ScatterEval(this, lock));
    return;
  }

  if (!lock->get_parent()->is_auth()) {
    // REPLICA

    if (lock->get_state() == LOCK_GSYNCS &&
	!lock->is_wrlocked()) {
      dout(10) << "scatter_eval no wrlocks, acking sync" << endl;
      int auth = lock->get_parent()->authority().first;
      if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
	bufferlist data;
	lock->encode_locked_state(data);
	mds->send_message_mds(new MLock(lock, LOCK_AC_SYNCACK, mds->get_nodeid(), data),
			      auth, MDS_PORT_LOCKER);
      }
      lock->set_state(LOCK_SYNC);
      lock->finish_waiters(ScatterLock::WAIT_STABLE);  // ?
    }

  } else {
    // AUTH
    
    // gsyncs -> sync?
    if (lock->get_state() == LOCK_GSYNCS &&
	!lock->is_gathering() &&
	!lock->is_wrlocked()) {
      dout(7) << "scatter_eval finished gather/un-wrlock on " << *lock
	      << " on " << *lock->get_parent() << endl;
      lock->set_state(LOCK_SYNC);
      lock->finish_waiters(ScatterLock::WAIT_STABLE|ScatterLock::WAIT_RD);
    }
    
    // gscatters -> scatter?
    if (lock->get_state() == LOCK_GSCATTERS &&
	!lock->is_rdlocked()) {
      assert(lock->get_parent()->is_auth());
      if (lock->get_parent()->is_replicated()) {
	// encode and bcast
	bufferlist data;
	lock->encode_locked_state(data);
	send_lock_message(lock, LOCK_AC_SCATTER, data);
      } 
      
      lock->set_state(LOCK_SCATTER);
      lock->get_wrlock();
      lock->finish_waiters(ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
      lock->put_wrlock();
    }
    
    // waiting for rd?
    if (lock->get_state() == LOCK_SCATTER &&
	!lock->is_wrlocked() &&
	lock->is_waiter_for(ScatterLock::WAIT_RD)) {
      dout(10) << "scatter_eval no wrlocks, read waiter, syncing" << endl;
      scatter_sync(lock);
    }
    
    // re-scatter?
    if (lock->get_state() == LOCK_SYNC && 
	!lock->is_rdlocked()) {
      dout(10) << "scatter_eval no rdlocks, scattering" << endl;
      scatter_scatter(lock);
    }
  }
}


void Locker::scatter_sync(ScatterLock *lock)
{
  dout(10) << "scatter_sync " << *lock
	   << " on " << *lock->get_parent() << endl;
  assert(lock->get_parent()->is_auth());
  
  if (lock->get_state() == LOCK_SYNC) return;
  assert(lock->get_state() == LOCK_SCATTER);
  
  // bcast
  if (lock->get_parent()->is_replicated()) {
    send_lock_message(lock, LOCK_AC_SYNC);
    lock->set_state(LOCK_GSYNCS);
    lock->init_gather();
  } 
  else if (lock->is_wrlocked()) {
    lock->set_state(LOCK_GSYNCS);
  } 
  else {    
    lock->set_state(LOCK_SYNC);
    lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
  }
}


void Locker::scatter_scatter(ScatterLock *lock)
{
  dout(10) << "scatter_scatter " << *lock
	   << " on " << *lock->get_parent() << endl;
  assert(lock->get_parent()->is_auth());
  
  if (lock->get_state() == LOCK_SCATTER) return;
  assert(lock->get_state() == LOCK_SYNC);

  if (lock->is_rdlocked()) {
    lock->set_state(LOCK_GSCATTERS);
  } else {
    if (lock->get_parent()->is_replicated()) {
      // encode and bcast
      bufferlist data;
      lock->encode_locked_state(data);
      send_lock_message(lock, LOCK_AC_SCATTER, data);
    } 
    lock->set_state(LOCK_SCATTER);
    lock->finish_waiters(ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
  }
}



void Locker::handle_scatter_lock(ScatterLock *lock, MLock *m)
{
  int from = m->get_asker();
  
  if (mds->is_rejoin()) {
    if (lock->get_parent()->is_rejoining()) {
      dout(7) << "handle_scatter_lock still rejoining " << *lock->get_parent()
	      << ", dropping " << *m << endl;
      delete m;
      return;
    }
  }

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_SCATTER);

    // wait for wrlocks to close?
    if (lock->is_wrlocked()) {
      dout(7) << "handle_scatter_lock has wrlocks, waiting on " << *lock
	      << " on " << *lock->get_parent() << endl;
      lock->set_state(LOCK_GSYNCS);
    } else {
      // encode and reply
      bufferlist data;
      lock->encode_locked_state(data);
      mds->send_message_mds(new MLock(lock, LOCK_AC_SYNCACK, mds->get_nodeid(), data),
			    from, MDS_PORT_LOCKER);
    }
    break;

  case LOCK_AC_SCATTER:
    assert(lock->get_state() == LOCK_SYNC);
    lock->decode_locked_state(m->get_data());
    lock->set_state(LOCK_SCATTER);
    lock->finish_waiters(ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
    break;

    // -- for auth --
  case LOCK_AC_SYNCACK:
    assert(lock->get_state() == LOCK_GSYNCS);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    lock->decode_locked_state(m->get_data());
    
    if (lock->is_gathering()) {
      dout(7) << "handle_scatter_lock " << *lock << " on " << *lock->get_parent()
	      << " from " << from << ", still gathering " << lock->get_gather_set()
	      << endl;
    } else {
      dout(7) << "handle_scatter_lock " << *lock << " on " << *lock->get_parent()
	      << " from " << from << ", last one" 
	      << endl;
      scatter_eval(lock);
    }
    break;
  }

  delete m;
}



// ==========================================================================
// file lock


bool Locker::file_rdlock_start(FileLock *lock, MDRequest *mdr)
{
  dout(7) << "file_rdlock_start " << *lock << " on " << *lock->get_parent() << endl;

  // can read?  grab ref.
  if (lock->can_rdlock(mdr)) {
    lock->get_rdlock();
    mdr->rdlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  }
  
  // can't read, and replicated.
  if (lock->can_rdlock_soon()) {
    // wait
    dout(7) << "file_rdlock_start can_rdlock_soon " << *lock << " on " << *lock->get_parent() << endl;
  } else {    
    if (lock->get_parent()->is_auth()) {
      // auth

      // FIXME or qsync?

      if (lock->is_stable()) {
        file_lock(lock);     // lock, bc easiest to back off ... FIXME
	
        if (lock->can_rdlock(mdr)) {
          lock->get_rdlock();
	  mdr->rdlocks.insert(lock);
	  mdr->locks.insert(lock);
          
          lock->finish_waiters(SimpleLock::WAIT_STABLE);
          return true;
        }
      } else {
        dout(7) << "file_rdlock_start waiting until stable on " << *lock << " on " << *lock->get_parent() << endl;
        lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
        return false;
      }
    } else {
      // replica
      if (lock->is_stable()) {
	
        // fw to auth
	CInode *in = (CInode*)lock->get_parent();
        int auth = in->authority().first;
        dout(7) << "file_rdlock_start " << *lock << " on " << *lock->get_parent() << " on replica and async, fw to auth " << auth << endl;
        assert(auth != mds->get_nodeid());
        mdcache->request_forward(mdr, auth);
        return false;
        
      } else {
        // wait until stable
        dout(7) << "inode_file_rdlock_start waiting until stable on " << *lock << " on " << *lock->get_parent() << endl;
        lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
        return false;
      }
    }
  }
  
  // wait
  dout(7) << "file_rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << endl;
  lock->add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
        
  return false;
}



void Locker::file_rdlock_finish(FileLock *lock, MDRequest *mdr)
{
  // drop ref
  assert(lock->can_rdlock(mdr));
  lock->put_rdlock();
  mdr->rdlocks.erase(lock);
  mdr->locks.erase(lock);

  dout(7) << "rdlock_finish on " << *lock << " on " << *lock->get_parent() << endl;

  if (!lock->is_rdlocked()) 
    file_eval(lock);
}


bool Locker::file_xlock_start(FileLock *lock, MDRequest *mdr)
{
  dout(7) << "file_xlock_start on " << *lock << " on " << *lock->get_parent() << endl;

  assert(lock->get_parent()->is_auth());  // remote file xlock not implemented

  // already xlocked by me?
  if (lock->get_xlocked_by() == mdr)
    return true;

  // can't write?
  if (!lock->can_xlock(mdr)) {
    
    // auth
    if (!lock->can_xlock_soon()) {
      if (!lock->is_stable()) {
	dout(7) << "file_xlock_start on auth, waiting for stable on " << *lock << " on " << *lock->get_parent() << endl;
	lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
	return false;
      }
      
      // initiate lock 
      file_lock(lock);
      
      // fall-thru to below.
    }
  } 
  
  // check again
  if (lock->can_xlock(mdr)) {
    assert(lock->get_parent()->is_auth());
    lock->get_xlock(mdr);
    mdr->locks.insert(lock);
    mdr->xlocks.insert(lock);
    return true;
  } else {
    dout(7) << "file_xlock_start on auth, waiting for write on " << *lock << " on " << *lock->get_parent() << endl;
    lock->add_waiter(SimpleLock::WAIT_WR, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
}


void Locker::file_xlock_finish(FileLock *lock, MDRequest *mdr)
{
  // drop ref
  assert(lock->can_xlock(mdr));
  lock->put_xlock();
  mdr->locks.erase(lock);
  mdr->xlocks.erase(lock);
  dout(7) << "file_xlock_finish on " << *lock << " on " << *lock->get_parent() << endl;

  assert(lock->get_parent()->is_auth());  // or implement remote xlocks

  // others waiting?
  lock->finish_waiters(SimpleLock::WAIT_WR, 0); 

  //// drop lock?
  //if (!lock->is_waiter_for(SimpleLock::WAIT_STABLE)) 
  file_eval(lock);
}


/*
 * ...
 *
 * also called after client caps are acked to us
 * - checks if we're in unstable sfot state and can now move on to next state
 * - checks if soft state should change (eg bc last writer closed)
 */
class C_Locker_FileEval : public Context {
  Locker *locker;
  FileLock *lock;
public:
  C_Locker_FileEval(Locker *l, FileLock *lk) : locker(l), lock(lk) {}
  void finish(int r) {
    locker->file_eval(lock);
  }
};


void Locker::file_eval(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();

  // unstable and ambiguous auth?
  if (!lock->is_stable() &&
      in->is_ambiguous_auth()) {
    dout(7) << "file_eval not stable and ambiguous auth, waiting on " << *in << endl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    in->add_waiter(CInode::WAIT_SINGLEAUTH, new C_Locker_FileEval(this, lock));
    return;
  }


  int issued = in->get_caps_issued();

  // [auth] finished gather?
  if (in->is_auth() &&
      !lock->is_stable() &&
      !lock->is_gathering()) {
    dout(7) << "file_eval finished mds gather on " << *lock << " on " << *lock->get_parent() << endl;

    switch (lock->get_state()) {
      // to lock
    case LOCK_GLOCKR:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
      if ((issued & ~CAP_FILE_RDCACHE) == 0) {
        lock->set_state(LOCK_LOCK);
        
        // waiters
        lock->get_rdlock();
        lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR|SimpleLock::WAIT_RD);
        lock->put_rdlock();
      }
      break;
      
      // to mixed
    case LOCK_GMIXEDR:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        lock->set_state(LOCK_MIXED);
        lock->finish_waiters(SimpleLock::WAIT_STABLE);
      }
      break;

    case LOCK_GMIXEDL:
      if ((issued & ~(CAP_FILE_WR)) == 0) {
        lock->set_state(LOCK_MIXED);

        if (in->is_replicated()) {
          // data
          bufferlist softdata;
	  lock->encode_locked_state(softdata);
          
          // bcast to replicas
	  send_lock_message(lock, LOCK_AC_MIXED, softdata);
        }

        lock->finish_waiters(SimpleLock::WAIT_STABLE);
      }
      break;

      // to loner
    case LOCK_GLONERR:
      if (issued == 0) {
        lock->set_state(LOCK_LONER);
        lock->finish_waiters(SimpleLock::WAIT_STABLE);
      }
      break;

    case LOCK_GLONERM:
      if ((issued & ~CAP_FILE_WR) == 0) {
        lock->set_state(LOCK_LONER);
        lock->finish_waiters(SimpleLock::WAIT_STABLE);
      }
      break;
      
      // to sync
    case LOCK_GSYNCL:
    case LOCK_GSYNCM:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        lock->set_state(LOCK_SYNC);
        
        { // bcast data to replicas
          bufferlist softdata;
          lock->encode_locked_state(softdata);
          
	  send_lock_message(lock, LOCK_AC_SYNC, softdata);
        }
        
        // waiters
        lock->get_rdlock();
        lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
        lock->put_rdlock();
      }
      break;
      
    default: 
      assert(0);
    }

    issue_caps(in);
  }
  
  // [replica] finished caps gather?
  if (!in->is_auth() &&
      !lock->is_stable()) {
    switch (lock->get_state()) {
    case LOCK_GMIXEDR:
      if ((issued & ~(CAP_FILE_RD)) == 0) {
        lock->set_state(LOCK_MIXED);
        
        // ack
        MLock *reply = new MLock(lock, LOCK_AC_MIXEDACK, mds->get_nodeid());
        mds->send_message_mds(reply, in->authority().first, MDS_PORT_LOCKER);
      }
      break;

    case LOCK_GLOCKR:
      if (issued == 0) {
        lock->set_state(LOCK_LOCK);
        
        // ack
        MLock *reply = new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid());
        mds->send_message_mds(reply, in->authority().first, MDS_PORT_LOCKER);
      }
      break;

    default:
      assert(0);
    }
  }

  // !stable -> do nothing.
  if (!lock->is_stable()) return; 


  // stable.
  assert(lock->is_stable());

  if (in->is_auth() &&
      !lock->is_xlocked()) {
    // [auth] 
    // and not xlocked!
    int wanted = in->get_caps_wanted();
    bool loner = (in->client_caps.size() == 1) && in->mds_caps_wanted.empty();
    dout(7) << "file_eval wanted=" << cap_string(wanted)
            << "  filelock=" << *lock << " on " << *lock->get_parent()
            << "  loner=" << loner
            << endl;

    // * -> loner?
    if (!lock->is_rdlocked() &&
        !lock->is_waiter_for(SimpleLock::WAIT_WR) &&
        (wanted & CAP_FILE_WR) &&
        loner &&
        lock->get_state() != LOCK_LONER) {
      dout(7) << "file_eval stable, bump to loner " << *lock << " on " << *lock->get_parent() << endl;
      file_loner(lock);
    }

    // * -> mixed?
    else if (!lock->is_rdlocked() &&
	     !lock->is_waiter_for(SimpleLock::WAIT_WR) &&
             (wanted & CAP_FILE_RD) &&
             (wanted & CAP_FILE_WR) &&
             !(loner && lock->get_state() == LOCK_LONER) &&
             lock->get_state() != LOCK_MIXED) {
      dout(7) << "file_eval stable, bump to mixed " << *lock << " on " << *lock->get_parent() << endl;
      file_mixed(lock);
    }

    // * -> sync?
    else if (!in->filelock.is_waiter_for(SimpleLock::WAIT_WR) &&
             !(wanted & (CAP_FILE_WR|CAP_FILE_WRBUFFER)) &&
             ((wanted & CAP_FILE_RD) || 
              in->is_replicated() || 
              (!loner && lock->get_state() == LOCK_LONER)) &&
             lock->get_state() != LOCK_SYNC) {
      dout(7) << "file_eval stable, bump to sync " << *lock << " on " << *lock->get_parent() << endl;
      file_sync(lock);
    }

    // * -> lock?  (if not replicated or open)
    else if (!in->is_replicated() &&
             wanted == 0 &&
             lock->get_state() != LOCK_LOCK) {
      file_lock(lock);
    }
  }
}


// mid

bool Locker::file_sync(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  dout(7) << "file_sync " << *lock << " on " << *lock->get_parent() << endl;  

  assert(in->is_auth());

  // check state
  if (lock->get_state() == LOCK_SYNC ||
      lock->get_state() == LOCK_GSYNCL ||
      lock->get_state() == LOCK_GSYNCM)
    return true;

  assert(lock->is_stable());

  int issued = in->get_caps_issued();

  assert((in->get_caps_wanted() & CAP_FILE_WR) == 0);

  if (lock->get_state() == LOCK_LOCK) {
    if (in->is_replicated()) {
      bufferlist softdata;
      lock->encode_locked_state(softdata);
      send_lock_message(lock, LOCK_AC_SYNC, softdata);
    }
    
    // change lock
    lock->set_state(LOCK_SYNC);

    issue_caps(in);    // reissue caps
    return true;
  }

  else if (lock->get_state() == LOCK_MIXED) {
    // writers?
    if (issued & CAP_FILE_WR) {
      // gather client write caps
      lock->set_state(LOCK_GSYNCM);
      issue_caps(in);
    } else {
      // no writers, go straight to sync
      if (in->is_replicated()) {
	bufferlist softdata;
	lock->encode_locked_state(softdata);
	send_lock_message(lock, LOCK_AC_SYNC, softdata);
      }
    
      // change lock
      lock->set_state(LOCK_SYNC);
    }
    return false;
  }

  else if (lock->get_state() == LOCK_LONER) {
    // writers?
    if (issued & CAP_FILE_WR) {
      // gather client write caps
      lock->set_state(LOCK_GSYNCL);
      issue_caps(in);
    } else {
      // no writers, go straight to sync
      if (in->is_replicated()) {
	bufferlist softdata;
	lock->encode_locked_state(softdata);
	send_lock_message(lock, LOCK_AC_SYNC, softdata);
      }

      // change lock
      lock->set_state(LOCK_SYNC);
    }
    return false;
  }
  else 
    assert(0); // wtf.

  return false;
}



void Locker::file_lock(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  dout(7) << "inode_file_lock " << *lock << " on " << *lock->get_parent() << endl;  

  assert(in->is_auth());
  
  // check state
  if (lock->get_state() == LOCK_LOCK ||
      lock->get_state() == LOCK_GLOCKR ||
      lock->get_state() == LOCK_GLOCKM ||
      lock->get_state() == LOCK_GLOCKL) 
    return;  // lock or locking

  assert(lock->is_stable());

  int issued = in->get_caps_issued();

  if (lock->get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
      
      // change lock
      lock->set_state(LOCK_GLOCKR);

      // call back caps
      if (issued) 
        issue_caps(in);
    } else {
      if (issued) {
        // call back caps
        lock->set_state(LOCK_GLOCKR);
        issue_caps(in);
      } else {
        lock->set_state(LOCK_LOCK);
      }
    }
  }

  else if (lock->get_state() == LOCK_MIXED) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();

      // change lock
      lock->set_state(LOCK_GLOCKM);
      
      // call back caps
      issue_caps(in);
    } else {
      //assert(issued);  // ??? -sage 2/19/06
      if (issued) {
        // change lock
        lock->set_state(LOCK_GLOCKM);
        
        // call back caps
        issue_caps(in);
      } else {
        lock->set_state(LOCK_LOCK);
      }
    }
      
  }
  else if (lock->get_state() == LOCK_LONER) {
    if (issued & CAP_FILE_WR) {
      // change lock
      lock->set_state(LOCK_GLOCKL);
  
      // call back caps
      issue_caps(in);
    } else {
      lock->set_state(LOCK_LOCK);
    }
  }
  else 
    assert(0); // wtf.
}


void Locker::file_mixed(FileLock *lock)
{
  dout(7) << "file_mixed " << *lock << " on " << *lock->get_parent() << endl;  

  CInode *in = (CInode*)lock->get_parent();
  assert(in->is_auth());
  
  // check state
  if (lock->get_state() == LOCK_GMIXEDR ||
      lock->get_state() == LOCK_GMIXEDL)
    return;     // mixed or mixing

  assert(lock->is_stable());

  int issued = in->get_caps_issued();

  if (lock->get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_MIXED);
      lock->init_gather();
    
      lock->set_state(LOCK_GMIXEDR);
      issue_caps(in);
    } else {
      if (issued) {
        lock->set_state(LOCK_GMIXEDR);
        issue_caps(in);
      } else {
        lock->set_state(LOCK_MIXED);
      }
    }
  }

  else if (lock->get_state() == LOCK_LOCK) {
    if (in->is_replicated()) {
      // data
      bufferlist softdata;
      lock->encode_locked_state(softdata);
      
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_MIXED, softdata);
    }

    // change lock
    lock->set_state(LOCK_MIXED);
    issue_caps(in);
  }

  else if (lock->get_state() == LOCK_LONER) {
    if (issued & CAP_FILE_WRBUFFER) {
      // gather up WRBUFFER caps
      lock->set_state(LOCK_GMIXEDL);
      issue_caps(in);
    }
    else if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_MIXED);
      lock->set_state(LOCK_MIXED);
      issue_caps(in);
    } else {
      lock->set_state(LOCK_MIXED);
      issue_caps(in);
    }
  }

  else 
    assert(0); // wtf.
}


void Locker::file_loner(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  dout(7) << "inode_file_loner " << *lock << " on " << *lock->get_parent() << endl;  

  assert(in->is_auth());

  // check state
  if (lock->get_state() == LOCK_LONER ||
      lock->get_state() == LOCK_GLONERR ||
      lock->get_state() == LOCK_GLONERM)
    return; 

  assert(lock->is_stable());
  assert((in->client_caps.size() == 1) && in->mds_caps_wanted.empty());
  
  if (lock->get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
      
      // change lock
      lock->set_state(LOCK_GLONERR);
    } else {
      // only one guy with file open, who gets it all, so
      lock->set_state(LOCK_LONER);
      issue_caps(in);
    }
  }

  else if (lock->get_state() == LOCK_LOCK) {
    // change lock.  ignore replicas; they don't know about LONER.
    lock->set_state(LOCK_LONER);
    issue_caps(in);
  }

  else if (lock->get_state() == LOCK_MIXED) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
      
      // change lock
      lock->set_state(LOCK_GLONERM);
    } else {
      lock->set_state(LOCK_LONER);
      issue_caps(in);
    }
  }

  else 
    assert(0);
}



// messenger

void Locker::handle_file_lock(FileLock *lock, MLock *m)
{
  if (mds->logger) mds->logger->inc("lif");

  CInode *in = (CInode*)lock->get_parent();
  int from = m->get_asker();

  if (mds->is_rejoin()) {
    if (in->is_rejoining()) {
      dout(7) << "handle_file_lock still rejoining " << *in
	      << ", dropping " << *m << endl;
      delete m;
      return;
    }
  }


  dout(7) << "handle_file_lock a=" << m->get_action() << " from " << from << " " 
	  << *in << " filelock=" << *lock << endl;  
  
  int issued = in->get_caps_issued();

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK ||
           lock->get_state() == LOCK_MIXED);
    
    lock->decode_locked_state(m->get_data());
    lock->set_state(LOCK_SYNC);
    
    // no need to reply.
    
    // waiters
    lock->get_rdlock();
    lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    lock->put_rdlock();
    file_eval(lock);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_MIXED);
    
    lock->set_state(LOCK_GLOCKR);
    
    // call back caps?
    if (issued & CAP_FILE_RD) {
      dout(7) << "handle_file_lock client readers, gathering caps on " << *in << endl;
      issue_caps(in);
      break;
    }
    if (lock->is_rdlocked()) {
      dout(7) << "handle_file_lock rdlocked, waiting before ack on " << *in << endl;
      break;
    } 
    
    // nothing to wait for, lock and ack.
    {
      lock->set_state(LOCK_LOCK);
      
      MLock *reply = new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid());
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
        MLock *reply = new MLock(lock, LOCK_AC_MIXEDACK, mds->get_nodeid());
        mds->send_message_mds(reply, from, MDS_PORT_LOCKER);
      }
    } else {
      // LOCK
      lock->set_state(LOCK_MIXED);
      
      // no ack needed.
    }

    issue_caps(in);
    
    // waiters
    lock->finish_waiters(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
    file_eval(lock);
    break;

 
    

    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->get_state() == LOCK_GLOCKR ||
           lock->get_state() == LOCK_GLOCKM ||
           lock->get_state() == LOCK_GLONERM ||
           lock->get_state() == LOCK_GLONERR);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);

    if (lock->is_gathering()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", last one" << endl;
      file_eval(lock);
    }
    break;
    
  case LOCK_AC_SYNCACK:
    assert(lock->get_state() == LOCK_GSYNCM);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    /* not used currently
    {
      // merge data  (keep largest size, mtime, etc.)
      int off = 0;
      in->decode_merge_file_state(m->get_data(), off);
    }
    */

    if (lock->is_gathering()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", last one" << endl;
      file_eval(lock);
    }
    break;

  case LOCK_AC_MIXEDACK:
    assert(lock->get_state() == LOCK_GMIXEDR);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->is_gathering()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << endl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", last one" << endl;
      file_eval(lock);
    }
    break;


  default:
    assert(0);
  }  
  
  delete m;
}






