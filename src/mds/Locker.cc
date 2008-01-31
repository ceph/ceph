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

#include "messages/MLock.h"
#include "messages/MDentryUnlink.h"

#include "messages/MClientRequest.h"
#include "messages/MClientFileCaps.h"

#include "messages/MMDSSlaveRequest.h"

#include <errno.h>
#include <assert.h>

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".locker "



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

  case CEPH_MSG_CLIENT_FILECAPS:
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
    mds->send_message_mds(m, it->first);
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
    mds->send_message_mds(m, it->first);
  }
}











bool Locker::acquire_locks(MDRequest *mdr,
			   set<SimpleLock*> &rdlocks,
			   set<SimpleLock*> &wrlocks,
			   set<SimpleLock*> &xlocks)
{
  if (mdr->done_locking) {
    dout(10) << "acquire_locks " << *mdr << " -- done locking" << dendl;    
    return true;  // at least we had better be!
  }
  dout(10) << "acquire_locks " << *mdr << dendl;

  set<SimpleLock*, SimpleLock::ptr_lt> sorted;  // sort everything we will lock
  set<SimpleLock*> mustpin = xlocks;            // items to authpin

  // xlocks
  for (set<SimpleLock*>::iterator p = xlocks.begin(); p != xlocks.end(); ++p) {
    dout(20) << " must xlock " << **p << " " << *(*p)->get_parent() << dendl;
    sorted.insert(*p);

    // augment xlock with a versionlock?
    if ((*p)->get_type() > LOCK_OTYPE_IVERSION) {
      // inode version lock?
      CInode *in = (CInode*)(*p)->get_parent();
      if (mdr->is_master()) {
	// master.  wrlock versionlock so we can pipeline inode updates to journal.
	wrlocks.insert(&in->versionlock);
      } else {
	// slave.  exclusively lock the inode version (i.e. block other journal updates)
	xlocks.insert(&in->versionlock);
	sorted.insert(&in->versionlock);
      }
    }
  }

  // wrlocks
  for (set<SimpleLock*>::iterator p = wrlocks.begin(); p != wrlocks.end(); ++p) {
    dout(20) << " must wrlock " << **p << " " << *(*p)->get_parent() << dendl;
    sorted.insert(*p);
    if ((*p)->get_parent()->is_auth())
      mustpin.insert(*p);
    else if ((*p)->get_type() == LOCK_OTYPE_IDIR &&
	     !(*p)->get_parent()->is_auth() && !((ScatterLock*)(*p))->can_wrlock()) { // we might have to request a scatter
      dout(15) << " will also auth_pin " << *(*p)->get_parent() << " in case we need to request a scatter" << dendl;
      mustpin.insert(*p);
    }
  }

  // rdlocks
  for (set<SimpleLock*>::iterator p = rdlocks.begin();
	 p != rdlocks.end();
       ++p) {
    dout(20) << " must rdlock " << **p << " " << *(*p)->get_parent() << dendl;
    sorted.insert(*p);
  }

 
  // AUTH PINS
  map<int, set<MDSCacheObject*> > mustpin_remote;  // mds -> (object set)
  
  // can i auth pin them all now?
  for (set<SimpleLock*>::iterator p = mustpin.begin();
       p != mustpin.end();
       ++p) {
    MDSCacheObject *object = (*p)->get_parent();

    dout(10) << " must authpin " << *object << dendl;

    if (mdr->is_auth_pinned(object)) 
      continue;
    
    if (!object->is_auth()) {
      if (object->is_ambiguous_auth()) {
	// wait
	dout(10) << " ambiguous auth, waiting to authpin " << *object << dendl;
	object->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	mds->locker->drop_locks(mdr);
	mdr->drop_local_auth_pins();
	return false;
      }
      mustpin_remote[object->authority().first].insert(object);
      continue;
    }
    if (!object->can_auth_pin()) {
      // wait
      dout(10) << " can't auth_pin (freezing?), waiting to authpin " << *object << dendl;
      object->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      mds->locker->drop_locks(mdr);
      mdr->drop_local_auth_pins();
      return false;
    }
  }

  // ok, grab local auth pins
  for (set<SimpleLock*>::iterator p = mustpin.begin();
       p != mustpin.end();
       ++p) {
    MDSCacheObject *object = (*p)->get_parent();
    if (mdr->is_auth_pinned(object)) {
      dout(10) << " already auth_pinned " << *object << dendl;
    } else if (object->is_auth()) {
      dout(10) << " auth_pinning " << *object << dendl;
      mdr->auth_pin(object);
    }
  }

  // request remote auth_pins
  if (!mustpin_remote.empty()) {
    for (map<int, set<MDSCacheObject*> >::iterator p = mustpin_remote.begin();
	 p != mustpin_remote.end();
	 ++p) {
      dout(10) << "requesting remote auth_pins from mds" << p->first << dendl;
      
      MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_AUTHPIN);
      for (set<MDSCacheObject*>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " req remote auth_pin of " << **q << dendl;
	MDSCacheObjectInfo info;
	(*q)->set_object_info(info);
	req->get_authpins().push_back(info);      
	mdr->pin(*q);
      }
      mds->send_message_mds(req, p->first);

      // put in waiting list
      assert(mdr->more()->waiting_on_slave.count(p->first) == 0);
      mdr->more()->waiting_on_slave.insert(p->first);
    }
    return false;
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
      SimpleLock *have = *existing;
      existing++;
      if (xlocks.count(*p) && mdr->xlocks.count(*p)) {
	dout(10) << " already xlocked " << *have << " " << *have->get_parent() << dendl;
      }
      else if (wrlocks.count(*p) && mdr->wrlocks.count(*p)) {
	dout(10) << " already wrlocked " << *have << " " << *have->get_parent() << dendl;
      }
      else if (rdlocks.count(*p) && mdr->rdlocks.count(*p)) {
	dout(10) << " already rdlocked " << *have << " " << *have->get_parent() << dendl;
      }
      else assert(0);
      continue;
    }
    
    // hose any stray locks
    while (existing != mdr->locks.end()) {
      SimpleLock *stray = *existing;
      existing++;
      dout(10) << " unlocking out-of-order " << *stray << " " << *stray->get_parent() << dendl;
      if (mdr->xlocks.count(stray)) 
	xlock_finish(stray, mdr);
      else if (mdr->wrlocks.count(stray))
	wrlock_finish(stray, mdr);
      else
	rdlock_finish(stray, mdr);
    }
      
    // lock
    if (xlocks.count(*p)) {
      if (!xlock_start(*p, mdr)) 
	return false;
      dout(10) << " got xlock on " << **p << " " << *(*p)->get_parent() << dendl;
    } else if (wrlocks.count(*p)) {
      if (!wrlock_start(*p, mdr)) 
	return false;
      dout(10) << " got wrlock on " << **p << " " << *(*p)->get_parent() << dendl;
    } else {
      if (!rdlock_start(*p, mdr)) 
	return false;
      dout(10) << " got rdlock on " << **p << " " << *(*p)->get_parent() << dendl;
    }
  }
    
  // any extra unneeded locks?
  while (existing != mdr->locks.end()) {
    SimpleLock *stray = *existing;
    existing++;
    dout(10) << " unlocking extra " << *stray << " " << *stray->get_parent() << dendl;
    if (mdr->xlocks.count(stray))
      xlock_finish(stray, mdr);
    else if (mdr->wrlocks.count(stray))
      wrlock_finish(stray, mdr);
    else
      rdlock_finish(stray, mdr);
  }

  return true;
}


void Locker::drop_locks(MDRequest *mdr)
{
  // leftover locks
  while (!mdr->xlocks.empty()) 
    xlock_finish(*mdr->xlocks.begin(), mdr);
  while (!mdr->rdlocks.empty()) 
    rdlock_finish(*mdr->rdlocks.begin(), mdr);
  while (!mdr->wrlocks.empty()) 
    wrlock_finish(*mdr->wrlocks.begin(), mdr);
}


// generics

bool Locker::rdlock_start(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IFILE:
    return file_rdlock_start((FileLock*)lock, mdr);
  case LOCK_OTYPE_IDIRFRAGTREE:
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
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IDIR:
    return scatter_rdlock_finish((ScatterLock*)lock, mdr);
  default:
    return simple_rdlock_finish(lock, mdr);
  }
}

bool Locker::wrlock_start(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IDIR:
    return scatter_wrlock_start((ScatterLock*)lock, mdr);
  case LOCK_OTYPE_IVERSION:
    return local_wrlock_start((LocalLock*)lock, mdr);
  default:
    assert(0); 
    return false;
  }
}

void Locker::wrlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IDIR:
    return scatter_wrlock_finish((ScatterLock*)lock, mdr);
  case LOCK_OTYPE_IVERSION:
    return local_wrlock_finish((LocalLock*)lock, mdr);
  default:
    assert(0);
  }
}

bool Locker::xlock_start(SimpleLock *lock, MDRequest *mdr)
{
  switch (lock->get_type()) {
  case LOCK_OTYPE_IFILE:
    return file_xlock_start((FileLock*)lock, mdr);
  case LOCK_OTYPE_IVERSION:
    return local_xlock_start((LocalLock*)lock, mdr);
  case LOCK_OTYPE_IDIRFRAGTREE:
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
  case LOCK_OTYPE_IVERSION:
    return local_xlock_finish((LocalLock*)lock, mdr);
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IDIR:
    assert(0);
  default:
    return simple_xlock_finish(lock, mdr);
  }
}



/** rejoin_set_state
 * @lock the lock 
 * @s the new state
 * @waiters list for anybody waiting on this lock
 */
void Locker::rejoin_set_state(SimpleLock *lock, int s, list<Context*>& waiters)
{
  if (!lock->is_stable()) {
    lock->set_state(s);
    lock->get_parent()->auth_unpin();
  } else {
    lock->set_state(s);
  }
  lock->take_waiting(SimpleLock::WAIT_ALL, waiters);
}




// file i/o -----------------------------------------

version_t Locker::issue_file_data_version(CInode *in)
{
  dout(7) << "issue_file_data_version on " << *in << dendl;
  return in->inode.file_data_version;
}


Capability* Locker::issue_new_caps(CInode *in,
				   int mode,
				   Session *session)
{
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << dendl;
  
  // my needs
  assert(session->inst.name.is_client());
  int my_client = session->inst.name.num();
  int my_want = 0;
  if (mode & FILE_MODE_R) my_want |= CEPH_CAP_RDCACHE  | CEPH_CAP_RD;
  if (mode & FILE_MODE_W) my_want |= CEPH_CAP_WRBUFFER | CEPH_CAP_WR;

  // register a capability
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
    // new cap
    cap = in->add_client_cap(my_client, in, session->caps);
    cap->set_wanted(my_want);
    cap->set_suppress(true); // suppress file cap messages for new cap (we'll bundle with the open() reply)
  } else {
    // make sure it has sufficient caps
    if (my_want & ~cap->wanted()) {
      // augment wanted caps for this client
      cap->set_wanted(cap->wanted() | my_want);
    }
  }

  int before = cap->pending();

  if (in->is_auth()) {
    // [auth] twiddle mode?
    if (in->filelock.is_stable()) 
      file_eval(&in->filelock);
  } else {
    // [replica] tell auth about any new caps wanted
    request_inode_file_caps(in);
  }

  // issue caps (pot. incl new one)
  issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  cap->issue(cap->pending());
  cap->set_last_open();
  
  int now = cap->pending();
  if (before != now &&
      (before & CEPH_CAP_WR) == 0 &&
      (now & CEPH_CAP_WR)) {
    // FIXME FIXME FIXME
  }
  
  // twiddle file_data_version?
  if ((before & CEPH_CAP_WRBUFFER) == 0 &&
      (now & CEPH_CAP_WRBUFFER)) {
    in->inode.file_data_version++;
    dout(7) << " incrementing file_data_version, now " << in->inode.file_data_version << " for " << *in << dendl;
  }

  return cap;
}



bool Locker::issue_caps(CInode *in)
{
  // allowed caps are determined by the lock mode.
  int allowed = in->filelock.caps_allowed();
  dout(7) << "issue_caps filelock allows=" << cap_string(allowed) 
          << " on " << *in << dendl;

  // count conflicts with
  int nissued = 0;        

  // client caps
  for (map<int, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       it++) {
    Capability *cap = it->second;
    if (cap->pending() != (cap->wanted() & allowed)) {
      // issue
      nissued++;

      int before = cap->pending();
      long seq = cap->issue(cap->wanted() & allowed);
      int after = cap->pending();

      // twiddle file_data_version?
      if (!(before & CEPH_CAP_WRBUFFER) &&
          (after & CEPH_CAP_WRBUFFER)) {
        dout(7) << "   incrementing file_data_version for " << *in << dendl;
        in->inode.file_data_version++;
      }

      if (seq > 0 && 
          !cap->is_suppress()) {
        dout(7) << "   sending MClientFileCaps to client" << it->first << " seq " << cap->get_last_seq()
		<< " new pending " << cap_string(cap->pending()) << " was " << cap_string(before) 
		<< dendl;
        mds->send_message_client(new MClientFileCaps(CEPH_CAP_OP_GRANT,
						     in->inode,
						     cap->get_last_seq(),
						     cap->pending(),
						     cap->wanted()),
				 it->first);
      }
    }
  }

  return (nissued == 0);  // true if no re-issued, no callbacks
}

void Locker::revoke_stale_caps(Session *session)
{
  dout(10) << "revoke_stale_caps for " << session->inst.name << dendl;
  
  for (xlist<Capability*>::iterator p = session->caps.begin(); !p.end(); ++p) {
    Capability *cap = *p;
    CInode *in = cap->get_inode();
    int issued = cap->issued();
    if (issued) {
      dout(10) << " revoking " << cap_string(issued) << " on " << *in << dendl;      
      cap->revoke();
      file_eval_gather(&in->filelock);
      if (in->is_auth()) {
	if (in->filelock.is_stable())
	  file_eval(&in->filelock);
      } else {
	request_inode_file_caps(in);
      }
    } else {
      dout(10) << " nothing issued on " << *in << dendl;
    }
    cap->set_stale(true);
  }
}

void Locker::resume_stale_caps(Session *session)
{
  dout(10) << "resume_stale_caps for " << session->inst.name << dendl;

  for (xlist<Capability*>::iterator p = session->caps.begin(); !p.end(); ++p) {
    Capability *cap = *p;
    CInode *in = cap->get_inode();
    if (cap->is_stale()) {
      dout(10) << " clearing stale flag on " << *in << dendl;
      cap->set_stale(false);
      if (in->is_auth() && in->filelock.is_stable())
	file_eval(&in->filelock);
      issue_caps(in);
    }
  }
}

class C_MDL_RequestInodeFileCaps : public Context {
  Locker *locker;
  CInode *in;
public:
  C_MDL_RequestInodeFileCaps(Locker *l, CInode *i) : locker(l), in(i) {}
  void finish(int r) {
    in->put(CInode::PIN_PTRWAITER);
    if (!in->is_auth())
      locker->request_inode_file_caps(in);
  }
};

void Locker::request_inode_file_caps(CInode *in)
{
  assert(!in->is_auth());

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
                 << dendl;
      }
      else if (in->replica_caps_wanted_keep_until.sec() == 0) {
        in->replica_caps_wanted_keep_until = g_clock.recent_now();
        in->replica_caps_wanted_keep_until.sec_ref() += 2;
        
        dout(7) << "request_inode_file_caps " << cap_string(wanted)
                 << " was " << cap_string(in->replica_caps_wanted) 
                 << " keeping until " << in->replica_caps_wanted_keep_until
                 << " on " << *in 
                 << dendl;
        return;
      } else {
        // wait longer
        return;
      }
    } else {
      in->replica_caps_wanted_keep_until.sec_ref() = 0;
    }
    assert(!in->is_auth());

    // wait for single auth
    if (in->is_ambiguous_auth()) {
      in->get(CInode::PIN_PTRWAITER);
      in->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
		     new C_MDL_RequestInodeFileCaps(this, in));
      return;
    }

    int auth = in->authority().first;
    dout(7) << "request_inode_file_caps " << cap_string(wanted)
            << " was " << cap_string(in->replica_caps_wanted) 
            << " on " << *in << " to mds" << auth << dendl;
    assert(!in->is_auth());

    in->replica_caps_wanted = wanted;

    if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN)
      mds->send_message_mds(new MInodeFileCaps(in->ino(), mds->get_nodeid(),
					       in->replica_caps_wanted),
			    auth);
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
    dout(7) << "handle_inode_file_caps still rejoining " << *in << ", dropping " << *m << dendl;
    delete m;
    return;
  }

  
  dout(7) << "handle_inode_file_caps replica mds" << m->get_from() << " wants caps " << cap_string(m->get_caps()) << " on " << *in << dendl;

  if (m->get_caps())
    in->mds_caps_wanted[m->get_from()] = m->get_caps();
  else
    in->mds_caps_wanted.erase(m->get_from());

  if (in->filelock.is_stable())
    try_file_eval(&in->filelock);  // ** may or may not be auth_pinned **
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
      dout(7) << "handle_client_file_caps on unknown ino " << m->get_ino() << ", dropping" << dendl;
    } else {
      dout(7) << "handle_client_file_caps no cap for client" << client << " on " << *in << dendl;
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
          << dendl;  
  
  // confirm caps
  int had = cap->confirm_receipt(m->get_seq(), m->get_caps());
  int has = cap->confirmed();

  // update wanted
  if (cap->wanted() != wanted) {
    if (m->get_seq() < cap->get_last_open()) {
      /* this is awkward.
	 client may be trying to release caps (i.e. inode closed, etc.) by setting reducing wanted
	 set.
	 but it may also be opening the same filename, not sure that it'll map to the same inode.
	 so, we don't want wanted reductions to clobber mds's notion of wanted unless we're
	 sure the client has seen all the latest caps.
      */
      dout(10) << "handle_client_file_caps ignoring wanted " << cap_string(m->get_wanted())
		<< " bc seq " << m->get_seq() << " < last open " << cap->get_last_open() << dendl;
    } else if (wanted == 0) {
      // outright release?
      dout(7) << " cap for client" << client << " is now null, removing from " << *in << dendl;
      in->remove_client_cap(client);
      if (!in->is_any_caps()) 
	in->xlist_open_file.remove_myself();  // unpin logsegment
      if (!in->is_auth())
	request_inode_file_caps(in);
    } else {
      cap->set_wanted(wanted);
    }
  }

  // merge in atime?
  if (m->get_atime() > in->inode.atime) {
      dout(7) << "  taking atime " << m->get_atime() << " > " 
              << in->inode.atime << " for " << *in << dendl;
    in->inode.atime = m->get_atime();
  }
  
  if ((has|had) & CEPH_CAP_WR) {
    bool dirty = false;

    // mtime
    if (m->get_mtime() > in->inode.mtime) {
      dout(7) << "  taking mtime " << m->get_mtime() << " > " 
              << in->inode.mtime << " for " << *in << dendl;
      in->inode.mtime = m->get_mtime();
      dirty = true;
    }
    // size
    if ((loff_t)m->get_size() > in->inode.size) {
      dout(7) << "  taking size " << m->get_size() << " > " 
              << in->inode.size << " for " << *in << dendl;
      in->inode.size = m->get_size();
      dirty = true;
    }

    if (dirty) 
      mds->mdlog->submit_entry(new EString("cap inode update dirty fixme"));
  }  

  // reevaluate, waiters
  if (!in->filelock.is_stable())
    file_eval_gather(&in->filelock);
  else if (in->is_auth())
    file_eval(&in->filelock);
  
  //in->finish_waiting(CInode::WAIT_CAPS, 0);  // note: any users for this? 

  delete m;
}










// locks ----------------------------------------------------------------

SimpleLock *Locker::get_lock(int lock_type, MDSCacheObjectInfo &info) 
{
  switch (lock_type) {
  case LOCK_OTYPE_DN:
    {
      // be careful; info.dirfrag may have incorrect frag; recalculate based on dname.
      CInode *diri = mdcache->get_inode(info.dirfrag.ino);
      frag_t fg;
      CDir *dir = 0;
      CDentry *dn = 0;
      if (diri) {
	fg = diri->pick_dirfrag(info.dname);
	dir = diri->get_dirfrag(fg);
	if (dir) 
	  dn = dir->lookup(info.dname);
      }
      if (!dn) {
	dout(7) << "get_lock don't have dn " << info.dirfrag.ino << " " << info.dname << dendl;
	return 0;
      }
      return &dn->lock;
    }

  case LOCK_OTYPE_IAUTH:
  case LOCK_OTYPE_ILINK:
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IFILE:
  case LOCK_OTYPE_IDIR:
    {
      CInode *in = mdcache->get_inode(info.ino);
      if (!in) {
	dout(7) << "get_lock don't have ino " << info.ino << dendl;
	return 0;
      }
      switch (lock_type) {
      case LOCK_OTYPE_IAUTH: return &in->authlock;
      case LOCK_OTYPE_ILINK: return &in->linklock;
      case LOCK_OTYPE_IDIRFRAGTREE: return &in->dirfragtreelock;
      case LOCK_OTYPE_IFILE: return &in->filelock;
      case LOCK_OTYPE_IDIR: return &in->dirlock;
      }
    }

  default:
    dout(7) << "get_lock don't know lock_type " << lock_type << dendl;
    assert(0);
    break;
  }

  return 0;  
}


void Locker::handle_lock(MLock *m)
{
  // nobody should be talking to us during recovery.
  assert(mds->is_rejoin() || mds->is_active() || mds->is_stopping());

  SimpleLock *lock = get_lock(m->get_lock_type(), m->get_object_info());
  if (!lock) {
    dout(10) << "don't have object " << m->get_object_info() << ", must have trimmed, dropping" << dendl;
    delete m;
    return;
  }

  switch (lock->get_type()) {
  case LOCK_OTYPE_DN:
  case LOCK_OTYPE_IAUTH:
  case LOCK_OTYPE_ILINK:
    handle_simple_lock(lock, m);
    break;
    
  case LOCK_OTYPE_IFILE:
    handle_file_lock((FileLock*)lock, m);
    break;
    
  case LOCK_OTYPE_IDIRFRAGTREE:
  case LOCK_OTYPE_IDIR:
    handle_scatter_lock((ScatterLock*)lock, m);
    break;

  default:
    dout(7) << "handle_lock got otype " << m->get_lock_type() << dendl;
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
	      << ", dropping " << *m << dendl;
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

    // special case: trim replica no-longer-null dentry?
    if (lock->get_type() == LOCK_OTYPE_DN) {
      CDentry *dn = (CDentry*)lock->get_parent();
      if (dn->is_null() && m->get_data().length() > 0) {
	dout(10) << "handle_simple_lock replica dentry null -> non-null, must trim " 
		 << *dn << dendl;
	assert(dn->get_num_ref() == 0);
	map<int, MCacheExpire*> expiremap;
	mdcache->trim_dentry(dn, expiremap);
	mdcache->send_expire_messages(expiremap);
      }
    }
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC);
    //||           lock->get_state() == LOCK_GLOCKR);
    
    // wait for readers to finish?
    if (lock->is_rdlocked()) {
      dout(7) << "handle_simple_lock has reader, waiting before ack on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      lock->set_state(LOCK_GLOCKR);
    } else {
      // update lock and reply
      lock->set_state(LOCK_LOCK);
      mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid()), from);
    }
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->get_state() == LOCK_GLOCKR);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->is_gathering()) {
      dout(7) << "handle_simple_lock " << *lock << " on " << *lock->get_parent() << " from " << from
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_simple_lock " << *lock << " on " << *lock->get_parent() << " from " << from
	      << ", last one" << dendl;
      simple_eval_gather(lock);
    }
    break;

  }

  delete m;
}

/* unused, currently.

class C_Locker_SimpleEval : public Context {
  Locker *locker;
  SimpleLock *lock;
public:
  C_Locker_SimpleEval(Locker *l, SimpleLock *lk) : locker(l), lock(lk) {}
  void finish(int r) {
    locker->try_simple_eval(lock);
  }
};

void Locker::try_simple_eval(SimpleLock *lock)
{
  // unstable and ambiguous auth?
  if (!lock->is_stable() &&
      lock->get_parent()->is_ambiguous_auth()) {
    dout(7) << "simple_eval not stable and ambiguous auth, waiting on " << *lock->get_parent() << dendl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_Locker_SimpleEval(this, lock));
    return;
  }

  if (!lock->get_parent()->is_auth()) {
    dout(7) << "try_simple_eval not auth for " << *lock->get_parent() << dendl;
    return;
  }

  if (!lock->get_parent()->can_auth_pin()) {
    dout(7) << "try_simple_eval can't auth_pin, waiting on " << *lock->get_parent() << dendl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    lock->get_parent()->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_Locker_SimpleEval(this, lock));
    return;
  }

  if (lock->is_stable())
    simple_eval(lock);
}
*/

void Locker::simple_eval_gather(SimpleLock *lock)
{
  dout(10) << "simple_eval_gather " << *lock << " on " << *lock->get_parent() << dendl;

  // finished gathering?
  if (lock->get_state() == LOCK_GLOCKR &&
      !lock->is_gathering() &&
      !lock->is_rdlocked()) {
    dout(7) << "simple_eval finished gather on " << *lock << " on " << *lock->get_parent() << dendl;

    // replica: tell auth
    if (!lock->get_parent()->is_auth()) {
      int auth = lock->get_parent()->authority().first;
      if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) 
	mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid()), 
			      lock->get_parent()->authority().first);
    }
    
    lock->set_state(LOCK_LOCK);
    lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR);

    if (lock->get_parent()->is_auth()) {
      lock->get_parent()->auth_unpin();

      // re-eval?
      simple_eval(lock);
    }
  }
}

void Locker::simple_eval(SimpleLock *lock)
{
  dout(10) << "simple_eval " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  if (lock->get_parent()->is_frozen()) return;

  // stable -> sync?
  if (!lock->is_xlocked() &&
      lock->get_state() != LOCK_SYNC &&
      !lock->is_waiter_for(SimpleLock::WAIT_WR)) {
    dout(7) << "simple_eval stable, syncing " << *lock 
	    << " on " << *lock->get_parent() << dendl;
    simple_sync(lock);
  }
  
}


// mid

void Locker::simple_sync(SimpleLock *lock)
{
  dout(7) << "simple_sync on " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());
  
  // check state
  if (lock->get_state() == LOCK_SYNC)
    return; // already sync
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
  dout(7) << "simple_lock on " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());
  
  // check state
  if (lock->get_state() == LOCK_LOCK) return;
  assert(lock->get_state() == LOCK_SYNC);
  
  if (lock->get_parent()->is_replicated()) {
    // bcast to replicas
    send_lock_message(lock, LOCK_AC_LOCK);
    
    // change lock
    lock->set_state(LOCK_GLOCKR);
    lock->init_gather();
    lock->get_parent()->auth_pin();
  } else {
    lock->set_state(LOCK_LOCK);
  }
}


// top

bool Locker::simple_rdlock_try(SimpleLock *lock, Context *con)
{
  dout(7) << "simple_rdlock_try on " << *lock << " on " << *lock->get_parent() << dendl;  

  // can read?  grab ref.
  if (lock->can_rdlock(0)) 
    return true;
  
  assert(!lock->get_parent()->is_auth());

  // wait!
  dout(7) << "simple_rdlock_try waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  if (con) lock->add_waiter(SimpleLock::WAIT_RD, con);
  return false;
}

bool Locker::simple_rdlock_start(SimpleLock *lock, MDRequest *mdr)
{
  dout(7) << "simple_rdlock_start  on " << *lock << " on " << *lock->get_parent() << dendl;  

  // can read?  grab ref.
  if (lock->can_rdlock(mdr)) {
    lock->get_rdlock();
    mdr->rdlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  }
  
  // wait!
  dout(7) << "simple_rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
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

  dout(7) << "simple_rdlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;
  
  // last one?
  if (!lock->is_rdlocked())
    simple_eval_gather(lock);
}

bool Locker::simple_xlock_start(SimpleLock *lock, MDRequest *mdr)
{
  dout(7) << "simple_xlock_start  on " << *lock << " on " << *lock->get_parent() << dendl;

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
    // this had better not be a remote xlock attempt!
    assert(!mdr->slave_request);

    // wait for single auth
    if (lock->get_parent()->is_ambiguous_auth()) {
      lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
				     new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }

    // send lock request
    int auth = lock->get_parent()->authority().first;
    mdr->more()->slaves.insert(auth);
    MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_XLOCK);
    r->set_lock_type(lock->get_type());
    lock->get_parent()->set_object_info(r->get_object_info());
    mds->send_message_mds(r, auth);
    
    // wait
    lock->add_waiter(SimpleLock::WAIT_REMOTEXLOCK, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
}


void Locker::simple_xlock_finish(SimpleLock *lock, MDRequest *mdr)
{
  dout(7) << "simple_xlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;

  // drop ref
  assert(lock->can_xlock(mdr));
  lock->put_xlock();
  assert(mdr);
  mdr->xlocks.erase(lock);
  mdr->locks.erase(lock);

  // remote xlock?
  if (!lock->get_parent()->is_auth()) {
    // tell auth
    dout(7) << "simple_xlock_finish releasing remote xlock on " << *lock->get_parent()  << dendl;
    int auth = lock->get_parent()->authority().first;
    if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
      MMDSSlaveRequest *slavereq = new MMDSSlaveRequest(mdr->reqid, MMDSSlaveRequest::OP_UNXLOCK);
      slavereq->set_lock_type(lock->get_type());
      lock->get_parent()->set_object_info(slavereq->get_object_info());
      mds->send_message_mds(slavereq, auth);
    }
  }

  // others waiting?
  lock->finish_waiters(SimpleLock::WAIT_WR, 0); 

  // eval?
  if (lock->get_parent()->is_auth())
    simple_eval(lock);
}



// dentry specific helpers

/** dentry_can_rdlock_trace
 * see if we can _anonymously_ rdlock an entire trace.  
 * if not, and req is specified, wait and retry that message.
 */
bool Locker::dentry_can_rdlock_trace(vector<CDentry*>& trace) 
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
      dout(10) << "can_rdlock_trace can't rdlock " << *dn << dendl;
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
       it++) {
    dout(10) << "dentry_anon_rdlock_trace_start rdlocking " << (*it)->lock << " " << **it << dendl;
    (*it)->lock.get_rdlock();
  }
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
	  << " on " << *lock->get_parent() << dendl;  

  // read on stable scattered replica?  
  if (lock->get_state() == LOCK_SCATTER &&
      !lock->get_parent()->is_auth()) {
    dout(7) << "scatter_rdlock_start  scatterlock read on a stable scattered replica, fw to auth" << dendl;
    mdcache->request_forward(mdr, lock->get_parent()->authority().first);
    return false;
  }

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
  lock->add_waiter(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));

  // initiate sync or tempsync?
  if (lock->is_stable() &&
      lock->get_parent()->is_auth()) {
    if (lock->get_parent()->is_replicated())
      scatter_tempsync(lock);
    else
      scatter_sync(lock);
  }

  return false;
}

void Locker::scatter_rdlock_finish(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_rdlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_rdlock();
  if (mdr) {
    mdr->rdlocks.erase(lock);
    mdr->locks.erase(lock);
  }
  
  scatter_eval_gather(lock);
}


bool Locker::scatter_wrlock_start(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_wrlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  // pre-twiddle?
  if (lock->get_parent()->is_auth() &&
      !lock->get_parent()->is_replicated() &&
      !lock->is_rdlocked() &&
      !lock->is_xlocked() &&
      lock->get_state() == LOCK_SYNC) 
    lock->set_state(LOCK_SCATTER);
  //scatter_scatter(lock);

  // can wrlock?
  if (lock->can_wrlock()) {
    lock->get_wrlock();
    mdr->wrlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  }

  // wait for write.
  lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, 
		   new C_MDS_RetryRequest(mdcache, mdr));
  
  // initiate scatter or lock?
  if (lock->is_stable()) {
    if (lock->get_parent()->is_auth()) {
      // auth.  scatter or lock?
      if (((CInode*)lock->get_parent())->has_subtree_root_dirfrag()) 
	scatter_scatter(lock);
      else
	scatter_lock(lock);
    } else {
      // replica.
      // auth should be auth_pinned (see acquire_locks wrlock weird mustpin case).
      int auth = lock->get_parent()->authority().first;
      dout(10) << "requesting scatter from auth on " 
	       << *lock << " on " << *lock->get_parent() << dendl;
      mds->send_message_mds(new MLock(lock, LOCK_AC_REQSCATTER, mds->get_nodeid()), auth);
    }
  }

  return false;
}

void Locker::scatter_wrlock_finish(ScatterLock *lock, MDRequest *mdr)
{
  dout(7) << "scatter_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_wrlock();
  if (mdr) {
    mdr->wrlocks.erase(lock);
    mdr->locks.erase(lock);
  }
  
  scatter_eval_gather(lock);
}


class C_Locker_ScatterEval : public Context {
  Locker *locker;
  ScatterLock *lock;
public:
  C_Locker_ScatterEval(Locker *l, ScatterLock *lk) : locker(l), lock(lk) {}
  void finish(int r) {
    lock->get_parent()->put(CInode::PIN_PTRWAITER);
    locker->try_scatter_eval(lock);
  }
};


void Locker::try_scatter_eval(ScatterLock *lock)
{
  // unstable and ambiguous auth?
  if (!lock->is_stable() &&
      lock->get_parent()->is_ambiguous_auth()) {
    dout(7) << "try_scatter_eval not stable and ambiguous auth, waiting on " << *lock->get_parent() << dendl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    lock->get_parent()->get(CInode::PIN_PTRWAITER);
    lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_Locker_ScatterEval(this, lock));
    return;
  }

  if (!lock->get_parent()->is_auth()) {
    dout(7) << "try_scatter_eval not auth for " << *lock->get_parent() << dendl;
    return;
  }

  if (!lock->get_parent()->can_auth_pin()) {
    dout(7) << "try_scatter_eval can't auth_pin, waiting on " << *lock->get_parent() << dendl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    lock->get_parent()->get(CInode::PIN_PTRWAITER);
    lock->get_parent()->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_Locker_ScatterEval(this, lock));
    return;
  }

  if (lock->is_stable())
    scatter_eval(lock);
}


void Locker::scatter_eval_gather(ScatterLock *lock)
{
  dout(10) << "scatter_eval_gather " << *lock << " on " << *lock->get_parent() << dendl;

  if (!lock->get_parent()->is_auth()) {
    // REPLICA

    if (lock->get_state() == LOCK_GLOCKC &&
	!lock->is_wrlocked()) {
      dout(10) << "scatter_eval no wrlocks, acking lock" << dendl;
      int auth = lock->get_parent()->authority().first;
      if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
	bufferlist data;
	lock->encode_locked_state(data);
	mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid(), data), auth);
      }
      lock->set_state(LOCK_LOCK);
    }
    
  } else {
    // AUTH

    // glocks|glockt -> lock?
    if ((lock->get_state() == LOCK_GLOCKS || 
	 lock->get_state() == LOCK_GLOCKT) &&
	!lock->is_gathering() &&
	!lock->is_rdlocked()) {
      dout(7) << "scatter_eval finished lock gather/un-rdlock on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      lock->set_state(LOCK_LOCK);
      lock->finish_waiters(ScatterLock::WAIT_XLOCK|ScatterLock::WAIT_STABLE);
      lock->get_parent()->auth_unpin();
    }
    
    // glockc -> lock?
    else if (lock->get_state() == LOCK_GLOCKC &&
	     !lock->is_gathering() &&
	     !lock->is_wrlocked()) {
      if (lock->is_updated()) {
	scatter_writebehind(lock);
      } else {
	dout(7) << "scatter_eval finished lock gather/un-wrlock on " << *lock
	      << " on " << *lock->get_parent() << dendl;
	lock->set_state(LOCK_LOCK);
	lock->finish_waiters(ScatterLock::WAIT_XLOCK|ScatterLock::WAIT_STABLE);
	lock->get_parent()->auth_unpin();
      }
    }

    // gSyncL -> sync?
    else if (lock->get_state() == LOCK_GSYNCL &&
	     !lock->is_wrlocked()) {
      dout(7) << "scatter_eval finished sync un-wrlock on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      if (lock->get_parent()->is_replicated()) {
	// encode and bcast
	bufferlist data;
	lock->encode_locked_state(data);
	send_lock_message(lock, LOCK_AC_SYNC, data);
      }
      lock->set_state(LOCK_SYNC);
      lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
      lock->get_parent()->auth_unpin();
    }

    // gscattert|gscatters -> scatter?
    else if ((lock->get_state() == LOCK_GSCATTERT ||
	      lock->get_state() == LOCK_GSCATTERS) &&
	     !lock->is_gathering() &&
	     !lock->is_rdlocked()) {
      dout(7) << "scatter_eval finished scatter un-rdlock(/gather) on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      if (lock->get_parent()->is_replicated()) {
	// encode and bcast
	bufferlist data;
	lock->encode_locked_state(data);
	send_lock_message(lock, LOCK_AC_SCATTER, data);
      }
      lock->set_state(LOCK_SCATTER);
      lock->finish_waiters(ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);      
      lock->get_parent()->auth_unpin();
    }

    // gTempsyncC|gTempsyncL -> tempsync
    else if ((lock->get_state() == LOCK_GTEMPSYNCC ||
	      lock->get_state() == LOCK_GTEMPSYNCL) &&
	     !lock->is_gathering() &&
	     !lock->is_wrlocked()) {
      if (lock->is_updated()) {
	scatter_writebehind(lock);
      } else {
	dout(7) << "scatter_eval finished tempsync gather/un-wrlock on " << *lock
		<< " on " << *lock->get_parent() << dendl;
	lock->set_state(LOCK_TEMPSYNC);
	lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
	lock->get_parent()->auth_unpin();
      }
    }


    // re-eval?
    if (lock->is_stable()) // && lock->get_parent()->can_auth_pin())
      scatter_eval(lock);
  }
}

void Locker::scatter_writebehind(ScatterLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  dout(10) << "scatter_writebehind " << in->inode.mtime << " on " << *lock << " on " << *in << dendl;

  // hack:
  if (in->is_base()) {
    dout(10) << "scatter_writebehind just clearing updated flag for base inode " << *in << dendl;
    lock->clear_updated();
    scatter_eval_gather(lock);
    return;
  }

  // journal write-behind.
  inode_t *pi = in->project_inode();
  pi->mtime = in->inode.mtime;   // make sure an intermediate version isn't goofing us up
  pi->version = in->pre_dirty();
  
  EUpdate *le = new EUpdate(mds->mdlog, "scatter writebehind");
  le->metablob.add_dir_context(in->get_parent_dn()->get_dir());
  le->metablob.add_primary_dentry(in->get_parent_dn(), true, 0, pi);
  
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_sync(new C_Locker_ScatterWB(this, lock, mds->mdlog->get_current_segment()));
}

void Locker::scatter_writebehind_finish(ScatterLock *lock, LogSegment *ls)
{
  CInode *in = (CInode*)lock->get_parent();
  dout(10) << "scatter_writebehind_finish on " << *lock << " on " << *in << dendl;
  in->pop_and_dirty_projected_inode(ls);
  lock->clear_updated();
  scatter_eval_gather(lock);
}

void Locker::scatter_eval(ScatterLock *lock)
{
  dout(10) << "scatter_eval " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  if (lock->get_parent()->is_frozen()) return;

  CInode *in = (CInode*)lock->get_parent();
  if (in->has_subtree_root_dirfrag() && !in->is_base()) {
    // i _should_ be scattered.
    if (!lock->is_rdlocked() &&
	!lock->is_xlocked() &&
	lock->get_state() != LOCK_SCATTER) {
      dout(10) << "scatter_eval no rdlocks|xlocks, am subtree root inode, scattering" << dendl;
      scatter_scatter(lock);
      autoscattered.push_back(&lock->xlistitem_autoscattered);
    }
  } else {
    // i _should_ be sync.
    lock->xlistitem_autoscattered.remove_myself(); 
    if (!lock->is_wrlocked() &&
	!lock->is_xlocked() &&
	lock->get_state() != LOCK_SYNC) {
      dout(10) << "scatter_eval no wrlocks|xlocks, not subtree root inode, syncing" << dendl;
      scatter_sync(lock);
    }
  }
}

void Locker::note_autoscattered(ScatterLock *lock)
{
  dout(10) << "note_autoscattered " << *lock << " on " << *lock->get_parent() << dendl;
  autoscattered.push_back(&lock->xlistitem_autoscattered);
}


/*
 * this is called by LogSegment::try_to_trim() when trying to 
 * flush dirty scattered data (e.g. inode->dirlock mtime) back
 * to the auth node.
 */
void Locker::scatter_try_unscatter(ScatterLock *lock, Context *c)
{
  dout(10) << "scatter_try_unscatter " << *lock << " on " << *lock->get_parent() << dendl;
  assert(!lock->get_parent()->is_auth());
  assert(!lock->get_parent()->is_ambiguous_auth());

  // request unscatter?
  int auth = lock->get_parent()->authority().first;
  if (lock->get_state() == LOCK_SCATTER &&
      mds->mdsmap->get_state(auth) >= MDSMap::STATE_ACTIVE) 
    mds->send_message_mds(new MLock(lock, LOCK_AC_REQUNSCATTER, mds->get_nodeid()), auth);
  
  // wait...
  lock->add_waiter(SimpleLock::WAIT_STABLE, c);
}


void Locker::scatter_sync(ScatterLock *lock)
{
  dout(10) << "scatter_sync " << *lock
	   << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  switch (lock->get_state()) {
  case LOCK_SYNC:
    return;    // already sync.

  case LOCK_TEMPSYNC:
    break;    // just do it.

  case LOCK_LOCK:
    if (lock->is_wrlocked() || lock->is_xlocked()) {
      lock->set_state(LOCK_GSYNCL);
      lock->get_parent()->auth_pin();
      return;
    }
    break; // do it.

  case LOCK_SCATTER:
    // lock first.  this is the slow way, incidentally.
    if (lock->get_parent()->is_replicated()) {
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
    } else {
      if (!lock->is_wrlocked()) {
	break; // do it now, we're fine
      }
    }
    lock->set_state(LOCK_GLOCKC);
    lock->get_parent()->auth_pin();
    return;

  default:
    assert(0);
  }
  
  // do sync
  if (lock->get_parent()->is_replicated()) {
    // encode and bcast
    bufferlist data;
    lock->encode_locked_state(data);
    send_lock_message(lock, LOCK_AC_SYNC, data);
  }

  lock->set_state(LOCK_SYNC);
  lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
}

void Locker::scatter_scatter(ScatterLock *lock)
{
  dout(10) << "scatter_scatter " << *lock
	   << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());
  
  lock->set_last_scatter(g_clock.now());

  switch (lock->get_state()) {
  case LOCK_SYNC:
    if (!lock->is_rdlocked() &&
	!lock->get_parent()->is_replicated())
      break; // do it
    if (lock->get_parent()->is_replicated()) {
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
    }
    lock->set_state(LOCK_GSCATTERS);
    lock->get_parent()->auth_pin();
    return;

  case LOCK_LOCK:
    if (lock->is_xlocked())
      return;  // sorry
    break; // do it.

  case LOCK_SCATTER:
    return; // did it.

  case LOCK_TEMPSYNC:
    if (lock->is_rdlocked()) {
      lock->set_state(LOCK_GSCATTERT);
      lock->get_parent()->auth_pin();
      return;
    }
    break; // do it

  default: 
    assert(0);
  }

  // do scatter
  if (lock->get_parent()->is_replicated()) {
    // encode and bcast
    bufferlist data;
    lock->encode_locked_state(data);
    send_lock_message(lock, LOCK_AC_SCATTER, data);
  } 
  lock->set_state(LOCK_SCATTER);
  lock->finish_waiters(ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
}

void Locker::scatter_lock(ScatterLock *lock)
{
  dout(10) << "scatter_lock " << *lock
	   << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  switch (lock->get_state()) {
  case LOCK_SYNC:
    if (!lock->is_rdlocked() &&
	!lock->get_parent()->is_replicated())
      break; // do it.

    if (lock->get_parent()->is_replicated()) {
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
    } 
    lock->set_state(LOCK_GLOCKS);
    lock->get_parent()->auth_pin();
    return;

  case LOCK_LOCK:
    return; // done.

  case LOCK_SCATTER:
    if (!lock->is_wrlocked() &&
	!lock->get_parent()->is_replicated()) {
      break; // do it.
    }

    if (lock->get_parent()->is_replicated()) {
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
    }
    lock->set_state(LOCK_GLOCKC);
    lock->get_parent()->auth_pin();
    return;

  case LOCK_TEMPSYNC:
    if (lock->is_rdlocked()) {
      lock->set_state(LOCK_GLOCKT);
      lock->get_parent()->auth_pin();
      return;
    }
    break; // do it.
  }

  // do lock
  lock->set_state(LOCK_LOCK);
  lock->finish_waiters(ScatterLock::WAIT_XLOCK|ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
}

void Locker::scatter_tempsync(ScatterLock *lock)
{
  dout(10) << "scatter_tempsync " << *lock
	   << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  switch (lock->get_state()) {
  case LOCK_SYNC:
    break;  // do it.

  case LOCK_LOCK:
    if (lock->is_wrlocked() ||
	lock->is_xlocked()) {
      lock->set_state(LOCK_GTEMPSYNCL);
      lock->get_parent()->auth_pin();
      return;
    }
    break; // do it.

  case LOCK_SCATTER:
    if (!lock->is_wrlocked() &&
	!lock->get_parent()->is_replicated()) {
      break; // do it.
    }
    
    if (lock->get_parent()->is_replicated()) {
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
    }
    lock->set_state(LOCK_GTEMPSYNCC);
    lock->get_parent()->auth_pin();
    return;

  case LOCK_TEMPSYNC:
    return; // done
  }
  
  // do tempsync
  lock->set_state(LOCK_TEMPSYNC);
  lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
}




void Locker::handle_scatter_lock(ScatterLock *lock, MLock *m)
{
  int from = m->get_asker();
  dout(10) << "handle_scatter_lock " << *m << " on " << *lock << " on " << *lock->get_parent() << dendl;
  
  if (mds->is_rejoin()) {
    if (lock->get_parent()->is_rejoining()) {
      dout(7) << "handle_scatter_lock still rejoining " << *lock->get_parent()
	      << ", dropping " << *m << dendl;
      delete m;
      return;
    }
  }

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK);
    lock->set_state(LOCK_SYNC);
    lock->decode_locked_state(m->get_data());
    lock->clear_updated();
    lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
    break;

  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SCATTER ||
	   lock->get_state() == LOCK_SYNC);

    // wait for wrlocks to close?
    if (lock->is_wrlocked()) {
      assert(lock->get_state() == LOCK_SCATTER);
      dout(7) << "handle_scatter_lock has wrlocks, waiting on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      lock->set_state(LOCK_GLOCKC);
    } else if (lock->is_rdlocked()) {
      assert(lock->get_state() == LOCK_SYNC);
      dout(7) << "handle_scatter_lock has rdlocks, waiting on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      lock->set_state(LOCK_GLOCKS);
    } else {
      dout(7) << "handle_scatter_lock has no rd|wrlocks, sending lockack for " << *lock
	      << " on " << *lock->get_parent() << dendl;
      
      // encode and reply
      bufferlist data;
      lock->encode_locked_state(data);
      mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid(), data), from);
      lock->set_state(LOCK_LOCK);
    }
    break;

  case LOCK_AC_SCATTER:
    assert(lock->get_state() == LOCK_LOCK);
    lock->decode_locked_state(m->get_data());
    lock->clear_updated();
    lock->set_state(LOCK_SCATTER);
    lock->finish_waiters(ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
    break;

    // -- for auth --
  case LOCK_AC_LOCKACK:
    assert(lock->get_state() == LOCK_GLOCKS ||
	   lock->get_state() == LOCK_GLOCKC ||
	   lock->get_state() == LOCK_GSCATTERS ||
	   lock->get_state() == LOCK_GTEMPSYNCC);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    lock->decode_locked_state(m->get_data());
    
    if (lock->is_gathering()) {
      dout(7) << "handle_scatter_lock " << *lock << " on " << *lock->get_parent()
	      << " from " << from << ", still gathering " << lock->get_gather_set()
	      << dendl;
    } else {
      dout(7) << "handle_scatter_lock " << *lock << " on " << *lock->get_parent()
	      << " from " << from << ", last one" 
	      << dendl;
      scatter_eval_gather(lock);
    }
    break;

  case LOCK_AC_REQSCATTER:
    if (lock->is_stable()) {
      /* NOTE: we can do this _even_ if !can_auth_pin (i.e. freezing)
       *  because the replica should be holding an auth_pin if they're
       *  doing this (and thus, we are freezing, not frozen, and indefinite
       *  starvation isn't an issue).
       */
      dout(7) << "handle_scatter_lock got scatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      scatter_scatter(lock);
    } else {
      dout(7) << "handle_scatter_lock ignoring scatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
    }
    break;

  case LOCK_AC_REQUNSCATTER:
    if (!lock->is_stable()) {
      dout(7) << "handle_scatter_lock ignoring now-unnecessary unscatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
    } else if (lock->get_parent()->can_auth_pin()) {
      dout(7) << "handle_scatter_lock got unscatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      scatter_lock(lock);
    } else {
      dout(7) << "handle_scatter_lock DROPPING unscatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      /* FIXME: if we can't auth_pin here, this request is effectively lost... */
    }
  }

  delete m;
}



void Locker::scatter_unscatter_autoscattered()
{
  /* 
   * periodically unscatter autoscattered locks
   */

  dout(10) << "scatter_unscatter_autoscattered" << dendl;
  
  utime_t now = g_clock.now();
  int n = autoscattered.size();
  while (!autoscattered.empty()) {
    ScatterLock *lock = autoscattered.front();
    
    // stop?
    if (lock->get_state() == LOCK_SCATTER &&
	now - lock->get_last_scatter() < 10.0) 
      break;
    
    autoscattered.pop_front();

    if (lock->get_state() == LOCK_SCATTER &&
	lock->get_parent()->is_replicated()) {
      if (((CInode*)lock->get_parent())->is_frozen() ||
	  ((CInode*)lock->get_parent())->is_freezing()) {
	// hrm.. requeue.
	dout(10) << "last_scatter " << lock->get_last_scatter() 
		 << ", now " << now << ", but frozen|freezing, requeueing" << dendl;
	autoscattered.push_back(&lock->xlistitem_autoscattered);	
      } else {
	dout(10) << "last_scatter " << lock->get_last_scatter() 
		 << ", now " << now << ", locking" << dendl;
	scatter_lock(lock);
      }
    }
    if (--n == 0) break;
  }
}



// ==========================================================================
// local lock


bool Locker::local_wrlock_start(LocalLock *lock, MDRequest *mdr)
{
  dout(7) << "local_wrlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  if (lock->can_wrlock()) {
    lock->get_wrlock();
    mdr->wrlocks.insert(lock);
    mdr->locks.insert(lock);
    return true;
  } else {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
}

void Locker::local_wrlock_finish(LocalLock *lock, MDRequest *mdr)
{
  dout(7) << "local_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_wrlock();
  mdr->wrlocks.erase(lock);
  mdr->locks.erase(lock);
}

bool Locker::local_xlock_start(LocalLock *lock, MDRequest *mdr)
{
  dout(7) << "local_xlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  if (lock->is_xlocked_by_other(mdr)) {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }

  lock->get_xlock(mdr);
  mdr->xlocks.insert(lock);
  mdr->locks.insert(lock);
  return true;
}

void Locker::local_xlock_finish(LocalLock *lock, MDRequest *mdr)
{
  dout(7) << "local_xlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_xlock();
  mdr->xlocks.erase(lock);
  mdr->locks.erase(lock);

  lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR);
}



// ==========================================================================
// file lock


bool Locker::file_rdlock_start(FileLock *lock, MDRequest *mdr)
{
  dout(7) << "file_rdlock_start " << *lock << " on " << *lock->get_parent() << dendl;

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
    dout(7) << "file_rdlock_start can_rdlock_soon " << *lock << " on " << *lock->get_parent() << dendl;
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
        dout(7) << "file_rdlock_start waiting until stable on " << *lock << " on " << *lock->get_parent() << dendl;
        lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
        return false;
      }
    } else {
      // replica
      if (lock->is_stable()) {
	
        // fw to auth
	CInode *in = (CInode*)lock->get_parent();
        int auth = in->authority().first;
        dout(7) << "file_rdlock_start " << *lock << " on " << *lock->get_parent() << " on replica and async, fw to auth " << auth << dendl;
        assert(auth != mds->get_nodeid());
        mdcache->request_forward(mdr, auth);
        return false;
        
      } else {
        // wait until stable
        dout(7) << "inode_file_rdlock_start waiting until stable on " << *lock << " on " << *lock->get_parent() << dendl;
        lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mdr));
        return false;
      }
    }
  }
  
  // wait
  dout(7) << "file_rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
        
  return false;
}



void Locker::file_rdlock_finish(FileLock *lock, MDRequest *mdr)
{
  dout(7) << "rdlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;

  // drop ref
  lock->put_rdlock();
  mdr->rdlocks.erase(lock);
  mdr->locks.erase(lock);

  if (!lock->is_rdlocked())
    file_eval_gather(lock);
}


bool Locker::file_xlock_start(FileLock *lock, MDRequest *mdr)
{
  dout(7) << "file_xlock_start on " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->get_parent()->is_auth());  // remote file xlock not implemented

  // already xlocked by me?
  if (lock->get_xlocked_by() == mdr)
    return true;

  // can't write?
  if (!lock->can_xlock(mdr)) {
    
    // auth
    if (!lock->can_xlock_soon()) {
      if (!lock->is_stable()) {
	dout(7) << "file_xlock_start on auth, waiting for stable on " << *lock << " on " << *lock->get_parent() << dendl;
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
    dout(7) << "file_xlock_start on auth, waiting for write on " << *lock << " on " << *lock->get_parent() << dendl;
    lock->add_waiter(SimpleLock::WAIT_WR, new C_MDS_RetryRequest(mdcache, mdr));
    return false;
  }
}


void Locker::file_xlock_finish(FileLock *lock, MDRequest *mdr)
{
  dout(7) << "file_xlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;

  // drop ref
  assert(lock->can_xlock(mdr));
  lock->put_xlock();
  mdr->locks.erase(lock);
  mdr->xlocks.erase(lock);

  assert(lock->get_parent()->is_auth());  // or implement remote xlocks

  // others waiting?
  lock->finish_waiters(SimpleLock::WAIT_WR, 0); 

  if (lock->get_parent()->is_auth())
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
    lock->get_parent()->put(CInode::PIN_PTRWAITER);
    locker->try_file_eval(lock);
  }
};

void Locker::try_file_eval(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();

  // unstable and ambiguous auth?
  if (!lock->is_stable() &&
      in->is_ambiguous_auth()) {
    dout(7) << "try_file_eval not stable and ambiguous auth, waiting on " << *in << dendl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    in->get(CInode::PIN_PTRWAITER);
    in->add_waiter(CInode::WAIT_SINGLEAUTH, new C_Locker_FileEval(this, lock));
    return;
  }

  if (!lock->get_parent()->is_auth()) {
    dout(7) << "try_file_eval not auth for " << *lock->get_parent() << dendl;
    return;
  }

  if (!lock->get_parent()->can_auth_pin()) {
    dout(7) << "try_file_eval can't auth_pin, waiting on " << *in << dendl;
    //if (!lock->get_parent()->is_waiter(MDSCacheObject::WAIT_SINGLEAUTH))
    in->get(CInode::PIN_PTRWAITER);
    in->add_waiter(CInode::WAIT_UNFREEZE, new C_Locker_FileEval(this, lock));
    return;
  }

  if (lock->is_stable())
    file_eval(lock);
}



void Locker::file_eval_gather(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  int issued = in->get_caps_issued();
 
  dout(7) << "file_eval_gather issued " << cap_string(issued)
	  << " vs " << cap_string(lock->caps_allowed())
	  << " on " << *lock << " on " << *lock->get_parent()
	  << dendl;

  if (lock->is_stable())
    return;  // nothing for us to do here!
  
  // [auth] finished gather?
  if (in->is_auth() &&
      !lock->is_gathering() &&
      ((issued & ~lock->caps_allowed()) == 0)) {
    dout(7) << "file_eval_gather finished gather" << dendl;
    
    switch (lock->get_state()) {
      // to lock
    case LOCK_GLOCKR:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
      lock->set_state(LOCK_LOCK);
      
      // waiters
      lock->get_rdlock();
      lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR|SimpleLock::WAIT_RD);
      lock->put_rdlock();
      lock->get_parent()->auth_unpin();
      break;
      
      // to mixed
    case LOCK_GMIXEDR:
      lock->set_state(LOCK_MIXED);
      lock->finish_waiters(SimpleLock::WAIT_STABLE);
      lock->get_parent()->auth_unpin();
      break;

    case LOCK_GMIXEDL:
      lock->set_state(LOCK_MIXED);
      
      if (in->is_replicated()) {
	// data
	bufferlist softdata;
	lock->encode_locked_state(softdata);
        
	// bcast to replicas
	send_lock_message(lock, LOCK_AC_MIXED, softdata);
      }
      
      lock->finish_waiters(SimpleLock::WAIT_STABLE);
      lock->get_parent()->auth_unpin();
      break;

      // to loner
    case LOCK_GLONERR:
      lock->set_state(LOCK_LONER);
      lock->finish_waiters(SimpleLock::WAIT_STABLE);
      lock->get_parent()->auth_unpin();
      break;

    case LOCK_GLONERM:
      lock->set_state(LOCK_LONER);
      lock->finish_waiters(SimpleLock::WAIT_STABLE);
      lock->get_parent()->auth_unpin();
      break;
      
      // to sync
    case LOCK_GSYNCL:
    case LOCK_GSYNCM:
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
      lock->get_parent()->auth_unpin();
      break;
      
    default: 
      assert(0);
    }

    issue_caps(in);

    // stable re-eval?
    if (lock->is_stable())    //&& lock->get_parent()->can_auth_pin())
      file_eval(lock);
  }
  
  // [replica] finished caps gather?
  if (!in->is_auth() &&
      ((issued & ~lock->caps_allowed()) == 0)) {
    switch (lock->get_state()) {
    case LOCK_GMIXEDR:
      { 
	lock->set_state(LOCK_MIXED);
	
	// ack
	MLock *reply = new MLock(lock, LOCK_AC_MIXEDACK, mds->get_nodeid());
	mds->send_message_mds(reply, in->authority().first);
      }
      break;

    case LOCK_GLOCKR:
      {
        lock->set_state(LOCK_LOCK);
        
        // ack
        MLock *reply = new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid());
        mds->send_message_mds(reply, in->authority().first);
      }
      break;

    default:
      assert(0);
    }
  }


}

void Locker::file_eval(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  int wanted = in->get_caps_wanted();
  bool loner = in->is_loner_cap();
  dout(7) << "file_eval wanted=" << cap_string(wanted)
	  << "  filelock=" << *lock << " on " << *lock->get_parent()
	  << "  loner=" << loner
	  << dendl;

  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  // not xlocked!
  if (lock->is_xlocked() || lock->get_parent()->is_frozen()) return;
  
  // * -> loner?
  if (!lock->is_rdlocked() &&
      !lock->is_waiter_for(SimpleLock::WAIT_WR) &&
      (wanted & CEPH_CAP_WR) &&
      loner &&
      lock->get_state() != LOCK_LONER) {
    dout(7) << "file_eval stable, bump to loner " << *lock << " on " << *lock->get_parent() << dendl;
    file_loner(lock);
  }

  // * -> mixed?
  else if (!lock->is_rdlocked() &&
	   !lock->is_waiter_for(SimpleLock::WAIT_WR) &&
	   (wanted & CEPH_CAP_RD) &&
	   (wanted & CEPH_CAP_WR) &&
	   !(loner && lock->get_state() == LOCK_LONER) &&
	   lock->get_state() != LOCK_MIXED) {
    dout(7) << "file_eval stable, bump to mixed " << *lock << " on " << *lock->get_parent() << dendl;
    file_mixed(lock);
  }
  
  // * -> sync?
  else if (!in->filelock.is_waiter_for(SimpleLock::WAIT_WR) &&
	   !(wanted & (CEPH_CAP_WR|CEPH_CAP_WRBUFFER)) &&
	   ((wanted & CEPH_CAP_RD) || 
	    in->is_replicated() || 
	    (!loner && lock->get_state() == LOCK_LONER)) &&
	   lock->get_state() != LOCK_SYNC) {
    dout(7) << "file_eval stable, bump to sync " << *lock << " on " << *lock->get_parent() << dendl;
    file_sync(lock);
  }
  
  // * -> lock?  (if not replicated or open)
  else if (!in->is_replicated() &&
	   wanted == 0 &&
	   lock->get_state() != LOCK_LOCK) {
    file_lock(lock);
  }
}


// mid

bool Locker::file_sync(FileLock *lock)
{
  CInode *in = (CInode*)lock->get_parent();
  dout(7) << "file_sync " << *lock << " on " << *lock->get_parent() << dendl;  

  assert(in->is_auth());
  assert(lock->is_stable());

  int issued = in->get_caps_issued();

  assert((in->get_caps_wanted() & CEPH_CAP_WR) == 0);

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
    if (issued & CEPH_CAP_WR) {
      // gather client write caps
      lock->set_state(LOCK_GSYNCM);
      lock->get_parent()->auth_pin();
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
    if (issued & CEPH_CAP_WR) {
      // gather client write caps
      lock->set_state(LOCK_GSYNCL);
      lock->get_parent()->auth_pin();
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
  dout(7) << "inode_file_lock " << *lock << " on " << *lock->get_parent() << dendl;  

  assert(in->is_auth());
  assert(lock->is_stable());

  int issued = in->get_caps_issued();

  if (lock->get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
      
      // change lock
      lock->set_state(LOCK_GLOCKR);
      lock->get_parent()->auth_pin();

      // call back caps
      if (issued) 
        issue_caps(in);
    } else {
      if (issued) {
        // call back caps
        lock->set_state(LOCK_GLOCKR);
	lock->get_parent()->auth_pin();
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
      lock->get_parent()->auth_pin();
      
      // call back caps
      issue_caps(in);
    } else {
      //assert(issued);  // ??? -sage 2/19/06
      if (issued) {
        // change lock
        lock->set_state(LOCK_GLOCKM);
	lock->get_parent()->auth_pin();
        
        // call back caps
        issue_caps(in);
      } else {
        lock->set_state(LOCK_LOCK);
      }
    }
      
  }
  else if (lock->get_state() == LOCK_LONER) {
    if (issued & CEPH_CAP_WR) {
      // change lock
      lock->set_state(LOCK_GLOCKL);
      lock->get_parent()->auth_pin();

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
  dout(7) << "file_mixed " << *lock << " on " << *lock->get_parent() << dendl;  

  CInode *in = (CInode*)lock->get_parent();
  assert(in->is_auth());
  assert(lock->is_stable());

  int issued = in->get_caps_issued();

  if (lock->get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_MIXED);
      lock->init_gather();
    
      lock->set_state(LOCK_GMIXEDR);
      lock->get_parent()->auth_pin();
      
      issue_caps(in);
    } else {
      if (issued) {
        lock->set_state(LOCK_GMIXEDR);
	lock->get_parent()->auth_pin();
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
    if (issued & CEPH_CAP_WRBUFFER) {
      // gather up WRBUFFER caps
      lock->set_state(LOCK_GMIXEDL);
      lock->get_parent()->auth_pin();
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
  dout(7) << "inode_file_loner " << *lock << " on " << *lock->get_parent() << dendl;  

  assert(in->is_auth());
  assert(lock->is_stable());

  assert((in->client_caps.size() == 1) && in->mds_caps_wanted.empty());
  
  if (lock->get_state() == LOCK_SYNC) {
    if (in->is_replicated()) {
      // bcast to replicas
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
      
      // change lock
      lock->set_state(LOCK_GLONERR);
      lock->get_parent()->auth_pin();
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
      lock->get_parent()->auth_pin();
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
  CInode *in = (CInode*)lock->get_parent();
  int from = m->get_asker();

  if (mds->is_rejoin()) {
    if (in->is_rejoining()) {
      dout(7) << "handle_file_lock still rejoining " << *in
	      << ", dropping " << *m << dendl;
      delete m;
      return;
    }
  }


  dout(7) << "handle_file_lock a=" << m->get_action() << " from " << from << " " 
	  << *in << " filelock=" << *lock << dendl;  
  
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
    file_eval_gather(lock);
    break;
    
  case LOCK_AC_LOCK:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_MIXED);
    
    lock->set_state(LOCK_GLOCKR);
    
    // call back caps?
    if (issued & CEPH_CAP_RD) {
      dout(7) << "handle_file_lock client readers, gathering caps on " << *in << dendl;
      issue_caps(in);
      break;
    }
    else if (lock->is_rdlocked()) {
      dout(7) << "handle_file_lock rdlocked, waiting before ack on " << *in << dendl;
      break;
    } 
    
    // nothing to wait for, lock and ack.
    {
      lock->set_state(LOCK_LOCK);
      
      MLock *reply = new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid());
      mds->send_message_mds(reply, from);
    }
    break;
    
  case LOCK_AC_MIXED:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_LOCK);
    
    if (lock->get_state() == LOCK_SYNC) {
      // MIXED
      if (issued & CEPH_CAP_RD) {
        // call back client caps
        lock->set_state(LOCK_GMIXEDR);
        issue_caps(in);
        break;
      } else {
        // no clients, go straight to mixed
        lock->set_state(LOCK_MIXED);

        // ack
        MLock *reply = new MLock(lock, LOCK_AC_MIXEDACK, mds->get_nodeid());
        mds->send_message_mds(reply, from);
      }
    } else {
      // LOCK
      lock->set_state(LOCK_MIXED);
      
      // no ack needed.
    }

    issue_caps(in);
    
    // waiters
    lock->finish_waiters(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
    file_eval_gather(lock);
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
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", last one" << dendl;
      file_eval_gather(lock);
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
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", last one" << dendl;
      file_eval_gather(lock);
    }
    break;

  case LOCK_AC_MIXEDACK:
    assert(lock->get_state() == LOCK_GMIXEDR);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->is_gathering()) {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_lock_inode_file " << *in << " from " << from
	      << ", last one" << dendl;
      file_eval_gather(lock);
    }
    break;


  default:
    assert(0);
  }  
  
  delete m;
}






