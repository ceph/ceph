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


#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Mutation.h"
#include "MDSContext.h"

#include "MDLog.h"
#include "MDSMap.h"

#include "events/EUpdate.h"
#include "events/EOpen.h"

#include "msg/Messenger.h"
#include "osdc/Objecter.h"

#include "messages/MInodeFileCaps.h"
#include "messages/MLock.h"
#include "messages/MClientLease.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"

#include "messages/MMDSSlaveRequest.h"

#include <errno.h>

#include "common/config.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_locker
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".locker ";
}


class LockerContext : public MDSInternalContextBase {
  protected:
  Locker *locker;
  MDSRank *get_mds()
  {
    return locker->mds;
  }

  public:
  explicit LockerContext(Locker *locker_) : locker(locker_) {
    assert(locker != NULL);
  }
};


/* This function DOES put the passed message before returning */
void Locker::dispatch(Message *m)
{

  switch (m->get_type()) {

    // inter-mds locking
  case MSG_MDS_LOCK:
    handle_lock(static_cast<MLock*>(m));
    break;
    // inter-mds caps
  case MSG_MDS_INODEFILECAPS:
    handle_inode_file_caps(static_cast<MInodeFileCaps*>(m));
    break;

    // client sync
  case CEPH_MSG_CLIENT_CAPS:
    handle_client_caps(static_cast<MClientCaps*>(m));

    break;
  case CEPH_MSG_CLIENT_CAPRELEASE:
    handle_client_cap_release(static_cast<MClientCapRelease*>(m));
    break;
  case CEPH_MSG_CLIENT_LEASE:
    handle_client_lease(static_cast<MClientLease*>(m));
    break;
    
  default:
    derr << "locker unknown message " << m->get_type() << dendl;
    assert(0 == "locker unknown message");
  }
}

void Locker::tick()
{
  scatter_tick();
  caps_tick();
}

/*
 * locks vs rejoin
 *
 * 
 *
 */

void Locker::send_lock_message(SimpleLock *lock, int msg)
{
  for (compact_map<mds_rank_t,unsigned>::iterator it = lock->get_parent()->replicas_begin();
       it != lock->get_parent()->replicas_end();
       ++it) {
    if (mds->mdsmap->get_state(it->first) < MDSMap::STATE_REJOIN) 
      continue;
    MLock *m = new MLock(lock, msg, mds->get_nodeid());
    mds->send_message_mds(m, it->first);
  }
}

void Locker::send_lock_message(SimpleLock *lock, int msg, const bufferlist &data)
{
  for (compact_map<mds_rank_t,unsigned>::iterator it = lock->get_parent()->replicas_begin();
       it != lock->get_parent()->replicas_end();
       ++it) {
    if (mds->mdsmap->get_state(it->first) < MDSMap::STATE_REJOIN) 
      continue;
    MLock *m = new MLock(lock, msg, mds->get_nodeid());
    m->set_data(data);
    mds->send_message_mds(m, it->first);
  }
}




void Locker::include_snap_rdlocks(set<SimpleLock*>& rdlocks, CInode *in)
{
  // rdlock ancestor snaps
  CInode *t = in;
  rdlocks.insert(&in->snaplock);
  while (t->get_projected_parent_dn()) {
    t = t->get_projected_parent_dn()->get_dir()->get_inode();
    rdlocks.insert(&t->snaplock);
  }
}

void Locker::include_snap_rdlocks_wlayout(set<SimpleLock*>& rdlocks, CInode *in,
					  file_layout_t **layout)
{
  //rdlock ancestor snaps
  CInode *t = in;
  rdlocks.insert(&in->snaplock);
  rdlocks.insert(&in->policylock);
  bool found_layout = false;
  while (t) {
    rdlocks.insert(&t->snaplock);
    if (!found_layout) {
      rdlocks.insert(&t->policylock);
      if (t->get_projected_inode()->has_layout()) {
        *layout = &t->get_projected_inode()->layout;
        found_layout = true;
      }
    }
    if (t->get_projected_parent_dn() &&
        t->get_projected_parent_dn()->get_dir())
      t = t->get_projected_parent_dn()->get_dir()->get_inode();
    else t = NULL;
  }
}

struct MarkEventOnDestruct {
  MDRequestRef& mdr;
  const char* message;
  bool mark_event;
  MarkEventOnDestruct(MDRequestRef& _mdr,
                      const char *_message) : mdr(_mdr),
                          message(_message),
                          mark_event(true) {}
  ~MarkEventOnDestruct() {
    if (mark_event)
      mdr->mark_event(message);
  }
};

/* If this function returns false, the mdr has been placed
 * on the appropriate wait list */
bool Locker::acquire_locks(MDRequestRef& mdr,
			   set<SimpleLock*> &rdlocks,
			   set<SimpleLock*> &wrlocks,
			   set<SimpleLock*> &xlocks,
			   map<SimpleLock*,mds_rank_t> *remote_wrlocks,
			   CInode *auth_pin_freeze,
			   bool auth_pin_nonblock)
{
  if (mdr->done_locking &&
      !mdr->is_slave()) {  // not on slaves!  master requests locks piecemeal.
    dout(10) << "acquire_locks " << *mdr << " - done locking" << dendl;    
    return true;  // at least we had better be!
  }
  dout(10) << "acquire_locks " << *mdr << dendl;

  MarkEventOnDestruct marker(mdr, "failed to acquire_locks");

  client_t client = mdr->get_client();

  set<SimpleLock*, SimpleLock::ptr_lt> sorted;  // sort everything we will lock
  set<MDSCacheObject*> mustpin;            // items to authpin

  // xlocks
  for (set<SimpleLock*>::iterator p = xlocks.begin(); p != xlocks.end(); ++p) {
    dout(20) << " must xlock " << **p << " " << *(*p)->get_parent() << dendl;
    sorted.insert(*p);
    mustpin.insert((*p)->get_parent());

    // augment xlock with a versionlock?
    if ((*p)->get_type() == CEPH_LOCK_DN) {
      CDentry *dn = (CDentry*)(*p)->get_parent();
      if (!dn->is_auth())
	continue;

      if (xlocks.count(&dn->versionlock))
	continue;  // we're xlocking the versionlock too; don't wrlock it!

      if (mdr->is_master()) {
	// master.  wrlock versionlock so we can pipeline dentry updates to journal.
	wrlocks.insert(&dn->versionlock);
      } else {
	// slave.  exclusively lock the dentry version (i.e. block other journal updates).
	// this makes rollback safe.
	xlocks.insert(&dn->versionlock);
	sorted.insert(&dn->versionlock);
      }
    }
    if ((*p)->get_type() > CEPH_LOCK_IVERSION) {
      // inode version lock?
      CInode *in = (CInode*)(*p)->get_parent();
      if (!in->is_auth())
	continue;
      if (mdr->is_master()) {
	// master.  wrlock versionlock so we can pipeline inode updates to journal.
	wrlocks.insert(&in->versionlock);
      } else {
	// slave.  exclusively lock the inode version (i.e. block other journal updates).
	// this makes rollback safe.
	xlocks.insert(&in->versionlock);
	sorted.insert(&in->versionlock);
      }
    }
  }

  // wrlocks
  for (set<SimpleLock*>::iterator p = wrlocks.begin(); p != wrlocks.end(); ++p) {
    MDSCacheObject *object = (*p)->get_parent();
    dout(20) << " must wrlock " << **p << " " << *object << dendl;
    sorted.insert(*p);
    if (object->is_auth())
      mustpin.insert(object);
    else if (!object->is_auth() &&
	     !(*p)->can_wrlock(client) &&  // we might have to request a scatter
	     !mdr->is_slave()) {           // if we are slave (remote_wrlock), the master already authpinned
      dout(15) << " will also auth_pin " << *object
	       << " in case we need to request a scatter" << dendl;
      mustpin.insert(object);
    }
  }

  // remote_wrlocks
  if (remote_wrlocks) {
    for (map<SimpleLock*,mds_rank_t>::iterator p = remote_wrlocks->begin(); p != remote_wrlocks->end(); ++p) {
      MDSCacheObject *object = p->first->get_parent();
      dout(20) << " must remote_wrlock on mds." << p->second << " "
	       << *p->first << " " << *object << dendl;
      sorted.insert(p->first);
      mustpin.insert(object);
    }
  }

  // rdlocks
  for (set<SimpleLock*>::iterator p = rdlocks.begin();
	 p != rdlocks.end();
       ++p) {
    MDSCacheObject *object = (*p)->get_parent();
    dout(20) << " must rdlock " << **p << " " << *object << dendl;
    sorted.insert(*p);
    if (object->is_auth())
      mustpin.insert(object);
    else if (!object->is_auth() &&
	     !(*p)->can_rdlock(client)) {      // we might have to request an rdlock
      dout(15) << " will also auth_pin " << *object
	       << " in case we need to request a rdlock" << dendl;
      mustpin.insert(object);
    }
  }

 
  // AUTH PINS
  map<mds_rank_t, set<MDSCacheObject*> > mustpin_remote;  // mds -> (object set)
  
  // can i auth pin them all now?
  marker.message = "failed to authpin local pins";
  for (set<MDSCacheObject*>::iterator p = mustpin.begin();
       p != mustpin.end();
       ++p) {
    MDSCacheObject *object = *p;

    dout(10) << " must authpin " << *object << dendl;

    if (mdr->is_auth_pinned(object)) 
      continue;
    
    if (!object->is_auth()) {
      if (!mdr->locks.empty())
	mds->locker->drop_locks(mdr.get());
      if (object->is_ambiguous_auth()) {
	// wait
	dout(10) << " ambiguous auth, waiting to authpin " << *object << dendl;
	object->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	mdr->drop_local_auth_pins();
	return false;
      }
      mustpin_remote[object->authority().first].insert(object);
      continue;
    }
    if (!object->can_auth_pin()) {
      // wait
      mds->locker->drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      if (auth_pin_nonblock) {
	dout(10) << " can't auth_pin (freezing?) " << *object << ", nonblocking" << dendl;
	mdr->aborted = true;
	return false;
      }
      dout(10) << " can't auth_pin (freezing?), waiting to authpin " << *object << dendl;
      object->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return false;
    }
  }

  // ok, grab local auth pins
  for (set<MDSCacheObject*>::iterator p = mustpin.begin();
       p != mustpin.end();
       ++p) {
    MDSCacheObject *object = *p;
    if (mdr->is_auth_pinned(object)) {
      dout(10) << " already auth_pinned " << *object << dendl;
    } else if (object->is_auth()) {
      dout(10) << " auth_pinning " << *object << dendl;
      mdr->auth_pin(object);
    }
  }

  // request remote auth_pins
  if (!mustpin_remote.empty()) {
    marker.message = "requesting remote authpins";
    for (map<MDSCacheObject*,mds_rank_t>::iterator p = mdr->remote_auth_pins.begin();
	 p != mdr->remote_auth_pins.end();
	 ++p) {
      if (mustpin.count(p->first)) {
	assert(p->second == p->first->authority().first);
	map<mds_rank_t, set<MDSCacheObject*> >::iterator q = mustpin_remote.find(p->second);
	if (q != mustpin_remote.end())
	  q->second.insert(p->first);
      }
    }
    for (map<mds_rank_t, set<MDSCacheObject*> >::iterator p = mustpin_remote.begin();
	 p != mustpin_remote.end();
	 ++p) {
      dout(10) << "requesting remote auth_pins from mds." << p->first << dendl;

      // wait for active auth
      if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(p->first)) {
	dout(10) << " mds." << p->first << " is not active" << dendl;
	if (mdr->more()->waiting_on_slave.empty())
	  mds->wait_for_active_peer(p->first, new C_MDS_RetryRequest(mdcache, mdr));
	return false;
      }
      
      MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						   MMDSSlaveRequest::OP_AUTHPIN);
      for (set<MDSCacheObject*>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " req remote auth_pin of " << **q << dendl;
	MDSCacheObjectInfo info;
	(*q)->set_object_info(info);
	req->get_authpins().push_back(info);
	if (*q == auth_pin_freeze)
	  (*q)->set_object_info(req->get_authpin_freeze());
	mdr->pin(*q);
      }
      if (auth_pin_nonblock)
	req->mark_nonblock();
      mds->send_message_mds(req, p->first);

      // put in waiting list
      assert(mdr->more()->waiting_on_slave.count(p->first) == 0);
      mdr->more()->waiting_on_slave.insert(p->first);
    }
    return false;
  }

  // caps i'll need to issue
  set<CInode*> issue_set;
  bool result = false;

  // acquire locks.
  // make sure they match currently acquired locks.
  set<SimpleLock*, SimpleLock::ptr_lt>::iterator existing = mdr->locks.begin();
  for (set<SimpleLock*, SimpleLock::ptr_lt>::iterator p = sorted.begin();
       p != sorted.end();
       ++p) {
    bool need_wrlock = !!wrlocks.count(*p);
    bool need_remote_wrlock = !!(remote_wrlocks && remote_wrlocks->count(*p));

    // already locked?
    if (existing != mdr->locks.end() && *existing == *p) {
      // right kind?
      SimpleLock *have = *existing;
      ++existing;
      if (xlocks.count(have) && mdr->xlocks.count(have)) {
	dout(10) << " already xlocked " << *have << " " << *have->get_parent() << dendl;
	continue;
      }
      if (mdr->remote_wrlocks.count(have)) {
	if (!need_remote_wrlock ||
	    mdr->remote_wrlocks[have] != (*remote_wrlocks)[have]) {
	  dout(10) << " unlocking remote_wrlock on wrong mds." << mdr->remote_wrlocks[have]
		   << " " << *have << " " << *have->get_parent() << dendl;
	  remote_wrlock_finish(have, mdr->remote_wrlocks[have], mdr.get());
	}
      }
      if (need_wrlock || need_remote_wrlock) {
	if (need_wrlock == !!mdr->wrlocks.count(have) &&
	    need_remote_wrlock == !!mdr->remote_wrlocks.count(have)) {
	  if (need_wrlock)
	    dout(10) << " already wrlocked " << *have << " " << *have->get_parent() << dendl;
	  if (need_remote_wrlock)
	    dout(10) << " already remote_wrlocked " << *have << " " << *have->get_parent() << dendl;
	  continue;
	}
      }
      if (rdlocks.count(have) && mdr->rdlocks.count(have)) {
	dout(10) << " already rdlocked " << *have << " " << *have->get_parent() << dendl;
	continue;
      }
    }
    
    // hose any stray locks
    if (*existing == *p) {
      assert(need_wrlock || need_remote_wrlock);
      SimpleLock *lock = *existing;
      if (mdr->wrlocks.count(lock)) {
	if (!need_wrlock)
	  dout(10) << " unlocking extra " << *lock << " " << *lock->get_parent() << dendl;
	else if (need_remote_wrlock) // acquire remote_wrlock first
	  dout(10) << " unlocking out-of-order " << *lock << " " << *lock->get_parent() << dendl;
	bool need_issue = false;
	wrlock_finish(lock, mdr.get(), &need_issue);
	if (need_issue)
	  issue_set.insert(static_cast<CInode*>(lock->get_parent()));
      }
      ++existing;
    }
    while (existing != mdr->locks.end()) {
      SimpleLock *stray = *existing;
      ++existing;
      dout(10) << " unlocking out-of-order " << *stray << " " << *stray->get_parent() << dendl;
      bool need_issue = false;
      if (mdr->xlocks.count(stray)) {
	xlock_finish(stray, mdr.get(), &need_issue);
      } else if (mdr->rdlocks.count(stray)) {
	rdlock_finish(stray, mdr.get(), &need_issue);
      } else {
	// may have acquired both wrlock and remore wrlock
	if (mdr->wrlocks.count(stray))
	  wrlock_finish(stray, mdr.get(), &need_issue);
	if (mdr->remote_wrlocks.count(stray))
	  remote_wrlock_finish(stray, mdr->remote_wrlocks[stray], mdr.get());
      }
      if (need_issue)
	issue_set.insert(static_cast<CInode*>(stray->get_parent()));
    }

    // lock
    if (mdr->locking && *p != mdr->locking) {
      cancel_locking(mdr.get(), &issue_set);
    }
    if (xlocks.count(*p)) {
      marker.message = "failed to xlock, waiting";
      if (!xlock_start(*p, mdr)) 
	goto out;
      dout(10) << " got xlock on " << **p << " " << *(*p)->get_parent() << dendl;
    } else if (need_wrlock || need_remote_wrlock) {
      if (need_remote_wrlock && !mdr->remote_wrlocks.count(*p)) {
        marker.message = "waiting for remote wrlocks";
	remote_wrlock_start(*p, (*remote_wrlocks)[*p], mdr);
	goto out;
      }
      if (need_wrlock && !mdr->wrlocks.count(*p)) {
        marker.message = "failed to wrlock, waiting";
	if (need_remote_wrlock && !(*p)->can_wrlock(mdr->get_client())) {
	  marker.message = "failed to wrlock, dropping remote wrlock and waiting";
	  // can't take the wrlock because the scatter lock is gathering. need to
	  // release the remote wrlock, so that the gathering process can finish.
	  remote_wrlock_finish(*p, mdr->remote_wrlocks[*p], mdr.get());
	  remote_wrlock_start(*p, (*remote_wrlocks)[*p], mdr);
	  goto out;
	}
	if (!wrlock_start(*p, mdr))
	  goto out;
	dout(10) << " got wrlock on " << **p << " " << *(*p)->get_parent() << dendl;
      }
    } else {
      marker.message = "failed to rdlock, waiting";
      if (!rdlock_start(*p, mdr)) 
	goto out;
      dout(10) << " got rdlock on " << **p << " " << *(*p)->get_parent() << dendl;
    }
  }
    
  // any extra unneeded locks?
  while (existing != mdr->locks.end()) {
    SimpleLock *stray = *existing;
    ++existing;
    dout(10) << " unlocking extra " << *stray << " " << *stray->get_parent() << dendl;
    bool need_issue = false;
    if (mdr->xlocks.count(stray)) {
      xlock_finish(stray, mdr.get(), &need_issue);
    } else if (mdr->rdlocks.count(stray)) {
      rdlock_finish(stray, mdr.get(), &need_issue);
    } else {
      // may have acquired both wrlock and remore wrlock
      if (mdr->wrlocks.count(stray))
	wrlock_finish(stray, mdr.get(), &need_issue);
      if (mdr->remote_wrlocks.count(stray))
	remote_wrlock_finish(stray, mdr->remote_wrlocks[stray], mdr.get());
    }
    if (need_issue)
      issue_set.insert(static_cast<CInode*>(stray->get_parent()));
  }

  mdr->done_locking = true;
  mdr->set_mds_stamp(ceph_clock_now(NULL));
  result = true;
  marker.message = "acquired locks";

 out:
  issue_caps_set(issue_set);
  return result;
}


void Locker::set_xlocks_done(MutationImpl *mut, bool skip_dentry)
{
  for (set<SimpleLock*>::iterator p = mut->xlocks.begin();
       p != mut->xlocks.end();
       ++p) {
    MDSCacheObject *object = (*p)->get_parent();
    assert(object->is_auth());
    if (skip_dentry &&
	((*p)->get_type() == CEPH_LOCK_DN || (*p)->get_type() == CEPH_LOCK_DVERSION))
      continue;
    dout(10) << "set_xlocks_done on " << **p << " " << *object << dendl;
    (*p)->set_xlock_done();
  }
}

void Locker::_drop_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  while (!mut->rdlocks.empty()) {
    bool ni = false;
    MDSCacheObject *p = (*mut->rdlocks.begin())->get_parent();
    rdlock_finish(*mut->rdlocks.begin(), mut, &ni);
    if (ni)
      pneed_issue->insert(static_cast<CInode*>(p));
  }
}

void Locker::_drop_non_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  set<mds_rank_t> slaves;

  while (!mut->xlocks.empty()) {
    SimpleLock *lock = *mut->xlocks.begin();
    MDSCacheObject *p = lock->get_parent();
    if (!p->is_auth()) {
      assert(lock->get_sm()->can_remote_xlock);
      slaves.insert(p->authority().first);
      lock->put_xlock();
      mut->locks.erase(lock);
      mut->xlocks.erase(lock);
      continue;
    }
    bool ni = false;
    xlock_finish(lock, mut, &ni);
    if (ni)
      pneed_issue->insert(static_cast<CInode*>(p));
  }

  while (!mut->remote_wrlocks.empty()) {
    map<SimpleLock*,mds_rank_t>::iterator p = mut->remote_wrlocks.begin();
    slaves.insert(p->second);
    if (mut->wrlocks.count(p->first) == 0)
      mut->locks.erase(p->first);
    mut->remote_wrlocks.erase(p);
  }

  while (!mut->wrlocks.empty()) {
    bool ni = false;
    MDSCacheObject *p = (*mut->wrlocks.begin())->get_parent();
    wrlock_finish(*mut->wrlocks.begin(), mut, &ni);
    if (ni)
      pneed_issue->insert(static_cast<CInode*>(p));
  }

  for (set<mds_rank_t>::iterator p = slaves.begin(); p != slaves.end(); ++p) {
    if (mds->mdsmap->get_state(*p) >= MDSMap::STATE_REJOIN) {
      dout(10) << "_drop_non_rdlocks dropping remote locks on mds." << *p << dendl;
      MMDSSlaveRequest *slavereq = new MMDSSlaveRequest(mut->reqid, mut->attempt,
							MMDSSlaveRequest::OP_DROPLOCKS);
      mds->send_message_mds(slavereq, *p);
    }
  }
}

void Locker::cancel_locking(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  SimpleLock *lock = mut->locking;
  assert(lock);
  dout(10) << "cancel_locking " << *lock << " on " << *mut << dendl;

  if (lock->get_parent()->is_auth()) {
    bool need_issue = false;
    if (lock->get_state() == LOCK_PREXLOCK) {
      _finish_xlock(lock, -1, &need_issue);
    } else if (lock->get_state() == LOCK_LOCK_XLOCK &&
	       lock->get_num_xlocks() == 0) {
      lock->set_state(LOCK_XLOCKDONE);
      eval_gather(lock, true, &need_issue);
    }
    if (need_issue)
      pneed_issue->insert(static_cast<CInode *>(lock->get_parent()));
  }
  mut->finish_locking(lock);
}

void Locker::drop_locks(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  // leftover locks
  set<CInode*> my_need_issue;
  if (!pneed_issue)
    pneed_issue = &my_need_issue;

  if (mut->locking)
    cancel_locking(mut, pneed_issue);
  _drop_non_rdlocks(mut, pneed_issue);
  _drop_rdlocks(mut, pneed_issue);

  if (pneed_issue == &my_need_issue)
    issue_caps_set(*pneed_issue);
  mut->done_locking = false;
}

void Locker::drop_non_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  set<CInode*> my_need_issue;
  if (!pneed_issue)
    pneed_issue = &my_need_issue;

  _drop_non_rdlocks(mut, pneed_issue);

  if (pneed_issue == &my_need_issue)
    issue_caps_set(*pneed_issue);
}

void Locker::drop_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  set<CInode*> my_need_issue;
  if (!pneed_issue)
    pneed_issue = &my_need_issue;

  _drop_rdlocks(mut, pneed_issue);

  if (pneed_issue == &my_need_issue)
    issue_caps_set(*pneed_issue);
}


// generics

void Locker::eval_gather(SimpleLock *lock, bool first, bool *pneed_issue, list<MDSInternalContextBase*> *pfinishers)
{
  dout(10) << "eval_gather " << *lock << " on " << *lock->get_parent() << dendl;
  assert(!lock->is_stable());

  int next = lock->get_next_state();

  CInode *in = 0;
  bool caps = lock->get_cap_shift();
  if (lock->get_type() != CEPH_LOCK_DN)
    in = static_cast<CInode *>(lock->get_parent());

  bool need_issue = false;

  int loner_issued = 0, other_issued = 0, xlocker_issued = 0;
  assert(!caps || in != NULL);
  if (caps && in->is_head()) {
    in->get_caps_issued(&loner_issued, &other_issued, &xlocker_issued,
			lock->get_cap_shift(), lock->get_cap_mask());
    dout(10) << " next state is " << lock->get_state_name(next) 
	     << " issued/allows loner " << gcap_string(loner_issued)
	     << "/" << gcap_string(lock->gcaps_allowed(CAP_LONER, next))
	     << " xlocker " << gcap_string(xlocker_issued)
	     << "/" << gcap_string(lock->gcaps_allowed(CAP_XLOCKER, next))
	     << " other " << gcap_string(other_issued)
	     << "/" << gcap_string(lock->gcaps_allowed(CAP_ANY, next))
	     << dendl;

    if (first && ((~lock->gcaps_allowed(CAP_ANY, next) & other_issued) ||
		  (~lock->gcaps_allowed(CAP_LONER, next) & loner_issued) ||
		  (~lock->gcaps_allowed(CAP_XLOCKER, next) & xlocker_issued)))
      need_issue = true;
  }

#define IS_TRUE_AND_LT_AUTH(x, auth) (x && ((auth && x <= AUTH) || (!auth && x < AUTH)))
  bool auth = lock->get_parent()->is_auth();
  if (!lock->is_gathering() &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_rdlock, auth) || !lock->is_rdlocked()) &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_wrlock, auth) || !lock->is_wrlocked()) &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_xlock, auth) || !lock->is_xlocked()) &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_lease, auth) || !lock->is_leased()) &&
      !(lock->get_parent()->is_auth() && lock->is_flushing()) &&  // i.e. wait for scatter_writebehind!
      (!caps || ((~lock->gcaps_allowed(CAP_ANY, next) & other_issued) == 0 &&
		 (~lock->gcaps_allowed(CAP_LONER, next) & loner_issued) == 0 &&
		 (~lock->gcaps_allowed(CAP_XLOCKER, next) & xlocker_issued) == 0)) &&
      lock->get_state() != LOCK_SYNC_MIX2 &&  // these states need an explicit trigger from the auth mds
      lock->get_state() != LOCK_MIX_SYNC2
      ) {
    dout(7) << "eval_gather finished gather on " << *lock
	    << " on " << *lock->get_parent() << dendl;

    if (lock->get_sm() == &sm_filelock) {
      assert(in);
      if (in->state_test(CInode::STATE_RECOVERING)) {
	dout(7) << "eval_gather finished gather, but still recovering" << dendl;
	return;
      } else if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
	dout(7) << "eval_gather finished gather, but need to recover" << dendl;
	mds->mdcache->queue_file_recover(in);
	mds->mdcache->do_file_recover();
	return;
      }
    }

    if (!lock->get_parent()->is_auth()) {
      // replica: tell auth
      mds_rank_t auth = lock->get_parent()->authority().first;

      if (lock->get_parent()->is_rejoining() &&
	  mds->mdsmap->get_state(auth) == MDSMap::STATE_REJOIN) {
	dout(7) << "eval_gather finished gather, but still rejoining "
		<< *lock->get_parent() << dendl;
	return;
      }

      if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
	switch (lock->get_state()) {
	case LOCK_SYNC_LOCK:
	  mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid()),
				auth);
	  break;

	case LOCK_MIX_SYNC:
	  {
	    MLock *reply = new MLock(lock, LOCK_AC_SYNCACK, mds->get_nodeid());
	    lock->encode_locked_state(reply->get_data());
	    mds->send_message_mds(reply, auth);
	    next = LOCK_MIX_SYNC2;
	    (static_cast<ScatterLock *>(lock))->start_flush();
	  }
	  break;

	case LOCK_MIX_SYNC2:
	  (static_cast<ScatterLock *>(lock))->finish_flush();
	  (static_cast<ScatterLock *>(lock))->clear_flushed();

	case LOCK_SYNC_MIX2:
	  // do nothing, we already acked
	  break;
	  
	case LOCK_SYNC_MIX:
	  { 
	    MLock *reply = new MLock(lock, LOCK_AC_MIXACK, mds->get_nodeid());
	    mds->send_message_mds(reply, auth);
	    next = LOCK_SYNC_MIX2;
	  }
	  break;

	case LOCK_MIX_LOCK:
	  {
	    bufferlist data;
	    lock->encode_locked_state(data);
	    mds->send_message_mds(new MLock(lock, LOCK_AC_LOCKACK, mds->get_nodeid(), data), auth);
	    (static_cast<ScatterLock *>(lock))->start_flush();
	    // we'll get an AC_LOCKFLUSHED to complete
	  }
	  break;

	default:
	  assert(0);
	}
      }
    } else {
      // auth

      // once the first (local) stage of mix->lock gather complete we can
      // gather from replicas
      if (lock->get_state() == LOCK_MIX_LOCK &&
	  lock->get_parent()->is_replicated()) {
	dout(10) << " finished (local) gather for mix->lock, now gathering from replicas" << dendl;
	send_lock_message(lock, LOCK_AC_LOCK);
	lock->init_gather();
	lock->set_state(LOCK_MIX_LOCK2);
	return;
      }

      if (lock->is_dirty() && !lock->is_flushed()) {
	scatter_writebehind(static_cast<ScatterLock *>(lock));
	mds->mdlog->flush();
	return;
      }
      lock->clear_flushed();
      
      switch (lock->get_state()) {
	// to mixed
      case LOCK_TSYN_MIX:
      case LOCK_SYNC_MIX:
      case LOCK_EXCL_MIX:
	in->start_scatter(static_cast<ScatterLock *>(lock));
	if (lock->get_parent()->is_replicated()) {
	  bufferlist softdata;
	  lock->encode_locked_state(softdata);
	  send_lock_message(lock, LOCK_AC_MIX, softdata);
	}
	(static_cast<ScatterLock *>(lock))->clear_scatter_wanted();
	break;

      case LOCK_XLOCK:
      case LOCK_XLOCKDONE:
	if (next != LOCK_SYNC)
	  break;
	// fall-thru

	// to sync
      case LOCK_EXCL_SYNC:
      case LOCK_LOCK_SYNC:
      case LOCK_MIX_SYNC:
      case LOCK_XSYN_SYNC:
	if (lock->get_parent()->is_replicated()) {
	  bufferlist softdata;
	  lock->encode_locked_state(softdata);
	  send_lock_message(lock, LOCK_AC_SYNC, softdata);
	}
	break;
      }

    }

    lock->set_state(next);
    
    if (lock->get_parent()->is_auth() &&
	lock->is_stable())
      lock->get_parent()->auth_unpin(lock);

    // drop loner before doing waiters
    if (caps &&
	in->is_head() &&
	in->is_auth() &&
	in->get_wanted_loner() != in->get_loner()) {
      dout(10) << "  trying to drop loner" << dendl;
      if (in->try_drop_loner()) {
	dout(10) << "  dropped loner" << dendl;
	need_issue = true;
      }
    }

    if (pfinishers)
      lock->take_waiting(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR|SimpleLock::WAIT_RD|SimpleLock::WAIT_XLOCK,
			 *pfinishers);
    else
      lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR|SimpleLock::WAIT_RD|SimpleLock::WAIT_XLOCK);
    
    if (caps && in->is_head())
      need_issue = true;

    if (lock->get_parent()->is_auth() &&
	lock->is_stable())
      try_eval(lock, &need_issue);
  }

  if (need_issue) {
    if (pneed_issue)
      *pneed_issue = true;
    else if (in->is_head())
      issue_caps(in);
  }

}

bool Locker::eval(CInode *in, int mask, bool caps_imported)
{
  bool need_issue = caps_imported;
  list<MDSInternalContextBase*> finishers;
  
  dout(10) << "eval " << mask << " " << *in << dendl;

  // choose loner?
  if (in->is_auth() && in->is_head()) {
    if (in->choose_ideal_loner() >= 0) {
      if (in->try_set_loner()) {
	dout(10) << "eval set loner to client." << in->get_loner() << dendl;
	need_issue = true;
	mask = -1;
      } else
	dout(10) << "eval want loner client." << in->get_wanted_loner() << " but failed to set it" << dendl;
    } else
      dout(10) << "eval doesn't want loner" << dendl;
  }

 retry:
  if (mask & CEPH_LOCK_IFILE)
    eval_any(&in->filelock, &need_issue, &finishers, caps_imported);
  if (mask & CEPH_LOCK_IAUTH)
    eval_any(&in->authlock, &need_issue, &finishers, caps_imported);
  if (mask & CEPH_LOCK_ILINK)
    eval_any(&in->linklock, &need_issue, &finishers, caps_imported);
  if (mask & CEPH_LOCK_IXATTR)
    eval_any(&in->xattrlock, &need_issue, &finishers, caps_imported);
  if (mask & CEPH_LOCK_INEST)
    eval_any(&in->nestlock, &need_issue, &finishers, caps_imported);
  if (mask & CEPH_LOCK_IFLOCK)
    eval_any(&in->flocklock, &need_issue, &finishers, caps_imported);
  if (mask & CEPH_LOCK_IPOLICY)
    eval_any(&in->policylock, &need_issue, &finishers, caps_imported);

  // drop loner?
  if (in->is_auth() && in->is_head() && in->get_wanted_loner() != in->get_loner()) {
    dout(10) << "  trying to drop loner" << dendl;
    if (in->try_drop_loner()) {
      dout(10) << "  dropped loner" << dendl;
      need_issue = true;

      if (in->get_wanted_loner() >= 0) {
	if (in->try_set_loner()) {
	  dout(10) << "eval end set loner to client." << in->get_loner() << dendl;
	  mask = -1;
	  goto retry;
	} else {
	  dout(10) << "eval want loner client." << in->get_wanted_loner() << " but failed to set it" << dendl;
	}
      }
    }
  }

  finish_contexts(g_ceph_context, finishers);

  if (need_issue && in->is_head())
    issue_caps(in);

  dout(10) << "eval done" << dendl;
  return need_issue;
}

class C_Locker_Eval : public LockerContext {
  MDSCacheObject *p;
  int mask;
public:
  C_Locker_Eval(Locker *l, MDSCacheObject *pp, int m) : LockerContext(l), p(pp), mask(m) {
    // We are used as an MDSCacheObject waiter, so should
    // only be invoked by someone already holding the big lock.
    assert(locker->mds->mds_lock.is_locked_by_me());
    p->get(MDSCacheObject::PIN_PTRWAITER);    
  }
  void finish(int r) {
    p->put(MDSCacheObject::PIN_PTRWAITER);
    locker->try_eval(p, mask);
  }
};

void Locker::try_eval(MDSCacheObject *p, int mask)
{
  // unstable and ambiguous auth?
  if (p->is_ambiguous_auth()) {
    dout(7) << "try_eval ambiguous auth, waiting on " << *p << dendl;
    p->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_Locker_Eval(this, p, mask));
    return;
  }

  if (p->is_auth() && p->is_frozen()) {
    dout(7) << "try_eval frozen, waiting on " << *p << dendl;
    p->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_Locker_Eval(this, p, mask));
    return;
  }

  if (mask & CEPH_LOCK_DN) {
    assert(mask == CEPH_LOCK_DN);
    bool need_issue = false;  // ignore this, no caps on dentries
    CDentry *dn = static_cast<CDentry *>(p);
    eval_any(&dn->lock, &need_issue);
  } else {
    CInode *in = static_cast<CInode *>(p);
    eval(in, mask);
  }
}

void Locker::try_eval(SimpleLock *lock, bool *pneed_issue)
{
  MDSCacheObject *p = lock->get_parent();

  // unstable and ambiguous auth?
  if (p->is_ambiguous_auth()) {
    dout(7) << "try_eval " << *lock << " ambiguousauth, waiting on " << *p << dendl;
    p->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_Locker_Eval(this, p, lock->get_type()));
    return;
  }
  
  if (!p->is_auth()) {
    dout(7) << "try_eval " << *lock << " not auth for " << *p << dendl;
    return;
  }

  if (p->is_frozen()) {
    dout(7) << "try_eval " << *lock << " frozen, waiting on " << *p << dendl;
    p->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_Locker_Eval(this, p, lock->get_type()));
    return;
  }

  /*
   * We could have a situation like:
   *
   * - mds A authpins item on mds B
   * - mds B starts to freeze tree containing item
   * - mds A tries wrlock_start on A, sends REQSCATTER to B
   * - mds B lock is unstable, sets scatter_wanted
   * - mds B lock stabilizes, calls try_eval.
   *
   * We can defer while freezing without causing a deadlock.  Honor
   * scatter_wanted flag here.  This will never get deferred by the
   * checks above due to the auth_pin held by the master.
   */
  if (lock->is_scatterlock()) {
    ScatterLock *slock = static_cast<ScatterLock *>(lock);
    if (slock->get_scatter_wanted() &&
	slock->get_state() != LOCK_MIX) {
      scatter_mix(slock, pneed_issue);
      if (!lock->is_stable())
	return;
    } else if (slock->get_unscatter_wanted() &&
        slock->get_state() != LOCK_LOCK) {
      simple_lock(slock, pneed_issue);
      if (!lock->is_stable()) {
        return;
      }
    }
  }

  if (lock->get_type() != CEPH_LOCK_DN && p->is_freezing()) {
    dout(7) << "try_eval " << *lock << " freezing, waiting on " << *p << dendl;
    p->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_Locker_Eval(this, p, lock->get_type()));
    return;
  }

  eval(lock, pneed_issue);
}

void Locker::eval_cap_gather(CInode *in, set<CInode*> *issue_set)
{
  bool need_issue = false;
  list<MDSInternalContextBase*> finishers;

  // kick locks now
  if (!in->filelock.is_stable())
    eval_gather(&in->filelock, false, &need_issue, &finishers);
  if (!in->authlock.is_stable())
    eval_gather(&in->authlock, false, &need_issue, &finishers);
  if (!in->linklock.is_stable())
    eval_gather(&in->linklock, false, &need_issue, &finishers);
  if (!in->xattrlock.is_stable())
    eval_gather(&in->xattrlock, false, &need_issue, &finishers);

  if (need_issue && in->is_head()) {
    if (issue_set)
      issue_set->insert(in);
    else
      issue_caps(in);
  }

  finish_contexts(g_ceph_context, finishers);
}

void Locker::eval_scatter_gathers(CInode *in)
{
  bool need_issue = false;
  list<MDSInternalContextBase*> finishers;

  dout(10) << "eval_scatter_gathers " << *in << dendl;

  // kick locks now
  if (!in->filelock.is_stable())
    eval_gather(&in->filelock, false, &need_issue, &finishers);
  if (!in->nestlock.is_stable())
    eval_gather(&in->nestlock, false, &need_issue, &finishers);
  if (!in->dirfragtreelock.is_stable())
    eval_gather(&in->dirfragtreelock, false, &need_issue, &finishers);
  
  if (need_issue && in->is_head())
    issue_caps(in);
  
  finish_contexts(g_ceph_context, finishers);
}

void Locker::eval(SimpleLock *lock, bool *need_issue)
{
  switch (lock->get_type()) {
  case CEPH_LOCK_IFILE:
    return file_eval(static_cast<ScatterLock*>(lock), need_issue);
  case CEPH_LOCK_IDFT:
  case CEPH_LOCK_INEST:
    return scatter_eval(static_cast<ScatterLock*>(lock), need_issue);
  default:
    return simple_eval(lock, need_issue);
  }
}


// ------------------
// rdlock

bool Locker::_rdlock_kick(SimpleLock *lock, bool as_anon)
{
  // kick the lock
  if (lock->is_stable()) {
    if (lock->get_parent()->is_auth()) {
      if (lock->get_sm() == &sm_scatterlock) {
	// not until tempsync is fully implemented
	//if (lock->get_parent()->is_replicated())
	//scatter_tempsync((ScatterLock*)lock);
	//else
	simple_sync(lock);
      } else if (lock->get_sm() == &sm_filelock) {
	CInode *in = static_cast<CInode*>(lock->get_parent());
	if (lock->get_state() == LOCK_EXCL &&
	    in->get_target_loner() >= 0 &&
	    !in->is_dir() && !as_anon)   // as_anon => caller wants SYNC, not XSYN
	  file_xsyn(lock);
	else
	  simple_sync(lock);
      } else
	simple_sync(lock);
      return true;
    } else {
      // request rdlock state change from auth
      mds_rank_t auth = lock->get_parent()->authority().first;
      if (mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
	dout(10) << "requesting rdlock from auth on "
		 << *lock << " on " << *lock->get_parent() << dendl;
	mds->send_message_mds(new MLock(lock, LOCK_AC_REQRDLOCK, mds->get_nodeid()), auth);
      }
      return false;
    }
  }
  if (lock->get_type() == CEPH_LOCK_IFILE) {
    CInode *in = static_cast<CInode *>(lock->get_parent());
    if (in->state_test(CInode::STATE_RECOVERING)) {
      mds->mdcache->recovery_queue.prioritize(in);
    }
  }

  return false;
}

bool Locker::rdlock_try(SimpleLock *lock, client_t client, MDSInternalContextBase *con)
{
  dout(7) << "rdlock_try on " << *lock << " on " << *lock->get_parent() << dendl;  

  // can read?  grab ref.
  if (lock->can_rdlock(client)) 
    return true;
  
  _rdlock_kick(lock, false);

  if (lock->can_rdlock(client)) 
    return true;

  // wait!
  if (con) {
    dout(7) << "rdlock_try waiting on " << *lock << " on " << *lock->get_parent() << dendl;
    lock->add_waiter(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_RD, con);
  }
  return false;
}

bool Locker::rdlock_start(SimpleLock *lock, MDRequestRef& mut, bool as_anon)
{
  dout(7) << "rdlock_start  on " << *lock << " on " << *lock->get_parent() << dendl;  

  // client may be allowed to rdlock the same item it has xlocked.
  //  UNLESS someone passes in as_anon, or we're reading snapped version here.
  if (mut->snapid != CEPH_NOSNAP)
    as_anon = true;
  client_t client = as_anon ? -1 : mut->get_client();

  CInode *in = 0;
  if (lock->get_type() != CEPH_LOCK_DN)
    in = static_cast<CInode *>(lock->get_parent());

  /*
  if (!lock->get_parent()->is_auth() &&
      lock->fw_rdlock_to_auth()) {
    mdcache->request_forward(mut, lock->get_parent()->authority().first);
    return false;
  }
  */

  while (1) {
    // can read?  grab ref.
    if (lock->can_rdlock(client)) {
      lock->get_rdlock();
      mut->rdlocks.insert(lock);
      mut->locks.insert(lock);
      return true;
    }

    // hmm, wait a second.
    if (in && !in->is_head() && in->is_auth() &&
	lock->get_state() == LOCK_SNAP_SYNC) {
      // okay, we actually need to kick the head's lock to get ourselves synced up.
      CInode *head = mdcache->get_inode(in->ino());
      assert(head);
      SimpleLock *hlock = head->get_lock(lock->get_type());
      if (hlock->get_state() != LOCK_SYNC) {
	dout(10) << "rdlock_start trying head inode " << *head << dendl;
	if (!rdlock_start(head->get_lock(lock->get_type()), mut, true)) // ** as_anon, no rdlock on EXCL **
	  return false;
	// oh, check our lock again then
      }
    }

    if (!_rdlock_kick(lock, as_anon))
      break;
  }

  // wait!
  int wait_on;
  if (lock->get_parent()->is_auth() && lock->is_stable())
    wait_on = SimpleLock::WAIT_RD;
  else
    wait_on = SimpleLock::WAIT_STABLE;  // REQRDLOCK is ignored if lock is unstable, so we need to retry.
  dout(7) << "rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->add_waiter(wait_on, new C_MDS_RetryRequest(mdcache, mut));
  nudge_log(lock);
  return false;
}

void Locker::nudge_log(SimpleLock *lock)
{
  dout(10) << "nudge_log " << *lock << " on " << *lock->get_parent() << dendl;
  if (lock->get_parent()->is_auth() && !lock->is_stable())    // as with xlockdone, or cap flush
    mds->mdlog->flush();
}

void Locker::rdlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue)
{
  // drop ref
  lock->put_rdlock();
  if (mut) {
    mut->rdlocks.erase(lock);
    mut->locks.erase(lock);
  }

  dout(7) << "rdlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;
  
  // last one?
  if (!lock->is_rdlocked()) {
    if (!lock->is_stable())
      eval_gather(lock, false, pneed_issue);
    else if (lock->get_parent()->is_auth())
      try_eval(lock, pneed_issue);
  }
}


bool Locker::can_rdlock_set(set<SimpleLock*>& locks)
{
  dout(10) << "can_rdlock_set " << locks << dendl;
  for (set<SimpleLock*>::iterator p = locks.begin(); p != locks.end(); ++p)
    if (!(*p)->can_rdlock(-1)) {
      dout(10) << "can_rdlock_set can't rdlock " << *p << " on " << *(*p)->get_parent() << dendl;
      return false;
    }
  return true;
}

bool Locker::rdlock_try_set(set<SimpleLock*>& locks)
{
  dout(10) << "rdlock_try_set " << locks << dendl;
  for (set<SimpleLock*>::iterator p = locks.begin(); p != locks.end(); ++p)
    if (!rdlock_try(*p, -1, NULL)) {
      dout(10) << "rdlock_try_set can't rdlock " << *p << " on " << *(*p)->get_parent() << dendl;
      return false;
    }
  return true;
}

void Locker::rdlock_take_set(set<SimpleLock*>& locks, MutationRef& mut)
{
  dout(10) << "rdlock_take_set " << locks << dendl;
  for (set<SimpleLock*>::iterator p = locks.begin(); p != locks.end(); ++p) {
    (*p)->get_rdlock();
    mut->rdlocks.insert(*p);
    mut->locks.insert(*p);
  }
}

// ------------------
// wrlock

void Locker::wrlock_force(SimpleLock *lock, MutationRef& mut)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_grab(static_cast<LocalLock*>(lock), mut);

  dout(7) << "wrlock_force  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->get_wrlock(true);
  mut->wrlocks.insert(lock);
  mut->locks.insert(lock);
}

bool Locker::wrlock_start(SimpleLock *lock, MDRequestRef& mut, bool nowait)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_start(static_cast<LocalLock*>(lock), mut);

  dout(10) << "wrlock_start " << *lock << " on " << *lock->get_parent() << dendl;

  CInode *in = static_cast<CInode *>(lock->get_parent());
  client_t client = mut->get_client();
  bool want_scatter = !nowait && lock->get_parent()->is_auth() &&
		      (in->has_subtree_or_exporting_dirfrag() ||
		       static_cast<ScatterLock*>(lock)->get_scatter_wanted());

  while (1) {
    // wrlock?
    if (lock->can_wrlock(client) &&
	(!want_scatter || lock->get_state() == LOCK_MIX)) {
      lock->get_wrlock();
      mut->wrlocks.insert(lock);
      mut->locks.insert(lock);
      return true;
    }

    if (lock->get_type() == CEPH_LOCK_IFILE &&
	in->state_test(CInode::STATE_RECOVERING)) {
      mds->mdcache->recovery_queue.prioritize(in);
    }

    if (!lock->is_stable())
      break;

    if (in->is_auth()) {
      // don't do nested lock state change if we have dirty scatterdata and
      // may scatter_writebehind or start_scatter, because nowait==true implies
      // that the caller already has a log entry open!
      if (nowait && lock->is_dirty())
	return false;

      if (want_scatter)
	scatter_mix(static_cast<ScatterLock*>(lock));
      else
	simple_lock(lock);

      if (nowait && !lock->can_wrlock(client))
	return false;
      
    } else {
      // replica.
      // auth should be auth_pinned (see acquire_locks wrlock weird mustpin case).
      mds_rank_t auth = lock->get_parent()->authority().first;
      if (mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
	dout(10) << "requesting scatter from auth on "
		 << *lock << " on " << *lock->get_parent() << dendl;
	mds->send_message_mds(new MLock(lock, LOCK_AC_REQSCATTER, mds->get_nodeid()), auth);
      }
      break;
    }
  }

  if (!nowait) {
    dout(7) << "wrlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
    lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
    nudge_log(lock);
  }
    
  return false;
}

void Locker::wrlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_finish(static_cast<LocalLock*>(lock), mut);

  dout(7) << "wrlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->put_wrlock();
  if (mut) {
    mut->wrlocks.erase(lock);
    if (mut->remote_wrlocks.count(lock) == 0)
      mut->locks.erase(lock);
  }

  if (!lock->is_wrlocked()) {
    if (!lock->is_stable())
      eval_gather(lock, false, pneed_issue);
    else if (lock->get_parent()->is_auth())
      try_eval(lock, pneed_issue);
  }
}


// remote wrlock

void Locker::remote_wrlock_start(SimpleLock *lock, mds_rank_t target, MDRequestRef& mut)
{
  dout(7) << "remote_wrlock_start mds." << target << " on " << *lock << " on " << *lock->get_parent() << dendl;

  // wait for active target
  if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(target)) {
    dout(7) << " mds." << target << " is not active" << dendl;
    if (mut->more()->waiting_on_slave.empty())
      mds->wait_for_active_peer(target, new C_MDS_RetryRequest(mdcache, mut));
    return;
  }

  // send lock request
  mut->start_locking(lock, target);
  mut->more()->slaves.insert(target);
  MMDSSlaveRequest *r = new MMDSSlaveRequest(mut->reqid, mut->attempt,
					     MMDSSlaveRequest::OP_WRLOCK);
  r->set_lock_type(lock->get_type());
  lock->get_parent()->set_object_info(r->get_object_info());
  mds->send_message_mds(r, target);

  assert(mut->more()->waiting_on_slave.count(target) == 0);
  mut->more()->waiting_on_slave.insert(target);
}

void Locker::remote_wrlock_finish(SimpleLock *lock, mds_rank_t target,
                                  MutationImpl *mut)
{
  // drop ref
  mut->remote_wrlocks.erase(lock);
  if (mut->wrlocks.count(lock) == 0)
    mut->locks.erase(lock);
  
  dout(7) << "remote_wrlock_finish releasing remote wrlock on mds." << target
	  << " " << *lock->get_parent()  << dendl;
  if (mds->mdsmap->get_state(target) >= MDSMap::STATE_REJOIN) {
    MMDSSlaveRequest *slavereq = new MMDSSlaveRequest(mut->reqid, mut->attempt,
						      MMDSSlaveRequest::OP_UNWRLOCK);
    slavereq->set_lock_type(lock->get_type());
    lock->get_parent()->set_object_info(slavereq->get_object_info());
    mds->send_message_mds(slavereq, target);
  }
}


// ------------------
// xlock

bool Locker::xlock_start(SimpleLock *lock, MDRequestRef& mut)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_xlock_start(static_cast<LocalLock*>(lock), mut);

  dout(7) << "xlock_start on " << *lock << " on " << *lock->get_parent() << dendl;
  client_t client = mut->get_client();

  // auth?
  if (lock->get_parent()->is_auth()) {
    // auth
    while (1) {
      if (lock->can_xlock(client)) {
	lock->set_state(LOCK_XLOCK);
	lock->get_xlock(mut, client);
	mut->xlocks.insert(lock);
	mut->locks.insert(lock);
	mut->finish_locking(lock);
	return true;
      }
      
      if (lock->get_type() == CEPH_LOCK_IFILE) {
	CInode *in = static_cast<CInode*>(lock->get_parent());
	if (in->state_test(CInode::STATE_RECOVERING)) {
	  mds->mdcache->recovery_queue.prioritize(in);
	}
      }

      if (!lock->is_stable() && (lock->get_state() != LOCK_XLOCKDONE ||
				 lock->get_xlock_by_client() != client ||
				 lock->is_waiter_for(SimpleLock::WAIT_STABLE)))
	break;

      if (lock->get_state() == LOCK_LOCK || lock->get_state() == LOCK_XLOCKDONE) {
	mut->start_locking(lock);
	simple_xlock(lock);
      } else {
	simple_lock(lock);
      }
    }
    
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
    nudge_log(lock);
    return false;
  } else {
    // replica
    assert(lock->get_sm()->can_remote_xlock);
    assert(!mut->slave_request);
    
    // wait for single auth
    if (lock->get_parent()->is_ambiguous_auth()) {
      lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
				     new C_MDS_RetryRequest(mdcache, mut));
      return false;
    }
    
    // wait for active auth
    mds_rank_t auth = lock->get_parent()->authority().first;
    if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
      dout(7) << " mds." << auth << " is not active" << dendl;
      if (mut->more()->waiting_on_slave.empty())
	mds->wait_for_active_peer(auth, new C_MDS_RetryRequest(mdcache, mut));
      return false;
    }

    // send lock request
    mut->more()->slaves.insert(auth);
    mut->start_locking(lock, auth);
    MMDSSlaveRequest *r = new MMDSSlaveRequest(mut->reqid, mut->attempt,
					       MMDSSlaveRequest::OP_XLOCK);
    r->set_lock_type(lock->get_type());
    lock->get_parent()->set_object_info(r->get_object_info());
    mds->send_message_mds(r, auth);

    assert(mut->more()->waiting_on_slave.count(auth) == 0);
    mut->more()->waiting_on_slave.insert(auth);

    return false;
  }
}

void Locker::_finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue)
{
  assert(!lock->is_stable());
  if (lock->get_num_rdlocks() == 0 &&
      lock->get_num_wrlocks() == 0 &&
      lock->get_num_client_lease() == 0 &&
      lock->get_type() != CEPH_LOCK_DN) {
    CInode *in = static_cast<CInode*>(lock->get_parent());
    client_t loner = in->get_target_loner();
    if (loner >= 0 && (xlocker < 0 || xlocker == loner)) {
      lock->set_state(LOCK_EXCL);
      lock->get_parent()->auth_unpin(lock);
      lock->finish_waiters(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR|SimpleLock::WAIT_RD);
      if (lock->get_cap_shift())
	*pneed_issue = true;
      if (lock->get_parent()->is_auth() &&
	  lock->is_stable())
	try_eval(lock, pneed_issue);
      return;
    }
  }
  // the xlocker may have CEPH_CAP_GSHARED, need to revoke it if next state is LOCK_LOCK
  eval_gather(lock, true, pneed_issue);
}

void Locker::xlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_xlock_finish(static_cast<LocalLock*>(lock), mut);

  dout(10) << "xlock_finish on " << *lock << " " << *lock->get_parent() << dendl;

  client_t xlocker = lock->get_xlock_by_client();

  // drop ref
  lock->put_xlock();
  assert(mut);
  mut->xlocks.erase(lock);
  mut->locks.erase(lock);
  
  bool do_issue = false;

  // remote xlock?
  if (!lock->get_parent()->is_auth()) {
    assert(lock->get_sm()->can_remote_xlock);

    // tell auth
    dout(7) << "xlock_finish releasing remote xlock on " << *lock->get_parent()  << dendl;
    mds_rank_t auth = lock->get_parent()->authority().first;
    if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
      MMDSSlaveRequest *slavereq = new MMDSSlaveRequest(mut->reqid, mut->attempt,
							MMDSSlaveRequest::OP_UNXLOCK);
      slavereq->set_lock_type(lock->get_type());
      lock->get_parent()->set_object_info(slavereq->get_object_info());
      mds->send_message_mds(slavereq, auth);
    }
    // others waiting?
    lock->finish_waiters(SimpleLock::WAIT_STABLE |
			 SimpleLock::WAIT_WR | 
			 SimpleLock::WAIT_RD, 0); 
  } else {
    if (lock->get_num_xlocks() == 0) {
      if (lock->get_state() == LOCK_LOCK_XLOCK)
	lock->set_state(LOCK_XLOCKDONE);
      _finish_xlock(lock, xlocker, &do_issue);
    }
  }
  
  if (do_issue) {
    CInode *in = static_cast<CInode*>(lock->get_parent());
    if (in->is_head()) {
      if (pneed_issue)
	*pneed_issue = true;
      else
	issue_caps(in);
    }
  }
}

void Locker::xlock_export(SimpleLock *lock, MutationImpl *mut)
{
  dout(10) << "xlock_export on " << *lock << " " << *lock->get_parent() << dendl;

  lock->put_xlock();
  mut->xlocks.erase(lock);
  mut->locks.erase(lock);

  MDSCacheObject *p = lock->get_parent();
  assert(p->state_test(CInode::STATE_AMBIGUOUSAUTH));  // we are exporting this (inode)

  if (!lock->is_stable())
    lock->get_parent()->auth_unpin(lock);

  lock->set_state(LOCK_LOCK);
}

void Locker::xlock_import(SimpleLock *lock)
{
  dout(10) << "xlock_import on " << *lock << " " << *lock->get_parent() << dendl;
  lock->get_parent()->auth_pin(lock);
}



// file i/o -----------------------------------------

version_t Locker::issue_file_data_version(CInode *in)
{
  dout(7) << "issue_file_data_version on " << *in << dendl;
  return in->inode.file_data_version;
}

class C_Locker_FileUpdate_finish : public LockerContext {
  CInode *in;
  MutationRef mut;
  bool share;
  client_t client;
  Capability *cap;
  MClientCaps *ack;
public:
  C_Locker_FileUpdate_finish(Locker *l, CInode *i, MutationRef& m,
				bool e=false, client_t c=-1,
				Capability *cp = 0,
				MClientCaps *ac = 0)
    : LockerContext(l), in(i), mut(m), share(e), client(c), cap(cp),
      ack(ac) {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) {
    locker->file_update_finish(in, mut, share, client, cap, ack);
  }
};

void Locker::file_update_finish(CInode *in, MutationRef& mut, bool share, client_t client,
				Capability *cap, MClientCaps *ack)
{
  dout(10) << "file_update_finish on " << *in << dendl;
  in->pop_and_dirty_projected_inode(mut->ls);
  in->put(CInode::PIN_PTRWAITER);

  mut->apply();
  
  if (ack) {
    Session *session = mds->get_session(client);
    if (session) {
      // "oldest flush tid" > 0 means client uses unique TID for each flush
      if (ack->get_oldest_flush_tid() > 0)
	session->add_completed_flush(ack->get_client_tid());
      mds->send_message_client_counted(ack, session);
    } else {
      dout(10) << " no session for client." << client << " " << *ack << dendl;
      ack->put();
    }
  }

  set<CInode*> need_issue;
  drop_locks(mut.get(), &need_issue);

  if (!in->is_head() && !in->client_snap_caps.empty()) {
    dout(10) << " client_snap_caps " << in->client_snap_caps << dendl;
    // check for snap writeback completion
    bool gather = false;
    compact_map<int,set<client_t> >::iterator p = in->client_snap_caps.begin();
    while (p != in->client_snap_caps.end()) {
      SimpleLock *lock = in->get_lock(p->first);
      assert(lock);
      dout(10) << " completing client_snap_caps for " << ccap_string(p->first)
	       << " lock " << *lock << " on " << *in << dendl;
      lock->put_wrlock();

      p->second.erase(client);
      if (p->second.empty()) {
	gather = true;
	in->client_snap_caps.erase(p++);
      } else
	++p;
    }
    if (gather) {
      if (in->client_snap_caps.empty())
	in->item_open_file.remove_myself();
      eval_cap_gather(in, &need_issue);
    }
  } else {
    if (cap && (cap->wanted() & ~cap->pending()) &&
	need_issue.count(in) == 0) {  // if we won't issue below anyway
      issue_caps(in, cap);
    }
  
    if (share && in->is_auth() &&
	(in->filelock.gcaps_allowed(CAP_LONER) & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER)))
      share_inode_max_size(in);
  }
  issue_caps_set(need_issue);

  // auth unpin after issuing caps
  mut->cleanup();
}

Capability* Locker::issue_new_caps(CInode *in,
				   int mode,
				   Session *session,
				   SnapRealm *realm,
				   bool is_replay)
{
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << dendl;
  bool is_new;

  // if replay, try to reconnect cap, and otherwise do nothing.
  if (is_replay) {
    mds->mdcache->try_reconnect_cap(in, session);
    return 0;
  }

  // my needs
  assert(session->info.inst.name.is_client());
  int my_client = session->info.inst.name.num();
  int my_want = ceph_caps_for_mode(mode);

  // register a capability
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
    // new cap
    cap = in->add_client_cap(my_client, session, realm);
    cap->set_wanted(my_want);
    cap->mark_new();
    cap->inc_suppress(); // suppress file cap messages for new cap (we'll bundle with the open() reply)
    is_new = true;
  } else {
    is_new = false;
    // make sure it wants sufficient caps
    if (my_want & ~cap->wanted()) {
      // augment wanted caps for this client
      cap->set_wanted(cap->wanted() | my_want);
    }
  }

  if (in->is_auth()) {
    // [auth] twiddle mode?
    eval(in, CEPH_CAP_LOCKS);

    if (!in->filelock.is_stable() ||
	!in->authlock.is_stable() ||
	!in->linklock.is_stable() ||
	!in->xattrlock.is_stable())
      mds->mdlog->flush();

  } else {
    // [replica] tell auth about any new caps wanted
    request_inode_file_caps(in);
  }

  // issue caps (pot. incl new one)
  //issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  //cap->issue(cap->pending());

  if (is_new)
    cap->dec_suppress();

  return cap;
}


void Locker::issue_caps_set(set<CInode*>& inset)
{
  for (set<CInode*>::iterator p = inset.begin(); p != inset.end(); ++p)
    issue_caps(*p);
}

bool Locker::issue_caps(CInode *in, Capability *only_cap)
{
  // allowed caps are determined by the lock mode.
  int all_allowed = in->get_caps_allowed_by_type(CAP_ANY);
  int loner_allowed = in->get_caps_allowed_by_type(CAP_LONER);
  int xlocker_allowed = in->get_caps_allowed_by_type(CAP_XLOCKER);

  client_t loner = in->get_loner();
  if (loner >= 0) {
    dout(7) << "issue_caps loner client." << loner
	    << " allowed=" << ccap_string(loner_allowed) 
	    << ", xlocker allowed=" << ccap_string(xlocker_allowed)
	    << ", others allowed=" << ccap_string(all_allowed)
	    << " on " << *in << dendl;
  } else {
    dout(7) << "issue_caps allowed=" << ccap_string(all_allowed) 
	    << ", xlocker allowed=" << ccap_string(xlocker_allowed)
	    << " on " << *in << dendl;
  }

  assert(in->is_head());

  // count conflicts with
  int nissued = 0;        

  // client caps
  map<client_t, Capability*>::iterator it;
  if (only_cap)
    it = in->client_caps.find(only_cap->get_client());
  else
    it = in->client_caps.begin();
  for (; it != in->client_caps.end(); ++it) {
    Capability *cap = it->second;
    if (cap->is_stale())
      continue;

    // do not issue _new_ bits when size|mtime is projected
    int allowed;
    if (loner == it->first)
      allowed = loner_allowed;
    else
      allowed = all_allowed;

    // add in any xlocker-only caps (for locks this client is the xlocker for)
    allowed |= xlocker_allowed & in->get_xlocker_mask(it->first);

    Session *session = mds->get_session(it->first);
    if (in->inode.inline_data.version != CEPH_INLINE_NONE &&
	!(session && session->connection &&
	  session->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)))
      allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);

    int pending = cap->pending();
    int wanted = cap->wanted();

    dout(20) << " client." << it->first
	     << " pending " << ccap_string(pending) 
	     << " allowed " << ccap_string(allowed) 
	     << " wanted " << ccap_string(wanted)
	     << dendl;

    // skip if suppress, and not revocation
    if (cap->is_suppress() && !(pending & ~allowed)) {
      dout(20) << "  suppressed and !revoke, skipping client." << it->first << dendl;
      continue;
    }

    // notify clients about deleted inode, to make sure they release caps ASAP.
    if (in->inode.nlink == 0)
      wanted |= CEPH_CAP_LINK_SHARED;

    // are there caps that the client _wants_ and can have, but aren't pending?
    // or do we need to revoke?
    if (((wanted & allowed) & ~pending) ||  // missing wanted+allowed caps
	(pending & ~allowed)) {             // need to revoke ~allowed caps.
      // issue
      nissued++;

      // include caps that clients generally like, while we're at it.
      int likes = in->get_caps_liked();      
      int before = pending;
      long seq;
      if (pending & ~allowed)
	seq = cap->issue((wanted|likes) & allowed & pending);  // if revoking, don't issue anything new.
      else
	seq = cap->issue((wanted|likes) & allowed);
      int after = cap->pending();

      if (cap->is_new()) {
	// haven't send caps to client yet
	if (before & ~after)
	  cap->confirm_receipt(seq, after);
      } else {
        dout(7) << "   sending MClientCaps to client." << it->first
		<< " seq " << cap->get_last_seq()
		<< " new pending " << ccap_string(after) << " was " << ccap_string(before) 
		<< dendl;

	int op = (before & ~after) ? CEPH_CAP_OP_REVOKE : CEPH_CAP_OP_GRANT;
	if (op == CEPH_CAP_OP_REVOKE) {
		revoking_caps.push_back(&cap->item_revoking_caps);
		revoking_caps_by_client[cap->get_client()].push_back(&cap->item_client_revoking_caps);
		cap->set_last_revoke_stamp(ceph_clock_now(g_ceph_context));
		cap->reset_num_revoke_warnings();
	}

	MClientCaps *m = new MClientCaps(op, in->ino(),
					 in->find_snaprealm()->inode->ino(),
					 cap->get_cap_id(), cap->get_last_seq(),
					 after, wanted, 0,
					 cap->get_mseq(),
                                         mds->get_osd_epoch_barrier());
	in->encode_cap_message(m, cap);

	mds->send_message_client_counted(m, it->first);
      }
    }

    if (only_cap)
      break;
  }

  return (nissued == 0);  // true if no re-issued, no callbacks
}

void Locker::issue_truncate(CInode *in)
{
  dout(7) << "issue_truncate on " << *in << dendl;
  
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) {
    Capability *cap = it->second;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_TRUNC,
				     in->ino(),
				     in->find_snaprealm()->inode->ino(),
				     cap->get_cap_id(), cap->get_last_seq(),
				     cap->pending(), cap->wanted(), 0,
				     cap->get_mseq(),
                                     mds->get_osd_epoch_barrier());
    in->encode_cap_message(m, cap);			     
    mds->send_message_client_counted(m, it->first);
  }

  // should we increase max_size?
  if (in->is_auth() && in->is_file())
    check_inode_max_size(in);
}

void Locker::revoke_stale_caps(Session *session)
{
  dout(10) << "revoke_stale_caps for " << session->info.inst.name << dendl;
  client_t client = session->get_client();

  for (xlist<Capability*>::iterator p = session->caps.begin(); !p.end(); ++p) {
    Capability *cap = *p;
    cap->mark_stale();
    CInode *in = cap->get_inode();
    int issued = cap->issued();
    if (issued & ~CEPH_CAP_PIN) {
      dout(10) << " revoking " << ccap_string(issued) << " on " << *in << dendl;      
      cap->revoke();

      if (in->is_auth() &&
	  in->inode.client_ranges.count(client))
	in->state_set(CInode::STATE_NEEDSRECOVER);

      if (!in->filelock.is_stable()) eval_gather(&in->filelock);
      if (!in->linklock.is_stable()) eval_gather(&in->linklock);
      if (!in->authlock.is_stable()) eval_gather(&in->authlock);
      if (!in->xattrlock.is_stable()) eval_gather(&in->xattrlock);

      if (in->is_auth()) {
	try_eval(in, CEPH_CAP_LOCKS);
      } else {
	request_inode_file_caps(in);
      }
    } else {
      dout(10) << " nothing issued on " << *in << dendl;
    }
  }
}

void Locker::resume_stale_caps(Session *session)
{
  dout(10) << "resume_stale_caps for " << session->info.inst.name << dendl;

  for (xlist<Capability*>::iterator p = session->caps.begin(); !p.end(); ++p) {
    Capability *cap = *p;
    CInode *in = cap->get_inode();
    assert(in->is_head());
    if (cap->is_stale()) {
      dout(10) << " clearing stale flag on " << *in << dendl;
      cap->clear_stale();
      if (!in->is_auth() || !eval(in, CEPH_CAP_LOCKS))
	issue_caps(in, cap);
    }
  }
}

void Locker::remove_stale_leases(Session *session)
{
  dout(10) << "remove_stale_leases for " << session->info.inst.name << dendl;
  xlist<ClientLease*>::iterator p = session->leases.begin();
  while (!p.end()) {
    ClientLease *l = *p;
    ++p;
    CDentry *parent = static_cast<CDentry*>(l->parent);
    dout(15) << " removing lease on " << *parent << dendl;
    parent->remove_client_lease(l, this);
  }
}


class C_MDL_RequestInodeFileCaps : public LockerContext {
  CInode *in;
public:
  C_MDL_RequestInodeFileCaps(Locker *l, CInode *i) : LockerContext(l), in(i) {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) {
    in->put(CInode::PIN_PTRWAITER);
    if (!in->is_auth())
      locker->request_inode_file_caps(in);
  }
};

void Locker::request_inode_file_caps(CInode *in)
{
  assert(!in->is_auth());

  int wanted = in->get_caps_wanted() & ~CEPH_CAP_PIN;
  if (wanted != in->replica_caps_wanted) {
    // wait for single auth
    if (in->is_ambiguous_auth()) {
      in->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
                     new C_MDL_RequestInodeFileCaps(this, in));
      return;
    }

    mds_rank_t auth = in->authority().first;
    if (mds->mdsmap->get_state(auth) == MDSMap::STATE_REJOIN) {
      mds->wait_for_active_peer(auth, new C_MDL_RequestInodeFileCaps(this, in));
      return;
    }

    dout(7) << "request_inode_file_caps " << ccap_string(wanted)
            << " was " << ccap_string(in->replica_caps_wanted) 
            << " on " << *in << " to mds." << auth << dendl;

    in->replica_caps_wanted = wanted;

    if (mds->mdsmap->is_clientreplay_or_active_or_stopping(auth))
      mds->send_message_mds(new MInodeFileCaps(in->ino(), in->replica_caps_wanted),
			    auth);
  }
}

/* This function DOES put the passed message before returning */
void Locker::handle_inode_file_caps(MInodeFileCaps *m)
{
  // nobody should be talking to us during recovery.
  assert(mds->is_clientreplay() || mds->is_active() || mds->is_stopping());

  // ok
  CInode *in = mdcache->get_inode(m->get_ino());
  mds_rank_t from = mds_rank_t(m->get_source().num());

  assert(in);
  assert(in->is_auth());

  dout(7) << "handle_inode_file_caps replica mds." << from << " wants caps " << ccap_string(m->get_caps()) << " on " << *in << dendl;

  if (m->get_caps())
    in->mds_caps_wanted[from] = m->get_caps();
  else
    in->mds_caps_wanted.erase(from);

  try_eval(in, CEPH_CAP_LOCKS);
  m->put();
}


class C_MDL_CheckMaxSize : public LockerContext {
  CInode *in;
  bool update_size;
  uint64_t newsize;
  bool update_max;
  uint64_t new_max_size;
  utime_t mtime;

public:
  C_MDL_CheckMaxSize(Locker *l, CInode *i, bool _update_size, uint64_t _newsize,
                     bool _update_max, uint64_t _new_max_size, utime_t _mtime) :
    LockerContext(l), in(i),
    update_size(_update_size), newsize(_newsize),
    update_max(_update_max), new_max_size(_new_max_size),
    mtime(_mtime)
  {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) {
    in->put(CInode::PIN_PTRWAITER);
    if (in->is_auth())
      locker->check_inode_max_size(in, false, update_size, newsize,
                                   update_max, new_max_size, mtime);
  }
};


void Locker::calc_new_client_ranges(CInode *in, uint64_t size, map<client_t,client_writeable_range_t>& new_ranges)
{
  inode_t *latest = in->get_projected_inode();
  uint64_t ms;
  if(latest->has_layout()) {
    ms = ROUND_UP_TO((size+1)<<1, latest->get_layout_size_increment());
  } else {
    // Layout-less directories like ~mds0/, have zero size
    ms = 0;
  }

  // increase ranges as appropriate.
  // shrink to 0 if no WR|BUFFER caps issued.
  for (map<client_t,Capability*>::iterator p = in->client_caps.begin();
       p != in->client_caps.end();
       ++p) {
    if ((p->second->issued() | p->second->wanted()) & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER)) {
      client_writeable_range_t& nr = new_ranges[p->first];
      nr.range.first = 0;
      if (latest->client_ranges.count(p->first)) {
	client_writeable_range_t& oldr = latest->client_ranges[p->first];
	nr.range.last = MAX(ms, oldr.range.last);
	nr.follows = oldr.follows;
      } else {
	nr.range.last = ms;
	nr.follows = in->first - 1;
      }
    }
  }
}

bool Locker::check_inode_max_size(CInode *in, bool force_wrlock,
				  bool update_size, uint64_t new_size,
				  bool update_max, uint64_t new_max_size,
				  utime_t new_mtime)
{
  assert(in->is_auth());
  assert(in->is_file());

  inode_t *latest = in->get_projected_inode();
  map<client_t, client_writeable_range_t> new_ranges;
  uint64_t size = latest->size;
  bool new_max = update_max;

  if (update_size) {
    new_size = size = MAX(size, new_size);
    new_mtime = MAX(new_mtime, latest->mtime);
    if (latest->size == new_size && latest->mtime == new_mtime)
      update_size = false;
  }

  uint64_t client_range_size = update_max ? new_max_size : size;

  calc_new_client_ranges(in, client_range_size, new_ranges);

  if (latest->client_ranges != new_ranges)
    new_max = true;

  if (!update_size && !new_max) {
    dout(20) << "check_inode_max_size no-op on " << *in << dendl;
    return false;
  }

  dout(10) << "check_inode_max_size new_ranges " << new_ranges
	   << " update_size " << update_size
	   << " on " << *in << dendl;

  if (in->is_frozen()) {
    dout(10) << "check_inode_max_size frozen, waiting on " << *in << dendl;
    C_MDL_CheckMaxSize *cms = new C_MDL_CheckMaxSize(this, in,
                                                     update_size, new_size,
                                                     update_max, new_max_size,
                                                     new_mtime);
    in->add_waiter(CInode::WAIT_UNFREEZE, cms);
    return false;
  }
  if (!force_wrlock && !in->filelock.can_wrlock(in->get_loner())) {
    // lock?
    if (in->filelock.is_stable()) {
      if (in->get_target_loner() >= 0)
	file_excl(&in->filelock);
      else
	simple_lock(&in->filelock);
    }
    if (!in->filelock.can_wrlock(in->get_loner())) {
      // try again later
      C_MDL_CheckMaxSize *cms = new C_MDL_CheckMaxSize(this, in,
                                                       update_size, new_size,
                                                       update_max, new_max_size,
                                                       new_mtime);

      in->filelock.add_waiter(SimpleLock::WAIT_STABLE, cms);
      dout(10) << "check_inode_max_size can't wrlock, waiting on " << *in << dendl;
      return false;    
    }
  }

  MutationRef mut(new MutationImpl);
  mut->ls = mds->mdlog->get_current_segment();
    
  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();

  if (new_max) {
    dout(10) << "check_inode_max_size client_ranges " << pi->client_ranges << " -> " << new_ranges << dendl;
    pi->client_ranges = new_ranges;
  }

  if (update_size) {
    dout(10) << "check_inode_max_size size " << pi->size << " -> " << new_size << dendl;
    pi->size = new_size;
    pi->rstat.rbytes = new_size;
    dout(10) << "check_inode_max_size mtime " << pi->mtime << " -> " << new_mtime << dendl;
    pi->mtime = new_mtime;
  }

  // use EOpen if the file is still open; otherwise, use EUpdate.
  // this is just an optimization to push open files forward into
  // newer log segments.
  LogEvent *le;
  EMetaBlob *metablob;
  if (in->is_any_caps_wanted() && in->last == CEPH_NOSNAP) {   
    EOpen *eo = new EOpen(mds->mdlog);
    eo->add_ino(in->ino());
    metablob = &eo->metablob;
    le = eo;
    mut->ls->open_files.push_back(&in->item_open_file);
  } else {
    EUpdate *eu = new EUpdate(mds->mdlog, "check_inode_max_size");
    metablob = &eu->metablob;
    le = eu;
  }
  mds->mdlog->start_entry(le);
  if (update_size) {  // FIXME if/when we do max_size nested accounting
    mdcache->predirty_journal_parents(mut, metablob, in, 0, PREDIRTY_PRIMARY);
    // no cow, here!
    CDentry *parent = in->get_projected_parent_dn();
    metablob->add_primary_dentry(parent, in, true);
  } else {
    metablob->add_dir_context(in->get_projected_parent_dn()->get_dir());
    mdcache->journal_dirty_inode(mut.get(), metablob, in);
  }
  mds->mdlog->submit_entry(le,
          new C_Locker_FileUpdate_finish(this, in, mut, true));
  wrlock_force(&in->filelock, mut);  // wrlock for duration of journal
  mut->auth_pin(in);

  // make max_size _increase_ timely
  if (new_max)
    mds->mdlog->flush();

  return true;
}


void Locker::share_inode_max_size(CInode *in, Capability *only_cap)
{
  /*
   * only share if currently issued a WR cap.  if client doesn't have it,
   * file_max doesn't matter, and the client will get it if/when they get
   * the cap later.
   */
  dout(10) << "share_inode_max_size on " << *in << dendl;
  map<client_t, Capability*>::iterator it;
  if (only_cap)
    it = in->client_caps.find(only_cap->get_client());
  else
    it = in->client_caps.begin();
  for (; it != in->client_caps.end(); ++it) {
    const client_t client = it->first;
    Capability *cap = it->second;
    if (cap->is_suppress())
      continue;
    if (cap->pending() & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER)) {
      dout(10) << "share_inode_max_size with client." << client << dendl;
      cap->inc_last_seq();
      MClientCaps *m = new MClientCaps(CEPH_CAP_OP_GRANT,
				       in->ino(),
				       in->find_snaprealm()->inode->ino(),
				       cap->get_cap_id(), cap->get_last_seq(),
				       cap->pending(), cap->wanted(), 0,
                                       cap->get_mseq(),
                                       mds->get_osd_epoch_barrier());
      in->encode_cap_message(m, cap);
      mds->send_message_client_counted(m, client);
    }
    if (only_cap)
      break;
  }
}

void Locker::adjust_cap_wanted(Capability *cap, int wanted, int issue_seq)
{
  if (ceph_seq_cmp(issue_seq, cap->get_last_issue()) == 0) {
    dout(10) << " wanted " << ccap_string(cap->wanted())
	     << " -> " << ccap_string(wanted) << dendl;
    cap->set_wanted(wanted);
  } else if (wanted & ~cap->wanted()) {
    dout(10) << " wanted " << ccap_string(cap->wanted())
	     << " -> " << ccap_string(wanted)
	     << " (added caps even though we had seq mismatch!)" << dendl;
    cap->set_wanted(wanted | cap->wanted());
  } else {
    dout(10) << " NOT changing wanted " << ccap_string(cap->wanted())
	     << " -> " << ccap_string(wanted)
	     << " (issue_seq " << issue_seq << " != last_issue "
	     << cap->get_last_issue() << ")" << dendl;
    return;
  }

  CInode *cur = cap->get_inode();
  if (!cur->is_auth()) {
    request_inode_file_caps(cur);
    return;
  }

  if (cap->wanted() == 0) {
    if (cur->item_open_file.is_on_list() &&
	!cur->is_any_caps_wanted()) {
      dout(10) << " removing unwanted file from open file list " << *cur << dendl;
      cur->item_open_file.remove_myself();
    }
  } else {
    if (cur->state_test(CInode::STATE_RECOVERING) &&
	(cap->wanted() & (CEPH_CAP_FILE_RD |
			  CEPH_CAP_FILE_WR))) {
      mds->mdcache->recovery_queue.prioritize(cur);
    }

    if (!cur->item_open_file.is_on_list()) {
      dout(10) << " adding to open file list " << *cur << dendl;
      assert(cur->last == CEPH_NOSNAP);
      LogSegment *ls = mds->mdlog->get_current_segment();
      EOpen *le = new EOpen(mds->mdlog);
      mds->mdlog->start_entry(le);
      le->add_clean_inode(cur);
      ls->open_files.push_back(&cur->item_open_file);
      mds->mdlog->submit_entry(le);
    }
  }
}



void Locker::_do_null_snapflush(CInode *head_in, client_t client)
{
  dout(10) << "_do_null_snapflush client." << client << " on " << *head_in << dendl;
  compact_map<snapid_t, set<client_t> >::iterator p = head_in->client_need_snapflush.begin();
  while (p != head_in->client_need_snapflush.end()) {
    snapid_t snapid = p->first;
    set<client_t>& clients = p->second;
    ++p;  // be careful, q loop below depends on this

    if (clients.count(client)) {
      dout(10) << " doing async NULL snapflush on " << snapid << " from client." << client << dendl;
      CInode *sin = mdcache->get_inode(head_in->ino(), snapid);
      if (!sin) {
	// hrm, look forward until we find the inode. 
	//  (we can only look it up by the last snapid it is valid for)
	dout(10) << " didn't have " << head_in->ino() << " snapid " << snapid << dendl;
	for (compact_map<snapid_t, set<client_t> >::iterator q = p;  // p is already at next entry
	     q != head_in->client_need_snapflush.end();
	     ++q) {
	  dout(10) << " trying snapid " << q->first << dendl;
	  sin = mdcache->get_inode(head_in->ino(), q->first);
	  if (sin) {
	    assert(sin->first <= snapid);
	    break;
	  }
	  dout(10) << " didn't have " << head_in->ino() << " snapid " << q->first << dendl;
	}
	if (!sin && head_in->is_multiversion())
	  sin = head_in;
	assert(sin);
      }
      _do_snap_update(sin, snapid, 0, sin->first - 1, client, NULL, NULL);
      head_in->remove_need_snapflush(sin, snapid, client);
    }
  }
}


bool Locker::should_defer_client_cap_frozen(CInode *in)
{
  /*
   * This policy needs to be AT LEAST as permissive as allowing a client request
   * to go forward, or else a client request can release something, the release
   * gets deferred, but the request gets processed and deadlocks because when the
   * caps can't get revoked.
   *
   * Currently, a request wait if anything locked is freezing (can't
   * auth_pin), which would avoid any deadlock with cap release.  Thus @in
   * _MUST_ be in the lock/auth_pin set.
   *
   * auth_pins==0 implies no unstable lock and not auth pinnned by
   * client request, otherwise continue even it's freezing.
   */
  return (in->is_freezing() && in->get_num_auth_pins() == 0) || in->is_frozen();
}

/*
 * This function DOES put the passed message before returning
 */
void Locker::handle_client_caps(MClientCaps *m)
{
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  client_t client = m->get_source().num();

  snapid_t follows = m->get_snap_follows();
  dout(7) << "handle_client_caps on " << m->get_ino()
	  << " tid " << m->get_client_tid() << " follows " << follows
	  << " op " << ceph_cap_op_name(m->get_op()) << dendl;

  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    if (mds->is_reconnect() &&
	m->get_dirty() && m->get_client_tid() > 0 &&
	!session->have_completed_flush(m->get_client_tid())) {
      mdcache->set_reconnected_dirty_caps(client, m->get_ino(), m->get_dirty());
    }
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (m->get_client_tid() > 0 &&
      session->have_completed_flush(m->get_client_tid())) {
    dout(7) << "handle_client_caps already flushed tid " << m->get_client_tid()
	    << " for client." << client << dendl;
    MClientCaps *ack;
    if (m->get_op() == CEPH_CAP_OP_FLUSHSNAP) {
      ack = new MClientCaps(CEPH_CAP_OP_FLUSHSNAP_ACK, m->get_ino(), 0, 0, 0, 0, 0,
			    m->get_dirty(), 0, mds->get_osd_epoch_barrier());
    } else {
      ack = new MClientCaps(CEPH_CAP_OP_FLUSH_ACK, m->get_ino(), 0, m->get_cap_id(),
			    m->get_seq(), m->get_caps(), 0, m->get_dirty(), 0,
			    mds->get_osd_epoch_barrier());
    }
    ack->set_snap_follows(follows);
    ack->set_client_tid(m->get_client_tid());
    mds->send_message_client_counted(ack, m->get_connection());
    if (m->get_op() == CEPH_CAP_OP_FLUSHSNAP) {
      m->put();
      return;
    } else {
      // fall-thru because the message may release some caps
      m->clear_dirty();
      m->set_op(CEPH_CAP_OP_UPDATE);
    }
  }

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (m->get_oldest_flush_tid() > 0) {
    if (session->trim_completed_flushes(m->get_oldest_flush_tid())) {
      mds->mdlog->get_current_segment()->touched_sessions.insert(session->info.inst.name);

      if (session->get_num_trim_flushes_warnings() > 0 &&
	  session->get_num_completed_flushes() * 2 < g_conf->mds_max_completed_flushes)
	session->reset_num_trim_flushes_warnings();
    } else {
      if (session->get_num_completed_flushes() >=
	  (g_conf->mds_max_completed_flushes << session->get_num_trim_flushes_warnings())) {
	session->inc_num_trim_flushes_warnings();
	stringstream ss;
	ss << "client." << session->get_client() << " does not advance its oldest_flush_tid ("
	   << m->get_oldest_flush_tid() << "), "
	   << session->get_num_completed_flushes()
	   << " completed flushes recorded in session\n";
	mds->clog->warn() << ss.str();
	dout(20) << __func__ << " " << ss.str() << dendl;
      }
    }
  }

  CInode *head_in = mdcache->get_inode(m->get_ino());
  if (!head_in) {
    if (mds->is_clientreplay()) {
      dout(7) << "handle_client_caps on unknown ino " << m->get_ino()
	<< ", will try again after replayed client requests" << dendl;
      mdcache->wait_replay_cap_reconnect(m->get_ino(), new C_MDS_RetryMessage(mds, m));
      return;
    }
    dout(1) << "handle_client_caps on unknown ino " << m->get_ino() << ", dropping" << dendl;
    m->put();
    return;
  }

  if (m->osd_epoch_barrier && !mds->objecter->have_map(m->osd_epoch_barrier)) {
    // Pause RADOS operations until we see the required epoch
    mds->objecter->set_epoch_barrier(m->osd_epoch_barrier);
  }

  if (mds->get_osd_epoch_barrier() < m->osd_epoch_barrier) {
    // Record the barrier so that we will retransmit it to clients
    mds->set_osd_epoch_barrier(m->osd_epoch_barrier);
  }

  CInode *in = head_in;
  if (follows > 0) {
    in = mdcache->pick_inode_snap(head_in, follows);
    if (in != head_in)
      dout(10) << " head inode " << *head_in << dendl;
  }
  dout(10) << "  cap inode " << *in << dendl;

  Capability *cap = 0;
  cap = in->get_client_cap(client);
  if (!cap && in != head_in)
    cap = head_in->get_client_cap(client);
  if (!cap) {
    dout(7) << "handle_client_caps no cap for client." << client << " on " << *in << dendl;
    m->put();
    return;
  }  
  assert(cap);

  // freezing|frozen?
  if (should_defer_client_cap_frozen(in)) {
    dout(7) << "handle_client_caps freezing|frozen on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, m));
    return;
  }
  if (ceph_seq_cmp(m->get_mseq(), cap->get_mseq()) < 0) {
    dout(7) << "handle_client_caps mseq " << m->get_mseq() << " < " << cap->get_mseq()
	    << ", dropping" << dendl;
    m->put();
    return;
  }

  int op = m->get_op();

  // flushsnap?
  if (op == CEPH_CAP_OP_FLUSHSNAP) {
    if (!in->is_auth()) {
      dout(7) << " not auth, ignoring flushsnap on " << *in << dendl;
      goto out;
    }

    SnapRealm *realm = in->find_snaprealm();
    snapid_t snap = realm->get_snap_following(follows);
    dout(10) << "  flushsnap follows " << follows << " -> snap " << snap << dendl;

    // we can prepare the ack now, since this FLUSHEDSNAP is independent of any
    // other cap ops.  (except possibly duplicate FLUSHSNAP requests, but worst
    // case we get a dup response, so whatever.)
    MClientCaps *ack = 0;
    if (m->get_dirty()) {
      ack = new MClientCaps(CEPH_CAP_OP_FLUSHSNAP_ACK, in->ino(), 0, 0, 0, 0, 0, m->get_dirty(), 0, mds->get_osd_epoch_barrier());
      ack->set_snap_follows(follows);
      ack->set_client_tid(m->get_client_tid());
      ack->set_oldest_flush_tid(m->get_oldest_flush_tid());
    }

    if (in == head_in ||
	(head_in->client_need_snapflush.count(snap) &&
	 head_in->client_need_snapflush[snap].count(client))) {
      dout(7) << " flushsnap snap " << snap
	      << " client." << client << " on " << *in << dendl;

      // this cap now follows a later snap (i.e. the one initiating this flush, or later)
      if (in == head_in)
	cap->client_follows = snap < CEPH_NOSNAP ? snap : realm->get_newest_seq();
   
      _do_snap_update(in, snap, m->get_dirty(), follows, client, m, ack);

      if (in != head_in)
	head_in->remove_need_snapflush(in, snap, client);
      
    } else {
      dout(7) << " not expecting flushsnap " << snap << " from client." << client << " on " << *in << dendl;
      if (ack)
	mds->send_message_client_counted(ack, m->get_connection());
    }
    goto out;
  }

  if (cap->get_cap_id() != m->get_cap_id()) {
    dout(7) << " ignoring client capid " << m->get_cap_id() << " != my " << cap->get_cap_id() << dendl;
  } else {
    // intermediate snap inodes
    while (in != head_in) {
      assert(in->last != CEPH_NOSNAP);
      if (in->is_auth() && m->get_dirty()) {
	dout(10) << " updating intermediate snapped inode " << *in << dendl;
	_do_cap_update(in, NULL, m->get_dirty(), follows, m, NULL);
      }
      in = mdcache->pick_inode_snap(head_in, in->last);
    }
 
    // head inode, and cap
    MClientCaps *ack = 0;

    int caps = m->get_caps();
    if (caps & ~cap->issued()) {
      dout(10) << " confirming not issued caps " << ccap_string(caps & ~cap->issued()) << dendl;
      caps &= cap->issued();
    }
    
    cap->confirm_receipt(m->get_seq(), caps);
    dout(10) << " follows " << follows
	     << " retains " << ccap_string(m->get_caps())
	     << " dirty " << ccap_string(m->get_dirty())
	     << " on " << *in << dendl;


    // missing/skipped snapflush?
    //  The client MAY send a snapflush if it is issued WR/EXCL caps, but
    //  presently only does so when it has actual dirty metadata.  But, we
    //  set up the need_snapflush stuff based on the issued caps.
    //  We can infer that the client WONT send a FLUSHSNAP once they have
    //  released all WR/EXCL caps (the FLUSHSNAP always comes before the cap
    //  update/release).
    if (!head_in->client_need_snapflush.empty()) {
      if ((cap->issued() & CEPH_CAP_ANY_FILE_WR) == 0) {
	_do_null_snapflush(head_in, client);
      } else {
	dout(10) << " revocation in progress, not making any conclusions about null snapflushes" << dendl;
      }
    }
    
    if (m->get_dirty() && in->is_auth()) {
      dout(7) << " flush client." << client << " dirty " << ccap_string(m->get_dirty()) 
	      << " seq " << m->get_seq() << " on " << *in << dendl;
      ack = new MClientCaps(CEPH_CAP_OP_FLUSH_ACK, in->ino(), 0, cap->get_cap_id(), m->get_seq(),
			    m->get_caps(), 0, m->get_dirty(), 0, mds->get_osd_epoch_barrier());
      ack->set_client_tid(m->get_client_tid());
      ack->set_oldest_flush_tid(m->get_oldest_flush_tid());
    }

    // filter wanted based on what we could ever give out (given auth/replica status)
    int new_wanted = m->get_wanted() & head_in->get_caps_allowed_ever();
    if (new_wanted != cap->wanted()) {
      if (new_wanted & ~cap->wanted()) {
	// exapnding caps.  make sure we aren't waiting for a log flush
	if (!in->filelock.is_stable() ||
	    !in->authlock.is_stable() ||
	    !in->xattrlock.is_stable())
	  mds->mdlog->flush();
      }

      adjust_cap_wanted(cap, new_wanted, m->get_issue_seq());
    }
      
    if (in->is_auth() &&
	_do_cap_update(in, cap, m->get_dirty(), follows, m, ack)) {
      // updated
      eval(in, CEPH_CAP_LOCKS);
      
      if (cap->wanted() & ~cap->pending())
	mds->mdlog->flush();
    } else {
      // no update, ack now.
      if (ack)
	mds->send_message_client_counted(ack, m->get_connection());
      
      bool did_issue = eval(in, CEPH_CAP_LOCKS);
      if (!did_issue && (cap->wanted() & ~cap->pending()))
	issue_caps(in, cap);
      if (cap->get_last_seq() == 0 &&
	  (cap->pending() & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER))) {
	cap->issue_norevoke(cap->issued());
	share_inode_max_size(in, cap);
      }
    }
  }

 out:
  m->put();
}


class C_Locker_RetryRequestCapRelease : public LockerContext {
  client_t client;
  ceph_mds_request_release item;
public:
  C_Locker_RetryRequestCapRelease(Locker *l, client_t c, const ceph_mds_request_release& it) :
    LockerContext(l), client(c), item(it) { }
  void finish(int r) {
    string dname;
    MDRequestRef null_ref;
    locker->process_request_cap_release(null_ref, client, item, dname);
  }
};

void Locker::process_request_cap_release(MDRequestRef& mdr, client_t client, const ceph_mds_request_release& item,
					 const string &dname)
{
  inodeno_t ino = (uint64_t)item.ino;
  uint64_t cap_id = item.cap_id;
  int caps = item.caps;
  int wanted = item.wanted;
  int seq = item.seq;
  int issue_seq = item.issue_seq;
  int mseq = item.mseq;

  CInode *in = mdcache->get_inode(ino);
  if (!in)
    return;

  if (dname.length()) {
    frag_t fg = in->pick_dirfrag(dname);
    CDir *dir = in->get_dirfrag(fg);
    if (dir) {
      CDentry *dn = dir->lookup(dname);
      if (dn) {
	ClientLease *l = dn->get_client_lease(client);
	if (l) {
	  dout(10) << "process_cap_release removing lease on " << *dn << dendl;
	  dn->remove_client_lease(l, this);
	} else {
	  dout(7) << "process_cap_release client." << client
		  << " doesn't have lease on " << *dn << dendl;
	}
      } else {
	dout(7) << "process_cap_release client." << client << " released lease on dn "
		<< dir->dirfrag() << "/" << dname << " which dne" << dendl;
      }
    }
  }

  Capability *cap = in->get_client_cap(client);
  if (!cap)
    return;

  dout(10) << "process_cap_release client." << client << " " << ccap_string(caps) << " on " << *in
	   << (mdr ? "" : " (DEFERRED, no mdr)")
	   << dendl;
    
  if (ceph_seq_cmp(mseq, cap->get_mseq()) < 0) {
    dout(7) << " mseq " << mseq << " < " << cap->get_mseq() << ", dropping" << dendl;
    return;
  }

  if (cap->get_cap_id() != cap_id) {
    dout(7) << " cap_id " << cap_id << " != " << cap->get_cap_id() << ", dropping" << dendl;
    return;
  }

  if (should_defer_client_cap_frozen(in)) {
    dout(7) << " frozen, deferring" << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE, new C_Locker_RetryRequestCapRelease(this, client, item));
    return;
  }
    
  if (caps & ~cap->issued()) {
    dout(10) << " confirming not issued caps " << ccap_string(caps & ~cap->issued()) << dendl;
    caps &= cap->issued();
  }
  cap->confirm_receipt(seq, caps);

  if (!in->client_need_snapflush.empty() &&
      (cap->issued() & CEPH_CAP_ANY_FILE_WR) == 0) {
    _do_null_snapflush(in, client);
  }

  adjust_cap_wanted(cap, wanted, issue_seq);
  
  if (mdr)
    cap->inc_suppress();
  eval(in, CEPH_CAP_LOCKS);
  if (mdr)
    cap->dec_suppress();
  
  // take note; we may need to reissue on this cap later
  if (mdr)
    mdr->cap_releases[in->vino()] = cap->get_last_seq();
}

class C_Locker_RetryKickIssueCaps : public LockerContext {
  CInode *in;
  client_t client;
  ceph_seq_t seq;
public:
  C_Locker_RetryKickIssueCaps(Locker *l, CInode *i, client_t c, ceph_seq_t s) :
    LockerContext(l), in(i), client(c), seq(s) {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) {
    locker->kick_issue_caps(in, client, seq);
    in->put(CInode::PIN_PTRWAITER);
  }
};

void Locker::kick_issue_caps(CInode *in, client_t client, ceph_seq_t seq)
{
  Capability *cap = in->get_client_cap(client);
  if (!cap || cap->get_last_sent() != seq)
    return;
  if (in->is_frozen()) {
    dout(10) << "kick_issue_caps waiting for unfreeze on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE,
	new C_Locker_RetryKickIssueCaps(this, in, client, seq));
    return;
  }
  dout(10) << "kick_issue_caps released at current seq " << seq
    << ", reissuing" << dendl;
  issue_caps(in, cap);
}

void Locker::kick_cap_releases(MDRequestRef& mdr)
{
  client_t client = mdr->get_client();
  for (map<vinodeno_t,ceph_seq_t>::iterator p = mdr->cap_releases.begin();
       p != mdr->cap_releases.end();
       ++p) {
    CInode *in = mdcache->get_inode(p->first);
    if (!in)
      continue;
    kick_issue_caps(in, client, p->second);
  }
}

static uint64_t calc_bounding(uint64_t t)
{
  t |= t >> 1;
  t |= t >> 2;
  t |= t >> 4;
  t |= t >> 8;
  t |= t >> 16;
  t |= t >> 32;
  return t + 1;
}

/**
 * m and ack might be NULL, so don't dereference them unless dirty != 0
 */
void Locker::_do_snap_update(CInode *in, snapid_t snap, int dirty, snapid_t follows, client_t client, MClientCaps *m, MClientCaps *ack)
{
  dout(10) << "_do_snap_update dirty " << ccap_string(dirty)
	   << " follows " << follows << " snap " << snap
	   << " on " << *in << dendl;

  if (snap == CEPH_NOSNAP) {
    // hmm, i guess snap was already deleted?  just ack!
    dout(10) << " wow, the snap following " << follows
	     << " was already deleted.  nothing to record, just ack." << dendl;
    if (ack)
      mds->send_message_client_counted(ack, m->get_connection());
    return;
  }

  EUpdate *le = new EUpdate(mds->mdlog, "snap flush");
  mds->mdlog->start_entry(le);
  MutationRef mut(new MutationImpl);
  mut->ls = mds->mdlog->get_current_segment();

  // normal metadata updates that we can apply to the head as well.

  // update xattrs?
  bool xattrs = false;
  map<string,bufferptr> *px = 0;
  if ((dirty & CEPH_CAP_XATTR_EXCL) && 
      m->xattrbl.length() &&
      m->head.xattr_version > in->get_projected_inode()->xattr_version)
    xattrs = true;

  old_inode_t *oi = 0;
  if (in->is_multiversion()) {
    oi = in->pick_old_inode(snap);
  }

  inode_t *pi;
  if (oi) {
    dout(10) << " writing into old inode" << dendl;
    pi = in->project_inode();
    pi->version = in->pre_dirty();
    if (snap > oi->first)
      in->split_old_inode(snap);
    pi = &oi->inode;
    if (xattrs)
      px = &oi->xattrs;
  } else {
    if (xattrs)
      px = new map<string,bufferptr>;
    pi = in->project_inode(px);
    pi->version = in->pre_dirty();
  }

  _update_cap_fields(in, dirty, m, pi);

  // xattr
  if (px) {
    dout(7) << " xattrs v" << pi->xattr_version << " -> " << m->head.xattr_version
	    << " len " << m->xattrbl.length() << dendl;
    pi->xattr_version = m->head.xattr_version;
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(*px, p);
  }

  if (pi->client_ranges.count(client)) {
    if (in->last == snap) {
      dout(10) << "  removing client_range entirely" << dendl;
      pi->client_ranges.erase(client);
    } else {
      dout(10) << "  client_range now follows " << snap << dendl;
      pi->client_ranges[client].follows = snap;
    }
  }

  mut->auth_pin(in);
  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY, 0, follows);
  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in, follows);

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (ack && ack->get_oldest_flush_tid() > 0)
    le->metablob.add_client_flush(metareqid_t(m->get_source(), ack->get_client_tid()),
				  ack->get_oldest_flush_tid());

  mds->mdlog->submit_entry(le, new C_Locker_FileUpdate_finish(this, in, mut,
								 false,
								 client, NULL,
								 ack));
}

/**
 * m might be NULL, so don't dereference it unless dirty != 0.
 */
void Locker::_update_cap_fields(CInode *in, int dirty, MClientCaps *m, inode_t *pi)
{

  if (dirty && m->get_ctime() > pi->ctime) {
    dout(7) << "  ctime " << pi->ctime << " -> " << m->get_ctime()
	    << " for " << *in << dendl;
    pi->ctime = m->get_ctime();
  }

  // file
  if (dirty & (CEPH_CAP_FILE_EXCL|CEPH_CAP_FILE_WR)) {
    utime_t atime = m->get_atime();
    utime_t mtime = m->get_mtime();
    uint64_t size = m->get_size();
    version_t inline_version = m->inline_version;
    
    if (((dirty & CEPH_CAP_FILE_WR) && mtime > pi->mtime) ||
	((dirty & CEPH_CAP_FILE_EXCL) && mtime != pi->mtime)) {
      dout(7) << "  mtime " << pi->mtime << " -> " << mtime
	      << " for " << *in << dendl;
      pi->mtime = mtime;
    }
    if (in->inode.is_file() &&   // ONLY if regular file
	size > pi->size) {
      dout(7) << "  size " << pi->size << " -> " << size
	      << " for " << *in << dendl;
      pi->size = size;
      pi->rstat.rbytes = size;
    }
    if (in->inode.is_file() &&
        (dirty & CEPH_CAP_FILE_WR) &&
        inline_version > pi->inline_data.version) {
      pi->inline_data.version = inline_version;
      if (inline_version != CEPH_INLINE_NONE && m->inline_data.length() > 0)
	pi->inline_data.get_data() = m->inline_data;
      else
	pi->inline_data.free_data();
    }
    if ((dirty & CEPH_CAP_FILE_EXCL) && atime != pi->atime) {
      dout(7) << "  atime " << pi->atime << " -> " << atime
	      << " for " << *in << dendl;
      pi->atime = atime;
    }
    if ((dirty & CEPH_CAP_FILE_EXCL) &&
	ceph_seq_cmp(pi->time_warp_seq, m->get_time_warp_seq()) < 0) {
      dout(7) << "  time_warp_seq " << pi->time_warp_seq << " -> " << m->get_time_warp_seq()
	      << " for " << *in << dendl;
      pi->time_warp_seq = m->get_time_warp_seq();
    }
  }
  // auth
  if (dirty & CEPH_CAP_AUTH_EXCL) {
    if (m->head.uid != pi->uid) {
      dout(7) << "  uid " << pi->uid
	      << " -> " << m->head.uid
	      << " for " << *in << dendl;
      pi->uid = m->head.uid;
    }
    if (m->head.gid != pi->gid) {
      dout(7) << "  gid " << pi->gid
	      << " -> " << m->head.gid
	      << " for " << *in << dendl;
      pi->gid = m->head.gid;
    }
    if (m->head.mode != pi->mode) {
      dout(7) << "  mode " << oct << pi->mode
	      << " -> " << m->head.mode << dec
	      << " for " << *in << dendl;
      pi->mode = m->head.mode;
    }
    if (m->get_btime() != pi->btime) {
      dout(7) << "  btime " << oct << pi->btime
	      << " -> " << m->get_btime() << dec
	      << " for " << *in << dendl;
      pi->btime = m->get_btime();
    }
  }

}

/*
 * update inode based on cap flush|flushsnap|wanted.
 *  adjust max_size, if needed.
 * if we update, return true; otherwise, false (no updated needed).
 */
bool Locker::_do_cap_update(CInode *in, Capability *cap,
			    int dirty, snapid_t follows, MClientCaps *m,
			    MClientCaps *ack)
{
  dout(10) << "_do_cap_update dirty " << ccap_string(dirty)
	   << " issued " << ccap_string(cap ? cap->issued() : 0)
	   << " wanted " << ccap_string(cap ? cap->wanted() : 0)
	   << " on " << *in << dendl;
  assert(in->is_auth());
  client_t client = m->get_source().num();
  inode_t *latest = in->get_projected_inode();

  // increase or zero max_size?
  uint64_t size = m->get_size();
  bool change_max = false;
  uint64_t old_max = latest->client_ranges.count(client) ? latest->client_ranges[client].range.last : 0;
  uint64_t new_max = old_max;
  
  if (in->is_file()) {
    bool forced_change_max = false;
    dout(20) << "inode is file" << dendl;
    if (cap && ((cap->issued() | cap->wanted()) & CEPH_CAP_ANY_FILE_WR)) {
      dout(20) << "client has write caps; m->get_max_size="
               << m->get_max_size() << "; old_max=" << old_max << dendl;
      if (m->get_max_size() > new_max) {
	dout(10) << "client requests file_max " << m->get_max_size()
		 << " > max " << old_max << dendl;
	change_max = true;
	forced_change_max = true;
	new_max = ROUND_UP_TO((m->get_max_size()+1) << 1, latest->get_layout_size_increment());
      } else {
	new_max = calc_bounding(size * 2);
	if (new_max < latest->get_layout_size_increment())
	  new_max = latest->get_layout_size_increment();

	if (new_max > old_max)
	  change_max = true;
	else
	  new_max = old_max;
      }
    } else {
      if (old_max) {
	change_max = true;
	new_max = 0;
      }
    }

    if (in->last == CEPH_NOSNAP &&
	change_max &&
	!in->filelock.can_wrlock(client) &&
	!in->filelock.can_force_wrlock(client)) {
      dout(10) << " i want to change file_max, but lock won't allow it (yet)" << dendl;
      if (in->filelock.is_stable()) {
	bool need_issue = false;
	if (cap)
	  cap->inc_suppress();
	if (in->mds_caps_wanted.empty() &&
	    (in->get_loner() >= 0 || (in->get_wanted_loner() >= 0 && in->try_set_loner()))) {
	  if (in->filelock.get_state() != LOCK_EXCL)
	    file_excl(&in->filelock, &need_issue);
	} else
	  simple_lock(&in->filelock, &need_issue);
	if (need_issue)
	  issue_caps(in);
	if (cap)
	  cap->dec_suppress();
      }
      if (!in->filelock.can_wrlock(client) &&
	  !in->filelock.can_force_wrlock(client)) {
	C_MDL_CheckMaxSize *cms = new C_MDL_CheckMaxSize(this, in,
	                                                 false, 0,
	                                                 forced_change_max,
	                                                 new_max,
	                                                 utime_t());

	in->filelock.add_waiter(SimpleLock::WAIT_STABLE, cms);
	change_max = false;
      }
    }
  }

  if (m->flockbl.length()) {
    int32_t num_locks;
    bufferlist::iterator bli = m->flockbl.begin();
    ::decode(num_locks, bli);
    for ( int i=0; i < num_locks; ++i) {
      ceph_filelock decoded_lock;
      ::decode(decoded_lock, bli);
      in->get_fcntl_lock_state()->held_locks.
	insert(pair<uint64_t, ceph_filelock>(decoded_lock.start, decoded_lock));
      ++in->get_fcntl_lock_state()->client_held_lock_counts[(client_t)(decoded_lock.client)];
    }
    ::decode(num_locks, bli);
    for ( int i=0; i < num_locks; ++i) {
      ceph_filelock decoded_lock;
      ::decode(decoded_lock, bli);
      in->get_flock_lock_state()->held_locks.
	insert(pair<uint64_t, ceph_filelock>(decoded_lock.start, decoded_lock));
      ++in->get_flock_lock_state()->client_held_lock_counts[(client_t)(decoded_lock.client)];
    }
  }

  if (!dirty && !change_max)
    return false;

  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  if (session->check_access(in, MAY_WRITE,
			    m->caller_uid, m->caller_gid, 0, 0) < 0) {
    session->put();
    dout(10) << "check_access failed, dropping cap update on " << *in << dendl;
    return false;
  }
  session->put();

  // do the update.
  EUpdate *le = new EUpdate(mds->mdlog, "cap update");
  mds->mdlog->start_entry(le);

  // xattrs update?
  map<string,bufferptr> *px = 0;
  if ((dirty & CEPH_CAP_XATTR_EXCL) && 
      m->xattrbl.length() &&
      m->head.xattr_version > in->get_projected_inode()->xattr_version)
    px = new map<string,bufferptr>;

  inode_t *pi = in->project_inode(px);
  pi->version = in->pre_dirty();

  MutationRef mut(new MutationImpl);
  mut->ls = mds->mdlog->get_current_segment();

  _update_cap_fields(in, dirty, m, pi);

  if (change_max) {
    dout(7) << "  max_size " << old_max << " -> " << new_max
	    << " for " << *in << dendl;
    if (new_max) {
      pi->client_ranges[client].range.first = 0;
      pi->client_ranges[client].range.last = new_max;
      pi->client_ranges[client].follows = in->first - 1;
    } else 
      pi->client_ranges.erase(client);
  }
    
  if (change_max || (dirty & (CEPH_CAP_FILE_EXCL|CEPH_CAP_FILE_WR))) 
    wrlock_force(&in->filelock, mut);  // wrlock for duration of journal

  // auth
  if (dirty & CEPH_CAP_AUTH_EXCL)
    wrlock_force(&in->authlock, mut);

  // xattr
  if (px) {
    dout(7) << " xattrs v" << pi->xattr_version << " -> " << m->head.xattr_version << dendl;
    pi->xattr_version = m->head.xattr_version;
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(*px, p);

    wrlock_force(&in->xattrlock, mut);
  }
  
  mut->auth_pin(in);
  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY, 0, follows);
  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in, follows);

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (ack && ack->get_oldest_flush_tid() > 0)
    le->metablob.add_client_flush(metareqid_t(m->get_source(), ack->get_client_tid()),
				  ack->get_oldest_flush_tid());

  mds->mdlog->submit_entry(le, new C_Locker_FileUpdate_finish(this, in, mut,
								 change_max,
								 client, cap,
								 ack));
  // only flush immediately if the lock is unstable, or unissued caps are wanted, or max_size is 
  // changing
  if (((dirty & (CEPH_CAP_FILE_EXCL|CEPH_CAP_FILE_WR)) && !in->filelock.is_stable()) ||
      ((dirty & CEPH_CAP_AUTH_EXCL) && !in->authlock.is_stable()) ||
      ((dirty & CEPH_CAP_XATTR_EXCL) && !in->xattrlock.is_stable()) ||
      (!dirty && (!in->filelock.is_stable() || !in->authlock.is_stable() || !in->xattrlock.is_stable())) ||  // nothing dirty + unstable lock -> probably a revoke?
      (change_max && new_max) ||         // max INCREASE
      (cap && (cap->wanted() & ~cap->pending())))
    mds->mdlog->flush();

  return true;
}

/* This function DOES put the passed message before returning */
void Locker::handle_client_cap_release(MClientCapRelease *m)
{
  client_t client = m->get_source().num();
  dout(10) << "handle_client_cap_release " << *m << dendl;

  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (m->osd_epoch_barrier && !mds->objecter->have_map(m->osd_epoch_barrier)) {
    // Pause RADOS operations until we see the required epoch
    mds->objecter->set_epoch_barrier(m->osd_epoch_barrier);
  }

  if (mds->get_osd_epoch_barrier() < m->osd_epoch_barrier) {
    // Record the barrier so that we will retransmit it to clients
    mds->set_osd_epoch_barrier(m->osd_epoch_barrier);
  }

  Session *session = static_cast<Session *>(m->get_connection()->get_priv());

  for (vector<ceph_mds_cap_item>::iterator p = m->caps.begin(); p != m->caps.end(); ++p) {
    _do_cap_release(client, inodeno_t((uint64_t)p->ino) , p->cap_id, p->migrate_seq, p->seq);
  }

  if (session) {
    session->notify_cap_release(m->caps.size());
  }

  m->put();
}

class C_Locker_RetryCapRelease : public LockerContext {
  client_t client;
  inodeno_t ino;
  uint64_t cap_id;
  ceph_seq_t migrate_seq;
  ceph_seq_t issue_seq;
public:
  C_Locker_RetryCapRelease(Locker *l, client_t c, inodeno_t i, uint64_t id,
			   ceph_seq_t mseq, ceph_seq_t seq) :
    LockerContext(l), client(c), ino(i), cap_id(id), migrate_seq(mseq), issue_seq(seq) {}
  void finish(int r) {
    locker->_do_cap_release(client, ino, cap_id, migrate_seq, issue_seq);
  }
};

void Locker::_do_cap_release(client_t client, inodeno_t ino, uint64_t cap_id,
			     ceph_seq_t mseq, ceph_seq_t seq)
{
  CInode *in = mdcache->get_inode(ino);
  if (!in) {
    dout(7) << "_do_cap_release missing ino " << ino << dendl;
    return;
  }
  Capability *cap = in->get_client_cap(client);
  if (!cap) {
    dout(7) << "_do_cap_release no cap for client" << client << " on "<< *in << dendl;
    return;
  }

  dout(7) << "_do_cap_release for client." << client << " on "<< *in << dendl;
  if (cap->get_cap_id() != cap_id) {
    dout(7) << " capid " << cap_id << " != " << cap->get_cap_id() << ", ignore" << dendl;
    return;
  }
  if (ceph_seq_cmp(mseq, cap->get_mseq()) < 0) {
    dout(7) << " mseq " << mseq << " < " << cap->get_mseq() << ", ignore" << dendl;
    return;
  }
  if (should_defer_client_cap_frozen(in)) {
    dout(7) << " freezing|frozen, deferring" << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE,
                  new C_Locker_RetryCapRelease(this, client, ino, cap_id, mseq, seq));
    return;
  }
  if (seq != cap->get_last_issue()) {
    dout(7) << " issue_seq " << seq << " != " << cap->get_last_issue() << dendl;
    // clean out any old revoke history
    cap->clean_revoke_from(seq);
    eval_cap_gather(in);
    return;
  }
  remove_client_cap(in, client);
}

/* This function DOES put the passed message before returning */

void Locker::remove_client_cap(CInode *in, client_t client)
{
  // clean out any pending snapflush state
  if (!in->client_need_snapflush.empty())
    _do_null_snapflush(in, client);

  in->remove_client_cap(client);

  if (in->is_auth()) {
    // make sure we clear out the client byte range
    if (in->get_projected_inode()->client_ranges.count(client) &&
	!(in->inode.nlink == 0 && !in->is_any_caps()))    // unless it's unlink + stray
      check_inode_max_size(in);
  } else {
    request_inode_file_caps(in);
  }
  
  try_eval(in, CEPH_CAP_LOCKS);
}


/**
 * Return true if any currently revoking caps exceed the
 * mds_revoke_cap_timeout threshold.
 */
bool Locker::any_late_revoking_caps(xlist<Capability*> const &revoking) const
{
    xlist<Capability*>::const_iterator p = revoking.begin();
    if (p.end()) {
      // No revoking caps at the moment
      return false;
    } else {
      utime_t now = ceph_clock_now(g_ceph_context);
      utime_t age = now - (*p)->get_last_revoke_stamp();
      if (age <= g_conf->mds_revoke_cap_timeout) {
          return false;
      } else {
          return true;
      }
    }
}


void Locker::get_late_revoking_clients(std::list<client_t> *result) const
{
  if (!any_late_revoking_caps(revoking_caps)) {
    // Fast path: no misbehaving clients, execute in O(1)
    return;
  }

  // Slow path: execute in O(N_clients)
  std::map<client_t, xlist<Capability*> >::const_iterator client_rc_iter;
  for (client_rc_iter = revoking_caps_by_client.begin();
       client_rc_iter != revoking_caps_by_client.end(); ++client_rc_iter) {
    xlist<Capability*> const &client_rc = client_rc_iter->second;
    bool any_late = any_late_revoking_caps(client_rc);
    if (any_late) {
        result->push_back(client_rc_iter->first);
    }
  }
}

// Hard-code instead of surfacing a config settings because this is
// really a hack that should go away at some point when we have better
// inspection tools for getting at detailed cap state (#7316)
#define MAX_WARN_CAPS 100

void Locker::caps_tick()
{
  utime_t now = ceph_clock_now(g_ceph_context);

  dout(20) << __func__ << " " << revoking_caps.size() << " revoking caps" << dendl;

  int i = 0;
  for (xlist<Capability*>::iterator p = revoking_caps.begin(); !p.end(); ++p) {
    Capability *cap = *p;

    utime_t age = now - cap->get_last_revoke_stamp();
    dout(20) << __func__ << " age = " << age << cap->get_client() << "." << cap->get_inode()->ino() << dendl;
    if (age <= g_conf->mds_revoke_cap_timeout) {
      dout(20) << __func__ << " age below timeout " << g_conf->mds_revoke_cap_timeout << dendl;
      break;
    } else {
      ++i;
      if (i > MAX_WARN_CAPS) {
        dout(1) << __func__ << " more than " << MAX_WARN_CAPS << " caps are late"
          << "revoking, ignoring subsequent caps" << dendl;
        break;
      }
    }
    // exponential backoff of warning intervals
    if (age > g_conf->mds_revoke_cap_timeout * (1 << cap->get_num_revoke_warnings())) {
      cap->inc_num_revoke_warnings();
      stringstream ss;
      ss << "client." << cap->get_client() << " isn't responding to mclientcaps(revoke), ino "
	 << cap->get_inode()->ino() << " pending " << ccap_string(cap->pending())
	 << " issued " << ccap_string(cap->issued()) << ", sent " << age << " seconds ago\n";
      mds->clog->warn() << ss.str();
      dout(20) << __func__ << " " << ss.str() << dendl;
    } else {
      dout(20) << __func__ << " silencing log message (backoff) for " << cap->get_client() << "." << cap->get_inode()->ino() << dendl;
    }
  }
}


void Locker::handle_client_lease(MClientLease *m)
{
  dout(10) << "handle_client_lease " << *m << dendl;

  assert(m->get_source().is_client());
  client_t client = m->get_source().num();

  CInode *in = mdcache->get_inode(m->get_ino(), m->get_last());
  if (!in) {
    dout(7) << "handle_client_lease don't have ino " << m->get_ino() << "." << m->get_last() << dendl;
    m->put();
    return;
  }
  CDentry *dn = 0;

  frag_t fg = in->pick_dirfrag(m->dname);
  CDir *dir = in->get_dirfrag(fg);
  if (dir) 
    dn = dir->lookup(m->dname);
  if (!dn) {
    dout(7) << "handle_client_lease don't have dn " << m->get_ino() << " " << m->dname << dendl;
    m->put();
    return;
  }
  dout(10) << " on " << *dn << dendl;

  // replica and lock
  ClientLease *l = dn->get_client_lease(client);
  if (!l) {
    dout(7) << "handle_client_lease didn't have lease for client." << client << " of " << *dn << dendl;
    m->put();
    return;
  } 

  switch (m->get_action()) {
  case CEPH_MDS_LEASE_REVOKE_ACK:
  case CEPH_MDS_LEASE_RELEASE:
    if (l->seq != m->get_seq()) {
      dout(7) << "handle_client_lease release - seq " << l->seq << " != provided " << m->get_seq() << dendl;
    } else {
      dout(7) << "handle_client_lease client." << client
	      << " on " << *dn << dendl;
      dn->remove_client_lease(l, this);
    }
    m->put();
    break;

  case CEPH_MDS_LEASE_RENEW:
    {
      dout(7) << "handle_client_lease client." << client << " renew on " << *dn
	      << (!dn->lock.can_lease(client)?", revoking lease":"") << dendl;
      if (dn->lock.can_lease(client)) {
	int pool = 1;   // fixme.. do something smart!
	m->h.duration_ms = (int)(1000 * mdcache->client_lease_durations[pool]);
	m->h.seq = ++l->seq;
	m->clear_payload();

	utime_t now = ceph_clock_now(g_ceph_context);
	now += mdcache->client_lease_durations[pool];
	mdcache->touch_client_lease(l, pool, now);

	mds->send_message_client_counted(m, m->get_connection());
      }
    }
    break;

  default:
    assert(0); // implement me
    break;
  }
}


void Locker::issue_client_lease(CDentry *dn, client_t client,
			       bufferlist &bl, utime_t now, Session *session)
{
  CInode *diri = dn->get_dir()->get_inode();
  if (!diri->is_stray() &&  // do not issue dn leases in stray dir!
      ((!diri->filelock.can_lease(client) &&
	(diri->get_client_cap_pending(client) & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL)) == 0)) &&
      dn->lock.can_lease(client)) {
    int pool = 1;   // fixme.. do something smart!
    // issue a dentry lease
    ClientLease *l = dn->add_client_lease(client, session);
    session->touch_lease(l);
    
    now += mdcache->client_lease_durations[pool];
    mdcache->touch_client_lease(l, pool, now);

    LeaseStat e;
    e.mask = 1 | CEPH_LOCK_DN;  // old and new bit values
    e.seq = ++l->seq;
    e.duration_ms = (int)(1000 * mdcache->client_lease_durations[pool]);
    ::encode(e, bl);
    dout(20) << "issue_client_lease seq " << e.seq << " dur " << e.duration_ms << "ms "
	     << " on " << *dn << dendl;
  } else {
    // null lease
    LeaseStat e;
    e.mask = 0;
    e.seq = 0;
    e.duration_ms = 0;
    ::encode(e, bl);
    dout(20) << "issue_client_lease no/null lease on " << *dn << dendl;
  }
}


void Locker::revoke_client_leases(SimpleLock *lock)
{
  int n = 0;
  CDentry *dn = static_cast<CDentry*>(lock->get_parent());
  for (map<client_t, ClientLease*>::iterator p = dn->client_lease_map.begin();
       p != dn->client_lease_map.end();
       ++p) {
    ClientLease *l = p->second;
    
    n++;
    assert(lock->get_type() == CEPH_LOCK_DN);

    CDentry *dn = static_cast<CDentry*>(lock->get_parent());
    int mask = 1 | CEPH_LOCK_DN; // old and new bits
    
    // i should also revoke the dir ICONTENT lease, if they have it!
    CInode *diri = dn->get_dir()->get_inode();
    mds->send_message_client_counted(new MClientLease(CEPH_MDS_LEASE_REVOKE, l->seq,
					      mask,
					      diri->ino(),
					      diri->first, CEPH_NOSNAP,
					      dn->get_name()),
			     l->client);
  }
  assert(n == lock->get_num_client_lease());
}



// locks ----------------------------------------------------------------

SimpleLock *Locker::get_lock(int lock_type, MDSCacheObjectInfo &info) 
{
  switch (lock_type) {
  case CEPH_LOCK_DN:
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
	  dn = dir->lookup(info.dname, info.snapid);
      }
      if (!dn) {
	dout(7) << "get_lock don't have dn " << info.dirfrag.ino << " " << info.dname << dendl;
	return 0;
      }
      return &dn->lock;
    }

  case CEPH_LOCK_IAUTH:
  case CEPH_LOCK_ILINK:
  case CEPH_LOCK_IDFT:
  case CEPH_LOCK_IFILE:
  case CEPH_LOCK_INEST:
  case CEPH_LOCK_IXATTR:
  case CEPH_LOCK_ISNAP:
  case CEPH_LOCK_IFLOCK:
  case CEPH_LOCK_IPOLICY:
    {
      CInode *in = mdcache->get_inode(info.ino, info.snapid);
      if (!in) {
	dout(7) << "get_lock don't have ino " << info.ino << dendl;
	return 0;
      }
      switch (lock_type) {
      case CEPH_LOCK_IAUTH: return &in->authlock;
      case CEPH_LOCK_ILINK: return &in->linklock;
      case CEPH_LOCK_IDFT: return &in->dirfragtreelock;
      case CEPH_LOCK_IFILE: return &in->filelock;
      case CEPH_LOCK_INEST: return &in->nestlock;
      case CEPH_LOCK_IXATTR: return &in->xattrlock;
      case CEPH_LOCK_ISNAP: return &in->snaplock;
      case CEPH_LOCK_IFLOCK: return &in->flocklock;
      case CEPH_LOCK_IPOLICY: return &in->policylock;
      }
    }

  default:
    dout(7) << "get_lock don't know lock_type " << lock_type << dendl;
    assert(0);
    break;
  }

  return 0;  
}

/* This function DOES put the passed message before returning */
void Locker::handle_lock(MLock *m)
{
  // nobody should be talking to us during recovery.
  assert(mds->is_rejoin() || mds->is_clientreplay() || mds->is_active() || mds->is_stopping());

  SimpleLock *lock = get_lock(m->get_lock_type(), m->get_object_info());
  if (!lock) {
    dout(10) << "don't have object " << m->get_object_info() << ", must have trimmed, dropping" << dendl;
    m->put();
    return;
  }

  switch (lock->get_type()) {
  case CEPH_LOCK_DN:
  case CEPH_LOCK_IAUTH:
  case CEPH_LOCK_ILINK:
  case CEPH_LOCK_ISNAP:
  case CEPH_LOCK_IXATTR:
  case CEPH_LOCK_IFLOCK:
  case CEPH_LOCK_IPOLICY:
    handle_simple_lock(lock, m);
    break;
    
  case CEPH_LOCK_IDFT:
  case CEPH_LOCK_INEST:
    //handle_scatter_lock((ScatterLock*)lock, m);
    //break;

  case CEPH_LOCK_IFILE:
    handle_file_lock(static_cast<ScatterLock*>(lock), m);
    break;
    
  default:
    dout(7) << "handle_lock got otype " << m->get_lock_type() << dendl;
    assert(0);
    break;
  }
}
 




// ==========================================================================
// simple lock

/** This function may take a reference to m if it needs one, but does
 * not put references. */
void Locker::handle_reqrdlock(SimpleLock *lock, MLock *m)
{
  MDSCacheObject *parent = lock->get_parent();
  if (parent->is_auth() &&
      lock->get_state() != LOCK_SYNC &&
      !parent->is_frozen()) {
    dout(7) << "handle_reqrdlock got rdlock request on " << *lock
	    << " on " << *parent << dendl;
    assert(parent->is_auth()); // replica auth pinned if they're doing this!
    if (lock->is_stable()) {
      simple_sync(lock);
    } else {
      dout(7) << "handle_reqrdlock delaying request until lock is stable" << dendl;
      lock->add_waiter(SimpleLock::WAIT_STABLE | MDSCacheObject::WAIT_UNFREEZE,
                       new C_MDS_RetryMessage(mds, m->get()));
    }
  } else {
    dout(7) << "handle_reqrdlock dropping rdlock request on " << *lock
	    << " on " << *parent << dendl;
    // replica should retry
  }
}

/* This function DOES put the passed message before returning */
void Locker::handle_simple_lock(SimpleLock *lock, MLock *m)
{
  int from = m->get_asker();
  
  dout(10) << "handle_simple_lock " << *m
	   << " on " << *lock << " " << *lock->get_parent() << dendl;

  if (mds->is_rejoin()) {
    if (lock->get_parent()->is_rejoining()) {
      dout(7) << "handle_simple_lock still rejoining " << *lock->get_parent()
	      << ", dropping " << *m << dendl;
      m->put();
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
    lock->set_state(LOCK_SYNC_LOCK);
    if (lock->is_leased())
      revoke_client_leases(lock);
    eval_gather(lock, true);
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->get_state() == LOCK_SYNC_LOCK ||
	   lock->get_state() == LOCK_SYNC_EXCL);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->is_gathering()) {
      dout(7) << "handle_simple_lock " << *lock << " on " << *lock->get_parent() << " from " << from
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_simple_lock " << *lock << " on " << *lock->get_parent() << " from " << from
	      << ", last one" << dendl;
      eval_gather(lock);
    }
    break;

  case LOCK_AC_REQRDLOCK:
    handle_reqrdlock(lock, m);
    break;

  }

  m->put();
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


void Locker::simple_eval(SimpleLock *lock, bool *need_issue)
{
  dout(10) << "simple_eval " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  if (lock->get_parent()->is_freezing_or_frozen()) {
    // dentry lock in unreadable state can block path traverse
    if ((lock->get_type() != CEPH_LOCK_DN ||
	 lock->get_state() == LOCK_SYNC ||
	 lock->get_parent()->is_frozen()))
      return;
  }

  if (mdcache->is_readonly()) {
    if (lock->get_state() != LOCK_SYNC) {
      dout(10) << "simple_eval read-only FS, syncing " << *lock << " on " << *lock->get_parent() << dendl;
      simple_sync(lock, need_issue);
    }
    return;
  }

  CInode *in = 0;
  int wanted = 0;
  if (lock->get_type() != CEPH_LOCK_DN) {
    in = static_cast<CInode*>(lock->get_parent());
    in->get_caps_wanted(&wanted, NULL, lock->get_cap_shift());
  }
  
  // -> excl?
  if (lock->get_state() != LOCK_EXCL &&
      in && in->get_target_loner() >= 0 &&
      (wanted & CEPH_CAP_GEXCL)) {
    dout(7) << "simple_eval stable, going to excl " << *lock 
	    << " on " << *lock->get_parent() << dendl;
    simple_excl(lock, need_issue);
  }

  // stable -> sync?
  else if (lock->get_state() != LOCK_SYNC &&
	   !lock->is_wrlocked() &&
	   ((!(wanted & CEPH_CAP_GEXCL) && !lock->is_waiter_for(SimpleLock::WAIT_WR)) ||
	    (lock->get_state() == LOCK_EXCL && in && in->get_target_loner() < 0))) {
    dout(7) << "simple_eval stable, syncing " << *lock 
	    << " on " << *lock->get_parent() << dendl;
    simple_sync(lock, need_issue);
  }
}


// mid

bool Locker::simple_sync(SimpleLock *lock, bool *need_issue)
{
  dout(7) << "simple_sync on " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  int old_state = lock->get_state();

  if (old_state != LOCK_TSYN) {

    switch (lock->get_state()) {
    case LOCK_MIX: lock->set_state(LOCK_MIX_SYNC); break;
    case LOCK_LOCK: lock->set_state(LOCK_LOCK_SYNC); break;
    case LOCK_XSYN: lock->set_state(LOCK_XSYN_SYNC); break;
    case LOCK_EXCL: lock->set_state(LOCK_EXCL_SYNC); break;
    default: assert(0);
    }

    int gather = 0;
    if (lock->is_wrlocked())
      gather++;
    
    if (lock->get_parent()->is_replicated() && old_state == LOCK_MIX) {
      send_lock_message(lock, LOCK_AC_SYNC);
      lock->init_gather();
      gather++;
    }
    
    if (in && in->is_head()) {
      if (in->issued_caps_need_gather(lock)) {
	if (need_issue)
	  *need_issue = true;
	else
	  issue_caps(in);
	gather++;
      }
    }
    
    bool need_recover = false;
    if (lock->get_type() == CEPH_LOCK_IFILE) {
      assert(in);
      if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
        mds->mdcache->queue_file_recover(in);
	need_recover = true;
        gather++;
      }
    }
    
    if (!gather && lock->is_dirty()) {
      lock->get_parent()->auth_pin(lock);
      scatter_writebehind(static_cast<ScatterLock*>(lock));
      mds->mdlog->flush();
      return false;
    }

    if (gather) {
      lock->get_parent()->auth_pin(lock);
      if (need_recover)
	mds->mdcache->do_file_recover();
      return false;
    }
  }

  if (lock->get_parent()->is_replicated()) {    // FIXME
    bufferlist data;
    lock->encode_locked_state(data);
    send_lock_message(lock, LOCK_AC_SYNC, data);
  }
  lock->set_state(LOCK_SYNC);
  lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
  if (in && in->is_head()) {
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
  }
  return true;
}

void Locker::simple_excl(SimpleLock *lock, bool *need_issue)
{
  dout(7) << "simple_excl on " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  switch (lock->get_state()) {
  case LOCK_LOCK: lock->set_state(LOCK_LOCK_EXCL); break;
  case LOCK_SYNC: lock->set_state(LOCK_SYNC_EXCL); break;
  case LOCK_XSYN: lock->set_state(LOCK_XSYN_EXCL); break;
  default: assert(0);
  }
  
  int gather = 0;
  if (lock->is_rdlocked())
    gather++;
  if (lock->is_wrlocked())
    gather++;

  if (lock->get_parent()->is_replicated() && 
      lock->get_state() != LOCK_LOCK_EXCL &&
      lock->get_state() != LOCK_XSYN_EXCL) {
    send_lock_message(lock, LOCK_AC_LOCK);
    lock->init_gather();
    gather++;
  }
  
  if (in && in->is_head()) {
    if (in->issued_caps_need_gather(lock)) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
      gather++;
    }
  }
  
  if (gather) {
    lock->get_parent()->auth_pin(lock);
  } else {
    lock->set_state(LOCK_EXCL);
    lock->finish_waiters(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
    if (in) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
    }
  }
}

void Locker::simple_lock(SimpleLock *lock, bool *need_issue)
{
  dout(7) << "simple_lock on " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());
  assert(lock->get_state() != LOCK_LOCK);
  
  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  int old_state = lock->get_state();

  switch (lock->get_state()) {
  case LOCK_SYNC: lock->set_state(LOCK_SYNC_LOCK); break;
  case LOCK_XSYN:
    file_excl(static_cast<ScatterLock*>(lock), need_issue);
    if (lock->get_state() != LOCK_EXCL)
      return;
    // fall-thru
  case LOCK_EXCL: lock->set_state(LOCK_EXCL_LOCK); break;
  case LOCK_MIX: lock->set_state(LOCK_MIX_LOCK);
    (static_cast<ScatterLock *>(lock))->clear_unscatter_wanted();
    break;
  case LOCK_TSYN: lock->set_state(LOCK_TSYN_LOCK); break;
  default: assert(0);
  }

  int gather = 0;
  if (lock->is_leased()) {
    gather++;
    revoke_client_leases(lock);
  }
  if (lock->is_rdlocked())
    gather++;
  if (in && in->is_head()) {
    if (in->issued_caps_need_gather(lock)) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
      gather++;
    }
  }

  bool need_recover = false;
  if (lock->get_type() == CEPH_LOCK_IFILE) {
    assert(in);
    if(in->state_test(CInode::STATE_NEEDSRECOVER)) {
      mds->mdcache->queue_file_recover(in);
      need_recover = true;
      gather++;
    }
  }

  if (lock->get_parent()->is_replicated() &&
      lock->get_state() == LOCK_MIX_LOCK &&
      gather) {
    dout(10) << " doing local stage of mix->lock gather before gathering from replicas" << dendl;
  } else {
    // move to second stage of gather now, so we don't send the lock action later.
    if (lock->get_state() == LOCK_MIX_LOCK)
      lock->set_state(LOCK_MIX_LOCK2);

    if (lock->get_parent()->is_replicated() &&
	lock->get_sm()->states[old_state].replica_state != LOCK_LOCK) {  // replica may already be LOCK
      gather++;
      send_lock_message(lock, LOCK_AC_LOCK);
      lock->init_gather();
    }
  }

  if (!gather && lock->is_dirty()) {
    lock->get_parent()->auth_pin(lock);
    scatter_writebehind(static_cast<ScatterLock*>(lock));
    mds->mdlog->flush();
    return;
  }

  if (gather) {
    lock->get_parent()->auth_pin(lock);
    if (need_recover)
      mds->mdcache->do_file_recover();
  } else {
    lock->set_state(LOCK_LOCK);
    lock->finish_waiters(ScatterLock::WAIT_XLOCK|ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
  }
}


void Locker::simple_xlock(SimpleLock *lock)
{
  dout(7) << "simple_xlock on " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  //assert(lock->is_stable());
  assert(lock->get_state() != LOCK_XLOCK);
  
  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  if (lock->is_stable())
    lock->get_parent()->auth_pin(lock);

  switch (lock->get_state()) {
  case LOCK_LOCK: 
  case LOCK_XLOCKDONE: lock->set_state(LOCK_LOCK_XLOCK); break;
  default: assert(0);
  }

  int gather = 0;
  if (lock->is_rdlocked())
    gather++;
  if (lock->is_wrlocked())
    gather++;
  
  if (in && in->is_head()) {
    if (in->issued_caps_need_gather(lock)) {
      issue_caps(in);
      gather++;
    }
  }

  if (!gather) {
    lock->set_state(LOCK_PREXLOCK);
    //assert("shouldn't be called if we are already xlockable" == 0);
  }
}





// ==========================================================================
// scatter lock

/*

Some notes on scatterlocks.

 - The scatter/gather is driven by the inode lock.  The scatter always
   brings in the latest metadata from the fragments.

 - When in a scattered/MIX state, fragments are only allowed to
   update/be written to if the accounted stat matches the inode's
   current version.

 - That means, on gather, we _only_ assimilate diffs for frag metadata
   that match the current version, because those are the only ones
   written during this scatter/gather cycle.  (Others didn't permit
   it.)  We increment the version and journal this to disk.

 - When possible, we also simultaneously update our local frag
   accounted stats to match.

 - On scatter, the new inode info is broadcast to frags, both local
   and remote.  If possible (auth and !frozen), the dirfrag auth
   should update the accounted state (if it isn't already up to date).
   Note that this may occur on both the local inode auth node and
   inode replicas, so there are two potential paths. If it is NOT
   possible, they need to mark_stale to prevent any possible writes.

 - A scatter can be to MIX (potentially writeable) or to SYNC (read
   only).  Both are opportunities to update the frag accounted stats,
   even though only the MIX case is affected by a stale dirfrag.

 - Because many scatter/gather cycles can potentially go by without a
   frag being able to update its accounted stats (due to being frozen
   by exports/refragments in progress), the frag may have (even very)
   old stat versions.  That's fine.  If when we do want to update it,
   we can update accounted_* and the version first.

*/

class C_Locker_ScatterWB : public LockerContext {
  ScatterLock *lock;
  MutationRef mut;
public:
  C_Locker_ScatterWB(Locker *l, ScatterLock *sl, MutationRef& m) :
    LockerContext(l), lock(sl), mut(m) {}
  void finish(int r) { 
    locker->scatter_writebehind_finish(lock, mut); 
  }
};

void Locker::scatter_writebehind(ScatterLock *lock)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind " << in->inode.mtime << " on " << *lock << " on " << *in << dendl;

  // journal
  MutationRef mut(new MutationImpl);
  mut->ls = mds->mdlog->get_current_segment();

  // forcefully take a wrlock
  lock->get_wrlock(true);
  mut->wrlocks.insert(lock);
  mut->locks.insert(lock);

  in->pre_cow_old_inode();  // avoid cow mayhem

  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();

  in->finish_scatter_gather_update(lock->get_type());
  lock->start_flush();

  EUpdate *le = new EUpdate(mds->mdlog, "scatter_writebehind");
  mds->mdlog->start_entry(le);

  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in);
  
  in->finish_scatter_gather_update_accounted(lock->get_type(), mut, &le->metablob);

  mds->mdlog->submit_entry(le, new C_Locker_ScatterWB(this, lock, mut));
}

void Locker::scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind_finish on " << *lock << " on " << *in << dendl;
  in->pop_and_dirty_projected_inode(mut->ls);

  lock->finish_flush();

  // if replicas may have flushed in a mix->lock state, send another
  // message so they can finish_flush().
  if (in->is_replicated()) {
    switch (lock->get_state()) {
    case LOCK_MIX_LOCK:
    case LOCK_MIX_LOCK2:
    case LOCK_MIX_EXCL:
    case LOCK_MIX_TSYN:
      send_lock_message(lock, LOCK_AC_LOCKFLUSHED);
    }
  }

  mut->apply();
  drop_locks(mut.get());
  mut->cleanup();

  if (lock->is_stable())
    lock->finish_waiters(ScatterLock::WAIT_STABLE);

  //scatter_eval_gather(lock);
}

void Locker::scatter_eval(ScatterLock *lock, bool *need_issue)
{
  dout(10) << "scatter_eval " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  if (lock->get_parent()->is_freezing_or_frozen()) {
    dout(20) << "  freezing|frozen" << dendl;
    return;
  }

  if (mdcache->is_readonly()) {
    if (lock->get_state() != LOCK_SYNC) {
      dout(10) << "scatter_eval read-only FS, syncing " << *lock << " on " << *lock->get_parent() << dendl;
      simple_sync(lock, need_issue);
    }
    return;
  }
  
  if (!lock->is_rdlocked() &&
      lock->get_state() != LOCK_MIX &&
      lock->get_scatter_wanted()) {
    dout(10) << "scatter_eval scatter_wanted, bump to mix " << *lock
	     << " on " << *lock->get_parent() << dendl;
    scatter_mix(lock, need_issue);
    return;
  }

  if (lock->get_type() == CEPH_LOCK_INEST) {
    // in general, we want to keep INEST writable at all times.
    if (!lock->is_rdlocked()) {
      if (lock->get_parent()->is_replicated()) {
	if (lock->get_state() != LOCK_MIX)
	  scatter_mix(lock, need_issue);
      } else {
	if (lock->get_state() != LOCK_LOCK)
	  simple_lock(lock, need_issue);
      }
    }
    return;
  }

  CInode *in = static_cast<CInode*>(lock->get_parent());
  if (!in->has_subtree_or_exporting_dirfrag() || in->is_base()) {
    // i _should_ be sync.
    if (!lock->is_wrlocked() &&
	lock->get_state() != LOCK_SYNC) {
      dout(10) << "scatter_eval no wrlocks|xlocks, not subtree root inode, syncing" << dendl;
      simple_sync(lock, need_issue);
    }
  }
}


/*
 * mark a scatterlock to indicate that the dir fnode has some dirty data
 */
void Locker::mark_updated_scatterlock(ScatterLock *lock)
{
  lock->mark_dirty();
  if (lock->get_updated_item()->is_on_list()) {
    dout(10) << "mark_updated_scatterlock " << *lock
	     << " - already on list since " << lock->get_update_stamp() << dendl;
  } else {
    updated_scatterlocks.push_back(lock->get_updated_item());
    utime_t now = ceph_clock_now(g_ceph_context);
    lock->set_update_stamp(now);
    dout(10) << "mark_updated_scatterlock " << *lock
	     << " - added at " << now << dendl;
  }
}

/*
 * this is called by scatter_tick and LogSegment::try_to_trim() when
 * trying to flush dirty scattered data (i.e. updated fnode) back to
 * the inode.
 *
 * we need to lock|scatter in order to push fnode changes into the
 * inode.dirstat.
 */
void Locker::scatter_nudge(ScatterLock *lock, MDSInternalContextBase *c, bool forcelockchange)
{
  CInode *p = static_cast<CInode *>(lock->get_parent());

  if (p->is_frozen() || p->is_freezing()) {
    dout(10) << "scatter_nudge waiting for unfreeze on " << *p << dendl;
    if (c) 
      p->add_waiter(MDSCacheObject::WAIT_UNFREEZE, c);
    else
      // just requeue.  not ideal.. starvation prone..
      updated_scatterlocks.push_back(lock->get_updated_item());
    return;
  }

  if (p->is_ambiguous_auth()) {
    dout(10) << "scatter_nudge waiting for single auth on " << *p << dendl;
    if (c) 
      p->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, c);
    else
      // just requeue.  not ideal.. starvation prone..
      updated_scatterlocks.push_back(lock->get_updated_item());
    return;
  }

  if (p->is_auth()) {
    int count = 0;
    while (true) {
      if (lock->is_stable()) {
	// can we do it now?
	//  (only if we're not replicated.. if we are, we really do need
	//   to nudge the lock state!)
	/*
	  actually, even if we're not replicated, we can't stay in MIX, because another mds
	  could discover and replicate us at any time.  if that happens while we're flushing,
	  they end up in MIX but their inode has the old scatterstat version.

	if (!forcelockchange && !lock->get_parent()->is_replicated() && lock->can_wrlock(-1)) {
	  dout(10) << "scatter_nudge auth, propagating " << *lock << " on " << *p << dendl;
	  scatter_writebehind(lock);
	  if (c)
	    lock->add_waiter(SimpleLock::WAIT_STABLE, c);
	  return;
	}
	*/

	if (mdcache->is_readonly()) {
	  if (lock->get_state() != LOCK_SYNC) {
	    dout(10) << "scatter_nudge auth, read-only FS, syncing " << *lock << " on " << *p << dendl;
	    simple_sync(static_cast<ScatterLock*>(lock));
	  }
	  break;
	}

	// adjust lock state
	dout(10) << "scatter_nudge auth, scatter/unscattering " << *lock << " on " << *p << dendl;
	switch (lock->get_type()) {
	case CEPH_LOCK_IFILE:
	  if (p->is_replicated() && lock->get_state() != LOCK_MIX)
	    scatter_mix(static_cast<ScatterLock*>(lock));
	  else if (lock->get_state() != LOCK_LOCK)
	    simple_lock(static_cast<ScatterLock*>(lock));
	  else
	    simple_sync(static_cast<ScatterLock*>(lock));
	  break;
	  
	case CEPH_LOCK_IDFT:
	case CEPH_LOCK_INEST:
	  if (p->is_replicated() && lock->get_state() != LOCK_MIX)
	    scatter_mix(lock);
	  else if (lock->get_state() != LOCK_LOCK)
	    simple_lock(lock);
	  else
	    simple_sync(lock);
	  break;
	default:
	  assert(0);
	}
	++count;
	if (lock->is_stable() && count == 2) {
	  dout(10) << "scatter_nudge oh, stable after two cycles." << dendl;
	  // this should only realy happen when called via
	  // handle_file_lock due to AC_NUDGE, because the rest of the
	  // time we are replicated or have dirty data and won't get
	  // called.  bailing here avoids an infinite loop.
	  assert(!c); 
	  break;
	}
      } else {
	dout(10) << "scatter_nudge auth, waiting for stable " << *lock << " on " << *p << dendl;
	if (c)
	  lock->add_waiter(SimpleLock::WAIT_STABLE, c);
	return;
      }
    }
  } else {
    dout(10) << "scatter_nudge replica, requesting scatter/unscatter of " 
	     << *lock << " on " << *p << dendl;
    // request unscatter?
    mds_rank_t auth = lock->get_parent()->authority().first;
    if (mds->mdsmap->is_clientreplay_or_active_or_stopping(auth))
      mds->send_message_mds(new MLock(lock, LOCK_AC_NUDGE, mds->get_nodeid()), auth);

    // wait...
    if (c)
      lock->add_waiter(SimpleLock::WAIT_STABLE, c);

    // also, requeue, in case we had wrong auth or something
    updated_scatterlocks.push_back(lock->get_updated_item());
  }
}

void Locker::scatter_tick()
{
  dout(10) << "scatter_tick" << dendl;
  
  // updated
  utime_t now = ceph_clock_now(g_ceph_context);
  int n = updated_scatterlocks.size();
  while (!updated_scatterlocks.empty()) {
    ScatterLock *lock = updated_scatterlocks.front();

    if (n-- == 0) break;  // scatter_nudge() may requeue; avoid looping
    
    if (!lock->is_dirty()) {
      updated_scatterlocks.pop_front();
      dout(10) << " removing from updated_scatterlocks " 
	       << *lock << " " << *lock->get_parent() << dendl;
      continue;
    }
    if (now - lock->get_update_stamp() < g_conf->mds_scatter_nudge_interval)
      break;
    updated_scatterlocks.pop_front();
    scatter_nudge(lock, 0);
  }
  mds->mdlog->flush();
}


void Locker::scatter_tempsync(ScatterLock *lock, bool *need_issue)
{
  dout(10) << "scatter_tempsync " << *lock
	   << " on " << *lock->get_parent() << dendl;
  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  assert(0 == "not fully implemented, at least not for filelock");

  CInode *in = static_cast<CInode *>(lock->get_parent());

  switch (lock->get_state()) {
  case LOCK_SYNC: assert(0);   // this shouldn't happen
  case LOCK_LOCK: lock->set_state(LOCK_LOCK_TSYN); break;
  case LOCK_MIX: lock->set_state(LOCK_MIX_TSYN); break;
  default: assert(0);
  }

  int gather = 0;
  if (lock->is_wrlocked())
    gather++;

  if (lock->get_cap_shift() &&
      in->is_head() &&
      in->issued_caps_need_gather(lock)) {
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
    gather++;
  }

  if (lock->get_state() == LOCK_MIX_TSYN &&
      in->is_replicated()) {
    lock->init_gather();
    send_lock_message(lock, LOCK_AC_LOCK);
    gather++;
  }

  if (gather) {
    in->auth_pin(lock);
  } else {
    // do tempsync
    lock->set_state(LOCK_TSYN);
    lock->finish_waiters(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
    if (lock->get_cap_shift()) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
    }
  }
}



// ==========================================================================
// local lock

void Locker::local_wrlock_grab(LocalLock *lock, MutationRef& mut)
{
  dout(7) << "local_wrlock_grab  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  assert(lock->get_parent()->is_auth());
  assert(lock->can_wrlock());
  assert(!mut->wrlocks.count(lock));
  lock->get_wrlock(mut->get_client());
  mut->wrlocks.insert(lock);
  mut->locks.insert(lock);
}

bool Locker::local_wrlock_start(LocalLock *lock, MDRequestRef& mut)
{
  dout(7) << "local_wrlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  assert(lock->get_parent()->is_auth());
  if (lock->can_wrlock()) {
    assert(!mut->wrlocks.count(lock));
    lock->get_wrlock(mut->get_client());
    mut->wrlocks.insert(lock);
    mut->locks.insert(lock);
    return true;
  } else {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
    return false;
  }
}

void Locker::local_wrlock_finish(LocalLock *lock, MutationImpl *mut)
{
  dout(7) << "local_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_wrlock();
  mut->wrlocks.erase(lock);
  mut->locks.erase(lock);
  if (lock->get_num_wrlocks() == 0) {
    lock->finish_waiters(SimpleLock::WAIT_STABLE |
                         SimpleLock::WAIT_WR |
                         SimpleLock::WAIT_RD);
  }
}

bool Locker::local_xlock_start(LocalLock *lock, MDRequestRef& mut)
{
  dout(7) << "local_xlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  assert(lock->get_parent()->is_auth());
  if (!lock->can_xlock_local()) {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
    return false;
  }

  lock->get_xlock(mut, mut->get_client());
  mut->xlocks.insert(lock);
  mut->locks.insert(lock);
  return true;
}

void Locker::local_xlock_finish(LocalLock *lock, MutationImpl *mut)
{
  dout(7) << "local_xlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_xlock();
  mut->xlocks.erase(lock);
  mut->locks.erase(lock);

  lock->finish_waiters(SimpleLock::WAIT_STABLE | 
		       SimpleLock::WAIT_WR | 
		       SimpleLock::WAIT_RD);
}



// ==========================================================================
// file lock


void Locker::file_eval(ScatterLock *lock, bool *need_issue)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  int loner_wanted, other_wanted;
  int wanted = in->get_caps_wanted(&loner_wanted, &other_wanted, CEPH_CAP_SFILE);
  dout(7) << "file_eval wanted=" << gcap_string(wanted)
	  << " loner_wanted=" << gcap_string(loner_wanted)
	  << " other_wanted=" << gcap_string(other_wanted)
	  << "  filelock=" << *lock << " on " << *lock->get_parent()
	  << dendl;

  assert(lock->get_parent()->is_auth());
  assert(lock->is_stable());

  if (lock->get_parent()->is_freezing_or_frozen())
    return;

  if (mdcache->is_readonly()) {
    if (lock->get_state() != LOCK_SYNC) {
      dout(10) << "file_eval read-only FS, syncing " << *lock << " on " << *lock->get_parent() << dendl;
      simple_sync(lock, need_issue);
    }
    return;
  }

  // excl -> *?
  if (lock->get_state() == LOCK_EXCL) {
    dout(20) << " is excl" << dendl;
    int loner_issued, other_issued, xlocker_issued;
    in->get_caps_issued(&loner_issued, &other_issued, &xlocker_issued, CEPH_CAP_SFILE);
    dout(7) << "file_eval loner_issued=" << gcap_string(loner_issued)
            << " other_issued=" << gcap_string(other_issued)
	    << " xlocker_issued=" << gcap_string(xlocker_issued)
	    << dendl;
    if (!((loner_wanted|loner_issued) & (CEPH_CAP_GEXCL|CEPH_CAP_GWR|CEPH_CAP_GBUFFER)) ||
	 (other_wanted & (CEPH_CAP_GEXCL|CEPH_CAP_GWR|CEPH_CAP_GRD)) ||
	(in->inode.is_dir() && in->multiple_nonstale_caps())) {  // FIXME.. :/
      dout(20) << " should lose it" << dendl;
      // we should lose it.
      //  loner  other   want
      //  R      R       SYNC
      //  R      R|W     MIX
      //  R      W       MIX
      //  R|W    R       MIX
      //  R|W    R|W     MIX
      //  R|W    W       MIX
      //  W      R       MIX
      //  W      R|W     MIX
      //  W      W       MIX
      // -> any writer means MIX; RD doesn't matter.
      if (((other_wanted|loner_wanted) & CEPH_CAP_GWR) ||
	  lock->is_waiter_for(SimpleLock::WAIT_WR))
	scatter_mix(lock, need_issue);
      else if (!lock->is_wrlocked())   // let excl wrlocks drain first
	simple_sync(lock, need_issue);
      else
	dout(10) << " waiting for wrlock to drain" << dendl;
    }    
  }

  // * -> excl?
  else if (lock->get_state() != LOCK_EXCL &&
	   !lock->is_rdlocked() &&
	   //!lock->is_waiter_for(SimpleLock::WAIT_WR) &&
	   ((wanted & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER)) ||
	    (in->inode.is_dir() && !in->has_subtree_or_exporting_dirfrag())) &&
	   in->get_target_loner() >= 0) {
    dout(7) << "file_eval stable, bump to loner " << *lock
	    << " on " << *lock->get_parent() << dendl;
    file_excl(lock, need_issue);
  }

  // * -> mixed?
  else if (lock->get_state() != LOCK_MIX &&
	   !lock->is_rdlocked() &&
	   //!lock->is_waiter_for(SimpleLock::WAIT_WR) &&
	   (lock->get_scatter_wanted() ||
	    (in->get_wanted_loner() < 0 && (wanted & CEPH_CAP_GWR)))) {
    dout(7) << "file_eval stable, bump to mixed " << *lock
	    << " on " << *lock->get_parent() << dendl;
    scatter_mix(lock, need_issue);
  }
  
  // * -> sync?
  else if (lock->get_state() != LOCK_SYNC &&
	   !lock->is_wrlocked() &&   // drain wrlocks first!
	   !lock->is_waiter_for(SimpleLock::WAIT_WR) &&
	   !(wanted & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER)) &&
	   !((lock->get_state() == LOCK_MIX) &&
	     in->is_dir() && in->has_subtree_or_exporting_dirfrag())  // if we are a delegation point, stay where we are
	   //((wanted & CEPH_CAP_RD) || 
	   //in->is_replicated() || 
	   //lock->get_num_client_lease() || 
	   //(!loner && lock->get_state() == LOCK_EXCL)) &&
	   ) {
    dout(7) << "file_eval stable, bump to sync " << *lock 
	    << " on " << *lock->get_parent() << dendl;
    simple_sync(lock, need_issue);
  }
}



void Locker::scatter_mix(ScatterLock *lock, bool *need_issue)
{
  dout(7) << "scatter_mix " << *lock << " on " << *lock->get_parent() << dendl;

  CInode *in = static_cast<CInode*>(lock->get_parent());
  assert(in->is_auth());
  assert(lock->is_stable());

  if (lock->get_state() == LOCK_LOCK) {
    in->start_scatter(lock);
    if (in->is_replicated()) {
      // data
      bufferlist softdata;
      lock->encode_locked_state(softdata);

      // bcast to replicas
      send_lock_message(lock, LOCK_AC_MIX, softdata);
    }

    // change lock
    lock->set_state(LOCK_MIX);
    lock->clear_scatter_wanted();
    if (lock->get_cap_shift()) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
    }
  } else {
    // gather?
    switch (lock->get_state()) {
    case LOCK_SYNC: lock->set_state(LOCK_SYNC_MIX); break;
    case LOCK_XSYN:
      file_excl(lock, need_issue);
      if (lock->get_state() != LOCK_EXCL)
	return;
      // fall-thru
    case LOCK_EXCL: lock->set_state(LOCK_EXCL_MIX); break;
    case LOCK_TSYN: lock->set_state(LOCK_TSYN_MIX); break;
    default: assert(0);
    }

    int gather = 0;
    if (lock->is_rdlocked())
      gather++;
    if (in->is_replicated()) {
      if (lock->get_state() != LOCK_EXCL_MIX &&   // EXCL replica is already LOCK
	  lock->get_state() != LOCK_XSYN_EXCL) {  // XSYN replica is already LOCK;  ** FIXME here too!
	send_lock_message(lock, LOCK_AC_MIX);
	lock->init_gather();
	gather++;
      }
    }
    if (lock->is_leased()) {
      revoke_client_leases(lock);
      gather++;
    }
    if (lock->get_cap_shift() &&
	in->is_head() &&
	in->issued_caps_need_gather(lock)) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
      gather++;
    }
    bool need_recover = false;
    if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
      mds->mdcache->queue_file_recover(in);
      need_recover = true;
      gather++;
    }

    if (gather) {
      lock->get_parent()->auth_pin(lock);
      if (need_recover)
	mds->mdcache->do_file_recover();
    } else {
      in->start_scatter(lock);
      lock->set_state(LOCK_MIX);
      lock->clear_scatter_wanted();
      if (in->is_replicated()) {
	bufferlist softdata;
	lock->encode_locked_state(softdata);
	send_lock_message(lock, LOCK_AC_MIX, softdata);
      }
      if (lock->get_cap_shift()) {
	if (need_issue)
	  *need_issue = true;
	else
	  issue_caps(in);
      }
    }
  }
}


void Locker::file_excl(ScatterLock *lock, bool *need_issue)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(7) << "file_excl " << *lock << " on " << *lock->get_parent() << dendl;  

  assert(in->is_auth());
  assert(lock->is_stable());

  assert((in->get_loner() >= 0 && in->mds_caps_wanted.empty()) ||
	 (lock->get_state() == LOCK_XSYN));  // must do xsyn -> excl -> <anything else>
  
  switch (lock->get_state()) {
  case LOCK_SYNC: lock->set_state(LOCK_SYNC_EXCL); break;
  case LOCK_MIX: lock->set_state(LOCK_MIX_EXCL); break;
  case LOCK_LOCK: lock->set_state(LOCK_LOCK_EXCL); break;
  case LOCK_XSYN: lock->set_state(LOCK_XSYN_EXCL); break;
  default: assert(0);
  }
  int gather = 0;
  
  if (lock->is_rdlocked())
    gather++;
  if (lock->is_wrlocked())
    gather++;

  if (in->is_replicated() &&
      lock->get_state() != LOCK_LOCK_EXCL &&
      lock->get_state() != LOCK_XSYN_EXCL) {  // if we were lock, replicas are already lock.
    send_lock_message(lock, LOCK_AC_LOCK);
    lock->init_gather();
    gather++;
  }
  if (lock->is_leased()) {
    revoke_client_leases(lock);
    gather++;
  }
  if (in->is_head() &&
      in->issued_caps_need_gather(lock)) {
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
    gather++;
  }
  bool need_recover = false;
  if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
    mds->mdcache->queue_file_recover(in);
    need_recover = true;
    gather++;
  }
  
  if (gather) {
    lock->get_parent()->auth_pin(lock);
    if (need_recover)
      mds->mdcache->do_file_recover();
  } else {
    lock->set_state(LOCK_EXCL);
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
  }
}

void Locker::file_xsyn(SimpleLock *lock, bool *need_issue)
{
  dout(7) << "file_xsyn on " << *lock << " on " << *lock->get_parent() << dendl;
  CInode *in = static_cast<CInode *>(lock->get_parent());
  assert(in->is_auth());
  assert(in->get_loner() >= 0 && in->mds_caps_wanted.empty());

  switch (lock->get_state()) {
  case LOCK_EXCL: lock->set_state(LOCK_EXCL_XSYN); break;
  default: assert(0);
  }
  
  int gather = 0;
  if (lock->is_wrlocked())
    gather++;

  if (in->is_head() &&
      in->issued_caps_need_gather(lock)) {
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
    gather++;
  }
  
  if (gather) {
    lock->get_parent()->auth_pin(lock);
  } else {
    lock->set_state(LOCK_XSYN);
    lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
  }
}

void Locker::file_recover(ScatterLock *lock)
{
  CInode *in = static_cast<CInode *>(lock->get_parent());
  dout(7) << "file_recover " << *lock << " on " << *in << dendl;

  assert(in->is_auth());
  //assert(lock->is_stable());
  assert(lock->get_state() == LOCK_PRE_SCAN); // only called from MDCache::start_files_to_recover()

  int gather = 0;
  
  /*
  if (in->is_replicated()
      lock->get_sm()->states[oldstate].replica_state != LOCK_LOCK) {
    send_lock_message(lock, LOCK_AC_LOCK);
    lock->init_gather();
    gather++;
  }
  */
  if (in->is_head() &&
      in->issued_caps_need_gather(lock)) {
    issue_caps(in);
    gather++;
  }

  lock->set_state(LOCK_SCAN);
  if (gather)
    in->state_set(CInode::STATE_NEEDSRECOVER);
  else
    mds->mdcache->queue_file_recover(in);
}


// messenger
/* This function DOES put the passed message before returning */
void Locker::handle_file_lock(ScatterLock *lock, MLock *m)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  int from = m->get_asker();

  if (mds->is_rejoin()) {
    if (in->is_rejoining()) {
      dout(7) << "handle_file_lock still rejoining " << *in
	      << ", dropping " << *m << dendl;
      m->put();
      return;
    }
  }

  dout(7) << "handle_file_lock a=" << get_lock_action_name(m->get_action())
	  << " on " << *lock
	  << " from mds." << from << " " 
	  << *in << dendl;

  bool caps = lock->get_cap_shift();
  
  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    assert(lock->get_state() == LOCK_LOCK ||
	   lock->get_state() == LOCK_MIX ||
	   lock->get_state() == LOCK_MIX_SYNC2);
    
    if (lock->get_state() == LOCK_MIX) {
      lock->set_state(LOCK_MIX_SYNC);
      eval_gather(lock, true);
      break;
    }

    (static_cast<ScatterLock *>(lock))->finish_flush();
    (static_cast<ScatterLock *>(lock))->clear_flushed();

    // ok
    lock->decode_locked_state(m->get_data());
    lock->set_state(LOCK_SYNC);

    lock->get_rdlock();
    if (caps)
      issue_caps(in);
    lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    lock->put_rdlock();
    break;
    
  case LOCK_AC_LOCK:
    switch (lock->get_state()) {
    case LOCK_SYNC: lock->set_state(LOCK_SYNC_LOCK); break;
    case LOCK_MIX: lock->set_state(LOCK_MIX_LOCK); break;
    default: assert(0);
    }

    eval_gather(lock, true);
    break;

  case LOCK_AC_LOCKFLUSHED:
    (static_cast<ScatterLock *>(lock))->finish_flush();
    (static_cast<ScatterLock *>(lock))->clear_flushed();
    break;
    
  case LOCK_AC_MIX:
    assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_LOCK ||
	   lock->get_state() == LOCK_SYNC_MIX2);
    
    if (lock->get_state() == LOCK_SYNC) {
      // MIXED
      lock->set_state(LOCK_SYNC_MIX);
      eval_gather(lock, true);
      break;
    } 
    
    // ok
    lock->decode_locked_state(m->get_data());
    lock->set_state(LOCK_MIX);

    if (caps)
      issue_caps(in);
    
    lock->finish_waiters(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
    assert(lock->get_state() == LOCK_SYNC_LOCK ||
           lock->get_state() == LOCK_MIX_LOCK ||
           lock->get_state() == LOCK_MIX_LOCK2 ||
           lock->get_state() == LOCK_MIX_EXCL ||
           lock->get_state() == LOCK_SYNC_EXCL ||
           lock->get_state() == LOCK_SYNC_MIX ||
	   lock->get_state() == LOCK_MIX_TSYN);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->get_state() == LOCK_MIX_LOCK ||
	lock->get_state() == LOCK_MIX_LOCK2 ||
	lock->get_state() == LOCK_MIX_EXCL ||
	lock->get_state() == LOCK_MIX_TSYN) {
      lock->decode_locked_state(m->get_data());
      // replica is waiting for AC_LOCKFLUSHED, eval_gather() should not
      // delay calling scatter_writebehind().
      lock->clear_flushed();
    }

    if (lock->is_gathering()) {
      dout(7) << "handle_file_lock " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_file_lock " << *in << " from " << from
	      << ", last one" << dendl;
      eval_gather(lock);
    }
    break;
    
  case LOCK_AC_SYNCACK:
    assert(lock->get_state() == LOCK_MIX_SYNC);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    lock->decode_locked_state(m->get_data());

    if (lock->is_gathering()) {
      dout(7) << "handle_file_lock " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_file_lock " << *in << " from " << from
	      << ", last one" << dendl;
      eval_gather(lock);
    }
    break;

  case LOCK_AC_MIXACK:
    assert(lock->get_state() == LOCK_SYNC_MIX);
    assert(lock->is_gathering(from));
    lock->remove_gather(from);
    
    if (lock->is_gathering()) {
      dout(7) << "handle_file_lock " << *in << " from " << from
	      << ", still gathering " << lock->get_gather_set() << dendl;
    } else {
      dout(7) << "handle_file_lock " << *in << " from " << from
	      << ", last one" << dendl;
      eval_gather(lock);
    }
    break;


    // requests....
  case LOCK_AC_REQSCATTER:
    if (lock->is_stable()) {
      /* NOTE: we can do this _even_ if !can_auth_pin (i.e. freezing)
       *  because the replica should be holding an auth_pin if they're
       *  doing this (and thus, we are freezing, not frozen, and indefinite
       *  starvation isn't an issue).
       */
      dout(7) << "handle_file_lock got scatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      if (lock->get_state() != LOCK_MIX)  // i.e., the reqscatter didn't race with an actual mix/scatter
	scatter_mix(lock);
    } else {
      dout(7) << "handle_file_lock got scatter request, !stable, marking scatter_wanted on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      lock->set_scatter_wanted();
    }
    break;

  case LOCK_AC_REQUNSCATTER:
    if (lock->is_stable()) {
      /* NOTE: we can do this _even_ if !can_auth_pin (i.e. freezing)
       *  because the replica should be holding an auth_pin if they're
       *  doing this (and thus, we are freezing, not frozen, and indefinite
       *  starvation isn't an issue).
       */
      dout(7) << "handle_file_lock got unscatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      if (lock->get_state() == LOCK_MIX)  // i.e., the reqscatter didn't race with an actual mix/scatter
	simple_lock(lock);  // FIXME tempsync?
    } else {
      dout(7) << "handle_file_lock ignoring unscatter request on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      lock->set_unscatter_wanted();
    }
    break;

  case LOCK_AC_REQRDLOCK:
    handle_reqrdlock(lock, m);
    break;

  case LOCK_AC_NUDGE:
    if (!lock->get_parent()->is_auth()) {
      dout(7) << "handle_file_lock IGNORING nudge on non-auth " << *lock
	      << " on " << *lock->get_parent() << dendl;
    } else if (!lock->get_parent()->is_replicated()) {
      dout(7) << "handle_file_lock IGNORING nudge on non-replicated " << *lock
	      << " on " << *lock->get_parent() << dendl;
    } else {
      dout(7) << "handle_file_lock trying nudge on " << *lock
	      << " on " << *lock->get_parent() << dendl;
      scatter_nudge(lock, 0, true);
      mds->mdlog->flush();
    }    
    break;

  default:
    assert(0);
  }  
  
  m->put();
}






