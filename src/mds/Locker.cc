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


#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "common/config.h"
#include "events/EOpen.h"
#include "events/EUpdate.h"
#include "Locker.h"
#include "MDBalancer.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDSRank.h"
#include "MDSMap.h"
#include "messages/MInodeFileCaps.h"
#include "messages/MMDSPeerRequest.h"
#include "Migrator.h"
#include "msg/Messenger.h"
#include "osdc/Objecter.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_context g_ceph_context
#define dout_prefix _prefix(_dout, mds)

using namespace std;

static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".locker ";
}


class LockerContext : public MDSContext {
protected:
  Locker *locker;
  MDSRank *get_mds() override
  {
    return locker->mds;
  }

public:
  explicit LockerContext(Locker *locker_) : locker(locker_) {
    ceph_assert(locker != NULL);
  }
};

class LockerLogContext : public MDSLogContextBase {
protected:
  Locker *locker;
  MDSRank *get_mds() override
  {
    return locker->mds;
  }

public:
  explicit LockerLogContext(Locker *locker_) : locker(locker_) {
    ceph_assert(locker != NULL);
  }
};

Locker::Locker(MDSRank *m, MDCache *c) :
  need_snapflush_inodes(member_offset(CInode, item_caps)), mds(m), mdcache(c) {}


void Locker::dispatch(const cref_t<Message> &m)
{

  switch (m->get_type()) {
    // inter-mds locking
  case MSG_MDS_LOCK:
    handle_lock(ref_cast<MLock>(m));
    break;
    // inter-mds caps
  case MSG_MDS_INODEFILECAPS:
    handle_inode_file_caps(ref_cast<MInodeFileCaps>(m));
    break;
    // client sync
  case CEPH_MSG_CLIENT_CAPS:
    handle_client_caps(ref_cast<MClientCaps>(m));
    break;
  case CEPH_MSG_CLIENT_CAPRELEASE:
    handle_client_cap_release(ref_cast<MClientCapRelease>(m));
    break;
  case CEPH_MSG_CLIENT_LEASE:
    handle_client_lease(ref_cast<MClientLease>(m));
    break;
  default:
    derr << "locker unknown message " << m->get_type() << dendl;
    ceph_abort_msg("locker unknown message");
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
  for (const auto &it : lock->get_parent()->get_replicas()) {
    if (mds->is_cluster_degraded() &&
	mds->mdsmap->get_state(it.first) < MDSMap::STATE_REJOIN)
      continue;
    auto m = make_message<MLock>(lock, msg, mds->get_nodeid());
    mds->send_message_mds(m, it.first);
  }
}

void Locker::send_lock_message(SimpleLock *lock, int msg, const bufferlist &data)
{
  for (const auto &it : lock->get_parent()->get_replicas()) {
    if (mds->is_cluster_degraded() &&
	mds->mdsmap->get_state(it.first) < MDSMap::STATE_REJOIN)
      continue;
    auto m = make_message<MLock>(lock, msg, mds->get_nodeid());
    m->set_data(data);
    mds->send_message_mds(m, it.first);
  }
}

bool Locker::try_rdlock_snap_layout(CInode *in, const MDRequestRef& mdr,
				    int n, bool want_layout)
{
  dout(10) << __func__ << " " << *mdr << " " << *in << dendl;
  // rdlock ancestor snaps
  inodeno_t root;
  int depth = -1;
  bool found_locked = false;
  bool found_layout = false;

  if (want_layout)
    ceph_assert(n == 0);

  client_t client = mdr->get_client();

  CInode *t = in;
  while (true) {
    ++depth;
    if (!found_locked && mdr->is_rdlocked(&t->snaplock))
      found_locked = true;

    if (!found_locked) {
      if (!t->snaplock.can_rdlock(client)) {
	t->snaplock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
	goto failed;
      }
      t->snaplock.get_rdlock();
      mdr->locks.emplace(&t->snaplock, MutationImpl::LockOp::RDLOCK);
      dout(20) << " got rdlock on " << t->snaplock << " " << *t << dendl;
    }
    if (want_layout && !found_layout) {
      if (!mdr->is_rdlocked(&t->policylock)) {
	if (!t->policylock.can_rdlock(client)) {
	  t->policylock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
	  goto failed;
	}
	t->policylock.get_rdlock();
	mdr->locks.emplace(&t->policylock, MutationImpl::LockOp::RDLOCK);
	dout(20) << " got rdlock on " << t->policylock << " " << *t << dendl;
      }
      if (t->get_projected_inode()->has_layout()) {
	mdr->dir_layout = t->get_projected_inode()->layout;
	found_layout = true;
      }
    }
    CDentry* pdn = t->get_projected_parent_dn();
    if (!pdn) {
      root = t->ino();
      break;
    }
    t = pdn->get_dir()->get_inode();
  }

  mdr->dir_root[n] = root;
  mdr->dir_depth[n] = depth;
  return true;

failed:
  dout(10) << __func__ << " failed" << dendl;

  drop_locks(mdr.get(), nullptr);
  mdr->drop_local_auth_pins();
  return false;
}

struct MarkEventOnDestruct {
  MDRequestRef mdr;
  std::string_view message;
  bool mark_event;
  MarkEventOnDestruct(const MDRequestRef& _mdr, std::string_view _message) :
      mdr(_mdr),
      message(_message),
      mark_event(true) {}
  ~MarkEventOnDestruct() {
    if (mark_event)
      mdr->mark_event(message);
  }
};

/* If this function returns false, the mdr has been placed
 * on the appropriate wait list */
bool Locker::acquire_locks(const MDRequestRef& mdr,
			   MutationImpl::LockOpVec& lov,
			   CInode *auth_pin_freeze,
			   bool auth_pin_nonblocking)
{
  dout(10) << "acquire_locks " << *mdr << dendl;

  MarkEventOnDestruct marker(mdr, "failed to acquire_locks");

  client_t client = mdr->get_client();

  set<MDSCacheObject*> mustpin;  // items to authpin
  if (auth_pin_freeze)
    mustpin.insert(auth_pin_freeze);

  // xlocks
  for (size_t i = 0; i < lov.size(); ++i) {
    auto& p = lov[i];
    SimpleLock *lock = p.lock;
    MDSCacheObject *object = lock->get_parent();

    if (p.is_xlock()) {
      if ((lock->get_type() == CEPH_LOCK_ISNAP ||
	   lock->get_type() == CEPH_LOCK_IPOLICY) &&
	  mds->is_cluster_degraded() &&
	  mdr->is_leader() &&
	  !mdr->is_queued_for_replay()) {
	// waiting for recovering mds, to guarantee replayed requests and mksnap/setlayout
	// get processed in proper order.
	bool wait = false;
	if (object->is_auth()) {
	  if (!mdr->is_xlocked(lock)) {
	    set<mds_rank_t> ls;
	    object->list_replicas(ls);
	    for (auto m : ls) {
	      if (mds->mdsmap->get_state(m) < MDSMap::STATE_ACTIVE) {
		wait = true;
		break;
	      }
	    }
	  }
	} else {
	  // if the lock is the latest locked one, it's possible that peer mds got the lock
	  // while there are recovering mds.
	  if (!mdr->is_xlocked(lock) || mdr->is_last_locked(lock))
	    wait = true;
	}
	if (wait) {
	  dout(10) << " must xlock " << *lock << " " << *object
		   << ", waiting for cluster recovered" << dendl;
	  mds->locker->drop_locks(mdr.get(), NULL);
	  mdr->drop_local_auth_pins();
	  mds->wait_for_cluster_recovered(new C_MDS_RetryRequest(mdcache, mdr));
	  return false;
	}
      }

      dout(20) << " must xlock " << *lock << " " << *object << dendl;

      mustpin.insert(object);

      // augment xlock with a versionlock?
      if (lock->get_type() == CEPH_LOCK_DN) {
	CDentry *dn = static_cast<CDentry*>(object);
	if (!dn->is_auth())
	  continue;
	if (mdr->is_leader()) {
	  // leader.  wrlock versionlock so we can pipeline dentry updates to journal.
	  lov.add_wrlock(&dn->versionlock, i + 1);
	} else {
	  // peer.  exclusively lock the dentry version (i.e. block other journal updates).
	  // this makes rollback safe.
	  lov.add_xlock(&dn->versionlock, i + 1);
	}
      }
      if (lock->get_type() >= CEPH_LOCK_IFIRST && lock->get_type() != CEPH_LOCK_IVERSION) {
	// inode version lock?
	CInode *in = static_cast<CInode*>(object);
	if (!in->is_auth())
	  continue;
	if (mdr->is_leader()) {
	  // leader.  wrlock versionlock so we can pipeline inode updates to journal.
	  lov.add_wrlock(&in->versionlock, i + 1);
	} else {
	  // peer.  exclusively lock the inode version (i.e. block other journal updates).
	  // this makes rollback safe.
	  lov.add_xlock(&in->versionlock, i + 1);
	}
      }
    } else if (p.is_wrlock()) {
      dout(20) << " must wrlock " << *lock << " " << *object << dendl;
      client_t _client = p.is_state_pin() ? lock->get_excl_client() : client;
      if (object->is_auth()) {
	mustpin.insert(object);
      } else if (!object->is_auth() &&
		 !lock->can_wrlock(_client) &&  // we might have to request a scatter
		 !mdr->is_peer()) {           // if we are peer (remote_wrlock), the leader already authpinned
	dout(15) << " will also auth_pin " << *object
		 << " in case we need to request a scatter" << dendl;
	mustpin.insert(object);
      }
    } else if (p.is_remote_wrlock()) {
      dout(20) << " must remote_wrlock on mds." << p.wrlock_target << " "
	       << *lock << " " << *object << dendl;
      mustpin.insert(object);
    } else if (p.is_rdlock()) {

      dout(20) << " must rdlock " << *lock << " " << *object << dendl;
      if (object->is_auth()) {
	mustpin.insert(object);
      } else if (!object->is_auth() &&
		 !lock->can_rdlock(client)) {      // we might have to request an rdlock
	dout(15) << " will also auth_pin " << *object
		 << " in case we need to request a rdlock" << dendl;
	mustpin.insert(object);
      }
    } else {
      ceph_assert(0 == "locker unknown lock operation");
    }
  }

  lov.sort_and_merge();
 
  // AUTH PINS
  map<mds_rank_t, set<MDSCacheObject*> > mustpin_remote;  // mds -> (object set)
  
  // can i auth pin them all now?
  marker.message = "failed to authpin local pins";
  for (const auto &p : mustpin) {
    MDSCacheObject *object = p;

    dout(10) << " must authpin " << *object << dendl;

    if (mdr->is_auth_pinned(object)) {
      if (object != (MDSCacheObject*)auth_pin_freeze)
	continue;
      if (mdr->more()->is_remote_frozen_authpin) {
	if (mdr->more()->rename_inode == auth_pin_freeze)
	  continue;
	// unfreeze auth pin for the wrong inode
	mustpin_remote[mdr->more()->rename_inode->authority().first].size();
      }
    }
    
    if (!object->is_auth()) {
      if (mdr->lock_cache) { // debug
	ceph_assert(mdr->lock_cache->opcode == CEPH_MDS_OP_UNLINK);
	CDentry *dn = mdr->dn[0].back();
	ceph_assert(dn->get_projected_linkage()->is_remote());
      }

      if (object->is_ambiguous_auth()) {
	// wait
	dout(10) << " ambiguous auth, waiting to authpin " << *object << dendl;
	mdr->disable_lock_cache();
	drop_locks(mdr.get());
	mdr->drop_local_auth_pins();
	marker.message = "waiting for single auth, object is being migrated";
	object->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	return false;
      }
      mustpin_remote[object->authority().first].insert(object);
      continue;
    }
    int err = 0;
    if (!object->can_auth_pin(&err)) {
      if (mdr->lock_cache) {
	CDir *dir;
	if (CInode *in = dynamic_cast<CInode*>(object)) {
	  ceph_assert(!in->is_frozen_inode() && !in->is_frozen_auth_pin());
	  dir = in->get_projected_parent_dir();
	} else if (CDentry *dn = dynamic_cast<CDentry*>(object)) {
	  dir = dn->get_dir();
	} else {
	  ceph_assert(0 == "unknown type of lock parent");
	}
	if (dir->get_inode() == mdr->lock_cache->get_dir_inode()) {
	  // forcibly auth pin if there is lock cache on parent dir
	  continue;
	}

	{ // debug
	  ceph_assert(mdr->lock_cache->opcode == CEPH_MDS_OP_UNLINK);
	  CDentry *dn = mdr->dn[0].back();
	  ceph_assert(dn->get_projected_linkage()->is_remote());
	}
      }

      // wait
      mdr->disable_lock_cache();
      drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      if (auth_pin_nonblocking) {
	dout(10) << " can't auth_pin (freezing?) " << *object << ", nonblocking" << dendl;
	mdr->aborted = true;
	return false;
      }
      if (err == MDSCacheObject::ERR_EXPORTING_TREE) {
	marker.message = "failed to authpin, subtree is being exported";
      } else if (err == MDSCacheObject::ERR_FRAGMENTING_DIR) {
	marker.message = "failed to authpin, dir is being fragmented";
      } else if (err == MDSCacheObject::ERR_EXPORTING_INODE) {
	marker.message = "failed to authpin, inode is being exported";
      }
      dout(10) << " can't auth_pin (freezing?), waiting to authpin " << *object << dendl;
      object->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));

      if (mdr->is_any_remote_auth_pin())
	notify_freeze_waiter(object);

      return false;
    }
  }

  // ok, grab local auth pins
  for (const auto& p : mustpin) {
    MDSCacheObject *object = p;
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
    for (const auto& p : mdr->object_states) {
      if (p.second.remote_auth_pinned == MDS_RANK_NONE)
	continue;
      ceph_assert(p.second.remote_auth_pinned == p.first->authority().first);
      auto q = mustpin_remote.find(p.second.remote_auth_pinned);
      if (q != mustpin_remote.end())
	q->second.insert(p.first);
    }

    for (auto& p : mustpin_remote) {
      dout(10) << "requesting remote auth_pins from mds." << p.first << dendl;

      // wait for active auth
      if (mds->is_cluster_degraded() &&
	  !mds->mdsmap->is_clientreplay_or_active_or_stopping(p.first)) {
	dout(10) << " mds." << p.first << " is not active" << dendl;
	if (mdr->more()->waiting_on_peer.empty())
	  mds->wait_for_active_peer(p.first, new C_MDS_RetryRequest(mdcache, mdr));
	return false;
      }
      
      auto req = make_message<MMDSPeerRequest>(mdr->reqid, mdr->attempt,
						MMDSPeerRequest::OP_AUTHPIN);
      for (auto& o : p.second) {
	dout(10) << " req remote auth_pin of " << *o << dendl;
	MDSCacheObjectInfo info;
	o->set_object_info(info);
	req->get_authpins().push_back(info);
	if (o == auth_pin_freeze)
	  o->set_object_info(req->get_authpin_freeze());
	mdr->pin(o);
      }
      if (auth_pin_nonblocking)
	req->mark_nonblocking();
      else if (!mdr->locks.empty())
	req->mark_notify_blocking();

      mds->send_message_mds(req, p.first);

      // put in waiting list
      auto ret = mdr->more()->waiting_on_peer.insert(p.first);
      ceph_assert(ret.second);
    }
    return false;
  }

  // caps i'll need to issue
  set<CInode*> issue_set;
  bool result = false;

  // acquire locks.
  // make sure they match currently acquired locks.
  for (const auto& p : lov) {
    auto lock = p.lock;
    if (p.is_xlock()) {
      if (mdr->is_xlocked(lock)) {
	dout(10) << " already xlocked " << *lock << " " << *lock->get_parent() << dendl;
	continue;
      }
      if (mdr->locking && lock != mdr->locking)
	cancel_locking(mdr.get(), &issue_set);
      if (!xlock_start(lock, mdr)) {
	marker.message = "failed to xlock, waiting";
	goto out;
      }
      dout(10) << " got xlock on " << *lock << " " << *lock->get_parent() << dendl;
    } else if (p.is_wrlock() || p.is_remote_wrlock()) {
      auto it = mdr->locks.find(lock);
      if (p.is_remote_wrlock()) {
	if (it != mdr->locks.end() && it->is_remote_wrlock()) {
	  dout(10) << " already remote_wrlocked " << *lock << " " << *lock->get_parent() << dendl;
	} else {
	  if (mdr->locking && lock != mdr->locking)
	    cancel_locking(mdr.get(), &issue_set);
	  marker.message = "waiting for remote wrlocks";
	  remote_wrlock_start(lock, p.wrlock_target, mdr);
	  goto out;
	}
      }
      if (p.is_wrlock()) {
	if (it != mdr->locks.end() && it->is_wrlock()) {
	  dout(10) << " already wrlocked " << *lock << " " << *lock->get_parent() << dendl;
	  continue;
	}
	client_t _client = p.is_state_pin() ? lock->get_excl_client() : client;
	if (p.is_remote_wrlock()) {
	  // nowait if we have already gotten remote wrlock
	  if (!wrlock_try(lock, mdr, _client)) {
	    marker.message = "failed to wrlock, dropping remote wrlock and waiting";
	    // can't take the wrlock because the scatter lock is gathering. need to
	    // release the remote wrlock, so that the gathering process can finish.
	    ceph_assert(it != mdr->locks.end());
	    remote_wrlock_finish(it, mdr.get());
	    remote_wrlock_start(lock, p.wrlock_target, mdr);
	    goto out;
	  }
	} else {
	  if (!wrlock_start(p, mdr)) {
	    ceph_assert(!p.is_remote_wrlock());
	    marker.message = "failed to wrlock, waiting";
	    goto out;
	  }
	}
	dout(10) << " got wrlock on " << *lock << " " << *lock->get_parent() << dendl;
      }
    } else {
      if (mdr->is_rdlocked(lock)) {
	dout(10) << " already rdlocked " << *lock << " " << *lock->get_parent() << dendl;
	continue;
      }

      ceph_assert(mdr->is_leader());
      if (lock->needs_recover()) {
	if (mds->is_cluster_degraded()) {
	  if (!mdr->is_queued_for_replay()) {
	    // see comments in SimpleLock::set_state_rejoin() and
	    // ScatterLock::encode_state_for_rejoin()
	    drop_locks(mdr.get());
	    mds->wait_for_cluster_recovered(new C_MDS_RetryRequest(mdcache, mdr));
	    dout(10) << " rejoin recovering " << *lock << " " << *lock->get_parent()
		     << ", waiting for cluster recovered" << dendl;
	    marker.message = "rejoin recovering lock, waiting for cluster recovered";
	    return false;
	  }
	} else {
	  lock->clear_need_recover();
	}
      }

      if (!rdlock_start(lock, mdr)) {
	marker.message = "failed to rdlock, waiting";
	goto out;
      }
      dout(10) << " got rdlock on " << *lock << " " << *lock->get_parent() << dendl;
    }
  }

  mdr->set_mds_stamp(ceph_clock_now());
  result = true;
  marker.message = "acquired locks";

 out:
  issue_caps_set(issue_set);
  return result;
}

void Locker::notify_freeze_waiter(MDSCacheObject *o)
{
  CDir *dir = NULL;
  if (CInode *in = dynamic_cast<CInode*>(o)) {
    if (!in->is_root())
      dir = in->get_parent_dir();
  } else if (CDentry *dn = dynamic_cast<CDentry*>(o)) {
    dir = dn->get_dir();
  } else {
    dir = dynamic_cast<CDir*>(o);
    ceph_assert(dir);
  }
  if (dir) {
    if (dir->is_freezing_dir())
      mdcache->fragment_freeze_inc_num_waiters(dir);
    if (dir->is_freezing_tree()) {
      while (!dir->is_freezing_tree_root())
	dir = dir->get_parent_dir();
      mdcache->migrator->export_freeze_inc_num_waiters(dir);
    }
  }
}

void Locker::set_xlocks_done(MutationImpl *mut, bool skip_dentry)
{
  for (const auto &p : mut->locks) {
    if (!p.is_xlock())
      continue;
    MDSCacheObject *obj = p.lock->get_parent();
    ceph_assert(obj->is_auth());
    if (skip_dentry &&
	(p.lock->get_type() == CEPH_LOCK_DN || p.lock->get_type() == CEPH_LOCK_DVERSION))
      continue;
    dout(10) << "set_xlocks_done on " << *p.lock << " " << *obj << dendl;
    p.lock->set_xlock_done();
  }
}

void Locker::_drop_locks(MutationImpl *mut, set<CInode*> *pneed_issue,
			 bool drop_rdlocks)
{
  set<mds_rank_t> peers;

  for (auto it = mut->locks.begin(); it != mut->locks.end(); ) {
    SimpleLock *lock = it->lock;
    MDSCacheObject *obj = lock->get_parent();

    if (it->is_xlock()) {
      if (obj->is_auth()) {
	bool ni = false;
	xlock_finish(it++, mut, &ni);
	if (ni)
	  pneed_issue->insert(static_cast<CInode*>(obj));
      } else {
	ceph_assert(lock->get_sm()->can_remote_xlock);
	peers.insert(obj->authority().first);
	lock->put_xlock();
	mut->locks.erase(it++);
      }
    } else if (it->is_wrlock() || it->is_remote_wrlock()) {
      if (it->is_remote_wrlock()) {
	peers.insert(it->wrlock_target);
	it->clear_remote_wrlock();
      }
      if (it->is_wrlock()) {
	bool ni = false;
	wrlock_finish(it++, mut, &ni);
	if (ni)
	  pneed_issue->insert(static_cast<CInode*>(obj));
      } else {
	mut->locks.erase(it++);
      }
    } else if (drop_rdlocks && it->is_rdlock()) {
      bool ni = false;
      rdlock_finish(it++, mut, &ni);
      if (ni)
	pneed_issue->insert(static_cast<CInode*>(obj));
    } else {
      ++it;
    }
  }

  if (drop_rdlocks) {
    if (mut->lock_cache) {
      put_lock_cache(mut->lock_cache);
      mut->lock_cache = nullptr;
    }
  }

  for (set<mds_rank_t>::iterator p = peers.begin(); p != peers.end(); ++p) {
    if (!mds->is_cluster_degraded() ||
	mds->mdsmap->get_state(*p) >= MDSMap::STATE_REJOIN) {
      dout(10) << "_drop_non_rdlocks dropping remote locks on mds." << *p << dendl;
      auto peerreq = make_message<MMDSPeerRequest>(mut->reqid, mut->attempt,
						     MMDSPeerRequest::OP_DROPLOCKS);
      mds->send_message_mds(peerreq, *p);
    }
  }
}

void Locker::cancel_locking(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  SimpleLock *lock = mut->locking;
  ceph_assert(lock);
  dout(10) << "cancel_locking " << *lock << " on " << *mut << dendl;

  if (lock->get_parent()->is_auth()) {
    bool need_issue = false;
    if (lock->get_state() == LOCK_PREXLOCK) {
      _finish_xlock(lock, -1, &need_issue);
    } else if (lock->get_state() == LOCK_LOCK_XLOCK) {
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
  _drop_locks(mut, pneed_issue, true);

  if (pneed_issue == &my_need_issue)
    issue_caps_set(*pneed_issue);
  mut->locking_state = 0;
}

void Locker::drop_non_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue)
{
  set<CInode*> my_need_issue;
  if (!pneed_issue)
    pneed_issue = &my_need_issue;

  _drop_locks(mut, pneed_issue, false);

  if (pneed_issue == &my_need_issue)
    issue_caps_set(*pneed_issue);
}

void Locker::drop_rdlocks_for_early_reply(MutationImpl *mut)
{
  set<CInode*> need_issue;

  for (auto it = mut->locks.begin(); it != mut->locks.end(); ) {
    if (!it->is_rdlock()) {
      ++it;
      continue;
    }
    SimpleLock *lock = it->lock;
    // make later mksnap/setlayout (at other mds) wait for this unsafe request
    if (lock->get_type() == CEPH_LOCK_ISNAP ||
	lock->get_type() == CEPH_LOCK_IPOLICY) {
      ++it;
      continue;
    }
    bool ni = false;
    rdlock_finish(it++, mut, &ni);
    if (ni)
      need_issue.insert(static_cast<CInode*>(lock->get_parent()));
  }

  issue_caps_set(need_issue);
}

void Locker::drop_locks_for_fragment_unfreeze(MutationImpl *mut)
{
  set<CInode*> need_issue;

  for (auto it = mut->locks.begin(); it != mut->locks.end(); ) {
    SimpleLock *lock = it->lock;
    if (lock->get_type() == CEPH_LOCK_IDFT) {
      ++it;
      continue;
    }
    bool ni = false;
    wrlock_finish(it++, mut, &ni);
    if (ni)
      need_issue.insert(static_cast<CInode*>(lock->get_parent()));
  }
  issue_caps_set(need_issue);
}

class C_MDL_DropCache : public LockerContext {
  MDLockCache *lock_cache;
public:
  C_MDL_DropCache(Locker *l, MDLockCache *lc) :
    LockerContext(l), lock_cache(lc) { }
  void finish(int r) override {
    locker->drop_locks(lock_cache);
    lock_cache->cleanup();
    delete lock_cache;
  }
};

void Locker::put_lock_cache(MDLockCache* lock_cache)
{
  ceph_assert(lock_cache->ref > 0);
  if (--lock_cache->ref > 0)
    return;

  ceph_assert(lock_cache->invalidating);

  lock_cache->detach_locks();

  CInode *diri = lock_cache->get_dir_inode();
  for (auto dir : lock_cache->auth_pinned_dirfrags) {
    if (dir->get_inode() != diri)
      continue;
    dir->enable_frozen_inode();
  }

  mds->queue_waiter(new C_MDL_DropCache(this, lock_cache));
}

int Locker::get_cap_bit_for_lock_cache(int op)
{
  switch(op) {
    case CEPH_MDS_OP_CREATE:
      return CEPH_CAP_DIR_CREATE;
    case CEPH_MDS_OP_UNLINK:
      return CEPH_CAP_DIR_UNLINK;
    default:
      ceph_assert(0 == "unsupported operation");
      return 0;
  }
}

void Locker::invalidate_lock_cache(MDLockCache *lock_cache)
{
  ceph_assert(lock_cache->item_cap_lock_cache.is_on_list());
  if (lock_cache->invalidating) {
    ceph_assert(!lock_cache->client_cap);
  } else {
    lock_cache->invalidating = true;
    lock_cache->detach_dirfrags();
  }

  Capability *cap = lock_cache->client_cap;
  if (cap) {
    int cap_bit = get_cap_bit_for_lock_cache(lock_cache->opcode);
    cap->clear_lock_cache_allowed(cap_bit);
    if (cap->issued() & cap_bit)
      issue_caps(lock_cache->get_dir_inode(), cap);
    else
      cap = nullptr;
  }

  if (!cap) {
    lock_cache->item_cap_lock_cache.remove_myself();
    put_lock_cache(lock_cache);
  }
}

void Locker::eval_lock_caches(Capability *cap)
{
  for (auto p = cap->lock_caches.begin(); !p.end(); ) {
    MDLockCache *lock_cache = *p;
    ++p;
    if (!lock_cache->invalidating)
      continue;
    int cap_bit = get_cap_bit_for_lock_cache(lock_cache->opcode);
    if (!(cap->issued() & cap_bit)) {
      lock_cache->item_cap_lock_cache.remove_myself();
      put_lock_cache(lock_cache);
    }
  }
}

// ask lock caches to release auth pins
void Locker::invalidate_lock_caches(CDir *dir)
{
  dout(10) << "invalidate_lock_caches on " << *dir << dendl;
  auto &lock_caches = dir->lock_caches_with_auth_pins;
  while (!lock_caches.empty()) {
    invalidate_lock_cache(lock_caches.front()->parent);
  }
}

// ask lock caches to release locks
void Locker::invalidate_lock_caches(SimpleLock *lock)
{
  dout(10) << "invalidate_lock_caches " << *lock << " on " << *lock->get_parent() << dendl;
  if (lock->is_cached()) {
    auto&& lock_caches = lock->get_active_caches();
    for (auto& lc : lock_caches)
      invalidate_lock_cache(lc);
  }
}

void Locker::create_lock_cache(const MDRequestRef& mdr, CInode *diri, file_layout_t *dir_layout)
{
  if (mdr->lock_cache)
    return;

  client_t client = mdr->get_client();
  int opcode = mdr->client_request->get_op();
  dout(10) << "create_lock_cache for client." << client << "/" << ceph_mds_op_name(opcode)<< " on " << *diri << dendl;

  if (!diri->is_auth()) {
    dout(10) << " dir inode is not auth, noop" << dendl;
    return;
  }

  if (mdr->has_more() && !mdr->more()->peers.empty()) {
    dout(10) << " there are peers requests for " << *mdr << ", noop" << dendl;
    return;
  }

  Capability *cap = diri->get_client_cap(client);
  if (!cap) {
    dout(10) << " there is no cap for client." << client << ", noop" << dendl;
    return;
  }

  for (auto p = cap->lock_caches.begin(); !p.end(); ++p) {
    if ((*p)->opcode == opcode) {
      dout(10) << " lock cache already exists for " << ceph_mds_op_name(opcode) << ", noop" << dendl;
      return;
    }
  }

  set<MDSCacheObject*> ancestors;
  for (CInode *in = diri; ; ) {
    CDentry *pdn = in->get_projected_parent_dn();
    if (!pdn)
      break;
    // ancestors.insert(pdn);
    in = pdn->get_dir()->get_inode();
    ancestors.insert(in);
  }

  for (auto& p : mdr->object_states) {
    if (p.first != diri && !ancestors.count(p.first))
      continue;
    auto& stat = p.second;
    if (stat.auth_pinned) {
      if (!p.first->can_auth_pin()) {
	dout(10) << " can't auth_pin(freezing?) lock parent " << *p.first << ", noop" << dendl;
	return;
      }
      if (CInode *in = dynamic_cast<CInode*>(p.first); in->is_parent_projected()) {
	CDir *dir = in->get_projected_parent_dir();
	if (!dir->can_auth_pin()) {
	  dout(10) << " can't auth_pin(!auth|freezing?) dirfrag " << *dir << ", noop" << dendl;
	  return;
	}
      }
    }
  }

  std::vector<CDir*> dfv;
  dfv.reserve(diri->get_num_dirfrags());

  diri->get_dirfrags(dfv);
  for (auto dir : dfv) {
    if (!dir->is_auth() || !dir->can_auth_pin()) {
      dout(10) << " can't auth_pin(!auth|freezing?) dirfrag " << *dir << ", noop" << dendl;
      return;
    }
    if (dir->is_any_freezing_or_frozen_inode()) {
      dout(10) << " there is freezing/frozen inode in " << *dir << ", noop" << dendl;
      return;
    }
  }

  for (auto& p : mdr->locks) {
    MDSCacheObject *obj = p.lock->get_parent();
    if (obj != diri && !ancestors.count(obj))
      continue;
    if (!p.lock->is_stable()) {
      dout(10) << " unstable " << *p.lock << " on " << *obj << ", noop" << dendl;
      return;
    }
  }

  auto lock_cache = new MDLockCache(cap, opcode);
  if (dir_layout)
    lock_cache->set_dir_layout(*dir_layout);
  cap->set_lock_cache_allowed(get_cap_bit_for_lock_cache(opcode));

  for (auto dir : dfv) {
    // prevent subtree migration
    lock_cache->auth_pin(dir);
    // prevent frozen inode
    dir->disable_frozen_inode();
  }

  for (auto& p : mdr->object_states) {
    if (p.first != diri && !ancestors.count(p.first))
      continue;
    auto& stat = p.second;
    if (stat.auth_pinned)
      lock_cache->auth_pin(p.first);
    else
      lock_cache->pin(p.first);

    if (CInode *in = dynamic_cast<CInode*>(p.first)) {
      CDentry *pdn = in->get_projected_parent_dn();
      if (pdn)
	dfv.push_back(pdn->get_dir());
    } else if (CDentry *dn = dynamic_cast<CDentry*>(p.first)) {
	dfv.push_back(dn->get_dir());
    } else {
      ceph_assert(0 == "unknown type of lock parent");
    }
  }
  lock_cache->attach_dirfrags(std::move(dfv));

  for (auto it = mdr->locks.begin(); it != mdr->locks.end(); ) {
    MDSCacheObject *obj = it->lock->get_parent();
    if (obj != diri && !ancestors.count(obj)) {
      ++it;
      continue;
    }
    unsigned lock_flag = 0;
    if (it->is_wrlock()) {
      // skip wrlocks that were added by MDCache::predirty_journal_parent()
      if (obj == diri)
	lock_flag = MutationImpl::LockOp::WRLOCK;
    } else {
      ceph_assert(it->is_rdlock());
      lock_flag = MutationImpl::LockOp::RDLOCK;
    }
    if (lock_flag) {
      lock_cache->emplace_lock(it->lock, lock_flag);
      mdr->locks.erase(it++);
    } else {
      ++it;
    }
  }
  lock_cache->attach_locks();

  lock_cache->ref++;
  mdr->lock_cache = lock_cache;
}

bool Locker::find_and_attach_lock_cache(const MDRequestRef& mdr, CInode *diri)
{
  if (mdr->lock_cache)
    return true;

  Capability *cap = diri->get_client_cap(mdr->get_client());
  if (!cap)
    return false;

  int opcode = mdr->client_request->get_op();
  for (auto p = cap->lock_caches.begin(); !p.end(); ++p) {
    MDLockCache *lock_cache = *p;
    if (lock_cache->opcode == opcode) {
      dout(10) << "found lock cache for " << ceph_mds_op_name(opcode) << " on " << *diri << dendl;
      mdr->lock_cache = lock_cache;
      mdr->lock_cache->ref++;
      return true;
    }
  }
  return false;
}

// generics

void Locker::eval_gather(SimpleLock *lock, bool first, bool *pneed_issue, MDSContext::vec *pfinishers)
{
  dout(10) << "eval_gather " << *lock << " on " << *lock->get_parent() << dendl;
  ceph_assert(!lock->is_stable());

  int next = lock->get_next_state();

  CInode *in = 0;
  bool caps = lock->get_cap_shift();
  if (lock->get_type() != CEPH_LOCK_DN)
    in = static_cast<CInode *>(lock->get_parent());

  bool need_issue = false;

  int loner_issued = 0, other_issued = 0, xlocker_issued = 0;
  ceph_assert(!caps || in != NULL);
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
      ceph_assert(in);
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

      if (!mds->is_cluster_degraded() ||
	  mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
	switch (lock->get_state()) {
	case LOCK_SYNC_LOCK:
	  mds->send_message_mds(make_message<MLock>(lock, LOCK_AC_LOCKACK, mds->get_nodeid()), auth);
	  break;

	case LOCK_MIX_SYNC:
	  {
	    auto reply = make_message<MLock>(lock, LOCK_AC_SYNCACK, mds->get_nodeid());
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
	    auto reply = make_message<MLock>(lock, LOCK_AC_MIXACK, mds->get_nodeid());
	    mds->send_message_mds(reply, auth);
	    next = LOCK_SYNC_MIX2;
	  }
	  break;

	case LOCK_MIX_LOCK:
	  {
	    bufferlist data;
	    lock->encode_locked_state(data);
	    mds->send_message_mds(make_message<MLock>(lock, LOCK_AC_LOCKACK, mds->get_nodeid(), data), auth);
	    (static_cast<ScatterLock *>(lock))->start_flush();
	    // we'll get an AC_LOCKFLUSHED to complete
	  }
	  break;

	default:
	  ceph_abort();
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
	return;
      }
      lock->clear_flushed();
      
      switch (lock->get_state()) {
	// to mixed
      case LOCK_TSYN_MIX:
      case LOCK_SYNC_MIX:
      case LOCK_EXCL_MIX:
      case LOCK_XSYN_MIX:
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
  MDSContext::vec finishers;
  
  dout(10) << "eval " << mask << " " << *in << dendl;

  // choose loner?
  if (in->is_auth() && in->is_head()) {
    client_t orig_loner = in->get_loner();
    if (in->choose_ideal_loner()) {
      dout(10) << "eval set loner: client." << orig_loner << " -> client." << in->get_loner() << dendl;
      need_issue = true;
      mask = -1;
    } else if (in->get_wanted_loner() != in->get_loner()) {
      dout(10) << "eval want loner: client." << in->get_wanted_loner() << " but failed to set it" << dendl;
      mask = -1;
    }
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
    if (in->try_drop_loner()) {
      need_issue = true;
      if (in->get_wanted_loner() >= 0) {
	dout(10) << "eval end set loner to client." << in->get_wanted_loner() << dendl;
	bool ok = in->try_set_loner();
	ceph_assert(ok);
	mask = -1;
	goto retry;
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
    ceph_assert(ceph_mutex_is_locked_by_me(locker->mds->mds_lock));
    p->get(MDSCacheObject::PIN_PTRWAITER);    
  }
  void finish(int r) override {
    locker->try_eval(p, mask);
    p->put(MDSCacheObject::PIN_PTRWAITER);
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
    ceph_assert(mask == CEPH_LOCK_DN);
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
   * checks above due to the auth_pin held by the leader.
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

  if (lock->get_type() != CEPH_LOCK_DN &&
      lock->get_type() != CEPH_LOCK_ISNAP &&
      lock->get_type() != CEPH_LOCK_IPOLICY &&
      p->is_freezing()) {
    dout(7) << "try_eval " << *lock << " freezing, waiting on " << *p << dendl;
    p->add_waiter(MDSCacheObject::WAIT_UNFREEZE, new C_Locker_Eval(this, p, lock->get_type()));
    return;
  }

  eval(lock, pneed_issue);
}

void Locker::eval_cap_gather(CInode *in, set<CInode*> *issue_set)
{
  bool need_issue = false;
  MDSContext::vec finishers;

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
  MDSContext::vec finishers;

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
      if (!mds->is_cluster_degraded() ||
	  mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
	dout(10) << "requesting rdlock from auth on "
		 << *lock << " on " << *lock->get_parent() << dendl;
	mds->send_message_mds(make_message<MLock>(lock, LOCK_AC_REQRDLOCK, mds->get_nodeid()), auth);
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

bool Locker::rdlock_try(SimpleLock *lock, client_t client)
{
  dout(7) << "rdlock_try on " << *lock << " on " << *lock->get_parent() << dendl;  

  // can read?  grab ref.
  if (lock->can_rdlock(client)) 
    return true;
  
  _rdlock_kick(lock, false);

  if (lock->can_rdlock(client)) 
    return true;

  return false;
}

bool Locker::rdlock_start(SimpleLock *lock, const MDRequestRef& mut, bool as_anon)
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
      mut->emplace_lock(lock, MutationImpl::LockOp::RDLOCK);
      return true;
    }

    // hmm, wait a second.
    if (in && !in->is_head() && in->is_auth() &&
	lock->get_state() == LOCK_SNAP_SYNC) {
      // okay, we actually need to kick the head's lock to get ourselves synced up.
      CInode *head = mdcache->get_inode(in->ino());
      ceph_assert(head);
      SimpleLock *hlock = head->get_lock(CEPH_LOCK_IFILE);
      if (hlock->get_state() == LOCK_SYNC)
	hlock = head->get_lock(lock->get_type());

      if (hlock->get_state() != LOCK_SYNC) {
	dout(10) << "rdlock_start trying head inode " << *head << dendl;
	if (!rdlock_start(hlock, mut, true)) // ** as_anon, no rdlock on EXCL **
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
  if (lock->get_parent()->is_auth() && lock->is_unstable_and_locked())    // as with xlockdone, or cap flush
    mds->mdlog->flush();
}

void Locker::rdlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue)
{
  ceph_assert(it->is_rdlock());
  SimpleLock *lock = it->lock;
  // drop ref
  lock->put_rdlock();
  if (mut)
    mut->locks.erase(it);

  dout(7) << "rdlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;
  
  // last one?
  if (!lock->is_rdlocked()) {
    if (!lock->is_stable())
      eval_gather(lock, false, pneed_issue);
    else if (lock->get_parent()->is_auth())
      try_eval(lock, pneed_issue);
  }
}

bool Locker::rdlock_try_set(MutationImpl::LockOpVec& lov, const MDRequestRef& mdr)
{
  dout(10) << __func__  << dendl;
  for (const auto& p : lov) {
    auto lock = p.lock;
    ceph_assert(p.is_rdlock());
    if (!mdr->is_rdlocked(lock) && !rdlock_try(lock, mdr->get_client())) {
      lock->add_waiter(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_RD,
                       new C_MDS_RetryRequest(mdcache, mdr));
      goto failed;
    }
    lock->get_rdlock();
    mdr->emplace_lock(lock, MutationImpl::LockOp::RDLOCK);
    dout(20) << " got rdlock on " << *lock << " " << *lock->get_parent() << dendl;
  }

  return true;
failed:
  dout(10) << __func__ << " failed" << dendl;
  drop_locks(mdr.get(), nullptr);
  mdr->drop_local_auth_pins();
  return false;
}

bool Locker::rdlock_try_set(MutationImpl::LockOpVec& lov, MutationRef& mut)
{
  dout(10) << __func__  << dendl;
  for (const auto& p : lov) {
    auto lock = p.lock;
    ceph_assert(p.is_rdlock());
    if (!lock->can_rdlock(mut->get_client()))
      return false;
    p.lock->get_rdlock();
    mut->emplace_lock(p.lock, MutationImpl::LockOp::RDLOCK);
  }
  return true;
}

// ------------------
// wrlock

void Locker::wrlock_force(SimpleLock *lock, MutationRef& mut)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_grab(static_cast<LocalLockC*>(lock), mut);

  dout(7) << "wrlock_force  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->get_wrlock(true);
  mut->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);
}

bool Locker::wrlock_try(SimpleLock *lock, const MutationRef& mut, client_t client)
{
  dout(10) << "wrlock_try " << *lock << " on " << *lock->get_parent() << dendl;
  if (client == -1)
    client = mut->get_client();

  while (1) {
    if (lock->can_wrlock(client)) {
      lock->get_wrlock();
      auto it = mut->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);
      it->flags |= MutationImpl::LockOp::WRLOCK; // may already remote_wrlocked
      return true;
    }
    if (!lock->is_stable())
      break;
    CInode *in = static_cast<CInode *>(lock->get_parent());
    if (!in->is_auth())
      break;
    // caller may already has a log entry open. To avoid calling
    // scatter_writebehind or start_scatter. don't change nest lock
    // state if it has dirty scatterdata.
    if (lock->is_dirty())
      break;
    // To avoid calling scatter_writebehind or start_scatter. don't
    // change nest lock state to MIX.
    ScatterLock *slock = static_cast<ScatterLock*>(lock);
    if (slock->get_scatter_wanted() || in->has_subtree_or_exporting_dirfrag())
      break;

    simple_lock(lock);
  }
  return false;
}

bool Locker::wrlock_start(const MutationImpl::LockOp &op, const MDRequestRef& mut)
{
  SimpleLock *lock = op.lock;
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_start(static_cast<LocalLockC*>(lock), mut);

  dout(10) << "wrlock_start " << *lock << " on " << *lock->get_parent() << dendl;

  CInode *in = static_cast<CInode *>(lock->get_parent());
  client_t client = op.is_state_pin() ? lock->get_excl_client() : mut->get_client();
  bool want_scatter = lock->get_parent()->is_auth() &&
		      (in->has_subtree_or_exporting_dirfrag() ||
		       static_cast<ScatterLock*>(lock)->get_scatter_wanted());

  while (1) {
    // wrlock?
    if (lock->can_wrlock(client) &&
	(!want_scatter || lock->get_state() == LOCK_MIX)) {
      lock->get_wrlock();
      auto it = mut->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);
      it->flags |= MutationImpl::LockOp::WRLOCK; // may already remote_wrlocked
      return true;
    }

    if (lock->get_type() == CEPH_LOCK_IFILE &&
	in->state_test(CInode::STATE_RECOVERING)) {
      mds->mdcache->recovery_queue.prioritize(in);
    }

    if (!lock->is_stable())
      break;

    if (in->is_auth()) {
      if (want_scatter)
	scatter_mix(static_cast<ScatterLock*>(lock));
      else
	simple_lock(lock);
    } else {
      // replica.
      // auth should be auth_pinned (see acquire_locks wrlock weird mustpin case).
      mds_rank_t auth = lock->get_parent()->authority().first;
      if (!mds->is_cluster_degraded() ||
	  mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
	dout(10) << "requesting scatter from auth on "
		 << *lock << " on " << *lock->get_parent() << dendl;
	mds->send_message_mds(make_message<MLock>(lock, LOCK_AC_REQSCATTER, mds->get_nodeid()), auth);
      }
      break;
    }
  }

  dout(7) << "wrlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
  nudge_log(lock);
    
  return false;
}

void Locker::wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue)
{
  ceph_assert(it->is_wrlock());
  SimpleLock* lock = it->lock;

  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_finish(it, mut);

  dout(7) << "wrlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->put_wrlock();

  if (it->is_remote_wrlock())
    it->clear_wrlock();
  else
    mut->locks.erase(it);

  if (lock->is_wrlocked()) {
    // Evaluate unstable lock after scatter_writebehind_finish(). Because
    // eval_gather() does not change lock's state when lock is flushing.
    if (!lock->is_stable() && lock->is_flushed() &&
	lock->get_parent()->is_auth())
      eval_gather(lock, false, pneed_issue);
  } else {
    if (!lock->is_stable())
      eval_gather(lock, false, pneed_issue);
    else if (lock->get_parent()->is_auth())
      try_eval(lock, pneed_issue);
  }
}


// remote wrlock

void Locker::remote_wrlock_start(SimpleLock *lock, mds_rank_t target, const MDRequestRef& mut)
{
  dout(7) << "remote_wrlock_start mds." << target << " on " << *lock << " on " << *lock->get_parent() << dendl;

  // wait for active target
  if (mds->is_cluster_degraded() &&
      !mds->mdsmap->is_clientreplay_or_active_or_stopping(target)) {
    dout(7) << " mds." << target << " is not active" << dendl;
    if (mut->more()->waiting_on_peer.empty())
      mds->wait_for_active_peer(target, new C_MDS_RetryRequest(mdcache, mut));
    return;
  }

  // send lock request
  mut->start_locking(lock, target);
  mut->more()->peers.insert(target);
  auto r = make_message<MMDSPeerRequest>(mut->reqid, mut->attempt, MMDSPeerRequest::OP_WRLOCK);
  r->set_lock_type(lock->get_type());
  lock->get_parent()->set_object_info(r->get_object_info());
  mds->send_message_mds(r, target);

  ceph_assert(mut->more()->waiting_on_peer.count(target) == 0);
  mut->more()->waiting_on_peer.insert(target);
}

void Locker::remote_wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut)
{
  ceph_assert(it->is_remote_wrlock());
  SimpleLock *lock = it->lock;
  mds_rank_t target = it->wrlock_target;

  if (it->is_wrlock())
    it->clear_remote_wrlock();
  else
    mut->locks.erase(it);
  
  dout(7) << "remote_wrlock_finish releasing remote wrlock on mds." << target
	  << " " << *lock->get_parent()  << dendl;
  if (!mds->is_cluster_degraded() ||
      mds->mdsmap->get_state(target) >= MDSMap::STATE_REJOIN) {
    auto peerreq = make_message<MMDSPeerRequest>(mut->reqid, mut->attempt, MMDSPeerRequest::OP_UNWRLOCK);
    peerreq->set_lock_type(lock->get_type());
    lock->get_parent()->set_object_info(peerreq->get_object_info());
    mds->send_message_mds(peerreq, target);
  }
}


// ------------------
// xlock

bool Locker::xlock_start(SimpleLock *lock, const MDRequestRef& mut)
{
  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_xlock_start(static_cast<LocalLockC*>(lock), mut);

  dout(7) << "xlock_start on " << *lock << " on " << *lock->get_parent() << dendl;
  client_t client = mut->get_client();

  CInode *in = nullptr;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  // auth?
  if (lock->get_parent()->is_auth()) {
    // auth
    while (1) {
      if (mut->locking && // started xlock (not preempt other request)
	  lock->can_xlock(client) &&
	  !(lock->get_state() == LOCK_LOCK_XLOCK &&	// client is not xlocker or
	    in && in->issued_caps_need_gather(lock))) { // xlocker does not hold shared cap
	lock->set_state(LOCK_XLOCK);
	lock->get_xlock(mut, client);
	mut->emplace_lock(lock, MutationImpl::LockOp::XLOCK);
	mut->finish_locking(lock);
	return true;
      }
      
      if (lock->get_type() == CEPH_LOCK_IFILE &&
	  in->state_test(CInode::STATE_RECOVERING)) {
	mds->mdcache->recovery_queue.prioritize(in);
      }

      if (!lock->is_stable() && (lock->get_state() != LOCK_XLOCKDONE ||
                                lock->get_xlock_by_client() != client ||
                                lock->is_waiter_for(SimpleLock::WAIT_STABLE)))
	break;
      // Avoid unstable XLOCKDONE state reset,
      // see: https://tracker.ceph.com/issues/49132
      if (lock->get_state() == LOCK_XLOCKDONE &&
          lock->get_type() == CEPH_LOCK_IFILE)
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
    ceph_assert(lock->get_sm()->can_remote_xlock);
    ceph_assert(!mut->peer_request);
    
    // wait for single auth
    if (lock->get_parent()->is_ambiguous_auth()) {
      lock->get_parent()->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
				     new C_MDS_RetryRequest(mdcache, mut));
      return false;
    }
    
    // wait for active auth
    mds_rank_t auth = lock->get_parent()->authority().first;
    if (mds->is_cluster_degraded() &&
	!mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
      dout(7) << " mds." << auth << " is not active" << dendl;
      if (mut->more()->waiting_on_peer.empty())
	mds->wait_for_active_peer(auth, new C_MDS_RetryRequest(mdcache, mut));
      return false;
    }

    // send lock request
    mut->more()->peers.insert(auth);
    mut->start_locking(lock, auth);
    auto r = make_message<MMDSPeerRequest>(mut->reqid, mut->attempt, MMDSPeerRequest::OP_XLOCK);
    r->set_lock_type(lock->get_type());
    lock->get_parent()->set_object_info(r->get_object_info());
    mds->send_message_mds(r, auth);

    ceph_assert(mut->more()->waiting_on_peer.count(auth) == 0);
    mut->more()->waiting_on_peer.insert(auth);

    return false;
  }
}

void Locker::_finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue)
{
  if (lock->get_type() != CEPH_LOCK_DN &&
      lock->get_type() != CEPH_LOCK_ISNAP &&
      lock->get_type() != CEPH_LOCK_IPOLICY &&
      lock->get_num_rdlocks() == 0 &&
      lock->get_num_wrlocks() == 0 &&
      !lock->is_leased() &&
      lock->get_state() != LOCK_XLOCKSNAP) {
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
  eval_gather(lock, lock->get_state() != LOCK_XLOCKSNAP, pneed_issue);
}

void Locker::xlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue)
{
  ceph_assert(it->is_xlock());
  SimpleLock *lock = it->lock;

  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_xlock_finish(it, mut);

  dout(10) << "xlock_finish on " << *lock << " " << *lock->get_parent() << dendl;

  client_t xlocker = lock->get_xlock_by_client();

  // drop ref
  lock->put_xlock();
  ceph_assert(mut);
  mut->locks.erase(it);
  
  bool do_issue = false;

  // remote xlock?
  if (!lock->get_parent()->is_auth()) {
    ceph_assert(lock->get_sm()->can_remote_xlock);

    // tell auth
    dout(7) << "xlock_finish releasing remote xlock on " << *lock->get_parent()  << dendl;
    mds_rank_t auth = lock->get_parent()->authority().first;
    if (!mds->is_cluster_degraded() ||
	mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
      auto peerreq = make_message<MMDSPeerRequest>(mut->reqid, mut->attempt, MMDSPeerRequest::OP_UNXLOCK);
      peerreq->set_lock_type(lock->get_type());
      lock->get_parent()->set_object_info(peerreq->get_object_info());
      mds->send_message_mds(peerreq, auth);
    }
    // others waiting?
    lock->finish_waiters(SimpleLock::WAIT_STABLE |
			 SimpleLock::WAIT_WR | 
			 SimpleLock::WAIT_RD, 0); 
  } else {
    if (lock->get_num_xlocks() == 0 &&
        lock->get_state() != LOCK_LOCK_XLOCK) { // no one is taking xlock
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

void Locker::xlock_export(const MutationImpl::lock_iterator& it, MutationImpl *mut)
{
  ceph_assert(it->is_xlock());
  SimpleLock *lock = it->lock;
  dout(10) << "xlock_export on " << *lock << " " << *lock->get_parent() << dendl;

  lock->put_xlock();
  mut->locks.erase(it);

  MDSCacheObject *p = lock->get_parent();
  ceph_assert(p->state_test(CInode::STATE_AMBIGUOUSAUTH));  // we are exporting this (inode)

  if (!lock->is_stable())
    lock->get_parent()->auth_unpin(lock);

  lock->set_state(LOCK_LOCK);
}

void Locker::xlock_import(SimpleLock *lock)
{
  dout(10) << "xlock_import on " << *lock << " " << *lock->get_parent() << dendl;
  lock->get_parent()->auth_pin(lock);
}

void Locker::xlock_downgrade(SimpleLock *lock, MutationImpl *mut)
{
  dout(10) << "xlock_downgrade on " << *lock << " " << *lock->get_parent() << dendl;
  auto it = mut->locks.find(lock);
  if (it->is_rdlock())
    return; // already downgraded

  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(it != mut->locks.end());
  ceph_assert(it->is_xlock());

  lock->set_xlock_done();
  lock->get_rdlock();
  xlock_finish(it, mut, nullptr);
  mut->emplace_lock(lock, MutationImpl::LockOp::RDLOCK);
}


// file i/o -----------------------------------------

version_t Locker::issue_file_data_version(CInode *in)
{
  dout(7) << "issue_file_data_version on " << *in << dendl;
  return in->get_inode()->file_data_version;
}

class C_Locker_FileUpdate_finish : public LockerLogContext {
  CInode *in;
  MutationRef mut;
  unsigned flags;
  client_t client;
  ref_t<MClientCaps> ack;
public:
  C_Locker_FileUpdate_finish(Locker *l, CInode *i, MutationRef& m, unsigned f,
                             const ref_t<MClientCaps> &ack, client_t c=-1)
    : LockerLogContext(l), in(i), mut(m), flags(f), client(c), ack(ack) {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) override {
    locker->file_update_finish(in, mut, flags, client, ack);
    in->put(CInode::PIN_PTRWAITER);
  }
};

enum {
  UPDATE_SHAREMAX = 1,
  UPDATE_NEEDSISSUE = 2,
  UPDATE_SNAPFLUSH = 4,
};

void Locker::file_update_finish(CInode *in, MutationRef& mut, unsigned flags,
				client_t client, const ref_t<MClientCaps> &ack)
{
  dout(10) << "file_update_finish on " << *in << dendl;

  mut->apply();

  if (ack) {
    Session *session = mds->get_session(client);
    if (session && !session->is_closed()) {
      // "oldest flush tid" > 0 means client uses unique TID for each flush
      if (ack->get_oldest_flush_tid() > 0)
        session->add_completed_flush(ack->get_client_tid());
      mds->send_message_client_counted(ack, session);
    } else {
      dout(10) << " no session for client." << client << " " << *ack << dendl;
    }
  }

  set<CInode*> need_issue;
  drop_locks(mut.get(), &need_issue);

  if (in->is_head()) {
    if ((flags & UPDATE_NEEDSISSUE) && need_issue.count(in) == 0) {
      Capability *cap = in->get_client_cap(client);
      if (cap && (cap->wanted() & ~cap->pending()))
	issue_caps(in, cap);
    }

    if ((flags & UPDATE_SHAREMAX) && in->is_auth() &&
	(in->filelock.gcaps_allowed(CAP_LONER) & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER)))
      share_inode_max_size(in);

  } else if ((flags & UPDATE_SNAPFLUSH) && !in->client_snap_caps.empty()) {
    dout(10) << " client_snap_caps " << in->client_snap_caps << dendl;
    // check for snap writeback completion
    in->client_snap_caps.erase(client);
    if (in->client_snap_caps.empty()) {
      for (int i = 0; i < num_cinode_locks; i++) {
	SimpleLock *lock = in->get_lock(cinode_lock_info[i].lock);
	ceph_assert(lock);
	lock->put_wrlock();
      }
      in->item_open_file.remove_myself();
      in->item_caps.remove_myself();
      eval_cap_gather(in, &need_issue);
    }
  }
  issue_caps_set(need_issue);

  mds->balancer->hit_inode(in, META_POP_IWR);

  // auth unpin after issuing caps
  mut->cleanup();
}

Capability* Locker::issue_new_caps(CInode *in,
				   int mode,
				   const MDRequestRef& mdr,
				   SnapRealm *realm)
{
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << dendl;
  Session *session = mdr->session;
  bool new_inode = (mdr->alloc_ino || mdr->used_prealloc_ino);

  // if replay or async, try to reconnect cap, and otherwise do nothing.
  if (new_inode && mdr->client_request->is_queued_for_replay())
    return mds->mdcache->try_reconnect_cap(in, session);

  // my needs
  ceph_assert(session->info.inst.name.is_client());
  client_t my_client = session->get_client();
  int my_want = ceph_caps_for_mode(mode);

  // register a capability
  Capability *cap = in->get_client_cap(my_client);
  if (!cap) {
    // new cap
    cap = in->add_client_cap(my_client, session, realm, new_inode);
    cap->set_wanted(my_want);
    cap->mark_new();
  } else {
    // make sure it wants sufficient caps
    if (my_want & ~cap->wanted()) {
      // augment wanted caps for this client
      cap->set_wanted(cap->wanted() | my_want);
    }
  }
  cap->inc_suppress(); // suppress file cap messages (we'll bundle with the request reply)

  if (in->is_auth()) {
    // [auth] twiddle mode?
    eval(in, CEPH_CAP_LOCKS);

    int all_allowed = -1, loner_allowed = -1, xlocker_allowed = -1;
    int allowed = get_allowed_caps(in, cap, all_allowed, loner_allowed,
                                   xlocker_allowed);

    if (_need_flush_mdlog(in, my_want & ~allowed, true))
      mds->mdlog->flush();

  } else {
    // [replica] tell auth about any new caps wanted
    request_inode_file_caps(in);
  }

  // issue caps (pot. incl new one)
  //issue_caps(in);  // note: _eval above may have done this already...

  // re-issue whatever we can
  //cap->issue(cap->pending());

  cap->dec_suppress();

  return cap;
}

void Locker::issue_caps_set(set<CInode*>& inset)
{
  for (set<CInode*>::iterator p = inset.begin(); p != inset.end(); ++p)
    issue_caps(*p);
}

class C_Locker_RevokeStaleCap : public LockerContext {
  CInode *in;
  client_t client;
public:
  C_Locker_RevokeStaleCap(Locker *l, CInode *i, client_t c) :
    LockerContext(l), in(i), client(c) {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) override {
    locker->revoke_stale_cap(in, client);
    in->put(CInode::PIN_PTRWAITER);
  }
};

int Locker::get_allowed_caps(CInode *in, Capability *cap,
                             int &all_allowed, int &loner_allowed,
                             int &xlocker_allowed)
{
  client_t client = cap->get_client();

  // allowed caps are determined by the lock mode.
  if (all_allowed == -1)
    all_allowed = in->get_caps_allowed_by_type(CAP_ANY);
  if (loner_allowed == -1)
    loner_allowed = in->get_caps_allowed_by_type(CAP_LONER);
  if (xlocker_allowed == -1)
    xlocker_allowed = in->get_caps_allowed_by_type(CAP_XLOCKER);

  client_t loner = in->get_loner();
  if (loner >= 0) {
    dout(7) << "get_allowed_caps loner client." << loner
	    << " allowed=" << ccap_string(loner_allowed) 
	    << ", xlocker allowed=" << ccap_string(xlocker_allowed)
	    << ", others allowed=" << ccap_string(all_allowed)
	    << " on " << *in << dendl;
  } else {
    dout(7) << "get_allowed_caps allowed=" << ccap_string(all_allowed) 
	    << ", xlocker allowed=" << ccap_string(xlocker_allowed)
	    << " on " << *in << dendl;
  }

  // do not issue _new_ bits when size|mtime is projected
  int allowed;
  if (loner == client)
    allowed = loner_allowed;
  else
    allowed = all_allowed;

  // add in any xlocker-only caps (for locks this client is the xlocker for)
  allowed |= xlocker_allowed & in->get_xlocker_mask(client);
  if (in->is_dir()) {
    allowed &= ~CEPH_CAP_ANY_DIR_OPS;
    if (allowed & CEPH_CAP_FILE_EXCL)
      allowed |= cap->get_lock_cache_allowed();
  }

  if ((in->get_inode()->inline_data.version != CEPH_INLINE_NONE &&
       cap->is_noinline()) ||
      (!in->get_inode()->layout.pool_ns.empty() &&
       cap->is_nopoolns()))
    allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);

  return allowed;
}

int Locker::issue_caps(CInode *in, Capability *only_cap)
{
  // count conflicts with
  int nissued = 0;
  int all_allowed = -1, loner_allowed = -1, xlocker_allowed = -1;

  ceph_assert(in->is_head());

  // client caps
  map<client_t, Capability>::iterator it;
  if (only_cap)
    it = in->client_caps.find(only_cap->get_client());
  else
    it = in->client_caps.begin();
  for (; it != in->client_caps.end(); ++it) {
    Capability *cap = &it->second;
    int allowed = get_allowed_caps(in, cap, all_allowed, loner_allowed,
                                   xlocker_allowed);
    int pending = cap->pending();
    int wanted = cap->wanted();

    dout(20) << " client." << it->first
	     << " pending " << ccap_string(pending) 
	     << " allowed " << ccap_string(allowed) 
	     << " wanted " << ccap_string(wanted)
	     << dendl;

    if (!(pending & ~allowed)) {
      // skip if suppress or new, and not revocation
      if (cap->is_new() || cap->is_suppress() || cap->is_stale()) {
	dout(20) << "  !revoke and new|suppressed|stale, skipping client." << it->first << dendl;
	continue;
      }
    } else {
      ceph_assert(!cap->is_new());
      if (cap->is_stale()) {
	dout(20) << "  revoke stale cap from client." << it->first << dendl;
	ceph_assert(!cap->is_valid());
	cap->issue(allowed & pending, false);
	mds->queue_waiter_front(new C_Locker_RevokeStaleCap(this, in, it->first));
	continue;
      }

      if (!cap->is_valid() && (pending & ~CEPH_CAP_PIN)) {
	// After stale->resume circle, client thinks it only has CEPH_CAP_PIN.
	// mds needs to re-issue caps, then do revocation.
	long seq = cap->issue(pending, true);

	dout(7) << "   sending MClientCaps to client." << it->first
		<< " seq " << seq << " re-issue " << ccap_string(pending) << dendl;

        if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_grant);

	auto m = make_message<MClientCaps>(CEPH_CAP_OP_GRANT, in->ino(),
					   in->find_snaprealm()->inode->ino(),
					   cap->get_cap_id(), cap->get_last_seq(),
					   pending, wanted, 0, cap->get_mseq(),
					   mds->get_osd_epoch_barrier());
	in->encode_cap_message(m, cap);

	mds->send_message_client_counted(m, cap->get_session());
      }
    }

    // notify clients about deleted inode, to make sure they release caps ASAP.
    if (in->get_inode()->nlink == 0)
      wanted |= CEPH_CAP_LINK_SHARED;

    // are there caps that the client _wants_ and can have, but aren't pending?
    // or do we need to revoke?
    if ((pending & ~allowed) ||			// need to revoke ~allowed caps.
	((wanted & allowed) & ~pending) ||	// missing wanted+allowed caps
	!cap->is_valid()) {			// after stale->resume circle
      // issue
      nissued++;

      // include caps that clients generally like, while we're at it.
      int likes = in->get_caps_liked();      
      int before = pending;
      long seq;
      if (pending & ~allowed)
	seq = cap->issue((wanted|likes) & allowed & pending, true);  // if revoking, don't issue anything new.
      else
	seq = cap->issue((wanted|likes) & allowed, true);
      int after = cap->pending();

      dout(7) << "   sending MClientCaps to client." << it->first
	      << " seq " << seq << " new pending " << ccap_string(after)
	      << " was " << ccap_string(before) << dendl;

      int op = (before & ~after) ? CEPH_CAP_OP_REVOKE : CEPH_CAP_OP_GRANT;
      if (op == CEPH_CAP_OP_REVOKE) {
        if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_revoke);
	revoking_caps.push_back(&cap->item_revoking_caps);
	revoking_caps_by_client[cap->get_client()].push_back(&cap->item_client_revoking_caps);
	cap->set_last_revoke_stamp(ceph_clock_now());
	cap->reset_num_revoke_warnings();
      } else {
        if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_grant);
      }

      auto m = make_message<MClientCaps>(op, in->ino(),
					 in->find_snaprealm()->inode->ino(),
					 cap->get_cap_id(), cap->get_last_seq(),
					 after, wanted, 0, cap->get_mseq(),
					 mds->get_osd_epoch_barrier());
      in->encode_cap_message(m, cap);

      mds->send_message_client_counted(m, cap->get_session());
    }

    if (only_cap)
      break;
  }

  return nissued;
}

void Locker::issue_truncate(CInode *in)
{
  dout(7) << "issue_truncate on " << *in << dendl;
  
  for (auto &p : in->client_caps) {
    if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_trunc);
    Capability *cap = &p.second;
    auto m = make_message<MClientCaps>(CEPH_CAP_OP_TRUNC,
                                       in->ino(),
                                       in->find_snaprealm()->inode->ino(),
                                       cap->get_cap_id(), cap->get_last_seq(),
                                       cap->pending(), cap->wanted(), 0,
                                       cap->get_mseq(),
                                       mds->get_osd_epoch_barrier());
    in->encode_cap_message(m, cap);			     
    mds->send_message_client_counted(m, p.first);
  }

  // should we increase max_size?
  if (in->is_auth() && in->is_file())
    check_inode_max_size(in);
}


void Locker::revoke_stale_cap(CInode *in, client_t client)
{
  dout(7) << __func__ << " client." << client << " on " << *in << dendl;
  Capability *cap = in->get_client_cap(client);
  if (!cap)
    return;

  if (cap->revoking() & CEPH_CAP_ANY_WR) {
    CachedStackStringStream css;
    mds->evict_client(client.v, false, g_conf()->mds_session_blocklist_on_timeout, *css, nullptr);
    return;
  }

  cap->revoke();

  if (in->is_auth() && in->get_inode()->client_ranges.count(cap->get_client()))
    in->state_set(CInode::STATE_NEEDSRECOVER);

  if (in->state_test(CInode::STATE_EXPORTINGCAPS))
    return;

  if (!in->filelock.is_stable())
    eval_gather(&in->filelock);
  if (!in->linklock.is_stable())
    eval_gather(&in->linklock);
  if (!in->authlock.is_stable())
    eval_gather(&in->authlock);
  if (!in->xattrlock.is_stable())
    eval_gather(&in->xattrlock);

  if (in->is_auth())
    try_eval(in, CEPH_CAP_LOCKS);
  else
    request_inode_file_caps(in);
}

bool Locker::revoke_stale_caps(Session *session)
{
  dout(10) << "revoke_stale_caps for " << session->info.inst.name << dendl;

  // invalidate all caps
  session->inc_cap_gen();

  bool ret = true;
  std::vector<CInode*> to_eval;

  for (auto p = session->caps.begin(); !p.end(); ) {
    Capability *cap = *p;
    ++p;
    if (!cap->is_notable()) {
      // the rest ones are not being revoked and don't have writeable range
      // and don't want exclusive caps or want file read/write. They don't
      // need recover, they don't affect eval_gather()/try_eval()
      break;
    }

    int revoking = cap->revoking();
    if (!revoking)
      continue;

    if (revoking & CEPH_CAP_ANY_WR) {
      ret = false;
      break;
    }

    int issued = cap->issued();
    CInode *in = cap->get_inode();
    dout(10) << " revoking " << ccap_string(issued) << " on " << *in << dendl;
    int revoked = cap->revoke();
    if (revoked & CEPH_CAP_ANY_DIR_OPS)
      eval_lock_caches(cap);

    if (in->is_auth() &&
	in->get_inode()->client_ranges.count(cap->get_client()))
      in->state_set(CInode::STATE_NEEDSRECOVER);

    // eval lock/inode may finish contexts, which may modify other cap's position
    // in the session->caps.
    to_eval.push_back(in);
  }

  for (auto in : to_eval) {
    if (in->state_test(CInode::STATE_EXPORTINGCAPS))
      continue;

    if (!in->filelock.is_stable())
      eval_gather(&in->filelock);
    if (!in->linklock.is_stable())
      eval_gather(&in->linklock);
    if (!in->authlock.is_stable())
      eval_gather(&in->authlock);
    if (!in->xattrlock.is_stable())
      eval_gather(&in->xattrlock);

    if (in->is_auth())
      try_eval(in, CEPH_CAP_LOCKS);
    else
      request_inode_file_caps(in);
  }

  return ret;
}

void Locker::resume_stale_caps(Session *session)
{
  dout(10) << "resume_stale_caps for " << session->info.inst.name << dendl;

  bool lazy = session->info.has_feature(CEPHFS_FEATURE_LAZY_CAP_WANTED);
  for (xlist<Capability*>::iterator p = session->caps.begin(); !p.end(); ) {
    Capability *cap = *p;
    ++p;
    if (lazy && !cap->is_notable())
      break; // see revoke_stale_caps()

    CInode *in = cap->get_inode();
    ceph_assert(in->is_head());
    dout(10) << " clearing stale flag on " << *in << dendl;

    if (in->state_test(CInode::STATE_EXPORTINGCAPS)) {
      // if export succeeds, the cap will be removed. if export fails,
      // we need to re-issue the cap if it's not stale.
      in->state_set(CInode::STATE_EVALSTALECAPS);
      continue;
    }

    if (!in->is_auth() || !eval(in, CEPH_CAP_LOCKS))
      issue_caps(in, cap);
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
  void finish(int r) override {
    if (!in->is_auth())
      locker->request_inode_file_caps(in);
    in->put(CInode::PIN_PTRWAITER);
  }
};

void Locker::request_inode_file_caps(CInode *in)
{
  ceph_assert(!in->is_auth());

  int wanted = in->get_caps_wanted() & in->get_caps_allowed_ever() & ~CEPH_CAP_PIN;
  if (wanted != in->replica_caps_wanted) {
    // wait for single auth
    if (in->is_ambiguous_auth()) {
      in->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, 
                     new C_MDL_RequestInodeFileCaps(this, in));
      return;
    }

    mds_rank_t auth = in->authority().first;
    if (mds->is_cluster_degraded() &&
	mds->mdsmap->get_state(auth) == MDSMap::STATE_REJOIN) {
      mds->wait_for_active_peer(auth, new C_MDL_RequestInodeFileCaps(this, in));
      return;
    }

    dout(7) << "request_inode_file_caps " << ccap_string(wanted)
            << " was " << ccap_string(in->replica_caps_wanted) 
            << " on " << *in << " to mds." << auth << dendl;

    in->replica_caps_wanted = wanted;

    if (!mds->is_cluster_degraded() ||
	mds->mdsmap->is_clientreplay_or_active_or_stopping(auth))
      mds->send_message_mds(make_message<MInodeFileCaps>(in->ino(), in->replica_caps_wanted), auth);
  }
}

void Locker::handle_inode_file_caps(const cref_t<MInodeFileCaps> &m)
{
  // nobody should be talking to us during recovery.
  if (mds->get_state() < MDSMap::STATE_CLIENTREPLAY) {
    if (mds->get_want_state() >= MDSMap::STATE_CLIENTREPLAY) {
      mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
      return;
    }
    ceph_abort_msg("got unexpected message during recovery");
  }

  // ok
  CInode *in = mdcache->get_inode(m->get_ino());
  mds_rank_t from = mds_rank_t(m->get_source().num());

  ceph_assert(in);
  ceph_assert(in->is_auth());

  dout(7) << "handle_inode_file_caps replica mds." << from << " wants caps " << ccap_string(m->get_caps()) << " on " << *in << dendl;

  if (mds->logger) mds->logger->inc(l_mdss_handle_inode_file_caps);

  in->set_mds_caps_wanted(from, m->get_caps());

  try_eval(in, CEPH_CAP_LOCKS);
}


class C_MDL_CheckMaxSize : public LockerContext {
  CInode *in;
  uint64_t new_max_size;
  uint64_t newsize;
  utime_t mtime;

public:
  C_MDL_CheckMaxSize(Locker *l, CInode *i, uint64_t _new_max_size,
                     uint64_t _newsize, utime_t _mtime) :
    LockerContext(l), in(i),
    new_max_size(_new_max_size), newsize(_newsize), mtime(_mtime)
  {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) override {
    if (in->is_auth())
      locker->check_inode_max_size(in, false, new_max_size, newsize, mtime);
    in->put(CInode::PIN_PTRWAITER);
  }
};

uint64_t Locker::calc_new_max_size(const CInode::inode_const_ptr &pi, uint64_t size)
{
  uint64_t new_max = (size + 1) << 1;
  uint64_t max_inc = g_conf()->mds_client_writeable_range_max_inc_objs;
  if (max_inc > 0) {
    max_inc *= pi->layout.object_size;
    new_max = std::min(new_max, size + max_inc);
  }
  return round_up_to(new_max, pi->get_layout_size_increment());
}

bool Locker::check_client_ranges(CInode *in, uint64_t size)
{
  const auto& latest = in->get_projected_inode();
  uint64_t ms;
  if (latest->has_layout()) {
    ms = calc_new_max_size(latest, size);
  } else {
    // Layout-less directories like ~mds0/, have zero size
    ms = 0;
  }

  auto it = latest->client_ranges.begin();
  for (auto &p : in->client_caps) {
    if ((p.second.issued() | p.second.wanted()) & CEPH_CAP_ANY_FILE_WR) {
      if (it == latest->client_ranges.end())
	return true;
      if (it->first != p.first)
	return true;
      if (ms > it->second.range.last)
	return true;
      ++it;
    }
  }
  return it != latest->client_ranges.end();
}

bool Locker::calc_new_client_ranges(CInode *in, uint64_t size, bool *max_increased)
{
  const auto& latest = in->get_projected_inode();
  uint64_t ms;
  if (latest->has_layout()) {
    ms = calc_new_max_size(latest, size);
  } else {
    // Layout-less directories like ~mds0/, have zero size
    ms = 0;
  }

  auto pi = in->_get_projected_inode();
  bool updated = false;

  // increase ranges as appropriate.
  // shrink to 0 if no WR|BUFFER caps issued.
  auto it = pi->client_ranges.begin();
  for (auto &p : in->client_caps) {
    if ((p.second.issued() | p.second.wanted()) & CEPH_CAP_ANY_FILE_WR) {
      while (it != pi->client_ranges.end() && it->first < p.first) {
	it = pi->client_ranges.erase(it);
	updated = true;
      }

      if (it != pi->client_ranges.end() && it->first == p.first) {
	if (ms > it->second.range.last) {
	  it->second.range.last = ms;
	  updated = true;
	  if (max_increased)
	    *max_increased = true;
	}
      } else {
	it = pi->client_ranges.emplace_hint(it, std::piecewise_construct,
					    std::forward_as_tuple(p.first),
					    std::forward_as_tuple());
	it->second.range.last = ms;
	it->second.follows = in->first - 1;
	updated = true;
	if (max_increased)
	  *max_increased = true;
      }
      p.second.mark_clientwriteable();
      ++it;
    } else {
      p.second.clear_clientwriteable();
    }
  }
  while (it != pi->client_ranges.end()) {
    it = pi->client_ranges.erase(it);
    updated = true;
  }
  if (updated) {
    if (pi->client_ranges.empty())
      in->clear_clientwriteable();
    else
      in->mark_clientwriteable();
  }
  return updated;
}

bool Locker::check_inode_max_size(CInode *in, bool force_wrlock,
				  uint64_t new_max_size, uint64_t new_size,
				  utime_t new_mtime)
{
  ceph_assert(in->is_auth());
  ceph_assert(in->is_file());

  const auto& latest = in->get_projected_inode();
  uint64_t size = latest->size;
  bool update_size = new_size > 0;

  if (update_size) {
    new_size = size = std::max(size, new_size);
    new_mtime = std::max(new_mtime, latest->mtime);
    if (latest->size == new_size && latest->mtime == new_mtime)
      update_size = false;
  }

  bool new_ranges = check_client_ranges(in, std::max(new_max_size, size));
  if (!update_size && !new_ranges) {
    dout(20) << "check_inode_max_size no-op on " << *in << dendl;
    return false;
  }

  dout(10) << "check_inode_max_size new_ranges " << new_ranges
	   << " update_size " << update_size
	   << " on " << *in << dendl;

  if (in->is_frozen()) {
    dout(10) << "check_inode_max_size frozen, waiting on " << *in << dendl;
    in->add_waiter(CInode::WAIT_UNFREEZE,
		   new C_MDL_CheckMaxSize(this, in, new_max_size, new_size, new_mtime));
    return false;
  } else if (!force_wrlock && !in->filelock.can_wrlock(in->get_loner())) {
    // lock?
    if (in->filelock.is_stable()) {
      if (in->get_target_loner() >= 0)
	file_excl(&in->filelock);
      else
	simple_lock(&in->filelock);
    }
    if (!in->filelock.can_wrlock(in->get_loner())) {
      dout(10) << "check_inode_max_size can't wrlock, waiting on " << *in << dendl;
      in->filelock.add_waiter(SimpleLock::WAIT_STABLE,
			      new C_MDL_CheckMaxSize(this, in, new_max_size, new_size, new_mtime));
      return false;
    }
  }

  MutationRef mut(new MutationImpl());
  mut->ls = mds->mdlog->get_current_segment();
    
  auto pi = in->project_inode(mut);
  pi.inode->version = in->pre_dirty();

  bool max_increased = false;
  if (new_ranges &&
      calc_new_client_ranges(in, std::max(new_max_size, size), &max_increased)) {
    dout(10) << "check_inode_max_size client_ranges "
	     << in->get_previous_projected_inode()->client_ranges
	     <<  " -> " << pi.inode->client_ranges << dendl;
  }

  if (update_size) {
    dout(10) << "check_inode_max_size size " << pi.inode->size << " -> " << new_size << dendl;
    pi.inode->size = new_size;
    pi.inode->rstat.rbytes = new_size;
    dout(10) << "check_inode_max_size mtime " << pi.inode->mtime << " -> " << new_mtime << dendl;
    pi.inode->mtime = new_mtime;
    if (new_mtime > pi.inode->ctime) {
      pi.inode->ctime = new_mtime;
      if (new_mtime > pi.inode->rstat.rctime)
	pi.inode->rstat.rctime = new_mtime;
    }
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
  } else {
    EUpdate *eu = new EUpdate(mds->mdlog, "check_inode_max_size");
    metablob = &eu->metablob;
    le = eu;
  }
  mds->mdlog->start_entry(le);

  mdcache->predirty_journal_parents(mut, metablob, in, 0, PREDIRTY_PRIMARY);
  // no cow, here!
  CDentry *parent = in->get_projected_parent_dn();
  metablob->add_primary_dentry(parent, in, true);
  mdcache->journal_dirty_inode(mut.get(), metablob, in);

  mds->mdlog->submit_entry(le, new C_Locker_FileUpdate_finish(this, in, mut,
      UPDATE_SHAREMAX, ref_t<MClientCaps>()));
  wrlock_force(&in->filelock, mut);  // wrlock for duration of journal
  mut->auth_pin(in);

  // make max_size _increase_ timely
  if (max_increased)
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
  map<client_t, Capability>::iterator it;
  if (only_cap)
    it = in->client_caps.find(only_cap->get_client());
  else
    it = in->client_caps.begin();
  for (; it != in->client_caps.end(); ++it) {
    const client_t client = it->first;
    Capability *cap = &it->second;
    if (cap->is_suppress())
      continue;
    if (cap->pending() & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER)) {
      dout(10) << "share_inode_max_size with client." << client << dendl;
      if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_grant);
      cap->inc_last_seq();
      auto m = make_message<MClientCaps>(CEPH_CAP_OP_GRANT,
                                         in->ino(),
                                         in->find_snaprealm()->inode->ino(),
                                         cap->get_cap_id(),
                                         cap->get_last_seq(),
                                         cap->pending(),
                                         cap->wanted(), 0,
                                         cap->get_mseq(),
                                         mds->get_osd_epoch_barrier());
      in->encode_cap_message(m, cap);
      mds->send_message_client_counted(m, client);
    }
    if (only_cap)
      break;
  }
}

bool Locker::_need_flush_mdlog(CInode *in, int wanted, bool lock_state_any)
{
  /* flush log if caps are wanted by client but corresponding lock is unstable and locked by
   * pending mutations. */
  if (((wanted & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR|CEPH_CAP_FILE_SHARED|CEPH_CAP_FILE_EXCL)) &&
       (lock_state_any ? in->filelock.is_locked() : in->filelock.is_unstable_and_locked())) ||
      ((wanted & (CEPH_CAP_AUTH_SHARED|CEPH_CAP_AUTH_EXCL)) &&
       (lock_state_any ? in->authlock.is_locked() : in->authlock.is_unstable_and_locked())) ||
      ((wanted & (CEPH_CAP_LINK_SHARED|CEPH_CAP_LINK_EXCL)) &&
       (lock_state_any ? in->linklock.is_locked() : in->linklock.is_unstable_and_locked())) ||
      ((wanted & (CEPH_CAP_XATTR_SHARED|CEPH_CAP_XATTR_EXCL)) &&
       (lock_state_any ? in->xattrlock.is_locked() : in->xattrlock.is_unstable_and_locked())))
    return true;
  return false;
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

  if (cap->wanted()) {
    if (cur->state_test(CInode::STATE_RECOVERING) &&
	(cap->wanted() & (CEPH_CAP_FILE_RD |
			  CEPH_CAP_FILE_WR))) {
      mds->mdcache->recovery_queue.prioritize(cur);
    }

    if (mdcache->open_file_table.should_log_open(cur)) {
      ceph_assert(cur->last == CEPH_NOSNAP);
      EOpen *le = new EOpen(mds->mdlog);
      mds->mdlog->start_entry(le);
      le->add_clean_inode(cur);
      mds->mdlog->submit_entry(le);
    }
  }
}

void Locker::snapflush_nudge(CInode *in)
{
  ceph_assert(in->last != CEPH_NOSNAP);
  if (in->client_snap_caps.empty())
    return;

  CInode *head = mdcache->get_inode(in->ino());
  // head inode gets unpinned when snapflush starts. It might get trimmed
  // before snapflush finishes.
  if (!head)
    return;

  ceph_assert(head->is_auth());
  if (head->client_need_snapflush.empty())
    return;

  SimpleLock *hlock = head->get_lock(CEPH_LOCK_IFILE);
  if (hlock->get_state() == LOCK_SYNC || !hlock->is_stable()) {
    hlock = NULL;
    for (int i = 0; i < num_cinode_locks; i++) {
      SimpleLock *lock = head->get_lock(cinode_lock_info[i].lock);
      if (lock->get_state() != LOCK_SYNC && lock->is_stable()) {
	hlock = lock;
	break;
      }
    }
  }
  if (hlock) {
    _rdlock_kick(hlock, true);
  } else {
    // also, requeue, in case of unstable lock
    need_snapflush_inodes.push_back(&in->item_caps);
  }
}

void Locker::mark_need_snapflush_inode(CInode *in)
{
  ceph_assert(in->last != CEPH_NOSNAP);
  if (!in->item_caps.is_on_list()) {
    need_snapflush_inodes.push_back(&in->item_caps);
    utime_t now = ceph_clock_now();
    in->last_dirstat_prop = now;
    dout(10) << "mark_need_snapflush_inode " << *in << " - added at " << now << dendl;
  }
}

bool Locker::is_revoking_any_caps_from(client_t client)
{
  auto it = revoking_caps_by_client.find(client);
  if (it == revoking_caps_by_client.end())
    return false;
  return !it->second.empty();
}

void Locker::_do_null_snapflush(CInode *head_in, client_t client, snapid_t last)
{
  dout(10) << "_do_null_snapflush client." << client << " on " << *head_in << dendl;
  for (auto p = head_in->client_need_snapflush.begin();
       p != head_in->client_need_snapflush.end() && p->first < last; ) {
    snapid_t snapid = p->first;
    auto &clients = p->second;
    ++p;  // be careful, q loop below depends on this

    if (clients.count(client)) {
      dout(10) << " doing async NULL snapflush on " << snapid << " from client." << client << dendl;
      CInode *sin = mdcache->pick_inode_snap(head_in, snapid - 1);
      ceph_assert(sin);
      ceph_assert(sin->first <= snapid);
      _do_snap_update(sin, snapid, 0, sin->first - 1, client, ref_t<MClientCaps>(), ref_t<MClientCaps>());
      head_in->remove_need_snapflush(sin, snapid, client);
    }
  }
}


bool Locker::should_defer_client_cap_frozen(CInode *in)
{
  if (in->is_frozen())
    return true;

  /*
   * This policy needs to be AT LEAST as permissive as allowing a client
   * request to go forward, or else a client request can release something,
   * the release gets deferred, but the request gets processed and deadlocks
   * because when the caps can't get revoked.
   *
   * No auth_pin implies that there is no unstable lock and @in is not auth
   * pinnned by client request. If parent dirfrag is auth pinned by a lock
   * cache, later request from lock cache owner may forcibly auth pin the @in.
   */
  if (in->is_freezing() && in->get_num_auth_pins() == 0) {
    CDir* dir = in->get_parent_dir();
    if (!dir || !dir->is_auth_pinned_by_lock_cache())
      return true;
  }
  return false;
}

void Locker::handle_client_caps(const cref_t<MClientCaps> &m)
{
  client_t client = m->get_source().num();
  snapid_t follows = m->get_snap_follows();
  auto op = m->get_op();
  auto dirty = m->get_dirty();
  dout(7) << "handle_client_caps "
	  << " on " << m->get_ino()
	  << " tid " << m->get_client_tid() << " follows " << follows
	  << " op " << ceph_cap_op_name(op)
	  << " flags 0x" << std::hex << m->flags << std::dec << dendl;

  Session *session = mds->get_session(m);
  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    if (!session) {
      dout(5) << " no session, dropping " << *m << dendl;
      return;
    }
    if (session->is_closed() ||
	session->is_closing() ||
	session->is_killing()) {
      dout(7) << " session closed|closing|killing, dropping " << *m << dendl;
      return;
    }
    if ((mds->is_reconnect() || mds->get_want_state() == MDSMap::STATE_RECONNECT) &&
	dirty && m->get_client_tid() > 0 &&
	!session->have_completed_flush(m->get_client_tid())) {
      mdcache->set_reconnected_dirty_caps(client, m->get_ino(), dirty,
					  op == CEPH_CAP_OP_FLUSHSNAP);
    }
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (mds->logger) mds->logger->inc(l_mdss_handle_client_caps);
  if (dirty) {
      if (mds->logger) mds->logger->inc(l_mdss_handle_client_caps_dirty);
  }

  if (m->get_client_tid() > 0 && session &&
      session->have_completed_flush(m->get_client_tid())) {
    dout(7) << "handle_client_caps already flushed tid " << m->get_client_tid()
	    << " for client." << client << dendl;
    ref_t<MClientCaps> ack;
    if (op == CEPH_CAP_OP_FLUSHSNAP) {
      if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_flushsnap_ack);
      ack = make_message<MClientCaps>(CEPH_CAP_OP_FLUSHSNAP_ACK, m->get_ino(), 0, 0, 0, 0, 0, dirty, 0, mds->get_osd_epoch_barrier());
    } else {
      if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_flush_ack);
      ack = make_message<MClientCaps>(CEPH_CAP_OP_FLUSH_ACK, m->get_ino(), 0, m->get_cap_id(), m->get_seq(), m->get_caps(), 0, dirty, 0, mds->get_osd_epoch_barrier());
    }
    ack->set_snap_follows(follows);
    ack->set_client_tid(m->get_client_tid());
    mds->send_message_client_counted(ack, m->get_connection());
    if (op == CEPH_CAP_OP_FLUSHSNAP) {
      return;
    } else {
      // fall-thru because the message may release some caps
      dirty = false;
      op = CEPH_CAP_OP_UPDATE;
    }
  }

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (m->get_oldest_flush_tid() > 0 && session) {
    if (session->trim_completed_flushes(m->get_oldest_flush_tid())) {
      mds->mdlog->get_current_segment()->touched_sessions.insert(session->info.inst.name);

      if (session->get_num_trim_flushes_warnings() > 0 &&
	  session->get_num_completed_flushes() * 2 < g_conf()->mds_max_completed_flushes)
	session->reset_num_trim_flushes_warnings();
    } else {
      if (session->get_num_completed_flushes() >=
	  (g_conf()->mds_max_completed_flushes << session->get_num_trim_flushes_warnings())) {
	session->inc_num_trim_flushes_warnings();
	CachedStackStringStream css;
	*css << "client." << session->get_client() << " does not advance its oldest_flush_tid ("
	     << m->get_oldest_flush_tid() << "), "
	     << session->get_num_completed_flushes()
	     << " completed flushes recorded in session";
	mds->clog->warn() << css->strv();
	dout(20) << __func__ << " " << css->strv() << dendl;
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

    /*
     * "handle_client_caps on unknown ino xxx” is normal after migrating a subtree
     * Sequence of events that cause this are:
     *   - client sends caps message to mds.a
     *   - mds finishes subtree migration, send cap export to client
     *   - mds trim its cache
     *   - mds receives cap messages from client
     */
    dout(7) << "handle_client_caps on unknown ino " << m->get_ino() << ", dropping" << dendl;
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

  dout(10) << " head inode " << *head_in << dendl;

  Capability *cap = 0;
  cap = head_in->get_client_cap(client);
  if (!cap) {
    dout(7) << "handle_client_caps no cap for client." << client << " on " << *head_in << dendl;
    return;
  }  
  ceph_assert(cap);

  // freezing|frozen?
  if (should_defer_client_cap_frozen(head_in)) {
    dout(7) << "handle_client_caps freezing|frozen on " << *head_in << dendl;
    head_in->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, m));
    return;
  }
  if (ceph_seq_cmp(m->get_mseq(), cap->get_mseq()) < 0) {
    dout(7) << "handle_client_caps mseq " << m->get_mseq() << " < " << cap->get_mseq()
	    << ", dropping" << dendl;
    return;
  }

  bool need_unpin = false;

  // flushsnap?
  if (op == CEPH_CAP_OP_FLUSHSNAP) {
    if (!head_in->is_auth()) {
      dout(7) << " not auth, ignoring flushsnap on " << *head_in << dendl;
      goto out;
    }

    SnapRealm *realm = head_in->find_snaprealm();
    snapid_t snap = realm->get_snap_following(follows);
    dout(10) << "  flushsnap follows " << follows << " -> snap " << snap << dendl;

    auto p = head_in->client_need_snapflush.begin();
    if (p != head_in->client_need_snapflush.end() && p->first < snap) {
      head_in->auth_pin(this); // prevent subtree frozen
      need_unpin = true;
      _do_null_snapflush(head_in, client, snap);
    }

    CInode *in = head_in;
    if (snap != CEPH_NOSNAP) {
      in = mdcache->pick_inode_snap(head_in, snap - 1);
      if (in != head_in)
	dout(10) << " snapped inode " << *in << dendl;
    }

    // we can prepare the ack now, since this FLUSHEDSNAP is independent of any
    // other cap ops.  (except possibly duplicate FLUSHSNAP requests, but worst
    // case we get a dup response, so whatever.)
    ref_t<MClientCaps> ack;
    if (dirty) {
      ack = make_message<MClientCaps>(CEPH_CAP_OP_FLUSHSNAP_ACK, in->ino(), 0, 0, 0, 0, 0, dirty, 0, mds->get_osd_epoch_barrier());
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
   
      _do_snap_update(in, snap, dirty, follows, client, m, ack);

      if (in != head_in)
	head_in->remove_need_snapflush(in, snap, client);
    } else {
      dout(7) << " not expecting flushsnap " << snap << " from client." << client << " on " << *in << dendl;
      if (ack) {
        if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_flushsnap_ack);
	mds->send_message_client_counted(ack, m->get_connection());
      }
    }
    goto out;
  }

  if (cap->get_cap_id() != m->get_cap_id()) {
    dout(7) << " ignoring client capid " << m->get_cap_id() << " != my " << cap->get_cap_id() << dendl;
  } else {
    CInode *in = head_in;
    if (follows > 0) {
      in = mdcache->pick_inode_snap(head_in, follows);
      // intermediate snap inodes
      while (in != head_in) {
	ceph_assert(in->last != CEPH_NOSNAP);
	if (in->is_auth() && dirty) {
	  dout(10) << " updating intermediate snapped inode " << *in << dendl;
	  _do_cap_update(in, NULL, dirty, follows, m, ref_t<MClientCaps>());
	}
	in = mdcache->pick_inode_snap(head_in, in->last);
      }
    }
 
    // head inode, and cap
    ref_t<MClientCaps> ack;

    int caps = m->get_caps();
    if (caps & ~cap->issued()) {
      dout(10) << " confirming not issued caps " << ccap_string(caps & ~cap->issued()) << dendl;
      caps &= cap->issued();
    }
    
    int revoked = cap->confirm_receipt(m->get_seq(), caps);
    dout(10) << " follows " << follows
	     << " retains " << ccap_string(m->get_caps())
	     << " dirty " << ccap_string(dirty)
	     << " on " << *in << dendl;

    if (revoked & CEPH_CAP_ANY_DIR_OPS)
      eval_lock_caches(cap);

    // missing/skipped snapflush?
    //  The client MAY send a snapflush if it is issued WR/EXCL caps, but
    //  presently only does so when it has actual dirty metadata.  But, we
    //  set up the need_snapflush stuff based on the issued caps.
    //  We can infer that the client WONT send a FLUSHSNAP once they have
    //  released all WR/EXCL caps (the FLUSHSNAP always comes before the cap
    //  update/release).
    if (!head_in->client_need_snapflush.empty()) {
      if (!(cap->issued() & CEPH_CAP_ANY_FILE_WR) &&
	  !(m->flags & MClientCaps::FLAG_PENDING_CAPSNAP)) {
	head_in->auth_pin(this); // prevent subtree frozen
	need_unpin = true;
	_do_null_snapflush(head_in, client);
      } else {
	dout(10) << " revocation in progress, not making any conclusions about null snapflushes" << dendl;
      }
    }
    if (cap->need_snapflush() && !(m->flags & MClientCaps::FLAG_PENDING_CAPSNAP))
      cap->clear_needsnapflush();

    if (dirty && in->is_auth()) {
      dout(7) << " flush client." << client << " dirty " << ccap_string(dirty)
	      << " seq " << m->get_seq() << " on " << *in << dendl;
      ack = make_message<MClientCaps>(CEPH_CAP_OP_FLUSH_ACK, in->ino(), 0, cap->get_cap_id(), m->get_seq(),
          m->get_caps(), 0, dirty, 0, mds->get_osd_epoch_barrier());
      ack->set_client_tid(m->get_client_tid());
      ack->set_oldest_flush_tid(m->get_oldest_flush_tid());
    }

    // filter wanted based on what we could ever give out (given auth/replica status)
    bool need_flush = m->flags & MClientCaps::FLAG_SYNC;
    int new_wanted = m->get_wanted();
    if (new_wanted != cap->wanted()) {
      if (!need_flush && in->is_auth() && (new_wanted & ~cap->pending())) {
	// exapnding caps.  make sure we aren't waiting for a log flush
	need_flush = _need_flush_mdlog(head_in, new_wanted & ~cap->pending());
      }

      adjust_cap_wanted(cap, new_wanted, m->get_issue_seq());
    }

    if (in->is_auth() &&
	_do_cap_update(in, cap, dirty, follows, m, ack, &need_flush)) {
      // updated
      eval(in, CEPH_CAP_LOCKS);

      if (!need_flush && (cap->wanted() & ~cap->pending()))
	need_flush = _need_flush_mdlog(in, cap->wanted() & ~cap->pending());
    } else {
      // no update, ack now.
      if (ack) {
        if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_flush_ack);
	mds->send_message_client_counted(ack, m->get_connection());
      }
      
      bool did_issue = eval(in, CEPH_CAP_LOCKS);
      if (!did_issue && (cap->wanted() & ~cap->pending()))
	issue_caps(in, cap);

      if (cap->get_last_seq() == 0 &&
	  (cap->pending() & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER))) {
	share_inode_max_size(in, cap);
      }
    }

    if (need_flush)
      mds->mdlog->flush();
  }

 out:
  if (need_unpin)
    head_in->auth_unpin(this);
}


class C_Locker_RetryRequestCapRelease : public LockerContext {
  client_t client;
  ceph_mds_request_release item;
public:
  C_Locker_RetryRequestCapRelease(Locker *l, client_t c, const ceph_mds_request_release& it) :
    LockerContext(l), client(c), item(it) { }
  void finish(int r) override {
    string dname;
    MDRequestRef null_ref;
    locker->process_request_cap_release(null_ref, client, item, dname);
  }
};

void Locker::process_request_cap_release(const MDRequestRef& mdr, client_t client, const ceph_mds_request_release& item,
					 std::string_view dname)
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
	  dout(10) << __func__ << " removing lease on " << *dn << dendl;
	  dn->remove_client_lease(l, this);
	} else {
	  dout(7) << __func__ << " client." << client
		  << " doesn't have lease on " << *dn << dendl;
	}
      } else {
	dout(7) << __func__ << " client." << client << " released lease on dn "
		<< dir->dirfrag() << "/" << dname << " which dne" << dendl;
      }
    }
  }

  Capability *cap = in->get_client_cap(client);
  if (!cap)
    return;

  dout(10) << __func__ << " client." << client << " " << ccap_string(caps) << " on " << *in
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
    
  if (mds->logger) mds->logger->inc(l_mdss_process_request_cap_release);

  if (caps & ~cap->issued()) {
    dout(10) << " confirming not issued caps " << ccap_string(caps & ~cap->issued()) << dendl;
    caps &= cap->issued();
  }
  int revoked = cap->confirm_receipt(seq, caps);
  if (revoked & CEPH_CAP_ANY_DIR_OPS)
    eval_lock_caches(cap);

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
  void finish(int r) override {
    locker->kick_issue_caps(in, client, seq);
    in->put(CInode::PIN_PTRWAITER);
  }
};

void Locker::kick_issue_caps(CInode *in, client_t client, ceph_seq_t seq)
{
  Capability *cap = in->get_client_cap(client);
  if (!cap || cap->get_last_seq() != seq)
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

void Locker::kick_cap_releases(const MDRequestRef& mdr)
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

__u32 Locker::get_xattr_total_length(CInode::mempool_xattr_map &xattr)
{
  __u32 total = 0;

  for (const auto &p : xattr)
    total += (p.first.length() + p.second.length());
  return total;
}

void Locker::decode_new_xattrs(CInode::mempool_inode *inode,
			       CInode::mempool_xattr_map *px,
			       const cref_t<MClientCaps> &m)
{
  CInode::mempool_xattr_map tmp;

  auto p = m->xattrbl.cbegin();
  decode_noshare(tmp, p);
  __u32 total = get_xattr_total_length(tmp);
  inode->xattr_version = m->head.xattr_version;
  if (total > mds->mdsmap->get_max_xattr_size()) {
    dout(1) << "Maximum xattr size exceeded: " << total
	    << " max size: " << mds->mdsmap->get_max_xattr_size() << dendl;
    // Ignore new xattr (!!!) but increase xattr version
    // XXX how to force the client to drop cached xattrs?
    inode->xattr_version++;
  } else {
    *px = std::move(tmp);
  }
}

/**
 * m and ack might be NULL, so don't dereference them unless dirty != 0
 */
void Locker::_do_snap_update(CInode *in, snapid_t snap, int dirty, snapid_t follows, client_t client, const cref_t<MClientCaps> &m, const ref_t<MClientCaps> &ack)
{
  dout(10) << "_do_snap_update dirty " << ccap_string(dirty)
	   << " follows " << follows << " snap " << snap
	   << " on " << *in << dendl;

  if (snap == CEPH_NOSNAP) {
    // hmm, i guess snap was already deleted?  just ack!
    dout(10) << " wow, the snap following " << follows
	     << " was already deleted.  nothing to record, just ack." << dendl;
    if (ack) {
      if (ack->get_op() == CEPH_CAP_OP_FLUSHSNAP_ACK) {
          if (mds->logger) mds->logger->inc(l_mdss_ceph_cap_op_flushsnap_ack);
      }
      mds->send_message_client_counted(ack, m->get_connection());
    }
    return;
  }

  EUpdate *le = new EUpdate(mds->mdlog, "snap flush");
  mds->mdlog->start_entry(le);
  MutationRef mut = new MutationImpl();
  mut->ls = mds->mdlog->get_current_segment();

  // normal metadata updates that we can apply to the head as well.

  // update xattrs?
  CInode::mempool_xattr_map *px = nullptr;
  bool xattrs = (dirty & CEPH_CAP_XATTR_EXCL) &&
                m->xattrbl.length() &&
                m->head.xattr_version > in->get_projected_inode()->xattr_version;

  CInode::mempool_old_inode *oi = nullptr;
  CInode::old_inode_map_ptr _old_inodes;
  if (in->is_any_old_inodes()) {
    auto last = in->pick_old_inode(snap);
    if (last) {
      _old_inodes = CInode::allocate_old_inode_map(*in->get_old_inodes());
      oi = &_old_inodes->at(last);
      if (snap > oi->first) {
	(*_old_inodes)[snap - 1] = *oi;;
	oi->first = snap;
      }
    }
  }

  CInode::mempool_inode *i;
  if (oi) {
    dout(10) << " writing into old inode" << dendl;
    auto pi = in->project_inode(mut);
    pi.inode->version = in->pre_dirty();
    i = &oi->inode;
    if (xattrs)
      px = &oi->xattrs;
  } else {
    auto pi = in->project_inode(mut, xattrs);
    pi.inode->version = in->pre_dirty();
    i = pi.inode.get();
    if (xattrs)
      px = pi.xattrs.get();
  }

  _update_cap_fields(in, dirty, m, i);

  // xattr
  if (xattrs) {
    dout(7) << " xattrs v" << i->xattr_version << " -> " << m->head.xattr_version
            << " len " << m->xattrbl.length() << dendl;
    decode_new_xattrs(i, px, m);
  }

  {
    auto it = i->client_ranges.find(client);
    if (it != i->client_ranges.end()) {
      if (in->last == snap) {
        dout(10) << "  removing client_range entirely" << dendl;
        i->client_ranges.erase(it);
      } else {
        dout(10) << "  client_range now follows " << snap << dendl;
        it->second.follows = snap;
      }
    }
  }

  if (_old_inodes)
    in->reset_old_inodes(std::move(_old_inodes));

  mut->auth_pin(in);
  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY, 0, follows);
  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in, follows);

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (ack && ack->get_oldest_flush_tid() > 0)
    le->metablob.add_client_flush(metareqid_t(m->get_source(), ack->get_client_tid()),
				  ack->get_oldest_flush_tid());

  mds->mdlog->submit_entry(le, new C_Locker_FileUpdate_finish(this, in, mut, UPDATE_SNAPFLUSH,
							      ack, client));
}

void Locker::_update_cap_fields(CInode *in, int dirty, const cref_t<MClientCaps> &m, CInode::mempool_inode *pi)
{
  if (dirty == 0)
    return;

  /* m must be valid if there are dirty caps */
  ceph_assert(m);
  uint64_t features = m->get_connection()->get_features();

  if (m->get_ctime() > pi->ctime) {
    dout(7) << "  ctime " << pi->ctime << " -> " << m->get_ctime()
	    << " for " << *in << dendl;
    pi->ctime = m->get_ctime();
    if (m->get_ctime() > pi->rstat.rctime)
      pi->rstat.rctime = m->get_ctime();
  }

  if ((features & CEPH_FEATURE_FS_CHANGE_ATTR) &&
      m->get_change_attr() > pi->change_attr) {
    dout(7) << "  change_attr " << pi->change_attr << " -> " << m->get_change_attr()
	    << " for " << *in << dendl;
    pi->change_attr = m->get_change_attr();
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
      if (mtime > pi->rstat.rctime)
	pi->rstat.rctime = mtime;
    }
    if (in->is_file() &&   // ONLY if regular file
	size > pi->size) {
      dout(7) << "  size " << pi->size << " -> " << size
	      << " for " << *in << dendl;
      pi->size = size;
      pi->rstat.rbytes = size;
    }
    if (in->is_file() &&
        (dirty & CEPH_CAP_FILE_WR) &&
        inline_version > pi->inline_data.version) {
      pi->inline_data.version = inline_version;
      if (inline_version != CEPH_INLINE_NONE && m->inline_data.length() > 0)
	pi->inline_data.set_data(m->inline_data);
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
    if (m->fscrypt_file.size())
      pi->fscrypt_file = m->fscrypt_file;
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
    if ((features & CEPH_FEATURE_FS_BTIME) && m->get_btime() != pi->btime) {
      dout(7) << "  btime " << oct << pi->btime
	      << " -> " << m->get_btime() << dec
	      << " for " << *in << dendl;
      pi->btime = m->get_btime();
    }
    if (m->fscrypt_auth.size())
      pi->fscrypt_auth = m->fscrypt_auth;
  }
}

/*
 * update inode based on cap flush|flushsnap|wanted.
 *  adjust max_size, if needed.
 * if we update, return true; otherwise, false (no updated needed).
 */
bool Locker::_do_cap_update(CInode *in, Capability *cap,
			    int dirty, snapid_t follows,
			    const cref_t<MClientCaps> &m, const ref_t<MClientCaps> &ack,
			    bool *need_flush)
{
  dout(10) << "_do_cap_update dirty " << ccap_string(dirty)
	   << " issued " << ccap_string(cap ? cap->issued() : 0)
	   << " wanted " << ccap_string(cap ? cap->wanted() : 0)
	   << " on " << *in << dendl;
  ceph_assert(in->is_auth());
  client_t client = m->get_source().num();
  const auto& latest = in->get_projected_inode();

  // increase or zero max_size?
  uint64_t size = m->get_size();
  bool change_max = false;
  uint64_t old_max = latest->get_client_range(client);
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
	new_max = calc_new_max_size(latest, m->get_max_size());
      } else {
	new_max = calc_new_max_size(latest, size);

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
	if (in->get_mds_caps_wanted().empty() &&
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
	                                                 forced_change_max ? new_max : 0,
	                                                 0, utime_t());

	in->filelock.add_waiter(SimpleLock::WAIT_STABLE, cms);
	change_max = false;
      }
    }
  }

  if (m->flockbl.length()) {
    int32_t num_locks;
    auto bli = m->flockbl.cbegin();
    decode(num_locks, bli);
    for ( int i=0; i < num_locks; ++i) {
      ceph_filelock decoded_lock;
      decode(decoded_lock, bli);
      in->get_fcntl_lock_state()->held_locks.
	insert(pair<uint64_t, ceph_filelock>(decoded_lock.start, decoded_lock));
      ++in->get_fcntl_lock_state()->client_held_lock_counts[(client_t)(decoded_lock.client)];
    }
    decode(num_locks, bli);
    for ( int i=0; i < num_locks; ++i) {
      ceph_filelock decoded_lock;
      decode(decoded_lock, bli);
      in->get_flock_lock_state()->held_locks.
	insert(pair<uint64_t, ceph_filelock>(decoded_lock.start, decoded_lock));
      ++in->get_flock_lock_state()->client_held_lock_counts[(client_t)(decoded_lock.client)];
    }
  }

  if (!dirty && !change_max)
    return false;

  Session *session = mds->get_session(m);
  if (session->check_access(in, MAY_WRITE,
			    m->caller_uid, m->caller_gid, NULL, 0, 0) < 0) {
    dout(10) << "check_access failed, dropping cap update on " << *in << dendl;
    return false;
  }

  // do the update.
  EUpdate *le = new EUpdate(mds->mdlog, "cap update");
  mds->mdlog->start_entry(le);

  bool xattr = (dirty & CEPH_CAP_XATTR_EXCL) &&
               m->xattrbl.length() &&
               m->head.xattr_version > in->get_projected_inode()->xattr_version;

  MutationRef mut(new MutationImpl());
  mut->ls = mds->mdlog->get_current_segment();

  auto pi = in->project_inode(mut, xattr);
  pi.inode->version = in->pre_dirty();

  _update_cap_fields(in, dirty, m, pi.inode.get());

  if (change_max) {
    dout(7) << "  max_size " << old_max << " -> " << new_max
	    << " for " << *in << dendl;
    if (new_max) {
      auto &cr = pi.inode->client_ranges[client];
      cr.range.first = 0;
      cr.range.last = new_max;
      cr.follows = in->first - 1;
      in->mark_clientwriteable();
      if (cap)
	cap->mark_clientwriteable();
    } else {
      pi.inode->client_ranges.erase(client);
      if (pi.inode->client_ranges.empty())
	in->clear_clientwriteable();
      if (cap)
	cap->clear_clientwriteable();
    }
  }
    
  if (change_max || (dirty & (CEPH_CAP_FILE_EXCL|CEPH_CAP_FILE_WR))) 
    wrlock_force(&in->filelock, mut);  // wrlock for duration of journal

  // auth
  if (dirty & CEPH_CAP_AUTH_EXCL)
    wrlock_force(&in->authlock, mut);

  // xattrs update?
  if (xattr) {
    dout(7) << " xattrs v" << pi.inode->xattr_version << " -> " << m->head.xattr_version << dendl;
    decode_new_xattrs(pi.inode.get(), pi.xattrs.get(), m);
    wrlock_force(&in->xattrlock, mut);
  }
  
  mut->auth_pin(in);
  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY, 0, follows);
  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in, follows);

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (ack && ack->get_oldest_flush_tid() > 0)
    le->metablob.add_client_flush(metareqid_t(m->get_source(), ack->get_client_tid()),
				  ack->get_oldest_flush_tid());

  unsigned update_flags = 0;
  if (change_max)
    update_flags |= UPDATE_SHAREMAX;
  if (cap)
    update_flags |= UPDATE_NEEDSISSUE;
  mds->mdlog->submit_entry(le, new C_Locker_FileUpdate_finish(this, in, mut, update_flags,
							      ack, client));
  if (need_flush && !*need_flush &&
      ((change_max && new_max) || // max INCREASE
       _need_flush_mdlog(in, dirty)))
    *need_flush = true;

  return true;
}

void Locker::handle_client_cap_release(const cref_t<MClientCapRelease> &m)
{
  client_t client = m->get_source().num();
  dout(10) << "handle_client_cap_release " << *m << dendl;

  if (!mds->is_clientreplay() && !mds->is_active() && !mds->is_stopping()) {
    mds->wait_for_replay(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (mds->logger) mds->logger->inc(l_mdss_handle_client_cap_release);

  if (m->osd_epoch_barrier && !mds->objecter->have_map(m->osd_epoch_barrier)) {
    // Pause RADOS operations until we see the required epoch
    mds->objecter->set_epoch_barrier(m->osd_epoch_barrier);
  }

  if (mds->get_osd_epoch_barrier() < m->osd_epoch_barrier) {
    // Record the barrier so that we will retransmit it to clients
    mds->set_osd_epoch_barrier(m->osd_epoch_barrier);
  }

  Session *session = mds->get_session(m);

  for (const auto &cap : m->caps) {
    _do_cap_release(client, inodeno_t((uint64_t)cap.ino) , cap.cap_id, cap.migrate_seq, cap.seq);
  }

  if (session) {
    session->notify_cap_release(m->caps.size());
  }
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
  void finish(int r) override {
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
  remove_client_cap(in, cap);
}

void Locker::remove_client_cap(CInode *in, Capability *cap, bool kill)
{
  client_t client = cap->get_client();
  // clean out any pending snapflush state
  if (!in->client_need_snapflush.empty())
    _do_null_snapflush(in, client);

  while (!cap->lock_caches.empty()) {
    MDLockCache* lock_cache = cap->lock_caches.front();
    lock_cache->client_cap = nullptr;
    invalidate_lock_cache(lock_cache);
  }

  bool notable = cap->is_notable();
  in->remove_client_cap(client);
  if (!notable)
    return;

  if (in->is_auth()) {
    // make sure we clear out the client byte range
    if (in->get_projected_inode()->client_ranges.count(client) &&
	!(in->get_inode()->nlink == 0 && !in->is_any_caps())) {  // unless it's unlink + stray
      if (kill)
	in->state_set(CInode::STATE_NEEDSRECOVER);
      else
	check_inode_max_size(in);
    }
  } else {
    request_inode_file_caps(in);
  }
  
  try_eval(in, CEPH_CAP_LOCKS);
}


/**
 * Return true if any currently revoking caps exceed the
 * session_timeout threshold.
 */
bool Locker::any_late_revoking_caps(xlist<Capability*> const &revoking,
                                    double timeout) const
{
    xlist<Capability*>::const_iterator p = revoking.begin();
    if (p.end()) {
      // No revoking caps at the moment
      return false;
    } else {
      utime_t now = ceph_clock_now();
      utime_t age = now - (*p)->get_last_revoke_stamp();
      if (age <= timeout) {
          return false;
      } else {
          return true;
      }
    }
}

std::set<client_t> Locker::get_late_revoking_clients(double timeout) const
{
  std::set<client_t> result;

  if (any_late_revoking_caps(revoking_caps, timeout)) {
    // Slow path: execute in O(N_clients)
    for (auto &p : revoking_caps_by_client) {
      if (any_late_revoking_caps(p.second, timeout)) {
        result.insert(p.first);
      }
    }
  } else {
    // Fast path: no misbehaving clients, execute in O(1)
  }
  return result;
}

// Hard-code instead of surfacing a config settings because this is
// really a hack that should go away at some point when we have better
// inspection tools for getting at detailed cap state (#7316)
#define MAX_WARN_CAPS 100

void Locker::caps_tick()
{
  utime_t now = ceph_clock_now();

  if (!need_snapflush_inodes.empty()) {
    // snap inodes that needs flush are auth pinned, they affect
    // subtree/difrarg freeze.
    utime_t cutoff = now;
    cutoff -= g_conf()->mds_freeze_tree_timeout / 3;

    CInode *last = need_snapflush_inodes.back();
    while (!need_snapflush_inodes.empty()) {
      CInode *in = need_snapflush_inodes.front();
      if (in->last_dirstat_prop >= cutoff)
	break;
      in->item_caps.remove_myself();
      snapflush_nudge(in);
      if (in == last)
	break;
    }
  }

  dout(20) << __func__ << " " << revoking_caps.size() << " revoking caps" << dendl;

  now = ceph_clock_now();
  int n = 0;
  for (xlist<Capability*>::iterator p = revoking_caps.begin(); !p.end(); ++p) {
    Capability *cap = *p;

    utime_t age = now - cap->get_last_revoke_stamp();
    dout(20) << __func__ << " age = " << age << " client." << cap->get_client() << "." << cap->get_inode()->ino() << dendl;
    if (age <= mds->mdsmap->get_session_timeout()) {
      dout(20) << __func__ << " age below timeout " << mds->mdsmap->get_session_timeout() << dendl;
      break;
    } else {
      ++n;
      if (n > MAX_WARN_CAPS) {
        dout(1) << __func__ << " more than " << MAX_WARN_CAPS << " caps are late"
          << "revoking, ignoring subsequent caps" << dendl;
        break;
      }
    }
    // exponential backoff of warning intervals
    if (age > mds->mdsmap->get_session_timeout() * (1 << cap->get_num_revoke_warnings())) {
      cap->inc_num_revoke_warnings();
      CachedStackStringStream css;
      *css << "client." << cap->get_client() << " isn't responding to mclientcaps(revoke), ino "
	   << cap->get_inode()->ino() << " pending " << ccap_string(cap->pending())
	   << " issued " << ccap_string(cap->issued()) << ", sent " << age << " seconds ago";
      mds->clog->warn() << css->strv();
      dout(20) << __func__ << " " << css->strv() << dendl;
    } else {
      dout(20) << __func__ << " silencing log message (backoff) for " << "client." << cap->get_client() << "." << cap->get_inode()->ino() << dendl;
    }
  }
}


void Locker::handle_client_lease(const cref_t<MClientLease> &m)
{
  dout(10) << "handle_client_lease " << *m << dendl;

  ceph_assert(m->get_source().is_client());
  client_t client = m->get_source().num();

  CInode *in = mdcache->get_inode(m->get_ino(), m->get_last());
  if (!in) {
    dout(7) << "handle_client_lease don't have ino " << m->get_ino() << "." << m->get_last() << dendl;
    return;
  }
  CDentry *dn = 0;

  frag_t fg = in->pick_dirfrag(m->dname);
  CDir *dir = in->get_dirfrag(fg);
  if (dir) 
    dn = dir->lookup(m->dname);
  if (!dn) {
    dout(7) << "handle_client_lease don't have dn " << m->get_ino() << " " << m->dname << dendl;
    return;
  }
  dout(10) << " on " << *dn << dendl;

  // replica and lock
  ClientLease *l = dn->get_client_lease(client);
  if (!l) {
    dout(7) << "handle_client_lease didn't have lease for client." << client << " of " << *dn << dendl;
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
    break;

  case CEPH_MDS_LEASE_RENEW:
    {
      dout(7) << "handle_client_lease client." << client << " renew on " << *dn
	      << (!dn->lock.can_lease(client)?", revoking lease":"") << dendl;
      if (dn->lock.can_lease(client)) {
        auto reply = make_message<MClientLease>(*m);
	int pool = 1;   // fixme.. do something smart!
	reply->h.duration_ms = (int)(1000 * mdcache->client_lease_durations[pool]);
	reply->h.seq = ++l->seq;
	reply->clear_payload();

	utime_t now = ceph_clock_now();
	now += mdcache->client_lease_durations[pool];
	mdcache->touch_client_lease(l, pool, now);

	mds->send_message_client_counted(reply, m->get_connection());
      }
    }
    break;

  default:
    ceph_abort(); // implement me
    break;
  }
}


void Locker::issue_client_lease(CDentry *dn, CInode *in, const MDRequestRef& mdr, utime_t now,
                                bufferlist &bl)
{
  client_t client = mdr->get_client();
  Session *session = mdr->session;

  CInode *diri = dn->get_dir()->get_inode();
  if (mdr->snapid == CEPH_NOSNAP &&
      dn->lock.can_lease(client) &&
      !diri->is_stray() &&  // do not issue dn leases in stray dir!
      !diri->filelock.can_lease(client) &&
      !(diri->get_client_cap_pending(client) & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL))) {
    int mask = 0;
    CDentry::linkage_t *dnl = dn->get_linkage(client, mdr);
    if (dnl->is_primary()) {
      ceph_assert(dnl->get_inode() == in);
      mask = CEPH_LEASE_PRIMARY_LINK;
    } else {
      if (dnl->is_remote())
        ceph_assert(dnl->get_remote_ino() == in->ino());
      else
        ceph_assert(!in);
    }
    // issue a dentry lease
    ClientLease *l = dn->add_client_lease(client, session);
    session->touch_lease(l);
    
    int pool = 1;   // fixme.. do something smart!
    now += mdcache->client_lease_durations[pool];
    mdcache->touch_client_lease(l, pool, now);

    LeaseStat lstat;
    lstat.mask = CEPH_LEASE_VALID | mask;
    lstat.duration_ms = (uint32_t)(1000 * mdcache->client_lease_durations[pool]);
    lstat.seq = ++l->seq;
    lstat.alternate_name = std::string(dn->alternate_name);
    encode_lease(bl, session->info, lstat);
    dout(20) << "issue_client_lease seq " << lstat.seq << " dur " << lstat.duration_ms << "ms "
	     << " on " << *dn << dendl;
  } else {
    // null lease
    LeaseStat lstat;
    lstat.mask = 0;
    lstat.alternate_name = std::string(dn->alternate_name);
    encode_lease(bl, session->info, lstat);
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
    ceph_assert(lock->get_type() == CEPH_LOCK_DN);

    CDentry *dn = static_cast<CDentry*>(lock->get_parent());
    int mask = 1 | CEPH_LOCK_DN; // old and new bits
    
    // i should also revoke the dir ICONTENT lease, if they have it!
    CInode *diri = dn->get_dir()->get_inode();
    auto lease = make_message<MClientLease>(CEPH_MDS_LEASE_REVOKE, l->seq, mask, diri->ino(), diri->first, CEPH_NOSNAP, dn->get_name());
    mds->send_message_client_counted(lease, l->client);
  }
}

void Locker::encode_lease(bufferlist& bl, const session_info_t& info,
			  const LeaseStat& ls)
{
  if (info.has_feature(CEPHFS_FEATURE_REPLY_ENCODING)) {
    ENCODE_START(2, 1, bl);
    encode(ls.mask, bl);
    encode(ls.duration_ms, bl);
    encode(ls.seq, bl);
    encode(ls.alternate_name, bl);
    ENCODE_FINISH(bl);
  }
  else {
    encode(ls.mask, bl);
    encode(ls.duration_ms, bl);
    encode(ls.seq, bl);
  }
}

// locks ----------------------------------------------------------------

SimpleLock *Locker::get_lock(int lock_type, const MDSCacheObjectInfo &info) 
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
    ceph_abort();
    break;
  }

  return 0;  
}

void Locker::handle_lock(const cref_t<MLock> &m)
{
  // nobody should be talking to us during recovery.
  ceph_assert(mds->is_rejoin() || mds->is_clientreplay() || mds->is_active() || mds->is_stopping());

  SimpleLock *lock = get_lock(m->get_lock_type(), m->get_object_info());
  if (!lock) {
    dout(10) << "don't have object " << m->get_object_info() << ", must have trimmed, dropping" << dendl;
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
    ceph_abort();
    break;
  }
}
 




// ==========================================================================
// simple lock

/** This function may take a reference to m if it needs one, but does
 * not put references. */
void Locker::handle_reqrdlock(SimpleLock *lock, const cref_t<MLock> &m)
{
  MDSCacheObject *parent = lock->get_parent();
  if (parent->is_auth() &&
      lock->get_state() != LOCK_SYNC &&
      !parent->is_frozen()) {
    dout(7) << "handle_reqrdlock got rdlock request on " << *lock
	    << " on " << *parent << dendl;
    ceph_assert(parent->is_auth()); // replica auth pinned if they're doing this!
    if (lock->is_stable()) {
      simple_sync(lock);
    } else {
      dout(7) << "handle_reqrdlock delaying request until lock is stable" << dendl;
      lock->add_waiter(SimpleLock::WAIT_STABLE | MDSCacheObject::WAIT_UNFREEZE,
                       new C_MDS_RetryMessage(mds, m));
    }
  } else {
    dout(7) << "handle_reqrdlock dropping rdlock request on " << *lock
	    << " on " << *parent << dendl;
    // replica should retry
  }
}

void Locker::handle_simple_lock(SimpleLock *lock, const cref_t<MLock> &m)
{
  int from = m->get_asker();
  
  dout(10) << "handle_simple_lock " << *m
	   << " on " << *lock << " " << *lock->get_parent() << dendl;

  if (mds->is_rejoin()) {
    if (lock->get_parent()->is_rejoining()) {
      dout(7) << "handle_simple_lock still rejoining " << *lock->get_parent()
	      << ", dropping " << *m << dendl;
      return;
    }
  }

  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    ceph_assert(lock->get_state() == LOCK_LOCK);
    lock->decode_locked_state(m->get_data());
    lock->set_state(LOCK_SYNC);
    lock->finish_waiters(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    break;
    
  case LOCK_AC_LOCK:
    ceph_assert(lock->get_state() == LOCK_SYNC);
    lock->set_state(LOCK_SYNC_LOCK);
    if (lock->is_leased())
      revoke_client_leases(lock);
    eval_gather(lock, true);
    if (lock->is_unstable_and_locked()) {
      if (lock->is_cached())
	invalidate_lock_caches(lock);
      mds->mdlog->flush();
    }
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
    ceph_assert(lock->get_state() == LOCK_SYNC_LOCK ||
	   lock->get_state() == LOCK_SYNC_EXCL);
    ceph_assert(lock->is_gathering(from));
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

  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());

  if (lock->get_parent()->is_freezing_or_frozen()) {
    // dentry/snap lock in unreadable state can block path traverse
    if ((lock->get_type() != CEPH_LOCK_DN &&
	 lock->get_type() != CEPH_LOCK_ISNAP &&
	 lock->get_type() != CEPH_LOCK_IPOLICY) ||
	 lock->get_state() == LOCK_SYNC ||
	 lock->get_parent()->is_frozen())
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
  if (lock->get_cap_shift()) {
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
  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());

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
    default: ceph_abort();
    }

    int gather = 0;
    if (lock->is_wrlocked()) {
      gather++;
      if (lock->is_cached())
	invalidate_lock_caches(lock);

      // After a client request is early replied the mdlog won't be flushed
      // immediately, but before safe replied the request will hold the write
      // locks. So if the client sends another request to a different MDS
      // daemon, which then needs to request read lock from current MDS daemon,
      // then that daemon maybe stuck at most for 5 seconds. Which will lead
      // the client stuck at most 5 seconds.
      //
      // Let's try to flush the mdlog when the write lock is held, which will
      // release the write locks after mdlog is successfully flushed.
      mds->mdlog->flush();
    }
    
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
      ceph_assert(in);
      if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
        mds->mdcache->queue_file_recover(in);
	need_recover = true;
        gather++;
      }
    }
    
    if (!gather && lock->is_dirty()) {
      lock->get_parent()->auth_pin(lock);
      scatter_writebehind(static_cast<ScatterLock*>(lock));
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
  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());

  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  switch (lock->get_state()) {
  case LOCK_LOCK: lock->set_state(LOCK_LOCK_EXCL); break;
  case LOCK_SYNC: lock->set_state(LOCK_SYNC_EXCL); break;
  case LOCK_XSYN: lock->set_state(LOCK_XSYN_EXCL); break;
  default: ceph_abort();
  }
  
  int gather = 0;
  if (lock->is_rdlocked())
    gather++;
  if (lock->is_wrlocked())
    gather++;
  if (gather && lock->is_cached())
    invalidate_lock_caches(lock);

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
  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());
  ceph_assert(lock->get_state() != LOCK_LOCK);
  
  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  int old_state = lock->get_state();

  switch (lock->get_state()) {
  case LOCK_SYNC: lock->set_state(LOCK_SYNC_LOCK); break;
  case LOCK_XSYN: lock->set_state(LOCK_XSYN_LOCK); break;
  case LOCK_EXCL: lock->set_state(LOCK_EXCL_LOCK); break;
  case LOCK_MIX: lock->set_state(LOCK_MIX_LOCK);
    (static_cast<ScatterLock *>(lock))->clear_unscatter_wanted();
    break;
  case LOCK_TSYN: lock->set_state(LOCK_TSYN_LOCK); break;
  default: ceph_abort();
  }

  int gather = 0;
  if (lock->is_leased()) {
    gather++;
    revoke_client_leases(lock);
  }
  if (lock->is_rdlocked()) {
    if (lock->is_cached())
      invalidate_lock_caches(lock);
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
    ceph_assert(in);
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
  ceph_assert(lock->get_parent()->is_auth());
  //assert(lock->is_stable());
  ceph_assert(lock->get_state() != LOCK_XLOCK);
  
  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  if (lock->is_stable())
    lock->get_parent()->auth_pin(lock);

  switch (lock->get_state()) {
  case LOCK_LOCK: 
  case LOCK_XLOCKDONE: lock->set_state(LOCK_LOCK_XLOCK); break;
  default: ceph_abort();
  }

  int gather = 0;
  if (lock->is_rdlocked())
    gather++;
  if (lock->is_wrlocked())
    gather++;
  if (gather && lock->is_cached())
    invalidate_lock_caches(lock);
  
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

class C_Locker_ScatterWB : public LockerLogContext {
  ScatterLock *lock;
  MutationRef mut;
public:
  C_Locker_ScatterWB(Locker *l, ScatterLock *sl, MutationRef& m) :
    LockerLogContext(l), lock(sl), mut(m) {}
  void finish(int r) override { 
    locker->scatter_writebehind_finish(lock, mut); 
  }
};

void Locker::scatter_writebehind(ScatterLock *lock)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind " << in->get_inode()->mtime << " on " << *lock << " on " << *in << dendl;

  // journal
  MutationRef mut(new MutationImpl());
  mut->ls = mds->mdlog->get_current_segment();

  // forcefully take a wrlock
  lock->get_wrlock(true);
  mut->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);

  in->pre_cow_old_inode();  // avoid cow mayhem

  auto pi = in->project_inode(mut);
  pi.inode->version = in->pre_dirty();

  in->finish_scatter_gather_update(lock->get_type(), mut);
  lock->start_flush();

  EUpdate *le = new EUpdate(mds->mdlog, "scatter_writebehind");
  mds->mdlog->start_entry(le);

  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in);
  
  in->finish_scatter_gather_update_accounted(lock->get_type(), &le->metablob);

  mds->mdlog->submit_entry(le, new C_Locker_ScatterWB(this, lock, mut));
  mds->mdlog->flush();
}

void Locker::scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind_finish on " << *lock << " on " << *in << dendl;

  mut->apply();

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

  drop_locks(mut.get());
  mut->cleanup();

  if (lock->is_stable())
    lock->finish_waiters(ScatterLock::WAIT_STABLE);

  //scatter_eval_gather(lock);
}

void Locker::scatter_eval(ScatterLock *lock, bool *need_issue)
{
  dout(10) << "scatter_eval " << *lock << " on " << *lock->get_parent() << dendl;

  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());

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
    utime_t now = ceph_clock_now();
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
void Locker::scatter_nudge(ScatterLock *lock, MDSContext *c, bool forcelockchange)
{
  CInode *p = static_cast<CInode *>(lock->get_parent());

  if (p->is_frozen() || p->is_freezing()) {
    dout(10) << "scatter_nudge waiting for unfreeze on " << *p << dendl;
    if (c) 
      p->add_waiter(MDSCacheObject::WAIT_UNFREEZE, c);
    else if (lock->is_dirty())
      // just requeue.  not ideal.. starvation prone..
      updated_scatterlocks.push_back(lock->get_updated_item());
    return;
  }

  if (p->is_ambiguous_auth()) {
    dout(10) << "scatter_nudge waiting for single auth on " << *p << dendl;
    if (c) 
      p->add_waiter(MDSCacheObject::WAIT_SINGLEAUTH, c);
    else if (lock->is_dirty())
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
	  ceph_abort();
	}
	++count;
	if (lock->is_stable() && count == 2) {
	  dout(10) << "scatter_nudge oh, stable after two cycles." << dendl;
	  // this should only realy happen when called via
	  // handle_file_lock due to AC_NUDGE, because the rest of the
	  // time we are replicated or have dirty data and won't get
	  // called.  bailing here avoids an infinite loop.
	  ceph_assert(!c); 
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
    if (!mds->is_cluster_degraded() || mds->mdsmap->is_clientreplay_or_active_or_stopping(auth)) {
      mds->send_message_mds(make_message<MLock>(lock, LOCK_AC_NUDGE, mds->get_nodeid()), auth);
    }

    // wait...
    if (c)
      lock->add_waiter(SimpleLock::WAIT_STABLE, c);

    // also, requeue, in case we had wrong auth or something
    if (lock->is_dirty())
      updated_scatterlocks.push_back(lock->get_updated_item());
  }
}

void Locker::scatter_tick()
{
  dout(10) << "scatter_tick" << dendl;
  
  // updated
  utime_t now = ceph_clock_now();
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
    if (now - lock->get_update_stamp() < g_conf()->mds_scatter_nudge_interval)
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
  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());

  ceph_abort_msg("not fully implemented, at least not for filelock");

  CInode *in = static_cast<CInode *>(lock->get_parent());

  switch (lock->get_state()) {
  case LOCK_SYNC: ceph_abort();   // this shouldn't happen
  case LOCK_LOCK: lock->set_state(LOCK_LOCK_TSYN); break;
  case LOCK_MIX: lock->set_state(LOCK_MIX_TSYN); break;
  default: ceph_abort();
  }

  int gather = 0;
  if (lock->is_wrlocked()) {
    if (lock->is_cached())
      invalidate_lock_caches(lock);
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

void Locker::local_wrlock_grab(LocalLockC *lock, MutationRef& mut)
{
  dout(7) << "local_wrlock_grab  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->can_wrlock());
  lock->get_wrlock(mut->get_client());

  auto it = mut->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);
  ceph_assert(it->is_wrlock());
}

bool Locker::local_wrlock_start(LocalLockC *lock, const MDRequestRef& mut)
{
  dout(7) << "local_wrlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  ceph_assert(lock->get_parent()->is_auth());
  if (lock->can_wrlock()) {
    lock->get_wrlock(mut->get_client());
    auto it = mut->emplace_lock(lock, MutationImpl::LockOp::WRLOCK);
    ceph_assert(it->is_wrlock());
    return true;
  } else {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
    return false;
  }
}

void Locker::local_wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut)
{
  ceph_assert(it->is_wrlock());
  LocalLockC *lock = static_cast<LocalLockC*>(it->lock);
  dout(7) << "local_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_wrlock();
  mut->locks.erase(it);
  if (lock->get_num_wrlocks() == 0) {
    lock->finish_waiters(SimpleLock::WAIT_STABLE |
                         SimpleLock::WAIT_WR |
                         SimpleLock::WAIT_RD);
  }
}

bool Locker::local_xlock_start(LocalLockC *lock, const MDRequestRef& mut)
{
  dout(7) << "local_xlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  ceph_assert(lock->get_parent()->is_auth());
  if (!lock->can_xlock_local()) {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mdcache, mut));
    return false;
  }

  lock->get_xlock(mut, mut->get_client());
  mut->emplace_lock(lock, MutationImpl::LockOp::XLOCK);
  return true;
}

void Locker::local_xlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut)
{
  ceph_assert(it->is_xlock());
  LocalLockC *lock = static_cast<LocalLockC*>(it->lock);
  dout(7) << "local_xlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_xlock();
  mut->locks.erase(it);

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

  ceph_assert(lock->get_parent()->is_auth());
  ceph_assert(lock->is_stable());

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
	(in->is_dir() && in->multiple_nonstale_caps())) {  // FIXME.. :/
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
	   in->get_target_loner() >= 0 &&
	   (in->is_dir() ?
	    !in->has_subtree_or_exporting_dirfrag() :
	    (wanted & (CEPH_CAP_GEXCL|CEPH_CAP_GWR|CEPH_CAP_GBUFFER)))) {
    dout(7) << "file_eval stable, bump to loner " << *lock
	    << " on " << *lock->get_parent() << dendl;
    file_excl(lock, need_issue);
  }

  // * -> mixed?
  else if (lock->get_state() != LOCK_MIX &&
	   !lock->is_rdlocked() &&
	   //!lock->is_waiter_for(SimpleLock::WAIT_WR) &&
	   (lock->get_scatter_wanted() ||
	    (in->get_target_loner() < 0 && (wanted & CEPH_CAP_GWR)))) {
    dout(7) << "file_eval stable, bump to mixed " << *lock
	    << " on " << *lock->get_parent() << dendl;
    scatter_mix(lock, need_issue);
  }
  
  // * -> sync?
  else if (lock->get_state() != LOCK_SYNC &&
	   !lock->is_wrlocked() &&   // drain wrlocks first!
	   !lock->is_waiter_for(SimpleLock::WAIT_WR) &&
	   !(wanted & CEPH_CAP_GWR) &&
	   !((lock->get_state() == LOCK_MIX) &&
	     in->is_dir() && in->has_subtree_or_exporting_dirfrag())  // if we are a delegation point, stay where we are
	   //((wanted & CEPH_CAP_RD) || 
	   //in->is_replicated() || 
	   //lock->is_leased() ||
	   //(!loner && lock->get_state() == LOCK_EXCL)) &&
	   ) {
    dout(7) << "file_eval stable, bump to sync " << *lock 
	    << " on " << *lock->get_parent() << dendl;
    simple_sync(lock, need_issue);
  }
  else if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
    mds->mdcache->queue_file_recover(in);
    mds->mdcache->do_file_recover();
  }
}



void Locker::scatter_mix(ScatterLock *lock, bool *need_issue)
{
  dout(7) << "scatter_mix " << *lock << " on " << *lock->get_parent() << dendl;

  CInode *in = static_cast<CInode*>(lock->get_parent());
  ceph_assert(in->is_auth());
  ceph_assert(lock->is_stable());

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
    case LOCK_EXCL: lock->set_state(LOCK_EXCL_MIX); break;
    case LOCK_XSYN: lock->set_state(LOCK_XSYN_MIX); break;
    case LOCK_TSYN: lock->set_state(LOCK_TSYN_MIX); break;
    default: ceph_abort();
    }

    int gather = 0;
    if (lock->is_rdlocked()) {
      if (lock->is_cached())
	invalidate_lock_caches(lock);
      gather++;
    }
    if (in->is_replicated()) {
      if (lock->get_state() == LOCK_SYNC_MIX) { // for the rest states, replicas are already LOCK
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

  ceph_assert(in->is_auth());
  ceph_assert(lock->is_stable());

  ceph_assert((in->get_loner() >= 0 && in->get_mds_caps_wanted().empty()) ||
	 (lock->get_state() == LOCK_XSYN));  // must do xsyn -> excl -> <anything else>
  
  switch (lock->get_state()) {
  case LOCK_SYNC: lock->set_state(LOCK_SYNC_EXCL); break;
  case LOCK_MIX: lock->set_state(LOCK_MIX_EXCL); break;
  case LOCK_LOCK: lock->set_state(LOCK_LOCK_EXCL); break;
  case LOCK_XSYN: lock->set_state(LOCK_XSYN_EXCL); break;
  default: ceph_abort();
  }
  int gather = 0;
  
  if (lock->is_rdlocked())
    gather++;
  if (lock->is_wrlocked())
    gather++;
  if (gather && lock->is_cached())
    invalidate_lock_caches(lock);

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
  ceph_assert(in->is_auth());
  ceph_assert(in->get_loner() >= 0 && in->get_mds_caps_wanted().empty());

  switch (lock->get_state()) {
  case LOCK_EXCL: lock->set_state(LOCK_EXCL_XSYN); break;
  default: ceph_abort();
  }
  
  int gather = 0;
  if (lock->is_wrlocked()) {
    if (lock->is_cached())
      invalidate_lock_caches(lock);
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

  ceph_assert(in->is_auth());
  //assert(lock->is_stable());
  ceph_assert(lock->get_state() == LOCK_PRE_SCAN); // only called from MDCache::start_files_to_recover()

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
void Locker::handle_file_lock(ScatterLock *lock, const cref_t<MLock> &m)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  int from = m->get_asker();

  if (mds->is_rejoin()) {
    if (in->is_rejoining()) {
      dout(7) << "handle_file_lock still rejoining " << *in
	      << ", dropping " << *m << dendl;
      return;
    }
  }

  dout(7) << "handle_file_lock a=" << lock->get_lock_action_name(m->get_action())
	  << " on " << *lock
	  << " from mds." << from << " " 
	  << *in << dendl;

  bool caps = lock->get_cap_shift();
  
  switch (m->get_action()) {
    // -- replica --
  case LOCK_AC_SYNC:
    ceph_assert(lock->get_state() == LOCK_LOCK ||
	   lock->get_state() == LOCK_MIX ||
	   lock->get_state() == LOCK_MIX_SYNC2);
    
    if (lock->get_state() == LOCK_MIX) {
      lock->set_state(LOCK_MIX_SYNC);
      eval_gather(lock, true);
      if (lock->is_unstable_and_locked()) {
	if (lock->is_cached())
	  invalidate_lock_caches(lock);
	mds->mdlog->flush();
      }
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
    default: ceph_abort();
    }

    eval_gather(lock, true);
    if (lock->is_unstable_and_locked()) {
      if (lock->is_cached())
	invalidate_lock_caches(lock);
      mds->mdlog->flush();
    }

    break;

  case LOCK_AC_LOCKFLUSHED:
    (static_cast<ScatterLock *>(lock))->finish_flush();
    (static_cast<ScatterLock *>(lock))->clear_flushed();
    // wake up scatter_nudge waiters
    if (lock->is_stable())
      lock->finish_waiters(SimpleLock::WAIT_STABLE);
    break;
    
  case LOCK_AC_MIX:
    ceph_assert(lock->get_state() == LOCK_SYNC ||
           lock->get_state() == LOCK_LOCK ||
	   lock->get_state() == LOCK_SYNC_MIX2);
    
    if (lock->get_state() == LOCK_SYNC) {
      // MIXED
      lock->set_state(LOCK_SYNC_MIX);
      eval_gather(lock, true);
      if (lock->is_unstable_and_locked()) {
	if (lock->is_cached())
	  invalidate_lock_caches(lock);
	mds->mdlog->flush();
      }
      break;
    } 

    // ok
    lock->set_state(LOCK_MIX);
    lock->decode_locked_state(m->get_data());

    if (caps)
      issue_caps(in);
    
    lock->finish_waiters(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
    break;


    // -- auth --
  case LOCK_AC_LOCKACK:
    ceph_assert(lock->get_state() == LOCK_SYNC_LOCK ||
           lock->get_state() == LOCK_MIX_LOCK ||
           lock->get_state() == LOCK_MIX_LOCK2 ||
           lock->get_state() == LOCK_MIX_EXCL ||
           lock->get_state() == LOCK_SYNC_EXCL ||
           lock->get_state() == LOCK_SYNC_MIX ||
	   lock->get_state() == LOCK_MIX_TSYN);
    ceph_assert(lock->is_gathering(from));
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
    ceph_assert(lock->get_state() == LOCK_MIX_SYNC);
    ceph_assert(lock->is_gathering(from));
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
    ceph_assert(lock->get_state() == LOCK_SYNC_MIX);
    ceph_assert(lock->is_gathering(from));
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
    ceph_abort();
  }  
}
