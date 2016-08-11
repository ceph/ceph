#include "MDCache.h"
#include "Locker.h"
#include "CObject.h"
#include "Mutation.h"

#include "MDSRank.h"
#include "MDSDaemon.h"

#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"

#define dout_subsys ceph_subsys_mds

Locker::Locker(MDSRank *_mds) :
  mds(_mds), mdcache(_mds->mdcache), server(_mds->server),
  updated_scatterlocks_mutex("Locker::updated_scatterlocks_mutex")
{

}

class LockerContext : public MDSInternalContextBase {
protected:
  Locker *locker;
  MDSRank *get_mds() { return locker->mds; }
public:
  explicit LockerContext(Locker *l) : locker(l) {
    assert(locker != NULL);
  }
};

/* This function DOES put the passed message before returning */
void Locker::dispatch(Message *m)
{
  switch (m->get_type()) {
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

}

void Locker::handle_client_caps(MClientCaps *m)
{
    dout(10) << "handle_client_caps " << *m << dendl;
}

void Locker::handle_client_cap_release(MClientCapRelease *m)
{
    dout(10) << "handle_client_cap_release " << *m << dendl;
}

void Locker::handle_client_lease(MClientLease *m)
{
    dout(10) << "handle_client_lease " << *m << dendl;
}


/* If this function returns false, the mdr has been placed
 * on the appropriate wait list */
int Locker::acquire_locks(MDRequestRef& mdr,
			  set<SimpleLock*> &rdlocks,
			  set<SimpleLock*> &wrlocks,
			  set<SimpleLock*> &xlocks)
{
  if (mdr->done_locking) {
    dout(10) << "acquire_locks " << *mdr << " - done locking" << dendl;    
    return true;  // at least we had better be!
  }
  dout(10) << "acquire_locks " << *mdr << dendl;

  set<SimpleLock*, SimpleLock::ptr_lt> sorted;  // sort everything we will lock

  // xlocks
  for (set<SimpleLock*>::iterator p = xlocks.begin(); p != xlocks.end(); ++p) {
    dout(20) << " must xlock " << **p << " " << *(*p)->get_parent() << dendl;
    sorted.insert(*p);

    // augment xlock with a versionlock?
    if ((*p)->get_type() == CEPH_LOCK_DN) {
      CDentry *dn = (CDentry*)(*p)->get_parent();

      if (xlocks.count(&dn->versionlock))
	continue;  // we're xlocking the versionlock too; don't wrlock it!

      wrlocks.insert(&dn->versionlock);
    }
    if ((*p)->get_type() > CEPH_LOCK_IVERSION) {
      // inode version lock?
      CInode *in = (CInode*)(*p)->get_parent();
      wrlocks.insert(&in->versionlock);
    }
  }

  // wrlocks
  for (set<SimpleLock*>::iterator p = wrlocks.begin(); p != wrlocks.end(); ++p) {
    CObject *object = (*p)->get_parent();
    dout(20) << " must wrlock " << **p << " " << *object << dendl;
    sorted.insert(*p);
  }

  // rdlocks
  for (set<SimpleLock*>::iterator p = rdlocks.begin();
	 p != rdlocks.end();
       ++p) {
    CObject *object = (*p)->get_parent();
    dout(20) << " must rdlock " << **p << " " << *object << dendl;
    sorted.insert(*p);
  }

  // caps i'll need to issue
  set<CInodeRef> issue_set;
  bool verify_path = false;
  int result = 0;

  // acquire locks.
  // make sure they match currently acquired locks.
  auto existing = mdr->locks.begin();
  for (auto p = sorted.begin(); p != sorted.end(); ++p) {
    // already locked?
    if (existing != mdr->locks.end() && *existing == *p) {
      // right kind?
      SimpleLock *have = *existing;
      ++existing;
      if (xlocks.count(have) && mdr->xlocks.count(have)) {
	dout(10) << " already xlocked " << *have << " " << *have->get_parent() << dendl;
	continue;
      }
      if (wrlocks.count(have) && mdr->wrlocks.count(have)) {
	dout(10) << " already wrlocked " << *have << " " << *have->get_parent() << dendl;
	continue;
      }
      if (rdlocks.count(have) && mdr->rdlocks.count(have)) {
	dout(10) << " already rdlocked " << *have << " " << *have->get_parent() << dendl;
	continue;
      }
    }
    
    // hose any stray locks
    while (existing != mdr->locks.end()) {
      SimpleLock *stray = *existing;
      ++existing;
      dout(10) << " unlocking out-of-order " << *stray << " " << *stray->get_parent() << dendl;
      CObjectRef object = stray->get_parent();
      bool need_issue = false;
      if (mdr->xlocks.count(stray)) {
	xlock_finish(stray, mdr.get(), &need_issue);
      } else if (mdr->wrlocks.count(stray)) {
	wrlock_finish(stray, mdr.get(), &need_issue);
      } else if (mdr->rdlocks.count(stray)) {
	rdlock_finish(stray, mdr.get(), &need_issue);
      } else {
	assert(0);
      }
      if (need_issue)
	issue_set.insert(static_cast<CInode*>(object.get()));
    }

    if ((*p)->get_type() <= CEPH_LOCK_DN)
      verify_path = true;

    // lock
    if (mdr->locking && *p != mdr->locking) {
      cancel_locking(mdr.get(), &issue_set);
    }
    if (xlocks.count(*p)) {
      if (!xlock_start(*p, mdr)) 
	goto out;
      dout(10) << " got xlock on " << **p << " " << *(*p)->get_parent() << dendl;
    } else if (wrlocks.count(*p)) {
      if (!wrlock_start(*p, mdr))
	goto out;
      dout(10) << " got wrlock on " << **p << " " << *(*p)->get_parent() << dendl;
    } else if (rdlocks.count(*p)) {
      if (!rdlock_start(*p, mdr)) 
	goto out;
      dout(10) << " got rdlock on " << **p << " " << *(*p)->get_parent() << dendl;
    } else {
      assert(0);
    }
  }
    
  // any extra unneeded locks?
  while (existing != mdr->locks.end()) {
    SimpleLock *stray = *existing;
    ++existing;
    dout(10) << " unlocking extra " << *stray << " " << *stray->get_parent() << dendl;
    CObjectRef object = stray->get_parent();
    bool need_issue = false;
    if (mdr->xlocks.count(stray)) {
      xlock_finish(stray, mdr.get(), &need_issue);
    } else if (mdr->wrlocks.count(stray)) {
      wrlock_finish(stray, mdr.get(), &need_issue);
    } else if (mdr->rdlocks.count(stray)) {
      rdlock_finish(stray, mdr.get(), &need_issue);
    } else {
      assert(0);
    }
    if (need_issue)
      issue_set.insert(static_cast<CInode*>(object.get()));
  }

  if (verify_path) {
    result = -EAGAIN;
  } else {
    mdr->done_locking = true;
    mdr->set_mds_stamp(ceph_clock_now(NULL));
    result = 1;
  }
out:
  issue_caps_set(issue_set);
  return result;
}

void Locker::set_xlocks_done(MutationImpl *mut, bool skip_dentry)
{
  for (auto p = mut->xlocks.begin(); p != mut->xlocks.end(); ++p) {
    CObject *object = (*p)->get_parent();
    bool dentry_lock = ((*p)->get_type() <= CEPH_LOCK_DN);
    if (dentry_lock && skip_dentry)
      continue;

    if (!dentry_lock)
      assert(mut->is_object_locked(object));
    CObject::Locker l(dentry_lock ? object : NULL);
    dout(10) << "set_xlocks_done on " << **p << " " << *object << dendl;
    (*p)->set_xlock_done();
  }
}

void Locker::_drop_rdlocks(MutationImpl *mut, set<CInodeRef> *pneed_issue)
{
  while (!mut->rdlocks.empty()) {
    SimpleLock *lock = *mut->rdlocks.begin();
    CObjectRef p = lock->get_parent();
    bool ni = false;
    rdlock_finish(lock, mut, &ni);
    if (ni)
      pneed_issue->insert(static_cast<CInode*>(p.get()));
  }
}

void Locker::_drop_non_rdlocks(MutationImpl *mut, set<CInodeRef> *pneed_issue,
			       set<CObject*> *pobjs)
{

  for (auto p = mut->xlocks.begin(); p != mut->xlocks.end(); ) {
    SimpleLock *lock = *(p++);
    if (pobjs && !pobjs->count(lock->get_parent()))
      continue;

    CObjectRef object = lock->get_parent();
    bool ni = false;

    xlock_finish(lock, mut, &ni, pobjs && lock->get_type() > CEPH_LOCK_DN);
    if (ni)
      pneed_issue->insert(static_cast<CInode*>(object.get()));
  }

  for (auto p = mut->wrlocks.begin(); p != mut->wrlocks.end(); ) {
    SimpleLock *lock = *(p++);
    if (pobjs && !pobjs->count(lock->get_parent()))
      continue;

    CObjectRef object = lock->get_parent();
    bool ni = false;
    wrlock_finish(lock, mut, &ni, pobjs && lock->get_type() > CEPH_LOCK_DN);
    if (ni)
      pneed_issue->insert(static_cast<CInode*>(object.get()));
  }
}


void Locker::drop_locks(MutationImpl *mut)
{
  // leftover locks
  set<CInodeRef> need_issue;

  if (mut->locking)
    cancel_locking(mut, &need_issue);
  _drop_non_rdlocks(mut, &need_issue);
  _drop_rdlocks(mut, &need_issue);

  issue_caps_set(need_issue);
  mut->done_locking = false;
}

void Locker::drop_non_rdlocks(MutationImpl *mut, set<CObject*>& objs)
{
  set<CInodeRef> need_issue;

  _drop_non_rdlocks(mut, &need_issue, &objs);

  issue_caps_set(need_issue, true);
}

void Locker::drop_rdlocks(MutationImpl *mut)
{
  set<CInodeRef> need_issue;

  _drop_rdlocks(mut, &need_issue);

  issue_caps_set(need_issue);
}

// generics
void Locker::eval_gather(SimpleLock *lock, bool first, bool *pneed_issue, list<MDSInternalContextBase*>* pfinishers)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(10) << "eval_gather " << *lock << " on " << *lock->get_parent() << dendl;
  assert(!lock->is_stable());

  CInode *in = NULL;
  if (lock->get_type() > CEPH_LOCK_DN)
    in = static_cast<CInode *>(lock->get_parent());

  int next = lock->get_next_state();
  bool caps = lock->get_cap_shift();
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

  if ((lock->get_sm()->states[next].can_rdlock <= AUTH || !lock->is_rdlocked()) &&
      (lock->get_sm()->states[next].can_wrlock <= AUTH || !lock->is_wrlocked()) &&
      (lock->get_sm()->states[next].can_xlock <= AUTH || !lock->is_xlocked()) &&
      (lock->get_sm()->states[next].can_lease <= AUTH || !lock->is_leased()) &&
      !lock->is_flushing() &&  // i.e. wait for scatter_writebehind!
      (!caps || ((~lock->gcaps_allowed(CAP_ANY, next) & other_issued) == 0 &&
		 (~lock->gcaps_allowed(CAP_LONER, next) & loner_issued) == 0 &&
		 (~lock->gcaps_allowed(CAP_XLOCKER, next) & xlocker_issued) == 0))) {
    dout(7) << "eval_gather finished gather on " << *lock
	    << " on " << *lock->get_parent() << dendl;

    if (lock->is_dirty() && !lock->is_flushed()) {
      scatter_writebehind(static_cast<ScatterLock *>(lock));
      //mds->mdlog->flush();
      return;
    }
    lock->clear_flushed();

    switch (lock->get_state()) {
      // to mixed
      case LOCK_TSYN_MIX:
      case LOCK_SYNC_MIX:
      case LOCK_EXCL_MIX:
	in->start_scatter(static_cast<ScatterLock *>(lock));
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
	break;
    }

  }

  lock->set_state(next);

  // drop loner before doing waiters
  if (caps &&
      in->is_head() &&
      in->get_wanted_loner() != in->get_loner()) {
    dout(10) << "  trying to drop loner" << dendl;
    if (in->try_drop_loner()) {
      dout(10) << "  dropped loner" << dendl;
      need_issue = true;
    }
  }

  if (pfinishers)
    lock->take_waiting(SimpleLock::WAIT_ALL, *pfinishers);
  else
    lock->finish_waiting(SimpleLock::WAIT_ALL);

  if (caps && in->is_head())
    need_issue = true;

  if (lock->is_stable())
    try_eval(lock, &need_issue);

  if (need_issue) {
    if (pneed_issue)
      *pneed_issue = true;
    else if (in->is_head())
      issue_caps(in);
  }
}

bool Locker::eval(CInode *in, int mask)
{
  in->mutex_assert_locked_by_me();

  bool need_issue = false; 
  list<MDSInternalContextBase*> finishers;
  
  dout(10) << "eval " << mask << " " << *in << dendl;

  // choose loner?
  if (in->is_head()) {
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
    eval_any(&in->filelock, &need_issue, &finishers);
  if (mask & CEPH_LOCK_IAUTH)
    eval_any(&in->authlock, &need_issue, &finishers);
  if (mask & CEPH_LOCK_ILINK)
    eval_any(&in->linklock, &need_issue, &finishers);
  if (mask & CEPH_LOCK_IXATTR)
    eval_any(&in->xattrlock, &need_issue, &finishers);
  if (mask & CEPH_LOCK_INEST)
    eval_any(&in->nestlock, &need_issue, &finishers);
  if (mask & CEPH_LOCK_IFLOCK)
    eval_any(&in->flocklock, &need_issue, &finishers);
  if (mask & CEPH_LOCK_IPOLICY)
    eval_any(&in->policylock, &need_issue, &finishers);

  // drop loner?
  if (in->is_head() && in->get_wanted_loner() != in->get_loner()) {
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

void Locker::try_eval(CObject *p, int mask)
{
  // unstable and ambiguous auth?
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
  eval(lock, pneed_issue);
}

void Locker::eval_cap_gather(CInode *in, set<CInodeRef> *issue_set)
{
  in->mutex_assert_locked_by_me();

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

bool Locker::rdlock_kick(SimpleLock *lock)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  // kick the lock
  if (lock->is_stable()) {
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
	  !in->is_dir())   // as_anon => caller wants SYNC, not XSYN
	file_xsyn(lock);
      else
	simple_sync(lock);
    } else
      simple_sync(lock);
    return true;
  }
  /*
  if (lock->get_type() == CEPH_LOCK_IFILE) {
    CInode *in = static_cast<CInode *>(lock->get_parent());
    if (in->state_test(CInode::STATE_RECOVERING)) {
      mdcache->recovery_queue.prioritize(in);
    }
  }
  */

  return false;
}

bool Locker::rdlock_start(SimpleLock *lock, MDRequestRef& mut)
{
  CObject::Locker l(lock->get_parent());

  dout(7) << "rdlock_start  on " << *lock << " on " << *lock->get_parent() << dendl;  

  client_t client = mut->get_client();

  /*
  CInode *in = 0;
  if (lock->get_type() > CEPH_LOCK_DN)
    in = static_cast<CInode *>(lock->get_parent());

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

    if (!rdlock_kick(lock))
      break;
  }

  // wait!
  int wait_on;
  if (lock->is_stable())
    wait_on = SimpleLock::WAIT_RD;
  else
    wait_on = SimpleLock::WAIT_STABLE;  // REQRDLOCK is ignored if lock is unstable, so we need to retry.
  dout(7) << "rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->add_waiter(wait_on, new C_MDS_RetryRequest(mds, mut));
  nudge_log(lock);
  return false;
}

void Locker::nudge_log(SimpleLock *lock)
{
  dout(10) << "nudge_log " << *lock << " on " << *lock->get_parent() << dendl;
  if (!lock->is_stable())    // as with xlockdone, or cap flush
	  ;
   // mds->mdlog->flush();
}

void Locker::rdlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue)
{
  CObject::Locker l(lock->get_parent());

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
    else
      try_eval(lock, pneed_issue);
  }
}

// ------------------
// wrlock

void Locker::wrlock_force(SimpleLock *lock, MutationImpl* mut)
{
  assert(mut->is_object_locked(lock->get_parent()));

  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_grab(static_cast<LocalLock*>(lock), mut);

  dout(7) << "wrlock_force  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->get_wrlock(true);
  mut->wrlocks.insert(lock);
  mut->locks.insert(lock);
}

bool Locker::wrlock_start(SimpleLock *lock, MutationImpl* mut)
{
  assert(mut->is_object_locked(lock->get_parent()));

  assert(lock->get_type() != CEPH_LOCK_IVERSION &&
	 lock->get_type() != CEPH_LOCK_DVERSION);

  dout(10) << "wrlock_start " << *lock << " on " << *lock->get_parent() << dendl;

  client_t client = mut->get_client();

  if (!lock->can_wrlock(client)) {

    /*
       if (lock->get_type() == CEPH_LOCK_IFILE &&
       in->state_test(CInode::STATE_RECOVERING)) {
       mdcache->recovery_queue.prioritize(in);
       }
       */

    if (!lock->is_stable())
      return false;

    // don't do nested lock state change if we have dirty scatterdata and
    // may scatter_writebehind or start_scatter, because nowait==true implies
    // that the caller already has a log entry open!
    if (lock->is_dirty())
      return false;

    simple_lock(lock);
  }

  // wrlock?
  if (lock->can_wrlock(client)) {
    lock->get_wrlock();
    mut->wrlocks.insert(lock);
    mut->locks.insert(lock);
    return true;
  }

  return false;
}

bool Locker::wrlock_start(SimpleLock *lock, MDRequestRef& mut)
{
  CObject::Locker l(lock->get_parent());

  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_start(static_cast<LocalLock*>(lock), mut);

  dout(10) << "wrlock_start " << *lock << " on " << *lock->get_parent() << dendl;

  CInode *in = static_cast<CInode *>(lock->get_parent());
  client_t client = mut->get_client();

  while (1) {
    // wrlock?
    if (lock->can_wrlock(client)) {
      lock->get_wrlock();
      mut->wrlocks.insert(lock);
      mut->locks.insert(lock);
      return true;
    }

    /*
    if (lock->get_type() == CEPH_LOCK_IFILE &&
	in->state_test(CInode::STATE_RECOVERING)) {
      mdcache->recovery_queue.prioritize(in);
    }
    */

    if (!lock->is_stable())
      break;


    simple_lock(lock);
  }

  dout(7) << "wrlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mds, mut));
  nudge_log(lock);
    
  return false;
}

void Locker::wrlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue, bool parent_locked)
{
  if (parent_locked)
    assert(mut->is_object_locked(lock->get_parent()));
  CObject::Locker l(parent_locked ? NULL : lock->get_parent());

  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_wrlock_finish(static_cast<LocalLock*>(lock), mut);

  dout(7) << "wrlock_finish on " << *lock << " on " << *lock->get_parent() << dendl;
  lock->put_wrlock();
  if (mut) {
    mut->wrlocks.erase(lock);
    mut->locks.erase(lock);
  }

  if (!lock->is_wrlocked()) {
    if (!lock->is_stable())
      eval_gather(lock, false, pneed_issue);
    else
      try_eval(lock, pneed_issue);
  }
}


// ------------------
// xlock

bool Locker::xlock_start(SimpleLock *lock, MDRequestRef& mut)
{
  CObject::Locker l(lock->get_parent());

  if (lock->get_type() == CEPH_LOCK_IVERSION ||
      lock->get_type() == CEPH_LOCK_DVERSION)
    return local_xlock_start(static_cast<LocalLock*>(lock), mut);

  dout(7) << "xlock_start on " << *lock << " on " << *lock->get_parent() << dendl;
  client_t client = mut->get_client();

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

    /*
    if (lock->get_type() == CEPH_LOCK_IFILE) {
      CInode *in = static_cast<CInode*>(lock->get_parent());
      if (in->state_test(CInode::STATE_RECOVERING)) {
	mdcache->recovery_queue.prioritize(in);
      }
    }
    */

    if (!lock->is_stable() && (lock->get_state() != LOCK_XLOCKDONE ||
	  lock->get_xlock_by_client() != client ||
	  lock->is_waiting_for(SimpleLock::WAIT_STABLE)))
      break;

    if (lock->get_state() == LOCK_LOCK || lock->get_state() == LOCK_XLOCKDONE) {
      mut->start_locking(lock);
      simple_xlock(lock);
    } else {
      simple_lock(lock);
    }
  }

  lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mds, mut));
  nudge_log(lock);
  return false;
}

void Locker::_finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();

  assert(!lock->is_stable());
  if (lock->get_num_rdlocks() == 0 &&
      lock->get_num_wrlocks() == 0 &&
      lock->get_num_client_lease() == 0 &&
      lock->get_type() != CEPH_LOCK_DN) {
    CInode *in = static_cast<CInode*>(lock->get_parent());
    client_t loner = in->get_target_loner();
    if (loner >= 0 && (xlocker < 0 || xlocker == loner)) {
      lock->set_state(LOCK_EXCL);
      lock->finish_waiting(SimpleLock::WAIT_STABLE|SimpleLock::WAIT_WR|SimpleLock::WAIT_RD);
      if (lock->get_cap_shift())
	*pneed_issue = true;
      if (lock->is_stable())
	try_eval(lock, pneed_issue);
      return;
    }
  }
  // the xlocker may have CEPH_CAP_GSHARED, need to revoke it if next state is LOCK_LOCK
  eval_gather(lock, true, pneed_issue);
}

void Locker::xlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue, bool parent_locked)
{
  if (parent_locked)
    assert(mut->is_object_locked(lock->get_parent()));
  CObject::Locker l(parent_locked ? NULL : lock->get_parent());

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

  if (lock->get_num_xlocks() == 0) {
    if (lock->get_state() == LOCK_LOCK_XLOCK)
      lock->set_state(LOCK_XLOCKDONE);
    _finish_xlock(lock, xlocker, &do_issue);
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

void Locker::cancel_locking(MutationImpl *mut, set<CInodeRef> *pneed_issue)
{
  SimpleLock *lock = mut->locking;
  assert(lock);
  dout(10) << "cancel_locking " << *lock << " on " << *mut << dendl;

  CObjectRef p = lock->get_parent();
  CObject::Locker l(p.get());

  bool need_issue = false;
  if (lock->get_state() == LOCK_PREXLOCK) {
    _finish_xlock(lock, -1, &need_issue);
  } else if (lock->get_state() == LOCK_LOCK_XLOCK &&
	     lock->get_num_xlocks() == 0) {
    lock->set_state(LOCK_XLOCKDONE);
    eval_gather(lock, true, &need_issue);
  }

  if (need_issue)
    pneed_issue->insert(static_cast<CInode*>(p.get()));

  mut->finish_locking(lock);
}

//* caller hold mutex
void Locker::simple_eval(SimpleLock *lock, bool *need_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();

  dout(10) << "simple_eval " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->is_stable());

  /*
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
  */

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
	   ((!(wanted & CEPH_CAP_GEXCL) && !lock->is_waiting_for(SimpleLock::WAIT_WR)) ||
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
      /*
    if (lock->get_type() == CEPH_LOCK_IFILE) {
      assert(in);
      if (in->state_test(CInode::STATE_NEEDSRECOVER)) {
        mds->mdcache->queue_file_recover(in);
	need_recover = true;
        gather++;
      }
    }
      */
    
    if (!gather && lock->is_dirty()) {
      scatter_writebehind(static_cast<ScatterLock*>(lock));
      //mds->mdlog->flush();
      return false;
    }

    if (gather) {
	    /*
      if (need_recover)
	mds->mdcache->do_file_recover();
	*/
      return false;
    }
  }

  lock->set_state(LOCK_SYNC);
  lock->finish_waiting(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
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
  } else {
    lock->set_state(LOCK_EXCL);
    lock->finish_waiting(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
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
  /*
  if (lock->get_type() == CEPH_LOCK_IFILE) {
    assert(in);
    if(in->state_test(CInode::STATE_NEEDSRECOVER)) {
      mds->mdcache->queue_file_recover(in);
      need_recover = true;
      gather++;
    }
  }
  */

    // move to second stage of gather now, so we don't send the lock action later.
    if (lock->get_state() == LOCK_MIX_LOCK)
      lock->set_state(LOCK_MIX_LOCK2);

  if (!gather && lock->is_dirty()) {
    scatter_writebehind(static_cast<ScatterLock*>(lock));
   // mds->mdlog->flush();
    return;
  }

  if (gather) {
	  /*
    if (need_recover)
      mds->mdcache->do_file_recover();
      */
  } else {
    lock->set_state(LOCK_LOCK);
    lock->finish_waiting(ScatterLock::WAIT_XLOCK|ScatterLock::WAIT_WR|ScatterLock::WAIT_STABLE);
  }
}


void Locker::simple_xlock(SimpleLock *lock)
{
  dout(7) << "simple_xlock on " << *lock << " on " << *lock->get_parent() << dendl;
  //assert(lock->is_stable());
  assert(lock->get_state() != LOCK_XLOCK);
  
  CInode *in = 0;
  if (lock->get_cap_shift())
    in = static_cast<CInode *>(lock->get_parent());

  if (lock->is_stable())

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

class C_Locker_ScatterWritebehind : public LockerContext {
  ScatterLock *lock;
  MutationRef mut;
public:
  C_Locker_ScatterWritebehind(Locker *l, ScatterLock *sl, MutationRef& m) :
    LockerContext(l), lock(sl), mut(m) {}
  void finish(int r) { 
    locker->do_scatter_writebehind(lock, mut); 
  }
};

class C_Locker_ScatterWBFinish : public LockerContext {
  ScatterLock *lock;
  MutationRef mut;
public:
  C_Locker_ScatterWBFinish(Locker *l, ScatterLock *sl, MutationRef& m) :
    LockerContext(l), lock(sl), mut(m) {}
  void finish(int r) { 
    locker->scatter_writebehind_finish(lock, mut); 
  }
};

void Locker::scatter_writebehind(ScatterLock *lock)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind  on " << *lock << " on " << *in << dendl;

  // journal
  MutationRef mut(new MutationImpl);
  // mut->ls = mds->mdlog->get_current_segment();

  // forcefully take a wrlock
  lock->get_wrlock(true);
  mut->wrlocks.insert(lock);
  mut->locks.insert(lock);
  mut->pin(in);

  lock->start_flush();

  // async
  mds->finisher->queue(new C_Locker_ScatterWritebehind(this, lock, mut));
}

void Locker::do_scatter_writebehind(ScatterLock *lock, MutationRef& mut)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());

  mdcache->lock_objects_for_update(mut.get(), in, false);
  if (!lock->is_flushing()) {
    mut->unlock_all_objects();
    drop_locks(mut.get());
    mut->cleanup();
    return;
  }

  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();

  in->finish_scatter_gather_update(lock->get_type(), mut);

  mut->add_projected_inode(in, true);

  //  EUpdate *le = new EUpdate(mds->mdlog, "scatter_writebehind");
  //  mds->mdlog->start_entry(le);

  mdcache->predirty_journal_parents(mut.get(), NULL, in, 0, PREDIRTY_PRIMARY);
  //  mdcache->journal_dirty_inode(mut.get(), &le->metablob, in);

  in->finish_scatter_gather_update_accounted(lock->get_type(), mut, NULL);

  //  mds->mdlog->submit_entry(le, new C_Locker_ScatterWB(this, lock, mut));
  mdcache->start_log_entry();
  // use finisher to simulate log flush
  mds->finisher->queue(new C_Locker_ScatterWBFinish(this, lock, mut));
  mdcache->submit_log_entry();
  
  mut->unlock_all_objects();

  mut->start_committing();
}

void Locker::scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut)
{
  mut->wait_committing();

  CInode *diri = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind_finish on " << *lock << " on " << *diri << dendl;

  mdcache->lock_objects_for_update(mut.get(), diri, true);

  for (;;) {
    CObject *o = mut->pop_early_projected_node();
    if (o == diri)
      break;
    CInode *child = NULL;
    CDir *dir = NULL;
    if ((child = dynamic_cast<CInode*>(o))) {
      child->mutex_lock();
      assert(child->get_parent_dn()->get_dir_inode() == diri);
    } else if ((dir = dynamic_cast<CDir*>(o))) {
      assert(dir->get_inode() == diri);
    }  else {
      assert(0);
    }
    if (child) {
      child->pop_and_dirty_projected_inode(mut->ls);
      child->mutex_unlock();
    } else if (dir) {
      dir->pop_and_dirty_projected_fnode(mut->ls);
    }
  }
  
  diri->pop_and_dirty_projected_inode(mut->ls);

  lock->finish_flush();

  mut->early_apply();
  mut->apply();

  drop_locks(mut.get());

  mut->cleanup();

  /*
  if (lock->is_stable())
    lock->finish_waiting(ScatterLock::WAIT_STABLE);
  scatter_eval_gather(lock);
  */
}

/* caller hold mutex */
void Locker::scatter_eval(ScatterLock *lock, bool *need_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();

  dout(10) << "scatter_eval " << *lock << " on " << *lock->get_parent() << dendl;

  assert(lock->is_stable());

  /*
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
  */

  if (lock->get_type() == CEPH_LOCK_INEST) {
    // in general, we want to keep INEST writable at all times.
    if (!lock->is_rdlocked()) {
      if (lock->get_state() != LOCK_LOCK)
	simple_lock(lock, need_issue);
    }
    return;
  }

  CInode *in = static_cast<CInode*>(lock->get_parent());
  if (in->is_base()) {
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
  lock->get_parent()->mutex_assert_locked_by_me();
  lock->mark_dirty();
  Mutex::Locker l(updated_scatterlocks_mutex);
  if (lock->get_updated_item()->is_on_list()) {
    dout(10) << "mark_updated_scatterlock " << *lock
	     << " - already on list since " << lock->get_update_stamp() << dendl;
  } else {
    lock->get_parent()->get(CObject::PIN_DIRTYSCATTERED);
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

      // adjust lock state
      dout(10) << "scatter_nudge auth, scatter/unscattering " << *lock << " on " << *p << dendl;
      switch (lock->get_type()) {
	case CEPH_LOCK_IFILE:
	  if (lock->get_state() != LOCK_LOCK)
	    simple_lock(static_cast<ScatterLock*>(lock));
	  else
	    simple_sync(static_cast<ScatterLock*>(lock));
	  break;

	case CEPH_LOCK_IDFT:
	case CEPH_LOCK_INEST:
	  if (lock->get_state() != LOCK_LOCK)
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
}

void Locker::scatter_tick()
{
  dout(10) << "scatter_tick" << dendl;
  
  // updated
  utime_t now = ceph_clock_now(g_ceph_context);

  updated_scatterlocks_mutex.Lock();
  int n = updated_scatterlocks.size();
  while (!updated_scatterlocks.empty()) {
    ScatterLock *lock = updated_scatterlocks.front();

    if (n-- == 0)
      break;  // scatter_nudge() may requeue; avoid looping

    updated_scatterlocks.pop_front();

    updated_scatterlocks_mutex.Unlock();

    bool stop = false;
    lock->get_parent()->mutex_lock();

    if (lock->is_dirty()) {
      if (now - lock->get_update_stamp() < g_conf->mds_scatter_nudge_interval)
	stop = true;
      scatter_nudge(lock, 0);
    }

    lock->get_parent()->mutex_unlock();

    lock->get_parent()->put(CObject::PIN_DIRTYSCATTERED);

    updated_scatterlocks_mutex.Lock();
    if (stop)
      break;
  }
  updated_scatterlocks_mutex.Unlock();
  //mds->mdlog->flush();
}

void Locker::scatter_tempsync(ScatterLock *lock, bool *need_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(10) << "scatter_tempsync " << *lock
	   << " on " << *lock->get_parent() << dendl;
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

  if (gather) {
  } else {
    // do tempsync
    lock->set_state(LOCK_TSYN);
    lock->finish_waiting(ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
    if (lock->get_cap_shift()) {
      if (need_issue)
	*need_issue = true;
      else
	issue_caps(in);
    }
  }
}

void Locker::scatter_mix(ScatterLock *lock, bool *need_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "scatter_mix " << *lock << " on " << *lock->get_parent() << dendl;
  assert(lock->is_stable());

  CInode *in = static_cast<CInode*>(lock->get_parent());

  if (lock->get_state() == LOCK_LOCK) {
    in->start_scatter(lock);

    // change lock
    lock->set_state(LOCK_MIX);
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

    if (!gather) {
      in->start_scatter(lock);
      lock->set_state(LOCK_MIX);
      if (lock->get_cap_shift()) {
	if (need_issue)
	  *need_issue = true;
	else
	  issue_caps(in);
      }
    }
  }
}


// ==========================================================================
// local lock

void Locker::local_wrlock_grab(LocalLock *lock, MutationImpl *mut)
{
  assert(mut->is_object_locked(lock->get_parent()));
  dout(7) << "local_wrlock_grab  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  assert(lock->can_wrlock());
  assert(!mut->wrlocks.count(lock));
  lock->get_wrlock(mut->get_client());
  mut->wrlocks.insert(lock);
  mut->locks.insert(lock);
}

bool Locker::local_wrlock_start(LocalLock *lock, MDRequestRef& mut)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "local_wrlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  if (lock->can_wrlock()) {
    assert(!mut->wrlocks.count(lock));
    lock->get_wrlock(mut->get_client());
    mut->wrlocks.insert(lock);
    mut->locks.insert(lock);
    return true;
  } else {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mds, mut));
    return false;
  }
}

void Locker::local_wrlock_finish(LocalLock *lock, MutationImpl *mut)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "local_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_wrlock();
  mut->wrlocks.erase(lock);
  mut->locks.erase(lock);
  if (lock->get_num_wrlocks() == 0) {
    lock->finish_waiting(SimpleLock::WAIT_STABLE |
                         SimpleLock::WAIT_WR |
                         SimpleLock::WAIT_RD);
  }
}

bool Locker::local_xlock_start(LocalLock *lock, MDRequestRef& mut)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "local_xlock_start  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  
  if (!lock->can_xlock_local()) {
    lock->add_waiter(SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mds, mut));
    return false;
  }

  lock->get_xlock(mut, mut->get_client());
  mut->xlocks.insert(lock);
  mut->locks.insert(lock);
  return true;
}

void Locker::local_xlock_finish(LocalLock *lock, MutationImpl *mut)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "local_xlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_xlock();
  mut->xlocks.erase(lock);
  mut->locks.erase(lock);

  lock->finish_waiting(SimpleLock::WAIT_STABLE | 
		       SimpleLock::WAIT_WR | 
		       SimpleLock::WAIT_RD);
}

// ==========================================================================
// file lock

void Locker::file_eval(ScatterLock *lock, bool *need_issue)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  in->mutex_assert_locked_by_me();

  int loner_wanted, other_wanted;
  int wanted = in->get_caps_wanted(&loner_wanted, &other_wanted, CEPH_CAP_SFILE);
  dout(7) << "file_eval wanted=" << gcap_string(wanted)
	  << " loner_wanted=" << gcap_string(loner_wanted)
	  << " other_wanted=" << gcap_string(other_wanted)
	  << "  filelock=" << *lock << " on " << *lock->get_parent()
	  << dendl;

  assert(lock->is_stable());

  /*
  if (lock->get_parent()->is_freezing_or_frozen())
    return;

  if (mdcache->is_readonly()) {
    if (lock->get_state() != LOCK_SYNC) {
      dout(10) << "file_eval read-only FS, syncing " << *lock << " on " << *lock->get_parent() << dendl;
      simple_sync(lock, need_issue);
    }
    return;
  }
  */

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
	 (other_wanted & (CEPH_CAP_GEXCL|CEPH_CAP_GWR|CEPH_CAP_GRD)) 
//	(in->inode.is_dir() && in->multiple_nonstale_caps()))
	    ) {  // FIXME.. :/
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
	  lock->is_waiting_for(SimpleLock::WAIT_WR))
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
	   ((wanted & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER)) ||
	   in->get_target_loner() >= 0)) {
    dout(7) << "file_eval stable, bump to loner " << *lock
	    << " on " << *lock->get_parent() << dendl;
    file_excl(lock, need_issue);
  }

  // * -> mixed?
  else if (lock->get_state() != LOCK_MIX &&
	   !lock->is_rdlocked() &&
	   (in->get_wanted_loner() < 0 && (wanted & CEPH_CAP_GWR))) {
    dout(7) << "file_eval stable, bump to mixed " << *lock
	    << " on " << *lock->get_parent() << dendl;
    scatter_mix(lock, need_issue);
  }
  
  // * -> sync?
  else if (lock->get_state() != LOCK_SYNC &&
	   !lock->is_wrlocked() &&   // drain wrlocks first!
	   !lock->is_waiting_for(SimpleLock::WAIT_WR) &&
	   !(wanted & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER))) {
    dout(7) << "file_eval stable, bump to sync " << *lock 
	    << " on " << *lock->get_parent() << dendl;
    simple_sync(lock, need_issue);
  }
}

void Locker::file_excl(ScatterLock *lock, bool *need_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();

  CInode *in = static_cast<CInode*>(lock->get_parent());
  dout(7) << "file_excl " << *lock << " on " << *lock->get_parent() << dendl;  

  assert(lock->is_stable());

  assert(in->get_loner() >= 0 ||
	 lock->get_state() == LOCK_XSYN);  // must do xsyn -> excl -> <anything else>
  
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
  
  if (!gather) {
    lock->set_state(LOCK_EXCL);
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
  }
}

void Locker::file_xsyn(SimpleLock *lock, bool *need_issue)
{
  lock->get_parent()->mutex_assert_locked_by_me();

  dout(7) << "file_xsyn on " << *lock << " on " << *lock->get_parent() << dendl;
  CInode *in = static_cast<CInode *>(lock->get_parent());
  assert(in->get_loner() >= 0);

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

  if (!gather) {
    lock->set_state(LOCK_XSYN);
    lock->finish_waiting(SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
  }
}

void Locker::issue_caps_set(set<CInodeRef>& inset, bool parents_locked)
{ 
  for (set<CInodeRef>::iterator p = inset.begin(); p != inset.end(); ++p) {
    CObject::Locker l(parents_locked ? NULL : p->get());
    issue_caps(p->get());
  }
}

bool Locker::issue_caps(CInode *in, Capability *only_cap)
{
  in->mutex_assert_locked_by_me();
  return true;
}

void Locker::revoke_client_leases(SimpleLock *lock)
{

}
