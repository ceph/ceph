#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"
#include "Mutation.h"
#include "MDLog.h"
#include "SessionMap.h"

#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"
#include "messages/MClientReply.h"

#include "events/EUpdate.h"
#include "events/EOpen.h"

#define dout_subsys ceph_subsys_mds
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".locker "

Locker::Locker(MDSRank *_mds) :
  mds(_mds), mdcache(_mds->mdcache), server(_mds->server),
  updated_scatterlocks_mutex("Locker::updated_scatterlocks_mutex")
{

}

class LockerContext : public MDSContextBase {
protected:
  Locker *locker;
  MDSRank *get_mds() { return locker->mds; }
public:
  explicit LockerContext(Locker *l) : locker(l) {
    assert(locker != NULL);
  }
};

class LockerLogContext : public MDSLogContextBase {
protected:
  Locker *locker;
  MDSRank *get_mds() { return locker->mds; }
public:
  explicit LockerLogContext(Locker *l) : locker(l) {
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

void Locker::finish_waiting(SimpleLock *lock, uint64_t mask)
{
  list<MDSContextBase*> ls;
  lock->take_waiting(mask, ls);
  for (auto p = ls.begin(); p != ls.end(); ) {
    if ((*p)->is_async()) {
      ++p;
    } else {
      mds->queue_context(*p);
      ls.erase(p++);
    }
  }
  finish_contexts(g_ceph_context, ls, 0);
}

void Locker::finish_waiting(CInode *in, uint64_t mask)
{
  list<MDSContextBase*> ls;
  in->take_waiting(mask, ls);
  for (auto p = ls.begin(); p != ls.end(); ) {
    if ((*p)->is_async()) {
      ++p;
    } else {
      mds->queue_context(*p);
      ls.erase(p++);
    }
  }
  finish_contexts(g_ceph_context, ls, 0);
}

/* If this function returns false, the mdr has been placed
 * on the appropriate wait list */
int Locker::acquire_locks(const MDRequestRef& mdr,
			  set<SimpleLock*> &rdlocks,
			  set<SimpleLock*> &wrlocks,
			  set<SimpleLock*> &xlocks)
{
  if (mdr->locking_done) {
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
  for (set<SimpleLock*>::iterator p = rdlocks.begin(); p != rdlocks.end(); ++p) {
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
	xlock_finish(stray, mdr, &need_issue);
      } else if (mdr->wrlocks.count(stray)) {
	wrlock_finish(stray, mdr, &need_issue);
      } else if (mdr->rdlocks.count(stray)) {
	rdlock_finish(stray, mdr, &need_issue);
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
      cancel_locking(mdr, &issue_set);
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
      xlock_finish(stray, mdr, &need_issue);
    } else if (mdr->wrlocks.count(stray)) {
      wrlock_finish(stray, mdr, &need_issue);
    } else if (mdr->rdlocks.count(stray)) {
      rdlock_finish(stray, mdr, &need_issue);
    } else {
      assert(0);
    }
    if (need_issue)
      issue_set.insert(static_cast<CInode*>(object.get()));
  }

  if (verify_path) {
    result = -EAGAIN;
  } else {
    mdr->locking_done = true;
    mdr->set_mds_stamp(ceph_clock_now(NULL));
    result = 1;
  }
out:
  issue_caps_set(issue_set);
  return result;
}

void Locker::set_xlocks_done(const MutationRef& mut, bool skip_dentry)
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

void Locker::_drop_rdlocks(const MutationRef& mut, set<CInodeRef> *pneed_issue)
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

void Locker::_drop_non_rdlocks(const MutationRef& mut, set<CInodeRef> *pneed_issue,
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


void Locker::drop_locks(const MutationRef& mut)
{
  // leftover locks
  set<CInodeRef> need_issue;

  if (mut->locking)
    cancel_locking(mut, &need_issue);
  _drop_non_rdlocks(mut, &need_issue);
  _drop_rdlocks(mut, &need_issue);

  issue_caps_set(need_issue);
  mut->locking_done = false;
}

void Locker::drop_non_rdlocks(const MutationRef& mut, set<CObject*>& objs)
{
  set<CInodeRef> need_issue;

  _drop_non_rdlocks(mut, &need_issue, &objs);

  issue_caps_set(need_issue, true);
}

void Locker::drop_rdlocks(const MutationRef& mut)
{
  set<CInodeRef> need_issue;

  _drop_rdlocks(mut, &need_issue);

  issue_caps_set(need_issue);
}

// generics
void Locker::eval_gather(SimpleLock *lock, bool first, bool *pneed_issue, uint64_t *p_finish_mask)
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

#define IS_TRUE_AND_LT_AUTH(x) (x && x <= AUTH)
  if ((IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_rdlock) || !lock->is_rdlocked()) &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_wrlock) || !lock->is_wrlocked()) &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_xlock) || !lock->is_xlocked()) &&
      (IS_TRUE_AND_LT_AUTH(lock->get_sm()->states[next].can_lease) || !lock->is_leased()) &&
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

    if (p_finish_mask)
      *p_finish_mask |= (SimpleLock::WAIT_ALL << lock->get_wait_shift());
    else
      finish_waiting(lock, SimpleLock::WAIT_ALL);

    if (caps && in->is_head())
      need_issue = true;

    if (lock->is_stable())
      try_eval(lock, &need_issue);
  }

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
  uint64_t finish_mask = 0;
  
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
    eval_any(&in->filelock, &need_issue, &finish_mask);
  if (mask & CEPH_LOCK_IAUTH)
    eval_any(&in->authlock, &need_issue, &finish_mask);
  if (mask & CEPH_LOCK_ILINK)
    eval_any(&in->linklock, &need_issue, &finish_mask);
  if (mask & CEPH_LOCK_IXATTR)
    eval_any(&in->xattrlock, &need_issue, &finish_mask);
  if (mask & CEPH_LOCK_INEST)
    eval_any(&in->nestlock, &need_issue, &finish_mask);
  if (mask & CEPH_LOCK_IFLOCK)
    eval_any(&in->flocklock, &need_issue, &finish_mask);
  if (mask & CEPH_LOCK_IPOLICY)
    eval_any(&in->policylock, &need_issue, &finish_mask);

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

  if (finish_mask)
    finish_waiting(in, finish_mask);

  if (need_issue && in->is_head())
    issue_caps(in);

  dout(10) << "eval done" << dendl;
  return need_issue;
}

void Locker::eval_any(SimpleLock *lock, bool *need_issue, uint64_t *p_finish_mask) {
  if (!lock->is_stable())
    eval_gather(lock, false, need_issue, p_finish_mask);
  else if (lock->get_parent()->is_auth())
    eval(lock, need_issue);
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
  uint64_t finish_mask = 0;

  // kick locks now
  if (!in->filelock.is_stable())
    eval_gather(&in->filelock, false, &need_issue, &finish_mask);
  if (!in->authlock.is_stable())
    eval_gather(&in->authlock, false, &need_issue, &finish_mask);
  if (!in->linklock.is_stable())
    eval_gather(&in->linklock, false, &need_issue, &finish_mask);
  if (!in->xattrlock.is_stable())
    eval_gather(&in->xattrlock, false, &need_issue, &finish_mask);

  if (need_issue && in->is_head()) {
    if (issue_set)
      issue_set->insert(in);
    else
      issue_caps(in);
  }

  finish_waiting(in, finish_mask);
}

void Locker::eval_scatter_gathers(CInode *in)
{
  bool need_issue = false;
  uint64_t finish_mask = 0;

  dout(10) << "eval_scatter_gathers " << *in << dendl;

  // kick locks now
  if (!in->filelock.is_stable())
    eval_gather(&in->filelock, false, &need_issue, &finish_mask);
  if (!in->nestlock.is_stable())
    eval_gather(&in->nestlock, false, &need_issue, &finish_mask);
  if (!in->dirfragtreelock.is_stable())
    eval_gather(&in->dirfragtreelock, false, &need_issue, &finish_mask);
  
  if (need_issue && in->is_head())
    issue_caps(in);
  
  finish_waiting(in, finish_mask);
}

void Locker::eval(SimpleLock *lock, bool *need_issue)
{
  if (lock->has_stable_waiter()) {
    return;
  }

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

bool Locker::rdlock_start(SimpleLock *lock, const MDRequestRef& mut)
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
      finish_locking(lock, mut);
      return true;
    }

    if (!rdlock_kick(lock))
      break;
  }

  // wait!
  dout(7) << "rdlock_start waiting on " << *lock << " on " << *lock->get_parent() << dendl;
  start_locking(lock, false, mut);
  lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mds, mut));
  nudge_log(lock);
  return false;
}

void Locker::nudge_log(SimpleLock *lock)
{
  dout(10) << "nudge_log " << *lock << " on " << *lock->get_parent() << dendl;
  if (lock->is_unstable_and_locked())    // as with xlockdone, or cap flush
    mds->mdlog->flush();
}

void Locker::rdlock_finish(SimpleLock *lock, const MutationRef& mut, bool *pneed_issue)
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

void Locker::wrlock_force(SimpleLock *lock, const MutationRef& mut)
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

bool Locker::wrlock_start(SimpleLock *lock, const MutationRef& mut)
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

bool Locker::wrlock_start(SimpleLock *lock, const MDRequestRef& mut)
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
      finish_locking(lock, mut);
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
  start_locking(lock, false, mut);
  lock->add_waiter(SimpleLock::WAIT_STABLE, new C_MDS_RetryRequest(mds, mut));
  nudge_log(lock);
    
  return false;
}

void Locker::wrlock_finish(SimpleLock *lock, const MutationRef& mut, bool *pneed_issue, bool parent_locked)
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

bool Locker::xlock_start(SimpleLock *lock, const MDRequestRef& mut)
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
      finish_locking(lock, mut);
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
      simple_xlock(lock);
    } else {
      simple_lock(lock);
    }
  }

  start_locking(lock, true, mut);
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
      finish_waiting(lock, SimpleLock::WAIT_ALL);
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

void Locker::xlock_finish(SimpleLock *lock, const MutationRef& mut, bool *pneed_issue, bool parent_locked)
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

void Locker::start_locking(SimpleLock *lock, bool xlock, const MDRequestRef& mut)
{
  if (mut->locking != lock) {
    mut->start_locking(lock, xlock);
    lock->inc_stable_waiter();
  }
}

void Locker::finish_locking(SimpleLock *lock, const MutationRef& mut)
{
  if (mut->locking == lock) {
    lock->dec_stable_waiter();
    mut->finish_locking(lock);
  }
}

void Locker::cancel_locking(const MutationRef& mut, set<CInodeRef> *pneed_issue)
{
  SimpleLock *lock = mut->locking;
  assert(lock);
  dout(10) << "cancel_locking " << *lock << " on " << *mut << dendl;

  CObjectRef p = lock->get_parent();
  CObject::Locker l(p.get());

  finish_locking(lock, mut);

  bool need_issue = false;
  if (mut->locking_xlock) {
    if (lock->get_state() == LOCK_PREXLOCK) {
      _finish_xlock(lock, -1, &need_issue);
    } else if (lock->get_state() == LOCK_LOCK_XLOCK &&
	lock->get_num_xlocks() == 0) {
      lock->set_state(LOCK_XLOCKDONE);
      eval_gather(lock, true, &need_issue);
    }
  } else {
    if (lock->is_stable() && !lock->has_stable_waiter())
      try_eval(lock, &need_issue);
  }

  if (need_issue)
    pneed_issue->insert(static_cast<CInode*>(p.get()));
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
  finish_waiting(lock, SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
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
    finish_waiting(lock, SimpleLock::WAIT_WR|SimpleLock::WAIT_STABLE);
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
    revoke_client_leases(lock);
    if (lock->is_leased())
      gather++;
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
    finish_waiting(lock, ScatterLock::WAIT_STABLE|ScatterLock::WAIT_XLOCK|ScatterLock::WAIT_WR);
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
  C_Locker_ScatterWritebehind(Locker *l, ScatterLock *sl, const MutationRef& m) :
    LockerContext(l), lock(sl), mut(m) {}
  void finish(int r) { 
    locker->do_scatter_writebehind(lock, mut, true); 
  }
};

class C_Locker_ScatterWBFinish : public LockerLogContext {
  ScatterLock *lock;
  MutationRef mut;
public:
  C_Locker_ScatterWBFinish(Locker *l, ScatterLock *sl, const MutationRef& m) :
    LockerLogContext(l), lock(sl), mut(m) {}
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

  do_scatter_writebehind(lock, mut, false);
}

void Locker::do_scatter_writebehind(ScatterLock *lock, const MutationRef& mut, bool async)
{
  CInode *in = static_cast<CInode*>(lock->get_parent());
  
  CDentry *parent_dn = NULL;
  if (async) {
    mdcache->lock_objects_for_update(mut, in, false);
    if (!lock->is_flushing()) {
      mut->unlock_all_objects();
      drop_locks(mut);
      mut->cleanup();
      return;
    }
    parent_dn = in->get_projected_parent_dn();
  } else {
    if (!in->is_base()) {
      parent_dn = in->get_projected_parent_dn();
      if (!parent_dn->get_dir_inode()->mutex_trylock()) {
	// async
	mds->queue_context(new C_Locker_ScatterWritebehind(this, lock, mut));
	return;
      }
      mut->add_locked_object(parent_dn->get_dir_inode());
    }
    mut->add_locked_object(in);
  }

  inode_t *pi = in->project_inode();
  pi->version = in->pre_dirty();

  in->finish_scatter_gather_update(lock->get_type(), mut);

  mut->add_projected_inode(in, true);

  EUpdate *le = new EUpdate(NULL, "scatter_writebehind");

  mdcache->predirty_journal_parents(mut, &le->metablob, in, NULL, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mut, &le->metablob, in);

  in->finish_scatter_gather_update_accounted(lock->get_type(), mut, &le->metablob);

  mds->mdlog->start_entry(le);
  mut->ls = mds->mdlog->get_current_segment();
  mds->mdlog->submit_entry(le, new C_Locker_ScatterWBFinish(this, lock, mut), true);
  
  if (!async) {
    mut->remove_locked_object(in);
    if (parent_dn)
      mut->unlock_object(parent_dn->get_dir_inode());
  }
  mut->unlock_all_objects();

  mut->start_committing();
}

void Locker::scatter_writebehind_finish(ScatterLock *lock, const MutationRef& mut)
{
  mut->wait_committing();

  CInode *diri = static_cast<CInode*>(lock->get_parent());
  dout(10) << "scatter_writebehind_finish on " << *lock << " on " << *diri << dendl;

  mdcache->lock_objects_for_update(mut, diri, true);

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

  drop_locks(mut);

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
void Locker::mark_updated_scatterlock(ScatterLock *lock, LogSegment *ls)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  lock->mark_dirty(ls);
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
void Locker::scatter_nudge(ScatterLock *lock, MDSContextBase *c, bool forcelockchange)
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
    finish_waiting(lock, ScatterLock::WAIT_RD|ScatterLock::WAIT_STABLE);
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
      if (lock->is_leased())
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

void Locker::local_wrlock_grab(LocalLock *lock, const MutationRef& mut)
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

bool Locker::local_wrlock_start(LocalLock *lock, const MDRequestRef& mut)
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

void Locker::local_wrlock_finish(LocalLock *lock, const MutationRef& mut)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "local_wrlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_wrlock();
  mut->wrlocks.erase(lock);
  mut->locks.erase(lock);
  if (lock->get_num_wrlocks() == 0) {
    finish_waiting(lock, SimpleLock::WAIT_ALL);
  }
}

bool Locker::local_xlock_start(LocalLock *lock, const MDRequestRef& mut)
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

void Locker::local_xlock_finish(LocalLock *lock, const MutationRef& mut)
{
  lock->get_parent()->mutex_assert_locked_by_me();
  dout(7) << "local_xlock_finish  on " << *lock
	  << " on " << *lock->get_parent() << dendl;  
  lock->put_xlock();
  mut->xlocks.erase(lock);
  mut->locks.erase(lock);

  finish_waiting(lock, SimpleLock::WAIT_ALL);
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
    if (lock->is_leased())
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
    finish_waiting(lock, SimpleLock::WAIT_RD|SimpleLock::WAIT_STABLE);
    if (need_issue)
      *need_issue = true;
    else
      issue_caps(in);
  }
}

Capability* Locker::issue_new_caps(CInode *in, int mode, Session *session, bool is_replay)
{
  in->mutex_assert_locked_by_me();
  dout(7) << "issue_new_caps for mode " << mode << " on " << *in << dendl;
  bool is_new;

  // if replay, try to reconnect cap, and otherwise do nothing.
  /*
     if (is_replay) {
     mds->mdcache->try_reconnect_cap(in, session);
     return 0;
     }
     */

  // my needs
  assert(session->info.inst.name.is_client());
  int my_want = ceph_caps_for_mode(mode);

  // register a capability
  Capability *cap = in->get_client_cap(session->get_client());
  if (!cap) {
    // new cap
    cap = in->add_client_cap(session);
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

  // [auth] twiddle mode?
  eval(in, CEPH_CAP_LOCKS);

  /*
  if (!in->filelock.is_stable() ||
      !in->authlock.is_stable() ||
      !in->linklock.is_stable() ||
      !in->xattrlock.is_stable())
    mds->mdlog->flush();
    */

  if (is_new)
    cap->dec_suppress();

  return cap;
}


bool Locker::issue_caps(CInode *in, client_t only_client)
{
  in->mutex_assert_locked_by_me();
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
  map<client_t, Capability*>::const_iterator it;
  if (only_client != -1)
    it = in->get_client_caps().find(only_client);
  else
    it = in->get_client_caps().begin();
  for (; it != in->get_client_caps().end(); ++it) {
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

    /*
    Session *session = mds->get_session(it->first);
    if (in->inode.inline_data.version != CEPH_INLINE_NONE &&
        !(session && session->connection &&
          session->connection->has_feature(CEPH_FEATURE_MDS_INLINE_DATA)))
      allowed &= ~(CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR);
      */

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
    if (in->get_inode()->nlink == 0)
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
	/*
        if (op == CEPH_CAP_OP_REVOKE) {
                revoking_caps.push_back(&cap->item_revoking_caps);
                revoking_caps_by_client[cap->get_client()].push_back(&cap->item_client_revoking_caps);
                cap->set_last_revoke_stamp(ceph_clock_now(g_ceph_context));
                cap->reset_num_revoke_warnings();
        }
	*/

        MClientCaps *m = new MClientCaps(op, in->ino(), MDS_INO_ROOT,
                                         cap->get_cap_id(), cap->get_last_seq(),
                                         after, wanted, 0,
                                         cap->get_mseq(), 0);
        in->encode_cap_message(m, cap);

        mds->send_message_client_counted(m, cap->get_session());
      }
    }

    if (only_client != -1)
      break;
  }

  return (nissued == 0);  // true if no re-issued, no callbacks
}

void Locker::issue_caps_set(set<CInodeRef>& inset, bool parents_locked)
{ 
  for (set<CInodeRef>::iterator p = inset.begin(); p != inset.end(); ++p) {
    CObject::Locker l(parents_locked ? NULL : p->get());
    issue_caps(p->get());
  }
}

void Locker::issue_truncate(CInode *in)
{
  in->mutex_assert_locked_by_me();
  dout(7) << "issue_truncate on " << *in << dendl;

  for (auto it = in->get_client_caps().begin();
       it != in->get_client_caps().end();
       ++it) {
    Capability *cap = it->second;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_TRUNC, in->ino(),
				     CEPH_INO_ROOT,
                                     cap->get_cap_id(), cap->get_last_seq(),
                                     cap->pending(), cap->wanted(), 0,
                                     cap->get_mseq(), 0);
    in->encode_cap_message(m, cap);
    mds->send_message_client_counted(m, cap->get_session());
  }

  // should we increase max_size?
  if (in->is_file())
    check_inode_max_size(in);
}

void Locker::revoke_stale_caps(Session *session, uint64_t sseq)
{
  session->mutex_assert_locked_by_me();
  dout(10) << "revoke_stale_caps for " << session->info.inst.name << dendl;
  client_t client = session->get_client();

  vector<CInodeRef> revoke_set;
  for (auto p = session->caps.begin(); !p.end(); ++p) {
    if (session->get_state_seq() != sseq)
      break;
    revoke_set.push_back((*p)->get_inode());
  }
  session->mutex_unlock();

  if (session->get_state_seq() != sseq)
    return;

  for (CInodeRef& in : revoke_set) {
    CObject::Locker l(in.get());

    if (session->get_state_seq() != sseq)
      break;

    Capability *cap = in->get_client_cap(client);
    int issued = 0;
    if (cap && !cap->is_stale()) {
      cap->mark_stale();
      issued = cap->issued();
    }

    if (issued & ~CEPH_CAP_PIN) {
      dout(10) << " revoking " << ccap_string(issued) << " on " << *in << dendl;
      cap->revoke();

      /* FIXME
      if (in->inode.client_ranges.count(client))
	in->state_set(CInode::STATE_NEEDSRECOVER);
	*/

      if (!in->filelock.is_stable()) eval_gather(&in->filelock);
      if (!in->linklock.is_stable()) eval_gather(&in->linklock);
      if (!in->authlock.is_stable()) eval_gather(&in->authlock);
      if (!in->xattrlock.is_stable()) eval_gather(&in->xattrlock);

      try_eval(in.get(), CEPH_CAP_LOCKS);
    } else {
      dout(10) << " nothing issued on " << *in << dendl;
    }
  }
  session->mutex_lock();
}

void Locker::resume_stale_caps(Session *session, uint64_t sseq)
{
  session->mutex_assert_locked_by_me();
  dout(10) << "resume_stale_caps for " << session->info.inst.name << dendl;
  client_t client = session->get_client();

  vector<CInodeRef> issue_set;
  for (auto p = session->caps.begin(); !p.end(); ++p) {
    if (session->get_state_seq() != sseq)
      break;
    issue_set.push_back((*p)->get_inode());
  }
  session->mutex_unlock();

  if (session->get_state_seq() != sseq)
    return;

  for (CInodeRef& in : issue_set) {
    CObject::Locker l(in.get());

    if (session->get_state_seq() != sseq)
      break;

    Capability *cap = in->get_client_cap(client);
    if (cap && cap->is_stale()) {
      dout(10) << " clearing stale flag on " << *in << dendl;
      cap->clear_stale();
      if (!eval(in.get(), CEPH_CAP_LOCKS))
	issue_caps(in.get(), client);
    }
  }
  session->mutex_lock();
}

class C_Locker_FileUpdateFinish : public LockerLogContext {
  CInode *in;
  MutationRef mut;
  bool share;
  client_t client;
  MClientCaps *ack;
  public:
  C_Locker_FileUpdateFinish(Locker *l, CInode *i, const MutationRef& m,
			    bool s, client_t c=-1, MClientCaps *ac = 0)
    : LockerLogContext(l), in(i), mut(m), share(s), client(c), ack(ac) { }
  void finish(int r) {
    locker->file_update_finish(in, mut, share, client, ack);
  }
};

void Locker::file_update_finish(CInode *in, const MutationRef& mut,
				bool share, client_t client, MClientCaps *ack)
{
  mut->wait_committing();

  dout(10) << "file_update_finish on " << *in << dendl;

  mdcache->lock_objects_for_update(mut, in, true);

  Capability *cap = (client == -1) ? NULL : in->get_client_cap(client);

  if (ack) {
    Session *session = cap ? cap->get_session() : NULL;
    if (session) {
      // "oldest flush tid" > 0 means client uses unique TID for each flush
/*
      if (ack->get_oldest_flush_tid() > 0)
	session->add_completed_flush(ack->get_client_tid());
*/
      mds->send_message_client_counted(ack, session);
    } else {
      dout(10) << " no session for client." << client << " " << *ack << dendl;
      ack->put();
    }
  }

  mut->early_apply();

  set<CObject*> objs;
  objs.insert(in);

  set<CInodeRef> need_issue;
  _drop_non_rdlocks(mut, &need_issue, &objs);

  if (cap && (cap->wanted() & ~cap->pending()) &&
      need_issue.empty()) { // if we won't issue below anyway
    issue_caps(in, client);
  }

  if (share && in->is_auth() &&
      (in->filelock.gcaps_allowed(CAP_LONER) & (CEPH_CAP_GWR|CEPH_CAP_GBUFFER))) {
    share_inode_max_size(in);
  }

  issue_caps_set(need_issue, true);

  mut->apply();

  drop_locks(mut);
  // auth unpin after issuing caps
  mut->cleanup();
}

void Locker::calc_new_client_ranges(CInode *in, uint64_t size,
                                    map<client_t,client_writeable_range_t> *new_ranges,
                                    bool *max_increased)
{ 
  const inode_t *latest = in->get_projected_inode();
  uint64_t ms;
  if(latest->has_layout()) {
    ms = ROUND_UP_TO((size+1)<<1, latest->get_layout_size_increment());
  } else {
    // Layout-less directories like ~mds0/, have zero size
    ms = 0;
  }
  
  // increase ranges as appropriate.
  // shrink to 0 if no WR|BUFFER caps issued.
  for (auto p = in->get_client_caps().begin();
       p != in->get_client_caps().end();
       ++p) {
    if ((p->second->issued() | p->second->wanted()) & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER)) {
      client_writeable_range_t& nr = (*new_ranges)[p->first];
      nr.range.first = 0;
      auto q = latest->client_ranges.find(p->first);
      if (q != latest->client_ranges.end()) {
        const client_writeable_range_t& oldr = q->second;
        if (ms > oldr.range.last)
          *max_increased = true;
        nr.range.last = MAX(ms, oldr.range.last);
        nr.follows = oldr.follows;
      } else {
        nr.range.last = ms;
        nr.follows = 1;
      }
    }
  }
}

class C_MDL_CheckMaxSize : public LockerContext {
  CInode *in;
public:
  C_MDL_CheckMaxSize(Locker *l, CInode *i) : LockerContext(l), in(i) {
    in->get(CInode::PIN_PTRWAITER);
  }
  void finish(int r) {
    CDentryRef parent_dn = in->get_lock_projected_parent_dn();
    in->mutex_lock();

    if (in->state_test(CInode::STATE_CHECKINGMAXSIZE)) {
      in->state_clear(CInode::STATE_CHECKINGMAXSIZE);
      uint64_t new_max_size = in->get_clear_wanted_max_size();
      locker->check_inode_max_size(in, true, false, new_max_size);
    }

    parent_dn->get_dir_inode()->mutex_unlock();
    in->mutex_unlock();
    in->put(CInode::PIN_PTRWAITER);
  }
};

void Locker::schedule_check_max_size(CInode *in, uint64_t new_max_size, bool wait_lock)
{
  in->mutex_assert_locked_by_me();

  in->set_wanted_max_size(new_max_size);
  if (in->state_test(CInode::STATE_CHECKINGMAXSIZE))
    return;

  C_MDL_CheckMaxSize *cms = new C_MDL_CheckMaxSize(this, in);
  if (wait_lock)
    in->filelock.add_waiter(SimpleLock::WAIT_STABLE, cms);
  else
    mds->queue_context(cms);

  in->state_set(CInode::STATE_CHECKINGMAXSIZE);
}

bool Locker::check_inode_max_size(CInode *in, bool parent_locked, bool force_wrlock,
				  uint64_t new_max_size, uint64_t new_size, utime_t new_mtime)
{
  in->mutex_assert_locked_by_me();
  assert(in->is_file());

  const inode_t *latest = in->get_projected_inode();
  map<client_t, client_writeable_range_t> new_ranges;
  uint64_t size = latest->size;
  bool update_size = new_size > 0;
  bool update_max = false;
  bool max_increased = false;

  if (update_size) {
    assert(force_wrlock);
    new_size = size = MAX(size, new_size);
    new_mtime = MAX(new_mtime, latest->mtime);
    if (latest->size == new_size && latest->mtime == new_mtime)
      update_size = false;
  }

  calc_new_client_ranges(in, max(new_max_size, size), &new_ranges, &max_increased);

  if (max_increased || latest->client_ranges != new_ranges)
    update_max = true;

  if (!update_size && !update_max) {
    dout(20) << "check_inode_max_size no-op on " << *in << dendl;
    return false;
  }

  dout(10) << "check_inode_max_size new_ranges " << new_ranges
    << " update_size " << update_size
    << " on " << *in << dendl;

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
      schedule_check_max_size(in, new_max_size, true);
      dout(10) << "check_inode_max_size can't wrlock, waiting on " << *in << dendl;
      return false;
    }
  }

  assert(!in->is_base());
  CDentry *parent_dn = in->get_projected_parent_dn();
  if (!parent_locked && !parent_dn->get_dir_inode()->mutex_trylock()) {
    schedule_check_max_size(in, new_max_size, false);
    dout(10) << "check_inode_max_size can't lock parent dn, do async update " << *in << dendl;
    return false;  
  }

  MutationRef mut(new MutationImpl);
  // mut->ls = mds->mdlog->get_current_segment();

  mut->pin(in);
  mut->add_locked_object(in);
  mut->add_locked_object(parent_dn->get_dir_inode());

  wrlock_force(&in->filelock, mut);  // wrlock for duration of journal

  inode_t *pi = in->project_inode();
  mut->add_projected_inode(in, true);
  pi->version = in->pre_dirty();

  if (update_max) {
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
  if (in->is_any_caps_wanted() && in->last == CEPH_NOSNAP) {
    EOpen *eo = new EOpen(NULL);
    eo->add_ino(in->ino());
    le = eo;
    //mut->ls->open_files.push_back(&in->item_open_file);
  } else {
    EUpdate *eu = new EUpdate(NULL, "check_inode_max_size");
    le = eu;
  }
  if (update_size) {  // FIXME if/when we do max_size nested accounting
    mdcache->predirty_journal_parents(mut, le->get_metablob(), in, NULL, PREDIRTY_PRIMARY);
    le->get_metablob()->add_primary_dentry(parent_dn, in, true);
  } else {
    mdcache->journal_dirty_inode(mut, le->get_metablob(), in);
  }

  mds->mdlog->start_entry(le);
  mut->ls = mds->mdlog->get_current_segment();
  mds->mdlog->submit_entry(le, new C_Locker_FileUpdateFinish(this, in, mut, true), max_increased);

  mut->remove_locked_object(in);
  if (parent_locked)
    mut->remove_locked_object(parent_dn->get_dir_inode());
  mut->unlock_all_objects();

  mut->start_committing();
  return true;
}

void Locker::share_inode_max_size(CInode *in, client_t only_client)
{
  in->mutex_assert_locked_by_me();
  /*
   * only share if currently issued a WR cap.  if client doesn't have it,
   * file_max doesn't matter, and the client will get it if/when they get
   * the cap later.
   */
  dout(10) << "share_inode_max_size on " << *in << dendl;
  map<client_t, Capability*>::const_iterator it;
  if (only_client != -1)
    it = in->get_client_caps().find(only_client);
  else
    it = in->get_client_caps().begin();
  for (; it != in->get_client_caps().end(); ++it) {
    const client_t client = it->first;
    Capability *cap = it->second;
    if (cap->is_suppress())
      continue;
    if (cap->pending() & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER)) {
      dout(10) << "share_inode_max_size with client." << client << dendl;
      cap->inc_last_seq();
      MClientCaps *m = new MClientCaps(CEPH_CAP_OP_GRANT,
                                       in->ino(),
				       CEPH_INO_ROOT,
                                       cap->get_cap_id(), cap->get_last_seq(),
                                       cap->pending(), cap->wanted(), 0,
                                       cap->get_mseq(), 0);
      in->encode_cap_message(m, cap);
      mds->send_message_client_counted(m, cap->get_session());
    }
    if (only_client != -1)
      break;
  }
}

bool Locker::_should_flush_log(CInode *in, int wanted)
{
  /* flush log if caps are wanted by client but corresponding lock is unstable and locked by
   *    * pending mutations. */
  if (((wanted & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR|CEPH_CAP_FILE_SHARED|CEPH_CAP_FILE_EXCL)) &&
       in->filelock.is_unstable_and_locked()) ||
      ((wanted & (CEPH_CAP_AUTH_SHARED|CEPH_CAP_AUTH_EXCL)) &&
       in->authlock.is_unstable_and_locked()) ||
      ((wanted & (CEPH_CAP_LINK_SHARED|CEPH_CAP_LINK_EXCL)) &&
       in->linklock.is_unstable_and_locked()) ||
      ((wanted & (CEPH_CAP_XATTR_SHARED|CEPH_CAP_XATTR_EXCL)) &&
       in->xattrlock.is_unstable_and_locked()))
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

#if 0
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
#endif
}

void Locker::handle_client_caps(MClientCaps *m)
{
  Session *session = mds->get_session(m);
  client_t client = session->get_client();

  snapid_t follows = m->get_snap_follows();
  dout(7) << "handle_client_caps on " << m->get_ino()
          << " tid " << m->get_client_tid() << " follows " << follows
          << " op " << ceph_cap_op_name(m->get_op()) << dendl;

#if 0
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
#endif

  CInodeRef head_in = mdcache->get_inode(m->get_ino());
  if (!head_in) {
    /*
    if (mds->is_clientreplay()) {
      dout(7) << "handle_client_caps on unknown ino " << m->get_ino()
        << ", will try again after replayed client requests" << dendl;
      mdcache->wait_replay_cap_reconnect(m->get_ino(), new C_MDS_RetryMessage(mds, m));
      return;
    }
    */
    dout(1) << "handle_client_caps on unknown ino " << m->get_ino() << ", dropping" << dendl;
    m->put();
    return;
  }

  CInode *in = head_in.get();

  MutationRef mut;
  if (m->get_dirty()) {
    mut.reset(new MutationImpl);
    mdcache->lock_objects_for_update(mut, in, false);
  } else {
    in->mutex_lock();
  }

  Capability *cap = head_in->get_client_cap(client);
  if (!cap) {
    dout(7) << "handle_client_caps no cap for client." << client << " on " << *in << dendl;
    goto out;
  }

  // freezing|frozen?
  if (ceph_seq_cmp(m->get_mseq(), cap->get_mseq()) < 0) {
    dout(7) << "handle_client_caps mseq " << m->get_mseq() << " < " << cap->get_mseq()
            << ", dropping" << dendl;
    goto out;
  }
                                              
  // flushsnap?
  if (m->get_op() == CEPH_CAP_OP_FLUSHSNAP) {
    assert(0);
  } 

  if (cap->get_cap_id() != m->get_cap_id()) {
    dout(7) << " ignoring client capid " << m->get_cap_id() << " != my " << cap->get_cap_id() << dendl;
  } else {
    // intermediate snap inodes
    /*
    while (in != head_in) {
      assert(in->last != CEPH_NOSNAP);
      if (in->is_auth() && m->get_dirty()) {
        dout(10) << " updating intermediate snapped inode " << *in << dendl;
        _do_cap_update(in, NULL, m->get_dirty(), follows, m);
      }
      in = mdcache->pick_inode_snap(head_in, in->last);
    }
    */

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


    if (m->get_dirty() && in->is_auth()) {
      dout(7) << " flush client." << client << " dirty " << ccap_string(m->get_dirty())
              << " seq " << m->get_seq() << " on " << *in << dendl;
      ack = new MClientCaps(CEPH_CAP_OP_FLUSH_ACK, in->ino(), 0, cap->get_cap_id(), m->get_seq(),
                            m->get_caps(), 0, m->get_dirty(), 0, 0);
      ack->set_client_tid(m->get_client_tid());
      ack->set_oldest_flush_tid(m->get_oldest_flush_tid());
    }

    bool need_flush = false;
    // filter wanted based on what we could ever give out (given auth/replica status)
    int new_wanted = m->get_wanted() & head_in->get_caps_allowed_ever();
    if (new_wanted != cap->wanted()) {
      if (new_wanted & ~cap->pending()) {
        // exapnding caps.  make sure we aren't waiting for a log flush
        need_flush = _should_flush_log(in, new_wanted & ~cap->pending());
      }
      adjust_cap_wanted(cap, new_wanted, m->get_issue_seq());
    }

    if (_do_cap_update(in, cap, m, ack, mut, &need_flush)) {
      // updated
      eval(in, CEPH_CAP_LOCKS);
      if (!need_flush && (cap->wanted() & ~cap->pending()))
        need_flush = _should_flush_log(in, cap->wanted() & ~cap->pending());
    } else {
      // no update, ack now.
      if (ack)
        mds->send_message_client_counted(ack, m->get_connection());

      bool did_issue = eval(in, CEPH_CAP_LOCKS);
      if (!did_issue && (cap->wanted() & ~cap->pending()))
        issue_caps(in, cap->get_client());

      if (cap->get_last_seq() == 0 &&
          (cap->pending() & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_BUFFER))) {
        cap->issue_norevoke(cap->issued());
        share_inode_max_size(in, cap->get_client());
      }
    }

    if (need_flush)
      mds->mdlog->flush();
  }

out:
  if (mut) {
    mut->unlock_all_objects();
    mut->start_committing();
  } else {
    in->mutex_unlock();
  }
  m->put();
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

/*
 * update inode based on cap flush|flushsnap|wanted.
 *  adjust max_size, if needed.
 * if we update, return true; otherwise, false (no updated needed).
 */
bool Locker::_do_cap_update(CInode *in, Capability *cap,
                            MClientCaps *m, MClientCaps *ack,
			    MutationRef& mut, bool *need_flush)
{
  client_t client = cap->get_client();
  int dirty = m->get_dirty();
  dout(10) << "_do_cap_update dirty " << ccap_string(dirty)
           << " issued " << ccap_string(cap ? cap->issued() : 0)
           << " wanted " << ccap_string(cap ? cap->wanted() : 0)
           << " on " << *in << dendl;
  const inode_t *latest = in->get_projected_inode();

  // increase or zero max_size?
  uint64_t size = m->get_size();
  bool change_max = false;

  uint64_t old_max = 0;
  {
    auto p = latest->client_ranges.find(client);
    if (p != latest->client_ranges.end())
      old_max = p->second.range.last;
  }
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

    if (in->is_head() && change_max) {
      if(!in->filelock.can_wrlock(client) &&
	 !in->filelock.can_force_wrlock(client)) {
	dout(10) << " i want to change file_max, but lock won't allow it (yet)" << dendl;
	if (in->filelock.is_stable()) {
	  bool need_issue = false;
	  if (cap)
	    cap->inc_suppress();
	  if (in->get_loner() >= 0 || (in->get_wanted_loner() >= 0 && in->try_set_loner())) {
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
	  schedule_check_max_size(in, forced_change_max ? new_max : 0, true);
	  change_max = false;
	}
      }
      if (change_max && !mut) {
	assert(!in->is_base());
	CDentry *parent_dn = in->get_projected_parent_dn();
	if (parent_dn->get_dir_inode()->mutex_trylock()) {
	  mut.reset(new MutationImpl);
	  mut->add_locked_object(in);
	  mut->add_locked_object(parent_dn->get_dir_inode());
	} else {
	  schedule_check_max_size(in, forced_change_max ? new_max : 0, false);
	  dout(10) << " i can't lock parent dn for updating max_size, do async update " << *in << dendl;
	  change_max = false;
	}
      }
    }
  }

  /*
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
  */

  if (!dirty && !change_max)
    return false;

  /*
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  if (session->check_access(in, MAY_WRITE,
                            m->caller_uid, m->caller_gid, 0, 0) < 0) {
    session->put();
    dout(10) << "check_access failed, dropping cap update on " << *in << dendl;
    return false;
  }
  seossion->put();
  */

  // do the update.
  assert(mut);

  EUpdate *le = new EUpdate(NULL, "cap_update");

  // xattrs update?
  bool update_xattr = false;
  xattr_map_t *px = NULL;
  if ((dirty & CEPH_CAP_XATTR_EXCL) &&
      m->xattrbl.length() &&
      m->head.xattr_version > in->get_projected_inode()->xattr_version) {
    update_xattr = true;
  }
  inode_t *pi = in->project_inode(update_xattr ? &px : NULL);
  mut->add_projected_inode(in, true);
  pi->version = in->pre_dirty();

  _update_cap_fields(in, dirty, m, pi);

  if (change_max) {
    dout(7) << "  max_size " << old_max << " -> " << new_max
            << " for " << *in << dendl;
    if (new_max) {
      pi->client_ranges[client].range.first = 0;
      pi->client_ranges[client].range.last = new_max;
      pi->client_ranges[client].follows = 1;
    } else
      pi->client_ranges.erase(client);
  }

  if (change_max || (dirty & (CEPH_CAP_FILE_EXCL|CEPH_CAP_FILE_WR)))
    wrlock_force(&in->filelock, mut);  // wrlock for duration of journal

  // auth
  if (dirty & CEPH_CAP_AUTH_EXCL)
    wrlock_force(&in->authlock, mut);

  // xattr
  if (update_xattr) {
    dout(7) << " xattrs v" << pi->xattr_version << " -> " << m->head.xattr_version << dendl;
    pi->xattr_version = m->head.xattr_version;
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(*px, p);

    wrlock_force(&in->xattrlock, mut);
  }

  mut->pin(in);

  mdcache->predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mut, &le->metablob, in);

  // "oldest flush tid" > 0 means client uses unique TID for each flush
  if (ack && ack->get_oldest_flush_tid() > 0)
    le->metablob.add_client_flush(metareqid_t(m->get_source(), ack->get_client_tid()),
                                  ack->get_oldest_flush_tid());

  mds->mdlog->start_entry(le);
  mut->ls = mds->mdlog->get_current_segment();
  mds->mdlog->submit_entry(le, new C_Locker_FileUpdateFinish(this, in, mut, change_max, client, ack));

  if (!*need_flush &&
      ((change_max && new_max > 0) || // max INCREASE
       _should_flush_log(in, dirty)))
    *need_flush = true;

  return true;
}


void Locker::process_request_cap_release(const MDRequestRef& mdr, const ceph_mds_request_release& item,
                                         const string &dname)
{
  client_t client = mdr->get_client();
  inodeno_t ino = (uint64_t)item.ino;
  uint64_t cap_id = item.cap_id;
  int caps = item.caps;
  int wanted = item.wanted;
  int seq = item.seq;
  int issue_seq = item.issue_seq;
  int mseq = item.mseq;

  CInodeRef in = mdcache->get_inode(ino);
  if (!in)
    return;

  CObject::Locker l(in.get());

  if (dname.length()) {
    frag_t fg = in->pick_dirfrag(dname);
    CDirRef dir = in->get_dirfrag(fg);
    CDentryRef dn = dir ? dir->lookup(dname) : NULL;
    if (dn) {
      dn->mutex_lock();
      DentryLease *l = dn->get_client_lease(client);
      if (l) {
	dout(10) << "process_cap_release removing lease on " << *dn << dendl;
	remove_client_lease(dn.get(), mdr->session);
      } else {
	dout(7) << "process_cap_release client." << client
		<< " doesn't have lease on " << *dn << dendl;
      }
      dn->mutex_unlock();
    } else {
      dout(7) << "process_cap_release client." << client << " released lease on dn "
	      << dir->dirfrag() << "/" << dname << " which dne" << dendl;
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

  if (caps & ~cap->issued()) {
    dout(10) << " confirming not issued caps " << ccap_string(caps & ~cap->issued()) << dendl;
    caps &= cap->issued();
  }
  cap->confirm_receipt(seq, caps);

  adjust_cap_wanted(cap, wanted, issue_seq);

  if (mdr)
    cap->inc_suppress();
  eval(in.get(), CEPH_CAP_LOCKS);
  if (mdr)
    cap->dec_suppress();

  // take note; we may need to reissue on this cap later
  /*
  if (mdr)
    mdr->cap_releases[in->vino()] = cap->get_last_seq();
    */
}

/* This function DOES put the passed message before returning */
void Locker::handle_client_cap_release(MClientCapRelease *m)
{
  Session *session = mds->get_session(m);
  dout(10) << "handle_client_cap_release " << *m << dendl;

  /*
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
  */

  for (vector<ceph_mds_cap_item>::iterator p = m->caps.begin(); p != m->caps.end(); ++p) {
    _do_cap_release(session, inodeno_t((uint64_t)p->ino) , p->cap_id, p->migrate_seq, p->seq);
  }

  /*
  if (session)
    session->notify_cap_release(m->caps.size());
   */

  m->put();
}

void Locker::_do_cap_release(Session *session, inodeno_t ino, uint64_t cap_id,
                             ceph_seq_t mseq, ceph_seq_t seq)
{
  client_t client = session->get_client();
  CInodeRef in = mdcache->get_inode(ino);
  if (!in) {
    dout(7) << "_do_cap_release missing ino " << ino << dendl;
    return;
  }

  CObject::Locker l(in.get());

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
  if (seq != cap->get_last_issue()) {
    dout(7) << " issue_seq " << seq << " != " << cap->get_last_issue() << dendl;
    // clean out any old revoke history
    cap->clean_revoke_from(seq);
    eval_cap_gather(in.get());
    return;
  }
  remove_client_cap(in.get(), session);
}

void Locker::remove_client_cap(CInode *in, Session *session)
{ 
  in->mutex_assert_locked_by_me();

  // clean out any pending snapflush state
  in->remove_client_cap(session); // FIXME: session

  // make sure we clear out the client byte range
  if (in->get_projected_inode()->client_ranges.count(session->get_client()) &&
      !(in->get_inode()->nlink == 0 && !in->is_any_caps()))    // unless it's unlink + stray
    check_inode_max_size(in);

  try_eval(in, CEPH_CAP_LOCKS);
}

void Locker::handle_client_lease(MClientLease *m)
{
  dout(10) << "handle_client_lease " << *m << dendl;
  assert(m->get_source().is_client());
  Session *session = mds->get_session(m);
  client_t client = session->get_client();

  CInodeRef in = mdcache->get_inode(m->get_ino(), m->get_last());
  if (!in) {
    dout(7) << "handle_client_lease don't have ino " << m->get_ino() << "." << m->get_last() << dendl;
    m->put();
    return;
  }

  in->mutex_lock();
  frag_t fg = in->pick_dirfrag(m->dname);
  CDirRef dir = in->get_dirfrag(fg);
  CDentryRef dn = dir ? dir->lookup(m->dname) : NULL;
  in->mutex_unlock();
  if (!dn) {
    dout(7) << "handle_client_lease don't have dn " << m->get_ino() << " " << m->dname << dendl;
    m->put();
    return;
  }
  dout(10) << " on " << *dn << dendl;

  dn->mutex_lock();
  // replica and lock
  DentryLease *l = dn->get_client_lease(client);
  if (!l) { 
    dout(7) << "handle_client_lease didn't have lease for client." << client << " of " << *dn << dendl;
    m->put();
    goto out_unlock;
  }

  switch (m->get_action()) {
  case CEPH_MDS_LEASE_REVOKE_ACK:
  case CEPH_MDS_LEASE_RELEASE:
    if (l->seq != m->get_seq()) {
      dout(7) << "handle_client_lease release - seq " << l->seq << " != provided " << m->get_seq() << dendl;
    } else {
      dout(7) << "handle_client_lease client." << client
              << " on " << *dn << dendl;
      remove_client_lease(dn.get(), session);
    }
    m->put();
    break;

  case CEPH_MDS_LEASE_RENEW:
    {
      dout(7) << "handle_client_lease client." << client << " renew on " << *dn
	      << (!dn->lock.can_lease(client)?", revoking lease":"") << dendl;
      if (dn->lock.can_lease(client)) {
	float duration = mdcache->get_lease_duration();
	utime_t ttl = ceph_clock_now(g_ceph_context);
	ttl = ceph_clock_now(g_ceph_context);
	ttl += duration;

	session->mutex_lock();
	l->ttl = ttl;
	session->touch_lease(l);
	session->mutex_unlock();

	m->h.duration_ms = (int)(1000 * duration);
	m->h.seq = ++l->seq;
	m->clear_payload();

	mds->send_message_client_counted(m, m->get_connection());
      }
    }
    break;

  default:
    assert(0); // implement me
    break;
  }
out_unlock:
  dn->mutex_unlock();
}

void Locker::issue_client_lease(CDentry *dn, bufferlist &bl, utime_t now, Session *session)
{
  dn->mutex_assert_locked_by_me();
  client_t client = session->get_client();
  CInode *diri = dn->get_dir_inode();
  diri->mutex_assert_locked_by_me();

  if (!diri->is_stray() &&  // do not issue dn leases in stray dir!
      !diri->filelock.can_lease(client) &&
      !(diri->get_client_cap_pending(client) & (CEPH_CAP_FILE_SHARED|CEPH_CAP_FILE_EXCL)) &&
      dn->lock.can_lease(client)) {
    int pool = 1;   // fixme.. do something smart!
    // issue a dentry lease
    float duration = mdcache->get_lease_duration();
    utime_t ttl = now;
    ttl += duration;
    DentryLease *l = dn->add_client_lease(session, ttl);

    LeaseStat e;
    e.mask = 1 | CEPH_LOCK_DN;  // old and new bit values
    e.seq = ++l->seq;
    e.duration_ms = (int)(1000 * duration);
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

void Locker::remove_client_lease(CDentry *dn, Session *session)
{
  dn->mutex_assert_locked_by_me();

  dn->remove_client_lease(session);

  if (!dn->lock.is_leased())
    try_eval(dn, CEPH_LOCK_DN);
}

void Locker::revoke_client_leases(SimpleLock *lock)
{
  assert(lock->get_type() == CEPH_LOCK_DN);
  CDentry *dn = static_cast<CDentry*>(lock->get_parent());
  dn->mutex_assert_locked_by_me();

  int nr = 0;
  utime_t now = ceph_clock_now(g_ceph_context);
  CInode *diri = dn->get_dir_inode();
  for (auto p = dn->get_client_leases().begin();
       p != dn->get_client_leases().end();
       ++p) {
    DentryLease *l = p->second;
    if (l->ttl < now) {
      remove_client_lease(dn, l->get_session());
      continue;
    }

    int mask = 1 | CEPH_LOCK_DN; // old and new bits
    mds->send_message_client_counted(new MClientLease(CEPH_MDS_LEASE_REVOKE, l->seq, mask,
						      diri->ino(), 2, CEPH_NOSNAP, dn->name),
				     l->get_session());
    nr++;
  }
  assert(nr == lock->get_num_client_lease());
}

void Locker::remove_stale_leases(Session *session, ceph_seq_t lseq)
{
  session->mutex_assert_locked_by_me();
  dout(10) << "remove_stale_leases for " << session->info.inst.name << dendl;
  client_t client = session->get_client();
  vector<CDentryRef> dn_set;
  for(auto p = session->leases.begin(); !p.end(); ++p) {
    if (ceph_seq_cmp((*p)->seq, lseq) <= 0)
      dn_set.push_back((*p)->get_dentry());
  }
  session->mutex_unlock();

  for (CDentryRef &dn : dn_set) {
    dn->mutex_lock();
    DentryLease *l = dn->get_client_lease(client);
    if (l && ceph_seq_cmp(l->seq, lseq) <= 0)
      remove_client_lease(dn.get(), session);
    dn->mutex_unlock();
  }

  session->mutex_lock();
}
