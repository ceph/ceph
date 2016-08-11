#ifndef CEPH_MDS_LOCKER_H
#define CEPH_MDS_LOCKER_H

class MDSRank;
class MDCache;
class Capability;
class MClientCaps;
class MClientCapRelease;
class MClientLease;

class Locker {
  MDSRank* const mds;
  MDCache* const &mdcache;
  Server* const &server;

  void nudge_log(SimpleLock *lock);

  friend class LockerContext;
public:
  Locker(MDSRank *_mds);
  void dispatch(Message *m);
  void tick();

  int acquire_locks(MDRequestRef& mdr, set<SimpleLock*> &rdlocks,
		    set<SimpleLock*> &wrlocks, set<SimpleLock*> &xlocks);
  void drop_locks(MutationImpl *mut);
  void drop_non_rdlocks(MutationImpl *mut, set<CObject*>& objs);
  void drop_rdlocks(MutationImpl *mut);
  void set_xlocks_done(MutationImpl *mut, bool skip_dentry=false);

protected:
  void handle_client_caps(MClientCaps *m);
  void handle_client_cap_release(MClientCapRelease *m);
  void handle_client_lease(MClientLease *m);

  // -- locks --
  void _drop_rdlocks(MutationImpl *mut, set<CInodeRef> *pneed_issue);
  void _drop_non_rdlocks(MutationImpl *mut, set<CInodeRef> *pneed_issue, set<CObject*> *pobjs=NULL);

  void eval_gather(SimpleLock *lock, bool first=false, bool *need_issue=0, list<MDSInternalContextBase*> *pfinishers=0);
  void eval(SimpleLock *lock, bool *need_issue);
  void eval_any(SimpleLock *lock, bool *need_issue, list<MDSInternalContextBase*> *pfinishers=0) {
    if (!lock->is_stable())
      eval_gather(lock, false, need_issue, pfinishers);
    else if (lock->get_parent()->is_auth())
      eval(lock, need_issue);
  }

  void eval_scatter_gathers(CInode *in);
  void eval_cap_gather(CInode *in, set<CInodeRef> *issue_set=0);

  bool eval(CInode *in, int mask); // parent mutex/
  void try_eval(CObject *p, int mask); // parent mutex
  void try_eval(SimpleLock *lock, bool *pneed_issue); 

  bool rdlock_kick(SimpleLock *lock);
  bool rdlock_start(SimpleLock *lock, MDRequestRef& mut);
  void rdlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue);
public:
  void wrlock_force(SimpleLock *lock, MutationImpl *mut);
  bool wrlock_start(SimpleLock *lock, MutationImpl *mut);
protected:
  bool wrlock_start(SimpleLock *lock, MDRequestRef& mut);
  void wrlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue, bool parent_locked=false);

  bool xlock_start(SimpleLock *lock, MDRequestRef& mut);
  void _finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue);
  void xlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue, bool parent_locked=false);
  void cancel_locking(MutationImpl *mut, set<CInodeRef> *pneed_issue);

public:
  void simple_eval(SimpleLock *lock, bool *need_issue);
protected:
  bool simple_sync(SimpleLock *lock, bool *need_issue=0);
  void simple_lock(SimpleLock *lock, bool *need_issue=0);
  void simple_excl(SimpleLock *lock, bool *need_issue=0);
  void simple_xlock(SimpleLock *lock);

  // scatter
public:
  void scatter_eval(ScatterLock *lock, bool *need_issue);        // public for MDCache::adjust_subtree_auth()
  void scatter_nudge(ScatterLock *lock, MDSInternalContextBase *c, bool forcelockchange=false);
  void mark_updated_scatterlock(ScatterLock *lock);
  void scatter_tick();
protected:
  xlist<ScatterLock*> updated_scatterlocks;
  Mutex updated_scatterlocks_mutex;

  void scatter_scatter(ScatterLock *lock, bool nowait=false);
  void scatter_tempsync(ScatterLock *lock, bool *need_issue=0);
  void scatter_writebehind(ScatterLock *lock);
  void do_scatter_writebehind(ScatterLock *lock, MutationRef& mut);
  void scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut);
  friend class C_Locker_ScatterWritebehind;
  friend class C_Locker_ScatterWBFinish;

public:
  void local_wrlock_grab(LocalLock *lock, MutationImpl *mut);
protected:
  bool local_wrlock_start(LocalLock *lock, MDRequestRef& mut);
  void local_wrlock_finish(LocalLock *lock, MutationImpl *mut);
  bool local_xlock_start(LocalLock *lock, MDRequestRef& mut);
  void local_xlock_finish(LocalLock *lock, MutationImpl *mut);

  // file
public:
  void file_eval(ScatterLock *lock, bool *need_issue);
protected:
  void scatter_mix(ScatterLock *lock, bool *need_issue=0);
  void file_excl(ScatterLock *lock, bool *need_issue=0);
  void file_xsyn(SimpleLock *lock, bool *need_issue=0);

public:
  bool issue_caps(CInode *in, Capability *only_cap=0); // parent mutex
  void issue_caps_set(set<CInodeRef>& inset, bool parents_locked=false);
  void revoke_client_leases(SimpleLock *lock);
};


#endif
