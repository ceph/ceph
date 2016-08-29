#ifndef CEPH_MDS_LOCKER_H
#define CEPH_MDS_LOCKER_H

#include "CObject.h"
#include "include/xlist.h"

class MDSRank;
class MDCache;
class Capability;
class SimpleLock;
class ScatterLock;
class LocalLock;
class MClientCaps;
class MClientCapRelease;
class MClientLease;

struct MutationImpl;
struct MDRequestImpl;
typedef ceph::shared_ptr<MutationImpl> MutationRef;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

class Locker {
public:
  MDSRank* const mds;
  MDCache* const &mdcache;
  Server* const &server;

protected:
  void nudge_log(SimpleLock *lock);
  void finish_waiting(SimpleLock *lock, uint64_t mask);
  void finish_waiting(CInode *in, uint64_t mask);

  friend class LockerContext;
public:
  Locker(MDSRank *_mds);
  void dispatch(Message *m);
  void tick();

  int acquire_locks(const MDRequestRef& mdr, set<SimpleLock*> &rdlocks,
		    set<SimpleLock*> &wrlocks, set<SimpleLock*> &xlocks);
  void drop_locks(const MutationRef& mut);
  void drop_non_rdlocks(const MutationRef& mut, set<CObject*>& objs);
  void drop_rdlocks(const MutationRef& mut);
  void set_xlocks_done(const MutationRef& mut, bool skip_dentry=false);

protected:

  // -- locks --
  void _drop_rdlocks(const MutationRef& mut, set<CInodeRef> *pneed_issue);
  void _drop_non_rdlocks(const MutationRef& mut, set<CInodeRef> *pneed_issue, set<CObject*> *pobjs=NULL);

  void eval_gather(SimpleLock *lock, bool first=false, bool *need_issue=0, uint64_t *p_finish_mask=0);
  void eval(SimpleLock *lock, bool *need_issue);
  void eval_any(SimpleLock *lock, bool *need_issue, uint64_t *p_finish_mask=0);

  void eval_scatter_gathers(CInode *in);
  void eval_cap_gather(CInode *in, set<CInodeRef> *issue_set=0);

  bool eval(CInode *in, int mask); // parent mutex/
  void try_eval(CObject *p, int mask); // parent mutex
  void try_eval(SimpleLock *lock, bool *pneed_issue); 

  bool rdlock_kick(SimpleLock *lock);
  bool rdlock_start(SimpleLock *lock, const MDRequestRef& mut);
  void rdlock_finish(SimpleLock *lock, const MutationRef& mut, bool *pneed_issue);
public:
  void wrlock_force(SimpleLock *lock, const MutationRef& mut);
  bool wrlock_start(SimpleLock *lock, const MutationRef& mut);
protected:
  bool wrlock_start(SimpleLock *lock, const MDRequestRef& mut);
  void wrlock_finish(SimpleLock *lock, const MutationRef& mut, bool *pneed_issue, bool parent_locked=false);

  bool xlock_start(SimpleLock *lock, const MDRequestRef& mut);
  void _finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue);
  void xlock_finish(SimpleLock *lock, const MutationRef& mut, bool *pneed_issue, bool parent_locked=false);
  void start_locking(SimpleLock *lock, bool xlock, const MDRequestRef& mut);
  void finish_locking(SimpleLock *lock, const MutationRef& mut);
  void cancel_locking(const MutationRef& mut, set<CInodeRef> *pneed_issue);

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
  void scatter_nudge(ScatterLock *lock, MDSContextBase *c, bool forcelockchange=false);
  void mark_updated_scatterlock(ScatterLock *lock);
  void scatter_tick();
protected:
  xlist<ScatterLock*> updated_scatterlocks;
  Mutex updated_scatterlocks_mutex;

  void scatter_scatter(ScatterLock *lock, bool nowait=false);
  void scatter_tempsync(ScatterLock *lock, bool *need_issue=0);
  void scatter_writebehind(ScatterLock *lock);
  void do_scatter_writebehind(ScatterLock *lock, const MutationRef& mut, bool async);
  void scatter_writebehind_finish(ScatterLock *lock, const MutationRef& mut);
  friend class C_Locker_ScatterWritebehind;
  friend class C_Locker_ScatterWBFinish;

public:
  void local_wrlock_grab(LocalLock *lock, const MutationRef& mut);
protected:
  bool local_wrlock_start(LocalLock *lock, const MDRequestRef& mut);
  void local_wrlock_finish(LocalLock *lock, const MutationRef& mut);
  bool local_xlock_start(LocalLock *lock, const MDRequestRef& mut);
  void local_xlock_finish(LocalLock *lock, const MutationRef& mut);

  // file
public:
  void file_eval(ScatterLock *lock, bool *need_issue);
protected:
  void scatter_mix(ScatterLock *lock, bool *need_issue=0);
  void file_excl(ScatterLock *lock, bool *need_issue=0);
  void file_xsyn(SimpleLock *lock, bool *need_issue=0);

public:
  Capability* issue_new_caps(CInode *in, int mode, Session *session, bool is_replay);
  bool issue_caps(CInode *in, client_t only_client=-1); // parent mutex
  void issue_caps_set(set<CInodeRef>& inset, bool parents_locked=false);

  void issue_truncate(CInode *in);
  void file_update_finish(CInode *in, const MutationRef& mut, bool share, client_t client,
		  	  MClientCaps *ack);
  void calc_new_client_ranges(CInode *in, uint64_t size,
			      map<client_t,client_writeable_range_t> *new_ranges,
			      bool *max_increased);


  bool check_inode_max_size(CInode *in, bool parent_locked=false, bool force_wrlock=false,
			    uint64_t new_max_size=0, uint64_t new_size=0,
			    utime_t new_mtime=utime_t());
  void schedule_check_max_size(CInode *in, uint64_t new_max_size, bool wait_lock);
  void share_inode_max_size(CInode *in, client_t only_client=-1);

  bool _should_flush_log(CInode *in, int wanted);
  void adjust_cap_wanted(Capability *cap, int wanted, int issue_seq);
  void handle_client_caps(MClientCaps *m);
  void process_request_cap_release(const MDRequestRef& mdr,
				   const ceph_mds_request_release& item, const string &dname);

  void _update_cap_fields(CInode *in, int dirty, MClientCaps *m, inode_t *pi);
  bool _do_cap_update(CInode *in, Capability *cap, MClientCaps *m, MClientCaps *ack,
		      MutationRef& mut, bool *flushed);
  void handle_client_cap_release(MClientCapRelease *m);
  void _do_cap_release(Session *session, inodeno_t ino, uint64_t cap_id,
		       ceph_seq_t mseq, ceph_seq_t seq);
  void remove_client_cap(CInode *in, Session *session);
  void revoke_stale_caps(Session *session, uint64_t sseq);
  void resume_stale_caps(Session *session, uint64_t sseq);

  void handle_client_lease(MClientLease *m);
  void remove_client_lease(CDentry *dn, Session *session);
  void issue_client_lease(CDentry *dn, bufferlist &bl, utime_t now, Session *session);
  void revoke_client_leases(SimpleLock *lock);
  void remove_stale_leases(Session *session, ceph_seq_t lseq);
};


#endif
