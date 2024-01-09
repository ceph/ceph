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

#ifndef CEPH_MDS_LOCKER_H
#define CEPH_MDS_LOCKER_H

#include "include/types.h"

#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"
#include "messages/MLock.h"

#include "CInode.h"
#include "SimpleLock.h"
#include "MDSContext.h"
#include "Mutation.h"
#include "messages/MClientReply.h"

struct SnapRealm;

class MDSRank;
class Session;
class CDentry;
class Capability;
class SimpleLock;
class ScatterLock;
class LocalLockC;

class Locker {
public:
  Locker(MDSRank *m, MDCache *c);

  SimpleLock *get_lock(int lock_type, const MDSCacheObjectInfo &info);
  
  void dispatch(const cref_t<Message> &m);
  void handle_lock(const cref_t<MLock> &m);

  void tick();

  void nudge_log(SimpleLock *lock);

  bool acquire_locks(const MDRequestRef& mdr,
		     MutationImpl::LockOpVec& lov,
		     CInode *auth_pin_freeze=NULL,
		     bool auth_pin_nonblocking=false);

  bool try_rdlock_snap_layout(CInode *in, const MDRequestRef& mdr,
			      int n=0, bool want_layout=false);

  void notify_freeze_waiter(MDSCacheObject *o);
  void cancel_locking(MutationImpl *mut, std::set<CInode*> *pneed_issue);
  void drop_locks(MutationImpl *mut, std::set<CInode*> *pneed_issue=0);
  void set_xlocks_done(MutationImpl *mut, bool skip_dentry=false);
  void drop_non_rdlocks(MutationImpl *mut, std::set<CInode*> *pneed_issue=0);
  void drop_rdlocks_for_early_reply(MutationImpl *mut);
  void drop_locks_for_fragment_unfreeze(MutationImpl *mut);

  int get_cap_bit_for_lock_cache(int op);
  void create_lock_cache(const MDRequestRef& mdr, CInode *diri, file_layout_t *dir_layout=nullptr);
  bool find_and_attach_lock_cache(const MDRequestRef& mdr, CInode *diri);
  void invalidate_lock_caches(CDir *dir);
  void invalidate_lock_caches(SimpleLock *lock);
  void invalidate_lock_cache(MDLockCache *lock_cache);
  void eval_lock_caches(Capability *cap);
  void put_lock_cache(MDLockCache* lock_cache);

  void eval_gather(SimpleLock *lock, bool first=false, bool *need_issue=0, MDSContext::vec *pfinishers=0);
  void eval(SimpleLock *lock, bool *need_issue);
  void eval_any(SimpleLock *lock, bool *need_issue, MDSContext::vec *pfinishers=0, bool first=false) {
    if (!lock->is_stable())
      eval_gather(lock, first, need_issue, pfinishers);
    else if (lock->get_parent()->is_auth())
      eval(lock, need_issue);
  }

  void eval_scatter_gathers(CInode *in);

  void eval_cap_gather(CInode *in, std::set<CInode*> *issue_set=0);

  bool eval(CInode *in, int mask, bool caps_imported=false);
  void try_eval(MDSCacheObject *p, int mask);
  void try_eval(SimpleLock *lock, bool *pneed_issue);

  bool _rdlock_kick(SimpleLock *lock, bool as_anon);
  bool rdlock_try(SimpleLock *lock, client_t client);
  bool rdlock_start(SimpleLock *lock, const MDRequestRef& mut, bool as_anon=false);
  void rdlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue);
  bool rdlock_try_set(MutationImpl::LockOpVec& lov, const MDRequestRef& mdr);
  bool rdlock_try_set(MutationImpl::LockOpVec& lov, MutationRef& mut);

  void wrlock_force(SimpleLock *lock, MutationRef& mut);
  bool wrlock_try(SimpleLock *lock, const MutationRef& mut, client_t client=-1);
  bool wrlock_start(const MutationImpl::LockOp &op, const MDRequestRef& mut);
  void wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue);

  void remote_wrlock_start(SimpleLock *lock, mds_rank_t target, const MDRequestRef& mut);
  void remote_wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut);

  bool xlock_start(SimpleLock *lock, const MDRequestRef& mut);
  void _finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue);
  void xlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue);

  void xlock_export(const MutationImpl::lock_iterator& it, MutationImpl *mut);
  void xlock_import(SimpleLock *lock);

  void try_simple_eval(SimpleLock *lock);
  bool simple_rdlock_try(SimpleLock *lock, MDSContext *con);

  bool simple_sync(SimpleLock *lock, bool *need_issue=0);

   // scatter
  void scatter_eval(ScatterLock *lock, bool *need_issue);        // public for MDCache::adjust_subtree_auth()

  void scatter_tick();
  void scatter_nudge(ScatterLock *lock, MDSContext *c, bool forcelockchange=false);

  void mark_updated_scatterlock(ScatterLock *lock);

  void handle_reqrdlock(SimpleLock *lock, const cref_t<MLock> &m);

  // caps

  // when to defer processing client cap release or writeback due to being
  // frozen.  the condition must be consistent across handle_client_caps and
  // process_request_cap_release to preserve ordering.
  bool should_defer_client_cap_frozen(CInode *in);

  void process_request_cap_release(const MDRequestRef& mdr, client_t client, const ceph_mds_request_release& r,
				   std::string_view dname);

  void kick_cap_releases(const MDRequestRef& mdr);
  void kick_issue_caps(CInode *in, client_t client, ceph_seq_t seq);

  void remove_client_cap(CInode *in, Capability *cap, bool kill=false);

  std::set<client_t> get_late_revoking_clients(double timeout) const;

  void snapflush_nudge(CInode *in);
  void mark_need_snapflush_inode(CInode *in);
  bool is_revoking_any_caps_from(client_t client);

  // local
  void local_wrlock_grab(LocalLockC *lock, MutationRef& mut);

  // file
  void file_eval(ScatterLock *lock, bool *need_issue);
  void file_recover(ScatterLock *lock);

  void mark_updated_Filelock(ScatterLock *lock);

  // -- file i/o --
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, const MDRequestRef& mdr, SnapRealm *conrealm);
  int get_allowed_caps(CInode *in, Capability *cap, int &all_allowed,
                       int &loner_allowed, int &xlocker_allowed);
  int issue_caps(CInode *in, Capability *only_cap=0);
  void issue_caps_set(std::set<CInode*>& inset);
  void issue_truncate(CInode *in);
  void revoke_stale_cap(CInode *in, client_t client);
  bool revoke_stale_caps(Session *session);
  void resume_stale_caps(Session *session);
  void remove_stale_leases(Session *session);

  void request_inode_file_caps(CInode *in);

  bool check_client_ranges(CInode *in, uint64_t size);
  bool calc_new_client_ranges(CInode *in, uint64_t size,
			      bool *max_increased=nullptr);
  bool check_inode_max_size(CInode *in, bool force_wrlock=false,
                            uint64_t newmax=0, uint64_t newsize=0,
			    utime_t mtime=utime_t());
  void share_inode_max_size(CInode *in, Capability *only_cap=0);

  // -- client leases --
  void handle_client_lease(const cref_t<MClientLease> &m);

  void issue_client_lease(CDentry *dn, CInode *in, const MDRequestRef &mdr, utime_t now, bufferlist &bl);
  void revoke_client_leases(SimpleLock *lock);
  static void encode_lease(bufferlist& bl, const session_info_t& info, const LeaseStat& ls);

protected:
  void send_lock_message(SimpleLock *lock, int msg);
  void send_lock_message(SimpleLock *lock, int msg, const bufferlist &data);

  // -- locks --
  void _drop_locks(MutationImpl *mut, std::set<CInode*> *pneed_issue, bool drop_rdlocks);

  void simple_eval(SimpleLock *lock, bool *need_issue);
  void handle_simple_lock(SimpleLock *lock, const cref_t<MLock> &m);

  void simple_lock(SimpleLock *lock, bool *need_issue=0);
  void simple_excl(SimpleLock *lock, bool *need_issue=0);
  void simple_xlock(SimpleLock *lock);

  void handle_scatter_lock(ScatterLock *lock, const cref_t<MLock> &m);
  bool scatter_scatter_fastpath(ScatterLock *lock);
  void scatter_scatter(ScatterLock *lock, bool nowait=false);
  void scatter_tempsync(ScatterLock *lock, bool *need_issue=0);

  void scatter_writebehind(ScatterLock *lock);

  void scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut);

  bool _need_flush_mdlog(CInode *in, int wanted_caps, bool lock_state_any=false);
  void adjust_cap_wanted(Capability *cap, int wanted, int issue_seq);
  void handle_client_caps(const cref_t<MClientCaps> &m);
  void _update_cap_fields(CInode *in, int dirty, const cref_t<MClientCaps> &m, CInode::mempool_inode *pi);
  void _do_snap_update(CInode *in, snapid_t snap, int dirty, snapid_t follows, client_t client, const cref_t<MClientCaps> &m, const ref_t<MClientCaps> &ack);
  void _do_null_snapflush(CInode *head_in, client_t client, snapid_t last=CEPH_NOSNAP);
  bool _do_cap_update(CInode *in, Capability *cap, int dirty, snapid_t follows, const cref_t<MClientCaps> &m,
		      const ref_t<MClientCaps> &ack, bool *need_flush=NULL);
  void handle_client_cap_release(const cref_t<MClientCapRelease> &m);
  void _do_cap_release(client_t client, inodeno_t ino, uint64_t cap_id, ceph_seq_t mseq, ceph_seq_t seq);
  void caps_tick();

  bool local_wrlock_start(LocalLockC *lock, const MDRequestRef& mut);
  void local_wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut);
  bool local_xlock_start(LocalLockC *lock, const MDRequestRef& mut);
  void local_xlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut);

  void handle_file_lock(ScatterLock *lock, const cref_t<MLock> &m);
  void scatter_mix(ScatterLock *lock, bool *need_issue=0);
  void file_excl(ScatterLock *lock, bool *need_issue=0);
  void file_xsyn(SimpleLock *lock, bool *need_issue=0);

  void handle_inode_file_caps(const cref_t<MInodeFileCaps> &m);

  void file_update_finish(CInode *in, MutationRef& mut, unsigned flags,
			  client_t client, const ref_t<MClientCaps> &ack);

  xlist<ScatterLock*> updated_scatterlocks;

  // Maintain a global list to quickly find if any caps are late revoking
  xlist<Capability*> revoking_caps;
  // Maintain a per-client list to find clients responsible for late ones quickly
  std::map<client_t, xlist<Capability*> > revoking_caps_by_client;

  elist<CInode*> need_snapflush_inodes;

private:
  friend class C_MDL_CheckMaxSize;
  friend class C_MDL_RequestInodeFileCaps;
  friend class C_Locker_FileUpdate_finish;
  friend class C_Locker_RetryCapRelease;
  friend class C_Locker_Eval;
  friend class C_Locker_ScatterWB;
  friend class LockerContext;
  friend class LockerLogContext;

  void handle_quiesce_failure(const MDRequestRef& mdr, std::string_view& marker);

  bool any_late_revoking_caps(xlist<Capability*> const &revoking, double timeout) const;
  uint64_t calc_new_max_size(const CInode::inode_const_ptr& pi, uint64_t size);
  __u32 get_xattr_total_length(CInode::mempool_xattr_map &xattr);
  void decode_new_xattrs(CInode::mempool_inode *inode,
			 CInode::mempool_xattr_map *px,
			 const cref_t<MClientCaps> &m);

  MDSRank *mds;
  MDCache *mdcache;
  xlist<ScatterLock*> updated_filelocks;
};
#endif
