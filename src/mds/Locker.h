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

#include <map>
#include <list>
#include <set>
#include <string_view>

class MDSRank;
class Session;
class CDentry;
struct SnapRealm;

class Capability;

class SimpleLock;
class ScatterLock;
class LocalLock;

#include "CInode.h"
#include "SimpleLock.h"
#include "MDSContext.h"
#include "Mutation.h"
#include "messages/MClientReply.h"

class Locker {
private:
  MDSRank *mds;
  MDCache *mdcache;
 
 public:
  Locker(MDSRank *m, MDCache *c);

  SimpleLock *get_lock(int lock_type, const MDSCacheObjectInfo &info);
  
  void dispatch(const Message::const_ref &m);
  void handle_lock(const MLock::const_ref &m);

  void tick();

  void nudge_log(SimpleLock *lock);

protected:
  void send_lock_message(SimpleLock *lock, int msg);
  void send_lock_message(SimpleLock *lock, int msg, const bufferlist &data);

  // -- locks --
  void _drop_locks(MutationImpl *mut, std::set<CInode*> *pneed_issue, bool drop_rdlocks);
public:
  void include_snap_rdlocks(CInode *in, MutationImpl::LockOpVec& lov);
  void include_snap_rdlocks_wlayout(CInode *in, MutationImpl::LockOpVec& lov,
				    file_layout_t **layout);

  bool acquire_locks(MDRequestRef& mdr,
		     MutationImpl::LockOpVec& lov,
		     CInode *auth_pin_freeze=NULL,
		     bool auth_pin_nonblock=false);

  void notify_freeze_waiter(MDSCacheObject *o);
  void cancel_locking(MutationImpl *mut, std::set<CInode*> *pneed_issue);
  void drop_locks(MutationImpl *mut, std::set<CInode*> *pneed_issue=0);
  void set_xlocks_done(MutationImpl *mut, bool skip_dentry=false);
  void drop_non_rdlocks(MutationImpl *mut, std::set<CInode*> *pneed_issue=0);
  void drop_rdlocks_for_early_reply(MutationImpl *mut);
  void drop_locks_for_fragment_unfreeze(MutationImpl *mut);

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
  bool rdlock_try(SimpleLock *lock, client_t client, MDSContext *c);
  bool rdlock_start(SimpleLock *lock, MDRequestRef& mut, bool as_anon=false);
  void rdlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue);
  bool can_rdlock_set(MutationImpl::LockOpVec& lov);
  void rdlock_take_set(MutationImpl::LockOpVec& lov, MutationRef& mut);

  void wrlock_force(SimpleLock *lock, MutationRef& mut);
  bool wrlock_start(const MutationImpl::LockOp &op, MDRequestRef& mut, bool nowait=false);
  void wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue);

  void remote_wrlock_start(SimpleLock *lock, mds_rank_t target, MDRequestRef& mut);
  void remote_wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut);

  bool xlock_start(SimpleLock *lock, MDRequestRef& mut);
  void _finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue);
  void xlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut, bool *pneed_issue);

  void xlock_export(const MutationImpl::lock_iterator& it, MutationImpl *mut);
  void xlock_import(SimpleLock *lock);


  // simple
public:
  void try_simple_eval(SimpleLock *lock);
  bool simple_rdlock_try(SimpleLock *lock, MDSContext *con);
protected:
  void simple_eval(SimpleLock *lock, bool *need_issue);
  void handle_simple_lock(SimpleLock *lock, const MLock::const_ref &m);

public:
  bool simple_sync(SimpleLock *lock, bool *need_issue=0);
protected:
  void simple_lock(SimpleLock *lock, bool *need_issue=0);
  void simple_excl(SimpleLock *lock, bool *need_issue=0);
  void simple_xlock(SimpleLock *lock);


  // scatter
public:
  void scatter_eval(ScatterLock *lock, bool *need_issue);        // public for MDCache::adjust_subtree_auth()

  void scatter_tick();
  void scatter_nudge(ScatterLock *lock, MDSContext *c, bool forcelockchange=false);

protected:
  void handle_scatter_lock(ScatterLock *lock, const MLock::const_ref &m);
  bool scatter_scatter_fastpath(ScatterLock *lock);
  void scatter_scatter(ScatterLock *lock, bool nowait=false);
  void scatter_tempsync(ScatterLock *lock, bool *need_issue=0);

  void scatter_writebehind(ScatterLock *lock);

  void scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut);

  xlist<ScatterLock*> updated_scatterlocks;
public:
  void mark_updated_scatterlock(ScatterLock *lock);


  void handle_reqrdlock(SimpleLock *lock, const MLock::const_ref &m);



  // caps

  // when to defer processing client cap release or writeback due to being
  // frozen.  the condition must be consistent across handle_client_caps and
  // process_request_cap_release to preserve ordering.
  bool should_defer_client_cap_frozen(CInode *in);

  void process_request_cap_release(MDRequestRef& mdr, client_t client, const ceph_mds_request_release& r,
				   std::string_view dname);

  void kick_cap_releases(MDRequestRef& mdr);
  void kick_issue_caps(CInode *in, client_t client, ceph_seq_t seq);

  void remove_client_cap(CInode *in, Capability *cap);

  std::vector<client_t> get_late_revoking_clients(double timeout) const;

private:
  bool any_late_revoking_caps(xlist<Capability*> const &revoking, double timeout) const;

protected:
  bool _need_flush_mdlog(CInode *in, int wanted_caps);
  void adjust_cap_wanted(Capability *cap, int wanted, int issue_seq);
  void handle_client_caps(const MClientCaps::const_ref &m);
  void _update_cap_fields(CInode *in, int dirty, const MClientCaps::const_ref &m, CInode::mempool_inode *pi);
  void _do_snap_update(CInode *in, snapid_t snap, int dirty, snapid_t follows, client_t client, const MClientCaps::const_ref &m, const MClientCaps::ref &ack);
  void _do_null_snapflush(CInode *head_in, client_t client, snapid_t last=CEPH_NOSNAP);
  bool _do_cap_update(CInode *in, Capability *cap, int dirty, snapid_t follows, const MClientCaps::const_ref &m,
		      const MClientCaps::ref &ack, bool *need_flush=NULL);
  void handle_client_cap_release(const MClientCapRelease::const_ref &m);
  void _do_cap_release(client_t client, inodeno_t ino, uint64_t cap_id, ceph_seq_t mseq, ceph_seq_t seq);
  void caps_tick();

  // Maintain a global list to quickly find if any caps are late revoking
  xlist<Capability*> revoking_caps;
  // Maintain a per-client list to find clients responsible for late ones quickly
  std::map<client_t, xlist<Capability*> > revoking_caps_by_client;

  elist<CInode*> need_snapflush_inodes;
public:
  void snapflush_nudge(CInode *in);
  void mark_need_snapflush_inode(CInode *in);

  // local
public:
  void local_wrlock_grab(LocalLock *lock, MutationRef& mut);
protected:
  bool local_wrlock_start(LocalLock *lock, MDRequestRef& mut);
  void local_wrlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut);
  bool local_xlock_start(LocalLock *lock, MDRequestRef& mut);
  void local_xlock_finish(const MutationImpl::lock_iterator& it, MutationImpl *mut);


  // file
public:
  void file_eval(ScatterLock *lock, bool *need_issue);
protected:
  void handle_file_lock(ScatterLock *lock, const MLock::const_ref &m);
  void scatter_mix(ScatterLock *lock, bool *need_issue=0);
  void file_excl(ScatterLock *lock, bool *need_issue=0);
  void file_xsyn(SimpleLock *lock, bool *need_issue=0);

public:
  void file_recover(ScatterLock *lock);

private:
  xlist<ScatterLock*> updated_filelocks;
public:
  void mark_updated_Filelock(ScatterLock *lock);

  // -- file i/o --
public:
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, Session *session, SnapRealm *conrealm, bool is_replay);
  bool issue_caps(CInode *in, Capability *only_cap=0);
  void issue_caps_set(std::set<CInode*>& inset);
  void issue_truncate(CInode *in);
  void revoke_stale_caps(Session *session);
  void resume_stale_caps(Session *session);
  void remove_stale_leases(Session *session);

public:
  void request_inode_file_caps(CInode *in);
protected:
  void handle_inode_file_caps(const MInodeFileCaps::const_ref &m);

  void file_update_finish(CInode *in, MutationRef& mut, unsigned flags,
			  client_t client, const MClientCaps::ref &ack);
private:
  uint64_t calc_new_max_size(CInode::mempool_inode *pi, uint64_t size);
public:
  void calc_new_client_ranges(CInode *in, uint64_t size, bool update,
			      CInode::mempool_inode::client_range_map* new_ranges,
			      bool *max_increased);
  bool check_inode_max_size(CInode *in, bool force_wrlock=false,
                            uint64_t newmax=0, uint64_t newsize=0,
			    utime_t mtime=utime_t());
  void share_inode_max_size(CInode *in, Capability *only_cap=0);

private:
  friend class C_MDL_CheckMaxSize;
  friend class C_MDL_RequestInodeFileCaps;
  friend class C_Locker_FileUpdate_finish;
  friend class C_Locker_RetryCapRelease;
  friend class C_Locker_Eval;
  friend class C_Locker_ScatterWB;
  friend class LockerContext;
  friend class LockerLogContext;

  
  // -- client leases --
public:
  void handle_client_lease(const MClientLease::const_ref &m);

  void issue_client_lease(CDentry *dn, client_t client, bufferlist &bl, utime_t now, Session *session);
  void revoke_client_leases(SimpleLock *lock);
  static void encode_lease(bufferlist& bl, const session_info_t& info, const LeaseStat& ls);
};


#endif
