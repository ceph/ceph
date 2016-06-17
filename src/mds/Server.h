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

#ifndef CEPH_MDS_SERVER_H
#define CEPH_MDS_SERVER_H

#include "MDSRank.h"

class OSDMap;
class PerfCounters;
class LogEvent;
class EMetaBlob;
class EUpdate;
class MMDSSlaveRequest;
struct SnapInfo;
class MClientRequest;
class MClientReply;
class MDLog;

struct MutationImpl;
struct MDRequestImpl;
typedef ceph::shared_ptr<MutationImpl> MutationRef;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

enum {
  l_mdss_first = 1000,
  l_mdss_handle_client_request,
  l_mdss_handle_slave_request,
  l_mdss_handle_client_session,
  l_mdss_dispatch_client_request,
  l_mdss_dispatch_slave_request,
  l_mdss_last,
};

class Server {
private:
  MDSRank *mds;
  MDCache *mdcache;
  MDLog *mdlog;
  PerfCounters *logger;

  // OSDMap full status, used to generate ENOSPC on some operations
  bool is_full;

  // State for while in reconnect
  MDSInternalContext *reconnect_done;
  int failed_reconnects;

  friend class MDSContinuation;
  friend class ServerContext;

public:
  bool terminating_sessions;

  explicit Server(MDSRank *m);
  ~Server() {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
    delete reconnect_done;
  }

  void create_logger();

  // message handler
  void dispatch(Message *m);

  void handle_osd_map();

  // -- sessions and recovery --
  utime_t  reconnect_start;
  set<client_t> client_reconnect_gather;  // clients i need a reconnect msg from.
  bool waiting_for_reconnect(client_t c) const;
  void dump_reconnect_status(Formatter *f) const;

  Session *get_session(Message *m);
  void handle_client_session(class MClientSession *m);
  void _session_logged(Session *session, uint64_t state_seq, 
		       bool open, version_t pv, interval_set<inodeno_t>& inos,version_t piv);
  version_t prepare_force_open_sessions(map<client_t,entity_inst_t> &cm,
					map<client_t,uint64_t>& sseqmap);
  void finish_force_open_sessions(map<client_t,entity_inst_t> &cm,
				  map<client_t,uint64_t>& sseqmap,
				  bool dec_import=true);
  void flush_client_sessions(set<client_t>& client_set, MDSGatherBuilder& gather);
  void finish_flush_session(Session *session, version_t seq);
  void terminate_sessions();
  void find_idle_sessions();
  void kill_session(Session *session, Context *on_safe);
  void journal_close_session(Session *session, int state, Context *on_safe);
  void reconnect_clients(MDSInternalContext *reconnect_done_);
  void handle_client_reconnect(class MClientReconnect *m);
  //void process_reconnect_cap(CInode *in, int from, ceph_mds_cap_reconnect& capinfo);
  void reconnect_gather_finish();
  void reconnect_tick();
  void recover_filelocks(CInode *in, bufferlist locks, int64_t client);

  void recall_client_state(float ratio);
  void force_clients_readonly();

  // -- requests --
  void handle_client_request(MClientRequest *m);

  void journal_and_reply(MDRequestRef& mdr, CInode *tracei, CDentry *tracedn,
			 LogEvent *le, MDSInternalContextBase *fin);
  void submit_mdlog_entry(LogEvent *le, MDSInternalContextBase *fin,
                          MDRequestRef& mdr, const char *evt);
  void dispatch_client_request(MDRequestRef& mdr);
  void early_reply(MDRequestRef& mdr, CInode *tracei, CDentry *tracedn);
  void respond_to_request(MDRequestRef& mdr, int r = 0);
  void set_trace_dist(Session *session, MClientReply *reply, CInode *in, CDentry *dn,
		      snapid_t snapid,
		      int num_dentries_wanted,
		      MDRequestRef& mdr);

  void encode_empty_dirstat(bufferlist& bl);
  void encode_infinite_lease(bufferlist& bl);
  void encode_null_lease(bufferlist& bl);

  void handle_slave_request(MMDSSlaveRequest *m);
  void handle_slave_request_reply(MMDSSlaveRequest *m);
  void dispatch_slave_request(MDRequestRef& mdr);
  void handle_slave_auth_pin(MDRequestRef& mdr);
  void handle_slave_auth_pin_ack(MDRequestRef& mdr, MMDSSlaveRequest *ack);

  // some helpers
  bool check_fragment_space(MDRequestRef& mdr, CDir *in);
  bool check_access(MDRequestRef& mdr, CInode *in, unsigned mask);
  bool _check_access(Session *session, CInode *in, unsigned mask, int caller_uid, int caller_gid, int setattr_uid, int setattr_gid);
  CDir *validate_dentry_dir(MDRequestRef& mdr, CInode *diri, const string& dname);
  CDir *traverse_to_auth_dir(MDRequestRef& mdr, vector<CDentry*> &trace, filepath refpath);
  CDentry *prepare_null_dentry(MDRequestRef& mdr, CDir *dir, const string& dname, bool okexist=false);
  CDentry *prepare_stray_dentry(MDRequestRef& mdr, CInode *in);
  CInode* prepare_new_inode(MDRequestRef& mdr, CDir *dir, inodeno_t useino, unsigned mode,
			    file_layout_t *layout=NULL);
  void journal_allocated_inos(MDRequestRef& mdr, EMetaBlob *blob);
  void apply_allocated_inos(MDRequestRef& mdr);

  CInode* rdlock_path_pin_ref(MDRequestRef& mdr, int n, set<SimpleLock*>& rdlocks, bool want_auth,
			      bool no_want_auth=false,
			      file_layout_t **layout=NULL,
			      bool no_lookup=false);
  CDentry* rdlock_path_xlock_dentry(MDRequestRef& mdr, int n,
                                    set<SimpleLock*>& rdlocks,
                                    set<SimpleLock*>& wrlocks,
				    set<SimpleLock*>& xlocks, bool okexist,
				    bool mustexist, bool alwaysxlock,
				    file_layout_t **layout=NULL);

  CDir* try_open_auth_dirfrag(CInode *diri, frag_t fg, MDRequestRef& mdr);


  // requests on existing inodes.
  void handle_client_getattr(MDRequestRef& mdr, bool is_lookup);
  void handle_client_lookup_ino(MDRequestRef& mdr,
				bool want_parent, bool want_dentry);
  void _lookup_ino_2(MDRequestRef& mdr, int r);
  void handle_client_readdir(MDRequestRef& mdr);
  void handle_client_file_setlock(MDRequestRef& mdr);
  void handle_client_file_readlock(MDRequestRef& mdr);

  void handle_client_setattr(MDRequestRef& mdr);
  void handle_client_setlayout(MDRequestRef& mdr);
  void handle_client_setdirlayout(MDRequestRef& mdr);

  int parse_layout_vxattr(string name, string value, const OSDMap& osdmap,
			  file_layout_t *layout, bool validate=true);
  int parse_quota_vxattr(string name, string value, quota_info_t *quota);
  void handle_set_vxattr(MDRequestRef& mdr, CInode *cur,
			 file_layout_t *dir_layout,
			 set<SimpleLock*> rdlocks,
			 set<SimpleLock*> wrlocks,
			 set<SimpleLock*> xlocks);
  void handle_remove_vxattr(MDRequestRef& mdr, CInode *cur,
			    set<SimpleLock*> rdlocks,
			    set<SimpleLock*> wrlocks,
			    set<SimpleLock*> xlocks);
  void handle_client_setxattr(MDRequestRef& mdr);
  void handle_client_removexattr(MDRequestRef& mdr);

  void handle_client_fsync(MDRequestRef& mdr);

  // open
  void handle_client_open(MDRequestRef& mdr);
  void handle_client_openc(MDRequestRef& mdr);  // O_CREAT variant.
  void do_open_truncate(MDRequestRef& mdr, int cmode);  // O_TRUNC variant.

  // namespace changes
  void handle_client_mknod(MDRequestRef& mdr);
  void handle_client_mkdir(MDRequestRef& mdr);
  void handle_client_symlink(MDRequestRef& mdr);

  // link
  void handle_client_link(MDRequestRef& mdr);
  void _link_local(MDRequestRef& mdr, CDentry *dn, CInode *targeti);
  void _link_local_finish(MDRequestRef& mdr,
			  CDentry *dn, CInode *targeti,
			  version_t, version_t);

  void _link_remote(MDRequestRef& mdr, bool inc, CDentry *dn, CInode *targeti);
  void _link_remote_finish(MDRequestRef& mdr, bool inc, CDentry *dn, CInode *targeti,
			   version_t);

  void handle_slave_link_prep(MDRequestRef& mdr);
  void _logged_slave_link(MDRequestRef& mdr, CInode *targeti);
  void _commit_slave_link(MDRequestRef& mdr, int r, CInode *targeti);
  void _committed_slave(MDRequestRef& mdr);  // use for rename, too
  void handle_slave_link_prep_ack(MDRequestRef& mdr, MMDSSlaveRequest *m);
  void do_link_rollback(bufferlist &rbl, mds_rank_t master, MDRequestRef& mdr);
  void _link_rollback_finish(MutationRef& mut, MDRequestRef& mdr);

  // unlink
  void handle_client_unlink(MDRequestRef& mdr);
  bool _dir_is_nonempty_unlocked(MDRequestRef& mdr, CInode *rmdiri);
  bool _dir_is_nonempty(MDRequestRef& mdr, CInode *rmdiri);
  void _unlink_local(MDRequestRef& mdr, CDentry *dn, CDentry *straydn);
  void _unlink_local_finish(MDRequestRef& mdr,
			    CDentry *dn, CDentry *straydn,
			    version_t);
  bool _rmdir_prepare_witness(MDRequestRef& mdr, mds_rank_t who, CDentry *dn, CDentry *straydn);
  void handle_slave_rmdir_prep(MDRequestRef& mdr);
  void _logged_slave_rmdir(MDRequestRef& mdr, CDentry *srcdn, CDentry *straydn);
  void _commit_slave_rmdir(MDRequestRef& mdr, int r);
  void handle_slave_rmdir_prep_ack(MDRequestRef& mdr, MMDSSlaveRequest *ack);
  void do_rmdir_rollback(bufferlist &rbl, mds_rank_t master, MDRequestRef& mdr);
  void _rmdir_rollback_finish(MDRequestRef& mdr, metareqid_t reqid, CDentry *dn, CDentry *straydn);

  // rename
  void handle_client_rename(MDRequestRef& mdr);
  void _rename_finish(MDRequestRef& mdr,
		      CDentry *srcdn, CDentry *destdn, CDentry *straydn);

  void handle_client_lssnap(MDRequestRef& mdr);
  void handle_client_mksnap(MDRequestRef& mdr);
  void _mksnap_finish(MDRequestRef& mdr, CInode *diri, SnapInfo &info);
  void handle_client_rmsnap(MDRequestRef& mdr);
  void _rmsnap_finish(MDRequestRef& mdr, CInode *diri, snapid_t snapid);
  void handle_client_renamesnap(MDRequestRef& mdr);
  void _renamesnap_finish(MDRequestRef& mdr, CInode *diri, snapid_t snapid);


  // helpers
  bool _rename_prepare_witness(MDRequestRef& mdr, mds_rank_t who, set<mds_rank_t> &witnesse,
			       CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  version_t _rename_prepare_import(MDRequestRef& mdr, CDentry *srcdn, bufferlist *client_map_bl);
  bool _need_force_journal(CInode *diri, bool empty);
  void _rename_prepare(MDRequestRef& mdr,
		       EMetaBlob *metablob, bufferlist *client_map_bl,
		       CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  /* set not_journaling=true if you're going to discard the results --
   * this bypasses the asserts to make sure we're journaling the right
   * things on the right nodes */
  void _rename_apply(MDRequestRef& mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn);

  // slaving
  void handle_slave_rename_prep(MDRequestRef& mdr);
  void handle_slave_rename_prep_ack(MDRequestRef& mdr, MMDSSlaveRequest *m);
  void handle_slave_rename_notify_ack(MDRequestRef& mdr, MMDSSlaveRequest *m);
  void _slave_rename_sessions_flushed(MDRequestRef& mdr);
  void _logged_slave_rename(MDRequestRef& mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void _commit_slave_rename(MDRequestRef& mdr, int r, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void do_rename_rollback(bufferlist &rbl, mds_rank_t master, MDRequestRef& mdr, bool finish_mdr=false);
  void _rename_rollback_finish(MutationRef& mut, MDRequestRef& mdr, CDentry *srcdn, version_t srcdnpv,
			       CDentry *destdn, CDentry *staydn, bool finish_mdr);

private:
  void reply_client_request(MDRequestRef& mdr, MClientReply *reply);
};

#endif
