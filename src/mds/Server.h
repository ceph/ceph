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

#include "MDS.h"

class PerfCounters;
class LogEvent;
class MDRequest;
class Mutation;
class EMetaBlob;
class EUpdate;
class MMDSSlaveRequest;
struct SnapInfo;

enum {
  l_mdss_first = 1000,
  l_mdss_hcreq,
  l_mdss_hsreq,
  l_mdss_hcsess,
  l_mdss_dcreq,
  l_mdss_dsreq,
  l_mdss_last,
};

class Server {
  MDS *mds;
  MDCache *mdcache;
  MDLog *mdlog;
  Messenger *messenger;
  PerfCounters *logger;

public:
  int failed_reconnects;

  bool terminating_sessions;

  Server(MDS *m) : 
    mds(m), 
    mdcache(mds->mdcache), mdlog(mds->mdlog),
    messenger(mds->messenger),
    logger(0),
    failed_reconnects(0),
    terminating_sessions(false) {
  }
  ~Server() {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
  }

  void create_logger();

  // message handler
  void dispatch(Message *m);


  // -- sessions and recovery --
  utime_t  reconnect_start;
  set<client_t> client_reconnect_gather;  // clients i need a reconnect msg from.

  Session *get_session(Message *m);
  void handle_client_session(class MClientSession *m);
  void _session_logged(Session *session, uint64_t state_seq, 
		       bool open, version_t pv, interval_set<inodeno_t>& inos,version_t piv);
  version_t prepare_force_open_sessions(map<client_t,entity_inst_t> &cm,
					map<client_t,uint64_t>& sseqmap);
  void finish_force_open_sessions(map<client_t,entity_inst_t> &cm,
					map<client_t,uint64_t>& sseqmap);
  void terminate_sessions();
  void find_idle_sessions();
  void kill_session(Session *session);
  void journal_close_session(Session *session, int state);
  void reconnect_clients();
  void handle_client_reconnect(class MClientReconnect *m);
  //void process_reconnect_cap(CInode *in, int from, ceph_mds_cap_reconnect& capinfo);
  void reconnect_gather_finish();
  void reconnect_tick();
  void recover_filelocks(CInode *in, bufferlist locks, int64_t client);

  void recall_client_state(float ratio);

  // -- requests --
  void handle_client_request(MClientRequest *m);

  void journal_and_reply(MDRequest *mdr, CInode *tracei, CDentry *tracedn, 
			 LogEvent *le, Context *fin);
  void dispatch_client_request(MDRequest *mdr);
  void early_reply(MDRequest *mdr, CInode *tracei, CDentry *tracedn);
  void reply_request(MDRequest *mdr, int r = 0, CInode *tracei = 0, CDentry *tracedn = 0);
  void reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei = 0, CDentry *tracedn = 0);
  void set_trace_dist(Session *session, MClientReply *reply, CInode *in, CDentry *dn,
		      snapid_t snapid,
		      int num_dentries_wanted,
		      MDRequest *mdr);

  void encode_empty_dirstat(bufferlist& bl);
  void encode_infinite_lease(bufferlist& bl);
  void encode_null_lease(bufferlist& bl);

  void handle_slave_request(MMDSSlaveRequest *m);
  void handle_slave_request_reply(MMDSSlaveRequest *m);
  void dispatch_slave_request(MDRequest *mdr);
  void handle_slave_auth_pin(MDRequest *mdr);
  void handle_slave_auth_pin_ack(MDRequest *mdr, MMDSSlaveRequest *ack);

  // some helpers
  CDir *validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname);
  CDir *traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath);
  CDentry *prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist=false);
  CDentry *prepare_stray_dentry(MDRequest *mdr, CInode *in);
  CInode* prepare_new_inode(MDRequest *mdr, CDir *dir, inodeno_t useino, unsigned mode,
			    ceph_file_layout *layout=NULL);
  void journal_allocated_inos(MDRequest *mdr, EMetaBlob *blob);
  void apply_allocated_inos(MDRequest *mdr);

  CInode* rdlock_path_pin_ref(MDRequest *mdr, int n, set<SimpleLock*>& rdlocks, bool want_auth,
			      bool no_want_auth=false,
			      ceph_file_layout **layout=NULL,
			      bool no_lookup=false);
  CDentry* rdlock_path_xlock_dentry(MDRequest *mdr, int n, set<SimpleLock*>& rdlocks, set<SimpleLock*>& wrlocks, 
				    set<SimpleLock*>& xlocks, bool okexist, bool mustexist, bool alwaysxlock,
				    ceph_file_layout **layout=NULL);

  CDir* try_open_auth_dirfrag(CInode *diri, frag_t fg, MDRequest *mdr);


  // requests on existing inodes.
  void handle_client_getattr(MDRequest *mdr, bool is_lookup);
  void handle_client_lookup_parent(MDRequest *mdr);
  void handle_client_lookup_hash(MDRequest *mdr);
  void _lookup_hash_2(MDRequest *mdr, int r);
  void _lookup_hash_3(MDRequest *mdr, int r);
  void handle_client_lookup_ino(MDRequest *mdr);
  void _lookup_ino(MDRequest *mdr);
  void _lookup_ino_2(MDRequest *mdr, int r);
  void _lookup_ino_3(MDRequest *mdr, int r);
  void handle_client_readdir(MDRequest *mdr);
  void handle_client_file_setlock(MDRequest *mdr);
  void handle_client_file_readlock(MDRequest *mdr);

  void handle_client_setattr(MDRequest *mdr);
  void handle_client_setlayout(MDRequest *mdr);
  void handle_client_setdirlayout(MDRequest *mdr);

  int parse_layout_vxattr(string name, string value, ceph_file_layout *layout);
  void handle_set_vxattr(MDRequest *mdr, CInode *cur,
			 ceph_file_layout *dir_layout,
			 set<SimpleLock*> rdlocks,
			 set<SimpleLock*> wrlocks,
			 set<SimpleLock*> xlocks);
  void handle_remove_vxattr(MDRequest *mdr, CInode *cur,
			    set<SimpleLock*> rdlocks,
			    set<SimpleLock*> wrlocks,
			    set<SimpleLock*> xlocks);
  void handle_client_setxattr(MDRequest *mdr);
  void handle_client_removexattr(MDRequest *mdr);

  void handle_client_fsync(MDRequest *mdr);

  // open
  void handle_client_open(MDRequest *mdr);
  void handle_client_openc(MDRequest *mdr);  // O_CREAT variant.
  void do_open_truncate(MDRequest *mdr, int cmode);  // O_TRUNC variant.

  // namespace changes
  void handle_client_mknod(MDRequest *mdr);
  void handle_client_mkdir(MDRequest *mdr);
  void handle_client_symlink(MDRequest *mdr);

  // link
  void handle_client_link(MDRequest *mdr);
  void _link_local(MDRequest *mdr, CDentry *dn, CInode *targeti);
  void _link_local_finish(MDRequest *mdr,
			  CDentry *dn, CInode *targeti,
			  version_t, version_t);

  void _link_remote(MDRequest *mdr, bool inc, CDentry *dn, CInode *targeti);
  void _link_remote_finish(MDRequest *mdr, bool inc, CDentry *dn, CInode *targeti,
			   version_t);

  void handle_slave_link_prep(MDRequest *mdr);
  void _logged_slave_link(MDRequest *mdr, CInode *targeti);
  void _commit_slave_link(MDRequest *mdr, int r, CInode *targeti);
  void _committed_slave(MDRequest *mdr);  // use for rename, too
  void handle_slave_link_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m);
  void do_link_rollback(bufferlist &rbl, int master, MDRequest *mdr);
  void _link_rollback_finish(Mutation *mut, MDRequest *mdr);

  // unlink
  void handle_client_unlink(MDRequest *mdr);
  bool _dir_is_nonempty_unlocked(MDRequest *mdr, CInode *rmdiri);
  bool _dir_is_nonempty(MDRequest *mdr, CInode *rmdiri);
  void _unlink_local(MDRequest *mdr, CDentry *dn, CDentry *straydn);
  void _unlink_local_finish(MDRequest *mdr, 
			    CDentry *dn, CDentry *straydn,
			    version_t);
  bool _rmdir_prepare_witness(MDRequest *mdr, int who, CDentry *dn, CDentry *straydn);
  void handle_slave_rmdir_prep(MDRequest *mdr);
  void _logged_slave_rmdir(MDRequest *mdr, CDentry *srcdn, CDentry *straydn);
  void _commit_slave_rmdir(MDRequest *mdr, int r);
  void handle_slave_rmdir_prep_ack(MDRequest *mdr, MMDSSlaveRequest *ack);
  void do_rmdir_rollback(bufferlist &rbl, int master, MDRequest *mdr);
  void _rmdir_rollback_finish(MDRequest *mdr, metareqid_t reqid, CDentry *dn, CDentry *straydn);

  // rename
  void handle_client_rename(MDRequest *mdr);
  void _rename_finish(MDRequest *mdr,
		      CDentry *srcdn, CDentry *destdn, CDentry *straydn);

  void handle_client_lssnap(MDRequest *mdr);
  void handle_client_mksnap(MDRequest *mdr);
  void _mksnap_finish(MDRequest *mdr, CInode *diri, SnapInfo &info);
  void handle_client_rmsnap(MDRequest *mdr);
  void _rmsnap_finish(MDRequest *mdr, CInode *diri, snapid_t snapid);

  // helpers
  bool _rename_prepare_witness(MDRequest *mdr, int who, set<int> &witnesse,
			       CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  version_t _rename_prepare_import(MDRequest *mdr, CDentry *srcdn, bufferlist *client_map_bl);
  bool _need_force_journal(CInode *diri, bool empty);
  void _rename_prepare(MDRequest *mdr,
		       EMetaBlob *metablob, bufferlist *client_map_bl,
		       CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  /* set not_journaling=true if you're going to discard the results --
   * this bypasses the asserts to make sure we're journaling the right
   * things on the right nodes */
  void _rename_apply(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn); 

  // slaving
  void handle_slave_rename_prep(MDRequest *mdr);
  void handle_slave_rename_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m);
  void _logged_slave_rename(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void _commit_slave_rename(MDRequest *mdr, int r, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void do_rename_rollback(bufferlist &rbl, int master, MDRequest *mdr, bool finish_mdr=false);
  void _rename_rollback_finish(Mutation *mut, MDRequest *mdr, CDentry *srcdn, version_t srcdnpv,
			       CDentry *destdn, CDentry *staydn, bool finish_mdr);

};

#endif
