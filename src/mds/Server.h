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

#ifndef __MDS_SERVER_H
#define __MDS_SERVER_H

#include "MDS.h"

class Logger;
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
  Logger *logger;

public:
  int failed_reconnects;

  Server(MDS *m) : 
    mds(m), 
    mdcache(mds->mdcache), mdlog(mds->mdlog),
    messenger(mds->messenger),
    logger(0),
    failed_reconnects(0) {
  }
  ~Server() {
    delete logger;
  }

  void reopen_logger(utime_t start, bool append);

  // message handler
  void dispatch(Message *m);


  // -- sessions and recovery --
  utime_t  reconnect_start;
  set<client_t> client_reconnect_gather;  // clients i need a reconnect msg from.

  Session *get_session(Message *m);
  void handle_client_session(class MClientSession *m);
  void _session_logged(Session *session, bool open, version_t pv, interval_set<inodeno_t>& inos,version_t piv);
  void _finish_session_purge(Session *);
  version_t prepare_force_open_sessions(map<client_t,entity_inst_t> &cm);
  void finish_force_open_sessions(map<client_t,entity_inst_t> &cm);
  void terminate_sessions();
  void find_idle_sessions();
  void end_session (Session *session);
  void reconnect_clients();
  void handle_client_reconnect(class MClientReconnect *m);
  //void process_reconnect_cap(CInode *in, int from, ceph_mds_cap_reconnect& capinfo);
  void reconnect_gather_finish();
  void reconnect_tick();

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
		      int num_dentries_wanted);

  void encode_empty_dirstat(bufferlist& bl);
  void encode_infinite_lease(bufferlist& bl);
  void encode_null_lease(bufferlist& bl);

  void handle_slave_request(MMDSSlaveRequest *m);
  void dispatch_slave_request(MDRequest *mdr);
  void handle_slave_auth_pin(MDRequest *mdr);
  void handle_slave_auth_pin_ack(MDRequest *mdr, MMDSSlaveRequest *ack);

  // some helpers
  CDir *validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname);
  CDir *traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath);
  CDentry *prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist=false);
  CInode* prepare_new_inode(MDRequest *mdr, CDir *dir, inodeno_t useino,
			    ceph_file_layout *layout=NULL);
  void journal_allocated_inos(MDRequest *mdr, EMetaBlob *blob);
  void apply_allocated_inos(MDRequest *mdr);

  CInode* rdlock_path_pin_ref(MDRequest *mdr, int n, set<SimpleLock*>& rdlocks, bool want_auth);
  CDentry* rdlock_path_xlock_dentry(MDRequest *mdr, int n, set<SimpleLock*>& rdlocks, set<SimpleLock*>& wrlocks, 
				    set<SimpleLock*>& xlocks, bool okexist, bool mustexist, bool alwaysxlock);

  CDir* try_open_auth_dirfrag(CInode *diri, frag_t fg, MDRequest *mdr);


  // requests on existing inodes.
  void handle_client_stat(MDRequest *mdr);
  void handle_client_lookup_parent(MDRequest *mdr);
  void handle_client_lookup_hash(MDRequest *mdr);
  void handle_client_readdir(MDRequest *mdr);

  void handle_client_setattr(MDRequest *mdr);
  void handle_client_setlayout(MDRequest *mdr);
  void handle_client_setxattr(MDRequest *mdr);
  void handle_client_removexattr(MDRequest *mdr);

  void handle_client_fsync(MDRequest *mdr);

  // open
  void handle_client_open(MDRequest *mdr);
  void handle_client_openc(MDRequest *mdr);  // O_CREAT variant.
  void handle_client_opent(MDRequest *mdr, int cmode);  // O_TRUNC variant.

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
  bool _dir_is_nonempty(MDRequest *mdr, CInode *rmdiri);
  void _unlink_local(MDRequest *mdr, CDentry *dn, CDentry *straydn);
  void _unlink_local_finish(MDRequest *mdr, 
			    CDentry *dn, CDentry *straydn,
			    version_t);    

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
  void _rename_prepare_witness(MDRequest *mdr, int who,
			       CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  version_t _rename_prepare_import(MDRequest *mdr, CDentry *srcdn, bufferlist *client_map_bl);
  void _rename_prepare(MDRequest *mdr,
		       EMetaBlob *metablob, bufferlist *client_map_bl,
		       CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void _rename_apply(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn); 

  // slaving
  void handle_slave_rename_prep(MDRequest *mdr);
  void handle_slave_rename_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m);
  void _logged_slave_rename(MDRequest *mdr, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void _commit_slave_rename(MDRequest *mdr, int r, CDentry *srcdn, CDentry *destdn, CDentry *straydn);
  void do_rename_rollback(bufferlist &rbl, int master, MDRequest *mdr);
  void _rename_rollback_finish(Mutation *mut, MDRequest *mdr, CInode *in, CDir *olddir);

};




#endif
