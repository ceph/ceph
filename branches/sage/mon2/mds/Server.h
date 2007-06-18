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

class LogEvent;
class C_MDS_rename_local_finish;
class MDRequest;

class Server {
  MDS *mds;
  MDCache *mdcache;
  MDLog *mdlog;
  Messenger *messenger;

public:
  Server(MDS *m) : 
    mds(m), 
    mdcache(mds->mdcache), mdlog(mds->mdlog),
    messenger(mds->messenger) {
  }

  // message handler
  void dispatch(Message *m);


  // -- sessions and recovery --
  utime_t  reconnect_start;
  set<int> client_reconnect_gather;  // clients i need a reconnect msg from.
  set<CInode*> reconnected_open_files;
  
  void handle_client_session(class MClientSession *m);
  void _session_logged(entity_inst_t ci, bool open, version_t cmapv);
  void reconnect_clients();
  void handle_client_reconnect(class MClientReconnect *m);
  void client_reconnect_failure(int from);
  void reconnect_finish();
  void terminate_sessions();
  
  // -- requests --
  void handle_client_request(MClientRequest *m);

  void dispatch_request(MDRequest *mdr);
  void reply_request(MDRequest *mdr, int r = 0, CInode *tracei = 0);
  void reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei);

  // some helpers
  CDir *validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname);
  CDir *traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath);
  CDentry *prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist=false);
  CInode* prepare_new_inode(MClientRequest *req, CDir *dir);

  CInode* rdlock_path_pin_ref(MDRequest *mdr, bool want_auth);
  CDentry* rdlock_path_xlock_dentry(MDRequest *mdr, bool okexist, bool mustexist);

  CDir* try_open_auth_dir(CInode *diri, frag_t fg, MDRequest *mdr);
  //CDir* try_open_dir(CInode *diri, frag_t fg, MDRequest *mdr);

  version_t predirty_dn_diri(CDentry *dn, class EMetaBlob *blob, utime_t mtime);
  void dirty_dn_diri(CDentry *dn, version_t dirpv, utime_t mtime);

  // requests on existing inodes.
  void handle_client_stat(MDRequest *mdr);
  void handle_client_utime(MDRequest *mdr);
  void handle_client_chmod(MDRequest *mdr);
  void handle_client_chown(MDRequest *mdr);
  void handle_client_readdir(MDRequest *mdr);
  int encode_dir_contents(CDir *dir, list<class InodeStat*>& inls, list<string>& dnls);
  void handle_client_truncate(MDRequest *mdr);
  void handle_client_fsync(MDRequest *mdr);

  // open
  void handle_client_open(MDRequest *mdr);
  void handle_client_openc(MDRequest *mdr);  // O_CREAT variant.
  void handle_client_opent(MDRequest *mdr);  // O_TRUNC variant.
  void _do_open(MDRequest *mdr, CInode *ref);

  set<CInode*> journal_open_queue; // to be journal
  list<Context*> journal_open_waiters;
  void queue_journal_open(CInode *in);
  void add_journal_open_waiter(Context *c) {
    journal_open_waiters.push_back(c);
  }
  void maybe_journal_opens() {
    if (journal_open_queue.size() >= (unsigned)g_conf.mds_log_eopen_size)
      journal_opens();
  }
  void journal_opens();

  // namespace changes
  void handle_client_mknod(MDRequest *mdr);
  void handle_client_mkdir(MDRequest *mdr);
  void handle_client_symlink(MDRequest *mdr);

  // link
  void handle_client_link(MDRequest *mdr);
  void _link_local(MDRequest *mdr, CDentry *dn, CInode *targeti);
  void _link_local_finish(MDRequest *mdr,
			  CDentry *dn, CInode *targeti,
			  version_t, utime_t, version_t, version_t);
  void _link_remote(MDRequest *mdr, CDentry *dn, CInode *targeti);

  // unlink
  void handle_client_unlink(MDRequest *mdr);
  bool _verify_rmdir(MDRequest *mdr, CInode *rmdiri);
  void _unlink_local(MDRequest *mdr, CDentry *dn);
  void _unlink_local_finish(MDRequest *mdr, 
			    CDentry *dn, CDentry *straydn,
			    version_t, utime_t, version_t, version_t);    
  void _unlink_remote(MDRequest *mdr, CDentry *dn);

  // rename
  bool _rename_open_dn(CDir *dir, CDentry *dn, bool mustexist, MDRequest *mdr);
  void handle_client_rename(MDRequest *mdr);
  void handle_client_rename_2(MDRequest *mdr,
                              CDentry *srcdn,
                              filepath& destpath,
                              vector<CDentry*>& trace,
                              int r);
  void _rename_local(MDRequest *mdr, CDentry *srcdn, CDentry *destdn);
  void _rename_local_reanchored(LogEvent *le, C_MDS_rename_local_finish *fin, 
				version_t atid1, version_t atid2);
  void _rename_local_finish(MDRequest *mdr,
			    CDentry *srcdn, CDentry *destdn, CDentry *straydn,
			    version_t srcpv, version_t destpv, version_t straypv, version_t ipv,
			    version_t ddirpv, version_t sdirpv, utime_t ictime,
			    version_t atid1, version_t atid2);
};




#endif
