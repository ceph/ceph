// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

  void dispatch(Message *m);

  // message handlers
  void handle_client_mount(class MClientMount *m);
  void handle_client_unmount(Message *m);
  void handle_client_request(MClientRequest *m);
  
  // requests
  void dispatch_request(MDRequest *mdr);
  void reply_request(MDRequest *mdr, int r = 0, CInode *tracei = 0);
  void reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei);

  // some helpers
  CInode *request_pin_ref(MDRequest *mdr);
  CDir *validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname);
  CDir *traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath);
  CDentry *prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist=false);
  CInode* prepare_new_inode(MClientRequest *req, CDir *dir);
  CDentry* rdlock_path_xlock_dentry(MDRequest *mdr, bool okexist, bool mustexist);
  CDir* try_open_auth_dir(CInode *diri, frag_t fg, MDRequest *mdr);
  
  CDir* try_open_dir(CInode *diri, frag_t fg, MDRequest *mdr);

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

  // namespace changes
  void handle_client_mknod(MDRequest *mdr);
  void handle_client_mkdir(MDRequest *mdr);
  void handle_client_symlink(MDRequest *mdr);

  // link
  void handle_client_link(MDRequest *mdr);
  void _link_local(MDRequest *mdr, CDentry *dn, CInode *targeti);
  void _link_local_finish(MDRequest *mdr,
			  CDentry *dn, CInode *targeti,
			  version_t, time_t, version_t);
  void _link_remote(MDRequest *mdr, CDentry *dn, CInode *targeti);

  // unlink
  void handle_client_unlink(MDRequest *mdr);
  bool _verify_rmdir(MDRequest *mdr, CInode *rmdiri);
  void _unlink_local(MDRequest *mdr, CDentry *dn);
  void _unlink_local_finish(MDRequest *mdr, 
			    CDentry *dn, CDentry *straydn,
			    version_t, time_t, version_t);    
  void _unlink_remote(MDRequest *mdr, CDentry *dn);

  // rename
  bool _rename_open_dn(CDir *dir, CDentry *dn, bool mustexist, MDRequest *mdr);
  void handle_client_rename(MDRequest *mdr);
  void handle_client_rename_2(MDRequest *mdr,
                              CDentry *srcdn,
                              filepath& destpath,
                              vector<CDentry*>& trace,
                              int r);
  void _rename_local(MDRequest *mdr,
		     CDentry *srcdn, 
		     CDir *destdir, CDentry *destdn, const string& destname);
  void _rename_local_reanchored(LogEvent *le, C_MDS_rename_local_finish *fin, 
				version_t atid1, version_t atid2);
  void _rename_local_finish(MDRequest *mdr,
			    CDentry *srcdn, CDentry *destdn, CDentry *straydn,
			    version_t srcpv, version_t destpv, version_t straypv, version_t ipv,
			    time_t ictime,
			    version_t atid1, version_t atid2);




};




#endif
