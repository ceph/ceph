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
#include "messages/MClientUpdate.h"
#include "messages/MClientUpdateReply.h"
#include "messages/MClientRenewal.h"
//#include "messages/MClientRenewalReply.h"

class LogEvent;

class Server {
  MDS *mds;
  MDCache *mdcache;
  MDLog *mdlog;
  Messenger *messenger;

  __uint64_t stat_ops;


public:
  Server(MDS *m) : 
    mds(m), 
    mdcache(mds->mdcache), mdlog(mds->mdlog),
    messenger(mds->messenger),
    stat_ops(0) {
  }

  void dispatch(Message *m);

  // generic request helpers
  void reply_request(MClientRequest *req, int r = 0, CInode *tracei = 0);
  void reply_request(MClientRequest *req, MClientReply *reply, CInode *tracei);
  
  void submit_update(MClientRequest *req, CInode *wrlockedi,
		     LogEvent *event,
		     Context *oncommit);

  void commit_request(MClientRequest *req,
                      MClientReply *reply,
                      CInode *tracei,
                      LogEvent *event,
                      LogEvent *event2 = 0);
  
  bool try_open_dir(CInode *in, MClientRequest *req);


  // clients
  void handle_client_mount(class MClientMount *m);
  void handle_client_unmount(Message *m);

  void handle_client_request(MClientRequest *m);
  void handle_client_request_2(MClientRequest *req, 
                               vector<CDentry*>& trace,
                               int r);

  // group updates
  void handle_client_update(MClientUpdate *m);
  
  // renewal requests
  void handle_client_renewal(MClientRenewal *m);
  
  // fs ops
  void handle_client_fstat(MClientRequest *req);

  // requests
  void dispatch_request(Message *m, CInode *ref);

  // inode request *req, CInode *ref;
  void handle_client_stat(MClientRequest *req, CInode *ref);
  void handle_client_utime(MClientRequest *req, CInode *ref);
  void handle_client_inode_soft_update_2(MClientRequest *req,
                                         MClientReply *reply,
                                         CInode *ref);
  void handle_client_chmod(MClientRequest *req, CInode *ref);
  void handle_client_chown(MClientRequest *req, CInode *ref);
  void handle_client_inode_hard_update_2(MClientRequest *req,
                                         MClientReply *reply,
                                         CInode *ref);

  // readdir
  void handle_client_readdir(MClientRequest *req, CInode *ref);
  int encode_dir_contents(CDir *dir, 
                          list<class InodeStat*>& inls,
                          list<string>& dnls);
  void handle_hash_readdir(MHashReaddir *m);
  void handle_hash_readdir_reply(MHashReaddirReply *m);
  void finish_hash_readdir(MClientRequest *req, CDir *dir); 

  // namespace changes
  void handle_client_mknod(MClientRequest *req, CInode *ref);
  void handle_client_link(MClientRequest *req, CInode *ref);
  void handle_client_link_2(int r, MClientRequest *req, CInode *ref, vector<CDentry*>& trace);
  void handle_client_link_finish(MClientRequest *req, CInode *ref,
                                 CDentry *dn, CInode *targeti);

  void handle_client_unlink(MClientRequest *req, CInode *ref);
  void handle_client_rename(MClientRequest *req, CInode *ref);
  void handle_client_rename_2(MClientRequest *req,
                              CInode *ref,
                              CInode *srcdiri,
                              CDir *srcdir,
                              CDentry *srcdn,
                              filepath& destpath,
                              vector<CDentry*>& trace,
                              int r);
  void handle_client_rename_local(MClientRequest *req, CInode *ref,
                                  string& srcpath, CInode *srcdiri, CDentry *srcdn, 
                                  string& destpath, CDir *destdir, CDentry *destdn, string& name);

  void handle_client_mkdir(MClientRequest *req, CInode *ref);
  void handle_client_rmdir(MClientRequest *req, CInode *ref);
  void handle_client_symlink(MClientRequest *req, CInode *ref);

  // file
  void handle_client_open(MClientRequest *req, CInode *ref);
  void handle_client_openc(MClientRequest *req, CInode *ref);
  void handle_client_release(MClientRequest *req, CInode *in);  
  void handle_client_truncate(MClientRequest *req, CInode *in);
  void handle_client_fsync(MClientRequest *req, CInode *in);


  // some helpers
  CInode *mknod(MClientRequest *req, CInode *ref, bool okexist=false);  // used by mknod, symlink, mkdir, openc

  CDir *validate_new_dentry_dir(MClientRequest *req, CInode *diri, string& dname);
  int prepare_mknod(MClientRequest *req, CInode *diri, 
		    CInode **pin, CDentry **pdn, 
		    bool okexist=false);

  // prediction stuff
  set<inodeno_t> parse_predictions(string pred_string);
  int put_bl_ss(bufferlist& bl);
  int get_bl_ss(bufferlist& bl);



};

class C_MDS_RetryRequest : public Context {
  MDS *mds;
  Message *req;   // MClientRequest or MLock
  CInode *ref;
 public:
  C_MDS_RetryRequest(MDS *mds, Message *req, CInode *ref) {
    assert(ref);
    this->mds = mds;
    this->req = req;
    this->ref = ref;
  }
  virtual void finish(int r) {
    mds->server->dispatch_request(req, ref);
  }
};



#endif
