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

#ifndef __MDS_LOCKER_H
#define __MDS_LOCKER_H

#include "include/types.h"

#include <map>
#include <list>
#include <set>
using std::map;
using std::list;
using std::set;

class MDS;
class CDir;
class CInode;
class CDentry;

class Message;

class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryUnlink;
class MLock;

class MClientRequest;


class Anchor;
class Capability;


class Locker {
private:
  MDS *mds;
  MDCache *mdcache;
  // count of capability id's used
  //int cap_id_count;
 
 public:
  //Locker(MDS *m, MDCache *c) : mds(m), mdcache(c), cap_id_count(0) {}  
  Locker(MDS *m, MDCache *c) : mds(m), mdcache(c) {}

  void dispatch(Message *m);

  void send_lock_message(CInode *in, int msg, int type);
  void send_lock_message(CInode *in, int msg, int type, bufferlist& data);
  void send_lock_message(CDentry *dn, int msg);

  // -- locks --
  // high level interface
 public:
  bool inode_hard_read_try(CInode *in, Context *con);
  bool inode_hard_read_start(CInode *in, MClientRequest *m);
  void inode_hard_read_finish(CInode *in);
  bool inode_hard_write_start(CInode *in, MClientRequest *m);
  void inode_hard_write_finish(CInode *in);
  bool inode_file_read_start(CInode *in, MClientRequest *m);
  void inode_file_read_finish(CInode *in);
  bool inode_file_write_start(CInode *in, MClientRequest *m);
  void inode_file_write_finish(CInode *in);

  void inode_hard_eval(CInode *in);
  void inode_file_eval(CInode *in);

 protected:
  void inode_hard_mode(CInode *in, int mode);
  void inode_file_mode(CInode *in, int mode);

  // low level triggers
  void inode_hard_sync(CInode *in);
  void inode_hard_lock(CInode *in);
  bool inode_file_sync(CInode *in);
  void inode_file_lock(CInode *in);
  void inode_file_mixed(CInode *in);
  void inode_file_loner(CInode *in);

  // messengers
  void handle_lock(MLock *m);
  void handle_lock_inode_hard(MLock *m);
  void handle_lock_inode_file(MLock *m);

  // -- file i/o --
 public:
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, MClientRequest *req);
  // returns a secured extended capability for the user
  ExtCap* issue_new_extcaps(CInode *in, int mode, MClientRequest *req);
  // sign the extcap
  bool issue_caps(CInode *in);

 protected:
  void handle_client_file_caps(class MClientFileCaps *m);

  void request_inode_file_caps(CInode *in);
  void handle_inode_file_caps(class MInodeFileCaps *m);


  // dirs
  void handle_lock_dir(MLock *m);

  // dentry locks
 public:
  bool dentry_xlock_start(CDentry *dn, 
                          Message *m, CInode *ref);
  void dentry_xlock_finish(CDentry *dn, bool quiet=false);
  void handle_lock_dn(MLock *m);
  void dentry_xlock_request(CDir *dir, string& dname, bool create,
                            Message *req, Context *onfinish);
  void dentry_xlock_request_finish(int r,
				   CDir *dir, string& dname, 
				   Message *req,
				   Context *finisher);


};


#endif
