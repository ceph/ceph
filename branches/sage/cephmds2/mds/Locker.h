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
 
 public:
  Locker(MDS *m, MDCache *c) : mds(m), mdcache(c) {}  

  void dispatch(Message *m);

  void send_lock_message(CInode *in, int msg, int type);
  void send_lock_message(CInode *in, int msg, int type, bufferlist& data);
  void send_lock_message(CDentry *dn, int msg);

  // -- locks --
  bool acquire_locks(MDRequest *mdr,
		     set<CDentry*> &dentry_rdlocks,
		     set<CDentry*> &dentry_xlocks,
		     set<CInode*> &inode_hard_rdlocks,
		     set<CInode*> &inode_hard_xlocks);


  // high level interface
 public:
  bool inode_hard_rdlock_try(CInode *in, Context *con);
  bool inode_hard_rdlock_start(CInode *in, MDRequest *mdr);
  void inode_hard_rdlock_finish(CInode *in, MDRequest *mdr);
  bool inode_hard_xlock_start(CInode *in, MDRequest *mdr);
  void inode_hard_xlock_finish(CInode *in, MDRequest *mdr);
  bool inode_file_rdlock_start(CInode *in, MDRequest *mdr);
  void inode_file_rdlock_finish(CInode *in, MDRequest *mdr);
  bool inode_file_xlock_start(CInode *in, MDRequest *mdr);
  void inode_file_xlock_finish(CInode *in, MDRequest *mdr);

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
  bool issue_caps(CInode *in);

 protected:
  void handle_client_file_caps(class MClientFileCaps *m);

  void request_inode_file_caps(CInode *in);
  void handle_inode_file_caps(class MInodeFileCaps *m);


  // dirs
  void handle_lock_dir(MLock *m);

  // dentry locks
  void _dentry_rdlock_finish(CDentry *dn, MDRequest *mdr);
 public:
  bool dentry_rdlock_start(CDentry *dn, MDRequest *mdr);
  void dentry_rdlock_finish(CDentry *dn, MDRequest *mdr);
  bool dentry_can_rdlock_trace(vector<CDentry*>& trace, MClientRequest *req);
  void dentry_anon_rdlock_trace_start(vector<CDentry*>& trace);
  void dentry_anon_rdlock_trace_finish(vector<CDentry*>& trace);

  bool dentry_xlock_start(CDentry *dn, MDRequest *mdr);
  void dentry_xlock_finish(CDentry *dn, MDRequest *mdr, bool quiet=false);
  //bool dentry_xlock_upgrade_from_rdlock(CDentry *dn, MDRequest *mdr);   // from rdlock
  void dentry_xlock_downgrade_to_rdlock(CDentry *dn, MDRequest *mdr); // to rdlock
  void handle_lock_dn(MLock *m);
  void dentry_xlock_request(CDir *dir, const string& dname, bool create,
                            Message *req, Context *onfinish);
  void dentry_xlock_request_finish(int r,
				   CDir *dir, const string& dname, 
				   Message *req,
				   Context *finisher);


};


#endif
