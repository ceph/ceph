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

#ifndef __MDS_RENAMER_H
#define __MDS_RENAMER_H

#include "include/types.h"

#include <map>
#include <set>
using std::map;
using std::set;

class MDS;
class MDCache;
class CDentry;
class CInode;
class CDir;

class Message;
class MRenameWarning;
class MRenameNotify;
class MRenameNotifyAck;
class MRename;
class MRenamePrep;
class MRenameReq;
class MRenameAck;

class Renamer {
  MDS *mds;
  MDCache *cache;

  // rename fun
  set<inodeno_t>                    stray_rename_warnings; // notifies i haven't seen
  map<inodeno_t, MRenameNotify*>    stray_rename_notifies;

  map<inodeno_t, set<int> >         rename_waiting_for_ack;



  void fix_renamed_dir(CDir *srcdir,
                       CInode *in,
                       CDir *destdir,
                       bool authchanged,   // _inode_ auth changed
                       int dirauth=-1);    // dirauth (for certain cases)
  

public:
  Renamer(MDS *m, MDCache *c) : mds(m), cache(c) {}
  
  void dispatch(Message *m);

  // RENAME
  // initiator
 public:
  void file_rename(CDentry *srcdn, CDentry *destdn, Context *c);
 protected:
  void handle_rename_ack(MRenameAck *m);              // dest -> init (almost always)
  void file_rename_finish(CDir *srcdir, CInode *in, Context *c);
  friend class C_MDC_RenameAck;

  // src
  void handle_rename_req(MRenameReq *m);              // dest -> src
  void file_rename_foreign_src(CDentry *srcdn, 
                               inodeno_t destdirino, string& destname, string& destpath, int destauth, 
                               int initiator);
  void file_rename_warn(CInode *in, set<int>& notify);
  void handle_rename_notify_ack(MRenameNotifyAck *m); // bystanders -> src
  void file_rename_ack(CInode *in, int initiator);
  friend class C_MDC_RenameNotifyAck;

  // dest
  void handle_rename_prep(MRenamePrep *m);            // init -> dest
  void handle_rename(MRename *m);                     // src -> dest
  void file_rename_notify(CInode *in, 
                          CDir *srcdir, string& srcname, CDir *destdir, string& destname,
                          set<int>& notify, int srcauth);

  // bystander
  void handle_rename_warning(MRenameWarning *m);      // src -> bystanders
  void handle_rename_notify(MRenameNotify *m);        // dest -> bystanders


};

#endif


