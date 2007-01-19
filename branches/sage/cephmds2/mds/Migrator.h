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

#ifndef __MDS_MIGRATOR_H
#define __MDS_MIGRATOR_H

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

class MExportDirDiscover;
class MExportDirDiscoverAck;
class MExportDirPrep;
class MExportDirPrepAck;
class MExportDirWarning;
class MExportDir;
class MExportDirNotify;
class MExportDirNotifyAck;
class MExportDirFinish;

class MHashDirDiscover;
class MHashDirDiscoverAck;
class MHashDirPrep;
class MHashDirPrepAck;
class MHashDir;
class MHashDirAck;
class MHashDirNotify;

class MUnhashDirPrep;
class MUnhashDirPrepAck;
class MUnhashDir;
class MUnhashDirAck;
class MUnhashDirNotify;
class MUnhashDirNotifyAck;

class EImportStart;

class Migrator {
private:
  MDS *mds;
  MDCache *cache;

  // export fun
  map<CDir*, set<int> >  export_notify_ack_waiting; // nodes i am waiting to get export_notify_ack's from
  map<CDir*, list<inodeno_t> > export_proxy_inos;
  map<CDir*, list<inodeno_t> > export_proxy_dirinos;
  
  set<inodeno_t>                    stray_export_warnings; // notifies i haven't seen
  map<inodeno_t, MExportDirNotify*> stray_export_notifies;
  
  // import muck
  map<inodeno_t, set<CDir*> > import_freeze_leaves;

  // hashing madness
  multimap<CDir*, int>   unhash_waiting;  // nodes i am waiting for UnhashDirAck's from
  multimap<inodeno_t, inodeno_t>    import_hashed_replicate_waiting;  // nodes i am waiting to discover to complete my import of a hashed dir
  // maps frozen_dir_ino's to waiting-for-discover ino's.
  multimap<inodeno_t, inodeno_t>    import_hashed_frozen_waiting;    // dirs i froze (for the above)
    
public:
  // -- cons --
  Migrator(MDS *m, MDCache *c) : mds(m), cache(c) {}

  void dispatch(Message*);

  // -- import/export --
  // exporter
 public:
  void export_dir(CDir *dir,
                  int mds);
  void export_empty_import(CDir *dir);

  void encode_export_inode(CInode *in, bufferlist& enc_state, int newauth);
  void decode_import_inode(CDentry *dn, bufferlist& bl, int &off, int oldauth);

 protected:
  void handle_export_dir_discover_ack(MExportDirDiscoverAck *m);
  void export_dir_frozen(CDir *dir, int dest);
  void handle_export_dir_prep_ack(MExportDirPrepAck *m);
  void export_dir_go(CDir *dir,
                     int dest);
  int export_dir_walk(MExportDir *req,
                      class C_Contexts *fin,
                      CDir *basedir,
                      CDir *dir,
                      int newauth);
  void export_dir_finish(CDir *dir);
  void handle_export_dir_notify_ack(MExportDirNotifyAck *m);
    
  friend class C_MDC_ExportFreeze;
  friend class C_MDS_ExportFinishLogged;

  // importer
  void handle_export_dir_discover(MExportDirDiscover *m);
  void handle_export_dir_discover_2(MExportDirDiscover *m, CInode *in, int r);
  void handle_export_dir_prep(MExportDirPrep *m);
  void handle_export_dir(MExportDir *m);
  void import_dir_logged_start(CDir *dir, int from,
			       list<inodeno_t> &imported_subdirs,
			       list<inodeno_t> &exports);
  void import_dir_logged_finish(CDir *dir);
  void handle_export_dir_finish(MExportDirFinish *m);
  int import_dir_block(bufferlist& bl,
                       int& off,
                       int oldauth,
                       CDir *import_root,
                       list<inodeno_t>& imported_subdirs,
		       EImportStart *le);
  void got_hashed_replica(CDir *import,
                          inodeno_t dir_ino,
                          inodeno_t replica_ino);

  friend class C_MDC_ExportDirDiscover;
  friend class C_MDS_ImportDirLoggedStart;
  friend class C_MDS_ImportDirLoggedFinish;

  // bystander
  void handle_export_dir_warning(MExportDirWarning *m);
  void handle_export_dir_notify(MExportDirNotify *m);

  void show_imports();

  // -- hashed directories --

  // HASH
 public:
  void hash_dir(CDir *dir);  // on auth
 protected:
  map< CDir*, set<int> >             hash_gather;
  map< CDir*, map< int, set<int> > > hash_notify_gather;
  map< CDir*, list<CInode*> >        hash_proxy_inos;

  // hash on auth
  void handle_hash_dir_discover_ack(MHashDirDiscoverAck *m);
  void hash_dir_complete(CDir *dir);
  void hash_dir_frozen(CDir *dir);
  void handle_hash_dir_prep_ack(MHashDirPrepAck *m);
  void hash_dir_go(CDir *dir);
  void handle_hash_dir_ack(MHashDirAck *m);
  void hash_dir_finish(CDir *dir);
  friend class C_MDC_HashFreeze;
  friend class C_MDC_HashComplete;

  // auth and non-auth
  void handle_hash_dir_notify(MHashDirNotify *m);

  // hash on non-auth
  void handle_hash_dir_discover(MHashDirDiscover *m);
  void handle_hash_dir_discover_2(MHashDirDiscover *m, CInode *in, int r);
  void handle_hash_dir_prep(MHashDirPrep *m);
  void handle_hash_dir(MHashDir *m);
  friend class C_MDC_HashDirDiscover;

  // UNHASH
 public:
  void unhash_dir(CDir *dir);   // on auth
 protected:
  map< CDir*, list<MUnhashDirAck*> > unhash_content;
  void import_hashed_content(CDir *dir, bufferlist& bl, int nden, int oldauth);

  // unhash on auth
  void unhash_dir_frozen(CDir *dir);
  void unhash_dir_prep(CDir *dir);
  void handle_unhash_dir_prep_ack(MUnhashDirPrepAck *m);
  void unhash_dir_go(CDir *dir);
  void handle_unhash_dir_ack(MUnhashDirAck *m);
  void handle_unhash_dir_notify_ack(MUnhashDirNotifyAck *m);
  void unhash_dir_finish(CDir *dir);
  friend class C_MDC_UnhashFreeze;
  friend class C_MDC_UnhashComplete;

  // unhash on all
  void unhash_dir_complete(CDir *dir);

  // unhash on non-auth
  void handle_unhash_dir_prep(MUnhashDirPrep *m);
  void unhash_dir_prep_frozen(CDir *dir);
  void unhash_dir_prep_finish(CDir *dir);
  void handle_unhash_dir(MUnhashDir *m);
  void handle_unhash_dir_notify(MUnhashDirNotify *m);
  friend class C_MDC_UnhashPrepFreeze;


};


#endif
