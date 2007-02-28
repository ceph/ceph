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

class MExportDir;
class MExportDirDiscover;
class MExportDirDiscoverAck;
class MExportDirPrep;
class MExportDirPrepAck;
class MExportDirWarning;
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

  // -- exports --
  // export stages.  used to clean up intelligently if there's a failure.
  const static int EXPORT_DISCOVERING   = 1;  // dest is disovering export dir
  const static int EXPORT_FREEZING      = 2;  // we're freezing the dir tree
  const static int EXPORT_LOGGINGSTART  = 3;  // we're logging EExportStart
  const static int EXPORT_PREPPING      = 4;  // sending dest spanning tree to export bounds
  const static int EXPORT_EXPORTING     = 5;  // sent actual export, waiting for acks
  const static int EXPORT_LOGGINGFINISH = 6; // logging EExportFinish
  
  // export fun
  map<CDir*, int>              export_state;
  map<CDir*, int>              export_peer;
  map<CDir*, set<CDir*> >      export_bounds;
  map<CDir*, list<bufferlist> > export_data;   // only during EXPORTING state
  map<CDir*, set<int> >        export_notify_ack_waiting; // nodes i am waiting to get export_notify_ack's from
  map<CDir*, list<inodeno_t> > export_proxy_inos;
  map<CDir*, list<inodeno_t> > export_proxy_dirinos;
  
  map<CDir*, list<Context*> >  export_finish_waiters;

  set<inodeno_t>                    stray_export_warnings; // notifies i haven't seen
  map<inodeno_t, MExportDirNotify*> stray_export_notifies;
  

  // -- imports --
  const static int IMPORT_DISCOVERED    = 1; // waiting for prep
  const static int IMPORT_PREPPING      = 2; // opening dirs on bounds
  const static int IMPORT_PREPPED       = 3; // opened bounds, waiting for import
  const static int IMPORT_LOGGINGSTART  = 4; // got import, logging EImportStart
  const static int IMPORT_ACKING        = 5; // logged, sent acks
  const static int IMPORT_LOGGINGFINISH = 6;

  map<inodeno_t,int>             import_state;
  map<inodeno_t,int>             import_peer;
  map<inodeno_t,set<inodeno_t> > import_bounds;


  // -- hashing madness --
  multimap<CDir*, int>   unhash_waiting;  // nodes i am waiting for UnhashDirAck's from
  multimap<inodeno_t, inodeno_t>    import_hashed_replicate_waiting;  // nodes i am waiting to discover to complete my import of a hashed dir
  // maps frozen_dir_ino's to waiting-for-discover ino's.
  multimap<inodeno_t, inodeno_t>    import_hashed_frozen_waiting;    // dirs i froze (for the above)



public:
  // -- cons --
  Migrator(MDS *m, MDCache *c) : mds(m), cache(c) {}

  void dispatch(Message*);

  
  // -- status --
  int is_exporting(CDir *dir) {
    if (export_state.count(dir)) return export_state[dir];
    return 0;
  }
  bool is_exporting() { return !export_state.empty(); }
  int is_importing(inodeno_t dirino) {
    if (import_state.count(dirino)) return import_state[dirino];
    return 0;
  }
  bool is_importing() { return !import_state.empty(); }
  const set<inodeno_t>& get_import_bounds(inodeno_t base) { 
    assert(import_bounds.count(base));
    return import_bounds[base];
  }


  // -- misc --
  void handle_mds_failure(int who);
  void show_imports();


  // -- import/export --
  // exporter
 public:
  void export_dir(CDir *dir,
                  int mds);
  void export_empty_import(CDir *dir);

  void encode_export_inode(CInode *in, bufferlist& enc_state, int newauth);
  void decode_import_inode(CDentry *dn, bufferlist& bl, int &off, int oldauth);

  void add_export_finish_waiter(CDir *dir, Context *c) {
    export_finish_waiters[dir].push_back(c);
  }
  void clear_export_proxy_pins(CDir *dir);

 protected:
  void handle_export_dir_discover_ack(MExportDirDiscoverAck *m);
  void export_dir_frozen(CDir *dir, int dest);
  void export_dir_frozen_logged(CDir *dir, MExportDirPrep *prep, int dest);
  void handle_export_dir_prep_ack(MExportDirPrepAck *m);
  void export_dir_go(CDir *dir,
                     int dest);
  int encode_export_dir(list<bufferlist>& dirstatelist,
                      class C_Contexts *fin,
                      CDir *basedir,
                      CDir *dir,
                      int newauth);
  void handle_export_dir_notify_ack(MExportDirNotifyAck *m);
  void reverse_export(CDir *dir);
  void export_dir_acked(CDir *dir);
  void export_dir_finish(CDir *dir);

  friend class C_MDC_ExportFreeze;
  friend class C_MDC_ExportStartLogged;
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
  int decode_import_dir(bufferlist& bl,
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
