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
 * Handles the import and export of  mds authorities and actual cache data.
 * See src/doc/exports.txt for a description.
 */

#ifndef CEPH_MDS_MIGRATOR_H
#define CEPH_MDS_MIGRATOR_H

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
class MExportDirCancel;
class MExportDirPrep;
class MExportDirPrepAck;
class MExportDir;
class MExportDirAck;
class MExportDirNotify;
class MExportDirNotifyAck;
class MExportDirFinish;

class MExportCaps;
class MExportCapsAck;

class EImportStart;

struct Mutation;

class Migrator {
private:
  MDS *mds;
  MDCache *cache;

  // -- exports --
public:
  // export stages.  used to clean up intelligently if there's a failure.
  const static int EXPORT_CANCELLED	= 0;  // cancelled
  const static int EXPORT_LOCKING	= 1;  // acquiring locks
  const static int EXPORT_DISCOVERING	= 2;  // dest is disovering export dir
  const static int EXPORT_FREEZING	= 3;  // we're freezing the dir tree
  const static int EXPORT_PREPPING	= 4;  // sending dest spanning tree to export bounds
  const static int EXPORT_WARNING	= 5;  // warning bystanders of dir_auth_pending
  const static int EXPORT_EXPORTING	= 6;  // sent actual export, waiting for ack
  const static int EXPORT_LOGGINGFINISH	= 7;  // logging EExportFinish
  const static int EXPORT_NOTIFYING	= 8;  // waiting for notifyacks
  static const char *get_export_statename(int s) {
    switch (s) {
    case EXPORT_LOCKING: return "locking";
    case EXPORT_DISCOVERING: return "discovering";
    case EXPORT_FREEZING: return "freezing";
    case EXPORT_PREPPING: return "prepping";
    case EXPORT_WARNING: return "warning";
    case EXPORT_EXPORTING: return "exporting";
    case EXPORT_LOGGINGFINISH: return "loggingfinish";
    case EXPORT_NOTIFYING: return "notifying";
    default: assert(0); return 0;
    }
  }

protected:
  // export fun
  struct export_state_t {
    int state;
    int peer;
    uint64_t tid;
    set<int> warning_ack_waiting;
    set<int> notify_ack_waiting;
    map<inodeno_t,map<client_t,Capability::Import> > peer_imported;
    list<Context*> waiting_for_finish;
    Mutation *mut;
    export_state_t() : mut(NULL) {}
  };

  map<CDir*, export_state_t>  export_state;
  
  list<pair<dirfrag_t,int> >  export_queue;

  // for deadlock detection
  struct freezing_state_t {
    utime_t start_time;
    int num_waiters;		// number of remote authpin waiters
    freezing_state_t() : num_waiters(0) {}
  };
  map<CDir*,freezing_state_t >	export_freezing_state;
  set<pair<utime_t,CDir*> >	export_freezing_dirs;

  // -- imports --
public:
  const static int IMPORT_DISCOVERING   = 1; // waiting for prep
  const static int IMPORT_DISCOVERED    = 2; // waiting for prep
  const static int IMPORT_PREPPING      = 3; // opening dirs on bounds
  const static int IMPORT_PREPPED       = 4; // opened bounds, waiting for import
  const static int IMPORT_LOGGINGSTART  = 5; // got import, logging EImportStart
  const static int IMPORT_ACKING        = 6; // logged EImportStart, sent ack, waiting for finish
  const static int IMPORT_FINISHING     = 7; // sent cap imports, waiting for finish
  const static int IMPORT_ABORTING      = 8; // notifying bystanders of an abort before unfreezing
  static const char *get_import_statename(int s) {
    switch (s) {
    case IMPORT_DISCOVERING: return "discovering";
    case IMPORT_DISCOVERED: return "discovered";
    case IMPORT_PREPPING: return "prepping";
    case IMPORT_PREPPED: return "prepped";
    case IMPORT_LOGGINGSTART: return "loggingstart";
    case IMPORT_ACKING: return "acking";
    case IMPORT_FINISHING: return "finishing";
    case IMPORT_ABORTING: return "aborting";
    default: assert(0); return 0;
    }
  }

protected:
  struct import_state_t {
    int state;
    int peer;
    uint64_t tid;
    set<int> bystanders;
    list<dirfrag_t> bound_ls;
    list<ScatterLock*> updated_scatterlocks;
    map<client_t,entity_inst_t> client_map;
    map<CInode*, map<client_t,Capability::Export> > peer_exports;
  };

  map<dirfrag_t, import_state_t>  import_state;

public:
  // -- cons --
  Migrator(MDS *m, MDCache *c) : mds(m), cache(c) {}

  void dispatch(Message*);

  void show_importing();
  void show_exporting();
  
  // -- status --
  int is_exporting(CDir *dir) {
    map<CDir*, export_state_t>::iterator it = export_state.find(dir);
    if (it != export_state.end()) return it->second.state;
    return 0;
  }
  bool is_exporting() { return !export_state.empty(); }
  int is_importing(dirfrag_t df) {
    map<dirfrag_t, import_state_t>::iterator it = import_state.find(df);
    if (it != import_state.end()) return it->second.state;
    return 0;
  }
  bool is_importing() { return !import_state.empty(); }

  bool is_ambiguous_import(dirfrag_t df) {
    map<dirfrag_t, import_state_t>::iterator p = import_state.find(df);
    if (p == import_state.end())
      return false;
    if (p->second.state >= IMPORT_LOGGINGSTART &&
	p->second.state < IMPORT_ABORTING)
      return true;
    return false;
  }

  int get_import_state(dirfrag_t df) {
    map<dirfrag_t, import_state_t>::iterator it = import_state.find(df);
    assert(it != import_state.end());
    return it->second.state;
  }
  int get_import_peer(dirfrag_t df) {
    map<dirfrag_t, import_state_t>::iterator it = import_state.find(df);
    assert(it != import_state.end());
    return it->second.peer;
  }

  int get_export_state(CDir *dir) {
    map<CDir*, export_state_t>::iterator it = export_state.find(dir);
    assert(it != export_state.end());
    return it->second.state;
  }
  // this returns true if we are export @dir,
  // and are not waiting for @who to be
  // be warned of ambiguous auth.
  // only returns meaningful results during EXPORT_WARNING state.
  bool export_has_warned(CDir *dir, int who) {
    map<CDir*, export_state_t>::iterator it = export_state.find(dir);
    assert(it != export_state.end());
    assert(it->second.state == EXPORT_WARNING);
    return (it->second.warning_ack_waiting.count(who) == 0);
  }

  bool export_has_notified(CDir *dir, int who) {
    map<CDir*, export_state_t>::iterator it = export_state.find(dir);
    assert(it != export_state.end());
    assert(it->second.state == EXPORT_NOTIFYING);
    return (it->second.notify_ack_waiting.count(who) == 0);
  }

  void export_freeze_inc_num_waiters(CDir *dir) {
    assert(is_exporting(dir));
    export_freezing_state[dir].num_waiters++;
  }
  void find_stale_export_freeze();

  // -- misc --
  void handle_mds_failure_or_stop(int who);

  void audit();

  // -- import/export --
  // exporter
 public:
  void dispatch_export_dir(MDRequest *mdr);
  void export_dir(CDir *dir, int dest);
  void export_empty_import(CDir *dir);

  void export_dir_nicely(CDir *dir, int dest);
  void maybe_do_queued_export();
  void clear_export_queue() {
    export_queue.clear();
  }
  
  void get_export_lock_set(CDir *dir, set<SimpleLock*>& locks);
  void get_export_client_set(CDir *dir, set<client_t> &client_set);
  void get_export_client_set(CInode *in, set<client_t> &client_set);

  void encode_export_inode(CInode *in, bufferlist& bl, 
			   map<client_t,entity_inst_t>& exported_client_map);
  void encode_export_inode_caps(CInode *in, bool auth_cap, bufferlist& bl,
				map<client_t,entity_inst_t>& exported_client_map);
  void finish_export_inode(CInode *in, utime_t now, int target,
			   map<client_t,Capability::Import>& peer_imported,
			   list<Context*>& finished);
  void finish_export_inode_caps(CInode *in, int target,
			        map<client_t,Capability::Import>& peer_imported);


  int encode_export_dir(bufferlist& exportbl,
			CDir *dir,
			map<client_t,entity_inst_t>& exported_client_map,
			utime_t now);
  void finish_export_dir(CDir *dir, utime_t now, int target,
			 map<inodeno_t,map<client_t,Capability::Import> >& peer_imported,
			 list<Context*>& finished);

  void add_export_finish_waiter(CDir *dir, Context *c) {
    map<CDir*, export_state_t>::iterator it = export_state.find(dir);
    assert(it != export_state.end());
    it->second.waiting_for_finish.push_back(c);
  }
  void clear_export_proxy_pins(CDir *dir);

  void export_caps(CInode *in);

 protected:
  void handle_export_discover_ack(MExportDirDiscoverAck *m);
  void export_frozen(CDir *dir);
  void handle_export_prep_ack(MExportDirPrepAck *m);
  void export_sessions_flushed(CDir *dir, uint64_t tid);
  void export_go(CDir *dir);
  void export_go_synced(CDir *dir, uint64_t tid);
  void export_try_cancel(CDir *dir);
  void export_reverse(CDir *dir);
  void export_notify_abort(CDir *dir, set<CDir*>& bounds);
  void handle_export_ack(MExportDirAck *m);
  void export_logged_finish(CDir *dir);
  void handle_export_notify_ack(MExportDirNotifyAck *m);
  void export_finish(CDir *dir);

  void export_freeze_finish(CDir *dir) {
    utime_t start = export_freezing_state[dir].start_time;
    export_freezing_dirs.erase(make_pair(start, dir));
    export_freezing_state.erase(dir);
  }

  friend class C_MDC_ExportFreeze;
  friend class C_MDS_ExportFinishLogged;
  friend class C_M_ExportGo;
  friend class C_M_ExportSessionsFlushed;

  // importer
  void handle_export_discover(MExportDirDiscover *m);
  void handle_export_cancel(MExportDirCancel *m);
  void handle_export_prep(MExportDirPrep *m);
  void handle_export_dir(MExportDir *m);

public:
  void decode_import_inode(CDentry *dn, bufferlist::iterator& blp, int oldauth, 
			   LogSegment *ls, uint64_t log_offset,
			   map<CInode*, map<client_t,Capability::Export> >& cap_imports,
			   list<ScatterLock*>& updated_scatterlocks);
  void decode_import_inode_caps(CInode *in, bool auth_cap, bufferlist::iterator &blp,
				map<CInode*, map<client_t,Capability::Export> >& cap_imports);
  void finish_import_inode_caps(CInode *in, int from, bool auth_cap,
				map<client_t,Capability::Export> &export_map,
				map<client_t,Capability::Import> &import_map);
  int decode_import_dir(bufferlist::iterator& blp,
			int oldauth,
			CDir *import_root,
			EImportStart *le, 
			LogSegment *ls,
			map<CInode*, map<client_t,Capability::Export> >& cap_imports,
			list<ScatterLock*>& updated_scatterlocks, utime_t now);

public:
  void import_reverse(CDir *dir);
protected:
  void import_reverse_discovering(dirfrag_t df);
  void import_reverse_discovered(dirfrag_t df, CInode *diri);
  void import_reverse_prepping(CDir *dir);
  void import_remove_pins(CDir *dir, set<CDir*>& bounds);
  void import_reverse_unfreeze(CDir *dir);
  void import_reverse_final(CDir *dir);
  void import_notify_abort(CDir *dir, set<CDir*>& bounds);
  void import_notify_finish(CDir *dir, set<CDir*>& bounds);
  void import_logged_start(dirfrag_t df, CDir *dir, int from,
			   map<client_t,entity_inst_t> &imported_client_map,
			   map<client_t,uint64_t>& sseqmap);
  void handle_export_finish(MExportDirFinish *m);
public:
  void import_finish(CDir *dir, bool notify, bool last=true);
protected:

  void handle_export_caps(MExportCaps *m);
  void logged_import_caps(CInode *in, 
			  int from,
			  map<CInode*, map<client_t,Capability::Export> >& cap_imports,
			  map<client_t,entity_inst_t>& client_map,
			  map<client_t,uint64_t>& sseqmap);


  friend class C_MDS_ImportDirLoggedStart;
  friend class C_MDS_ImportDirLoggedFinish;
  friend class C_M_LoggedImportCaps;

  // bystander
  void handle_export_notify(MExportDirNotify *m);

};


#endif
