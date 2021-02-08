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

#include "MDSContext.h"

#include <map>
#include <list>
#include <set>
#include <string_view>

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirFinish.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MGatherCaps.h"

class MDSRank;
class CDir;
class CInode;
class CDentry;
class Session;
class EImportStart;

class Migrator {
public:
  // export stages.  used to clean up intelligently if there's a failure.
  const static int EXPORT_CANCELLED	= 0;  // cancelled
  const static int EXPORT_CANCELLING	= 1;  // waiting for cancel notifyacks
  const static int EXPORT_LOCKING	= 2;  // acquiring locks
  const static int EXPORT_DISCOVERING	= 3;  // dest is disovering export dir
  const static int EXPORT_FREEZING	= 4;  // we're freezing the dir tree
  const static int EXPORT_PREPPING	= 5;  // sending dest spanning tree to export bounds
  const static int EXPORT_WARNING	= 6;  // warning bystanders of dir_auth_pending
  const static int EXPORT_EXPORTING	= 7;  // sent actual export, waiting for ack
  const static int EXPORT_LOGGINGFINISH	= 8;  // logging EExportFinish
  const static int EXPORT_NOTIFYING	= 9;  // waiting for notifyacks

  // -- imports --
  const static int IMPORT_DISCOVERING   = 1; // waiting for prep
  const static int IMPORT_DISCOVERED    = 2; // waiting for prep
  const static int IMPORT_PREPPING      = 3; // opening dirs on bounds
  const static int IMPORT_PREPPED       = 4; // opened bounds, waiting for import
  const static int IMPORT_LOGGINGSTART  = 5; // got import, logging EImportStart
  const static int IMPORT_ACKING        = 6; // logged EImportStart, sent ack, waiting for finish
  const static int IMPORT_FINISHING     = 7; // sent cap imports, waiting for finish
  const static int IMPORT_ABORTING      = 8; // notifying bystanders of an abort before unfreezing

  // -- cons --
  Migrator(MDSRank *m, MDCache *c);

  static std::string_view get_export_statename(int s) {
    switch (s) {
    case EXPORT_CANCELLING: return "cancelling";
    case EXPORT_LOCKING: return "locking";
    case EXPORT_DISCOVERING: return "discovering";
    case EXPORT_FREEZING: return "freezing";
    case EXPORT_PREPPING: return "prepping";
    case EXPORT_WARNING: return "warning";
    case EXPORT_EXPORTING: return "exporting";
    case EXPORT_LOGGINGFINISH: return "loggingfinish";
    case EXPORT_NOTIFYING: return "notifying";
    default: ceph_abort(); return std::string_view();
    }
  }

  static std::string_view get_import_statename(int s) {
    switch (s) {
    case IMPORT_DISCOVERING: return "discovering";
    case IMPORT_DISCOVERED: return "discovered";
    case IMPORT_PREPPING: return "prepping";
    case IMPORT_PREPPED: return "prepped";
    case IMPORT_LOGGINGSTART: return "loggingstart";
    case IMPORT_ACKING: return "acking";
    case IMPORT_FINISHING: return "finishing";
    case IMPORT_ABORTING: return "aborting";
    default: ceph_abort(); return std::string_view();
    }
  }

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

  void dispatch(const cref_t<Message> &);

  void show_importing();
  void show_exporting();

  int get_num_exporting() const { return export_state.size(); }
  int get_export_queue_size() const { return export_queue.size(); }
  
  // -- status --
  int is_exporting(CDir *dir) const {
    auto it = export_state.find(dir);
    if (it != export_state.end()) return it->second.state;
    return 0;
  }
  bool is_exporting() const { return !export_state.empty(); }
  int is_importing(dirfrag_t df) const {
    auto it = import_state.find(df);
    if (it != import_state.end()) return it->second.state;
    return 0;
  }
  bool is_importing() const { return !import_state.empty(); }

  bool is_ambiguous_import(dirfrag_t df) const {
    auto it = import_state.find(df);
    if (it == import_state.end())
      return false;
    if (it->second.state >= IMPORT_LOGGINGSTART &&
	it->second.state < IMPORT_ABORTING)
      return true;
    return false;
  }

  int get_import_state(dirfrag_t df) const {
    auto it = import_state.find(df);
    ceph_assert(it != import_state.end());
    return it->second.state;
  }
  int get_import_peer(dirfrag_t df) const {
    auto it = import_state.find(df);
    ceph_assert(it != import_state.end());
    return it->second.peer;
  }

  int get_export_state(CDir *dir) const {
    auto it = export_state.find(dir);
    ceph_assert(it != export_state.end());
    return it->second.state;
  }
  // this returns true if we are export @dir,
  // and are not waiting for @who to be
  // be warned of ambiguous auth.
  // only returns meaningful results during EXPORT_WARNING state.
  bool export_has_warned(CDir *dir, mds_rank_t who) {
    auto it = export_state.find(dir);
    ceph_assert(it != export_state.end());
    ceph_assert(it->second.state == EXPORT_WARNING);
    return (it->second.warning_ack_waiting.count(who) == 0);
  }

  bool export_has_notified(CDir *dir, mds_rank_t who) const {
    auto it = export_state.find(dir);
    ceph_assert(it != export_state.end());
    ceph_assert(it->second.state == EXPORT_NOTIFYING);
    return (it->second.notify_ack_waiting.count(who) == 0);
  }

  void export_freeze_inc_num_waiters(CDir *dir) {
    auto it = export_state.find(dir);
    ceph_assert(it != export_state.end());
    it->second.num_remote_waiters++;
  }
  void find_stale_export_freeze();

  // -- misc --
  void handle_mds_failure_or_stop(mds_rank_t who);

  void audit();

  // -- import/export --
  // exporter
  void dispatch_export_dir(MDRequestRef& mdr, int count);
  void export_dir(CDir *dir, mds_rank_t dest);
  void export_empty_import(CDir *dir);

  void export_dir_nicely(CDir *dir, mds_rank_t dest);
  void maybe_do_queued_export();
  void clear_export_queue() {
    export_queue.clear();
    export_queue_gen++;
  }
  
  void maybe_split_export(CDir* dir, uint64_t max_size, bool null_okay,
			  vector<pair<CDir*, size_t> >& results);

  bool export_try_grab_locks(CDir *dir, MutationRef& mut);
  void get_export_client_set(CDir *dir, std::set<client_t> &client_set);
  void get_export_client_set(CInode *in, std::set<client_t> &client_set);

  void encode_export_inode(CInode *in, bufferlist& bl, 
			   std::map<client_t,entity_inst_t>& exported_client_map,
			   std::map<client_t,client_metadata_t>& exported_client_metadata_map);
  void encode_export_inode_caps(CInode *in, bool auth_cap, bufferlist& bl,
				std::map<client_t,entity_inst_t>& exported_client_map,
				std::map<client_t,client_metadata_t>& exported_client_metadata_map);
  void finish_export_inode(CInode *in, mds_rank_t target,
			   std::map<client_t,Capability::Import>& peer_imported,
			   MDSContext::vec& finished);
  void finish_export_inode_caps(CInode *in, mds_rank_t target,
			        std::map<client_t,Capability::Import>& peer_imported);


  void encode_export_dir(bufferlist& exportbl,
			CDir *dir,
			std::map<client_t,entity_inst_t>& exported_client_map,
			std::map<client_t,client_metadata_t>& exported_client_metadata_map,
                        uint64_t &num_exported);
  void finish_export_dir(CDir *dir, mds_rank_t target,
			 std::map<inodeno_t,std::map<client_t,Capability::Import> >& peer_imported,
			 MDSContext::vec& finished, int *num_dentries);

  void clear_export_proxy_pins(CDir *dir);

  void export_caps(CInode *in);

  void decode_import_inode(CDentry *dn, bufferlist::const_iterator& blp,
			   mds_rank_t oldauth, LogSegment *ls,
			   std::map<CInode*, std::map<client_t,Capability::Export> >& cap_imports,
			   std::list<ScatterLock*>& updated_scatterlocks);
  void decode_import_inode_caps(CInode *in, bool auth_cap, bufferlist::const_iterator &blp,
				std::map<CInode*, std::map<client_t,Capability::Export> >& cap_imports);
  void finish_import_inode_caps(CInode *in, mds_rank_t from, bool auth_cap,
				const std::map<client_t,pair<Session*,uint64_t> >& smap,
				const std::map<client_t,Capability::Export> &export_map,
				std::map<client_t,Capability::Import> &import_map);
  void decode_import_dir(bufferlist::const_iterator& blp,
			mds_rank_t oldauth,
			CDir *import_root,
			EImportStart *le, 
			LogSegment *ls,
			std::map<CInode*, std::map<client_t,Capability::Export> >& cap_imports,
			std::list<ScatterLock*>& updated_scatterlocks, int &num_imported);

  void import_reverse(CDir *dir);

  void import_finish(CDir *dir, bool notify, bool last=true);

protected:
  struct export_base_t {
    export_base_t(dirfrag_t df, mds_rank_t d, unsigned c, uint64_t g) :
      dirfrag(df), dest(d), pending_children(c), export_queue_gen(g) {}
    dirfrag_t dirfrag;
    mds_rank_t dest;
    unsigned pending_children;
    uint64_t export_queue_gen;
    bool restart = false;
  };

  // export fun
  struct export_state_t {
    export_state_t() {}

    int state = 0;
    mds_rank_t peer = MDS_RANK_NONE;
    uint64_t tid = 0;
    std::set<mds_rank_t> warning_ack_waiting;
    std::set<mds_rank_t> notify_ack_waiting;
    std::map<inodeno_t,std::map<client_t,Capability::Import> > peer_imported;
    MutationRef mut;
    size_t approx_size = 0;
    // for freeze tree deadlock detection
    utime_t last_cum_auth_pins_change;
    int last_cum_auth_pins = 0;
    int num_remote_waiters = 0; // number of remote authpin waiters
    std::shared_ptr<export_base_t> parent;
  };

  // import fun
  struct import_state_t {
    import_state_t() : mut() {}
    int state = 0;
    mds_rank_t peer = 0;
    uint64_t tid = 0;
    std::set<mds_rank_t> bystanders;
    std::list<dirfrag_t> bound_ls;
    std::list<ScatterLock*> updated_scatterlocks;
    std::map<client_t,pair<Session*,uint64_t> > session_map;
    std::map<CInode*, std::map<client_t,Capability::Export> > peer_exports;
    MutationRef mut;
  };

  typedef map<CDir*, export_state_t>::iterator export_state_iterator;

  friend class C_MDC_ExportFreeze;
  friend class C_MDS_ExportFinishLogged;
  friend class C_M_ExportGo;
  friend class C_M_ExportSessionsFlushed;
  friend class C_MDS_ExportDiscover;
  friend class C_MDS_ExportPrep;
  friend class MigratorContext;
  friend class MigratorLogContext;
  friend class C_MDS_ImportDirLoggedStart;
  friend class C_MDS_ImportDirLoggedFinish;
  friend class C_M_LoggedImportCaps;

  void handle_export_discover_ack(const cref_t<MExportDirDiscoverAck> &m);
  void export_frozen(CDir *dir, uint64_t tid);
  void handle_export_prep_ack(const cref_t<MExportDirPrepAck> &m);
  void export_sessions_flushed(CDir *dir, uint64_t tid);
  void export_go(CDir *dir);
  void export_go_synced(CDir *dir, uint64_t tid);
  void export_try_cancel(CDir *dir, bool notify_peer=true);
  void export_cancel_finish(export_state_iterator& it);
  void export_reverse(CDir *dir, export_state_t& stat);
  void export_notify_abort(CDir *dir, export_state_t& stat, std::set<CDir*>& bounds);
  void handle_export_ack(const cref_t<MExportDirAck> &m);
  void export_logged_finish(CDir *dir);
  void handle_export_notify_ack(const cref_t<MExportDirNotifyAck> &m);
  void export_finish(CDir *dir);
  void child_export_finish(std::shared_ptr<export_base_t>& parent, bool success);
  void encode_export_prep_trace(bufferlist& bl, CDir *bound, CDir *dir, export_state_t &es,
                               set<inodeno_t> &inodes_added, set<dirfrag_t> &dirfrags_added);
  void decode_export_prep_trace(bufferlist::const_iterator& blp, mds_rank_t oldauth, MDSContext::vec &finished);

  void handle_gather_caps(const cref_t<MGatherCaps> &m);

  // importer
  void handle_export_discover(const cref_t<MExportDirDiscover> &m, bool started=false);
  void handle_export_cancel(const cref_t<MExportDirCancel> &m);
  void handle_export_prep(const cref_t<MExportDirPrep> &m, bool did_assim=false);
  void handle_export_dir(const cref_t<MExportDir> &m);

  void import_reverse_discovering(dirfrag_t df);
  void import_reverse_discovered(dirfrag_t df, CInode *diri);
  void import_reverse_prepping(CDir *dir, import_state_t& stat);
  void import_remove_pins(CDir *dir, std::set<CDir*>& bounds);
  void import_reverse_unfreeze(CDir *dir);
  void import_reverse_final(CDir *dir);
  void import_notify_abort(CDir *dir, std::set<CDir*>& bounds);
  void import_notify_finish(CDir *dir, std::set<CDir*>& bounds);
  void import_logged_start(dirfrag_t df, CDir *dir, mds_rank_t from,
			   std::map<client_t,pair<Session*,uint64_t> >& imported_session_map);
  void handle_export_finish(const cref_t<MExportDirFinish> &m);

  void handle_export_caps(const cref_t<MExportCaps> &m);
  void handle_export_caps_ack(const cref_t<MExportCapsAck> &m);
  void logged_import_caps(CInode *in,
			  mds_rank_t from,
			  std::map<client_t,pair<Session*,uint64_t> >& imported_session_map,
			  std::map<CInode*, std::map<client_t,Capability::Export> >& cap_imports);

  // bystander
  void handle_export_notify(const cref_t<MExportDirNotify> &m);

  std::map<CDir*, export_state_t>  export_state;

  uint64_t total_exporting_size = 0;
  unsigned num_locking_exports = 0; // exports in locking state (approx_size == 0)

  std::list<pair<dirfrag_t,mds_rank_t> >  export_queue;
  uint64_t export_queue_gen = 1;

  std::map<dirfrag_t, import_state_t>  import_state;

private:
  MDSRank *mds;
  MDCache *mdcache;
  uint64_t max_export_size = 0;
  bool inject_session_race = false;
};

#endif
