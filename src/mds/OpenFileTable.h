// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef OPEN_FILE_TABLE_H
#define OPEN_FILE_TABLE_H

#include "mdstypes.h"
#include "Anchor.h"

#include "MDSContext.h"

class CDir;
class CInode;
class MDSRank;

struct ObjectOperation;

class OpenFileTable
{
public:
  explicit OpenFileTable(MDSRank *m);
  ~OpenFileTable();

  void add_inode(CInode *in);
  void remove_inode(CInode *in);
  void add_dirfrag(CDir *dir);
  void remove_dirfrag(CDir *dir);
  void notify_link(CInode *in);
  void notify_unlink(CInode *in);
  bool is_any_dirty() const { return !dirty_items.empty(); }

  void commit(MDSContext *c, uint64_t log_seq, int op_prio);
  uint64_t get_committed_log_seq() const { return committed_log_seq; }
  bool is_any_committing() const { return num_pending_commit > 0; }

  void load(MDSContext *c);
  bool is_loaded() const { return load_done; }
  void wait_for_load(MDSContext *c) {
    ceph_assert(!load_done);
    waiting_for_load.push_back(c);
  }

  bool prefetch_inodes();
  bool is_prefetched() const { return prefetch_state == DONE; }
  void wait_for_prefetch(MDSContext *c) {
    ceph_assert(!is_prefetched());
    waiting_for_prefetch.push_back(c);
  }

  bool should_log_open(CInode *in);

  void note_destroyed_inos(uint64_t seq, const std::vector<inodeno_t>& inos);
  void trim_destroyed_inos(uint64_t seq);

protected:
  friend class C_IO_OFT_Recover;
  friend class C_IO_OFT_Load;
  friend class C_IO_OFT_Save;
  friend class C_IO_OFT_Journal;
  friend class C_OFT_OpenInoFinish;

  uint64_t MAX_ITEMS_PER_OBJ = g_conf().get_val<uint64_t>("osd_deep_scrub_large_omap_object_key_threshold");
  static const unsigned MAX_OBJECTS = 1024; // (1024 * osd_deep_scrub_large_omap_object_key_threshold) items at most

  static const int DIRTY_NEW	= -1;
  static const int DIRTY_UNDEF	= -2;

  unsigned num_pending_commit = 0;
  void _encode_header(bufferlist& bl, int j_state);
  void _commit_finish(int r, uint64_t log_seq, MDSContext *fin);
  void _journal_finish(int r, uint64_t log_seq, MDSContext *fin,
		       std::map<unsigned, std::vector<ObjectOperation> >& ops);

  void get_ref(CInode *in, frag_t fg=-1U);
  void put_ref(CInode *in, frag_t fg=-1U);

  object_t get_object_name(unsigned idx) const;

  void _reset_states() {
    omap_num_objs = 0;
    omap_num_items.resize(0);
    journal_state = JOURNAL_NONE;
    loaded_journals.clear();
    loaded_anchor_map.clear();
  }
  void _read_omap_values(const std::string& key, unsigned idx, bool first);
  void _load_finish(int op_r, int header_r, int values_r,
		    unsigned idx, bool first, bool more,
                    bufferlist &header_bl,
		    std::map<std::string, bufferlist> &values);
  void _recover_finish(int r);

  void _open_ino_finish(inodeno_t ino, int r);
  void _prefetch_inodes();
  void _prefetch_dirfrags();

  void _get_ancestors(const Anchor& parent,
		      std::vector<inode_backpointer_t>& ancestors,
		      mds_rank_t& auth_hint);

  MDSRank *mds;

  version_t omap_version = 0;

  uint32_t omap_num_objs = 0;
  std::vector<unsigned> omap_num_items;

  std::map<inodeno_t, OpenedAnchor> anchor_map;

  std::map<inodeno_t, int> dirty_items; // ino -> dirty state

  uint64_t committed_log_seq = 0;
  uint64_t committing_log_seq = 0;

  enum {
    JOURNAL_NONE = 0,
    JOURNAL_START = 1,
    JOURNAL_FINISH = 2,
  };
  int journal_state = 0;

  std::vector<std::map<std::string, bufferlist> > loaded_journals;
  std::map<inodeno_t, RecoveredAnchor> loaded_anchor_map;
  MDSContext::vec waiting_for_load;
  bool load_done = false;

  enum {
    DIR_INODES = 1,
    DIRFRAGS = 2,
    FILE_INODES = 3,
    DONE = 4,
  };
  unsigned prefetch_state = 0;
  unsigned num_opening_inodes = 0;
  MDSContext::vec waiting_for_prefetch;

  std::map<uint64_t, std::vector<inodeno_t> > logseg_destroyed_inos;
  std::set<inodeno_t> destroyed_inos_set;

  std::unique_ptr<PerfCounters> logger;
};

#endif
