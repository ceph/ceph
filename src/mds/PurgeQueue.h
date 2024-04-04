// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef PURGE_QUEUE_H_
#define PURGE_QUEUE_H_

#include "include/compact_set.h"
#include "common/Finisher.h"
#include "mds/MDSMap.h"
#include "osdc/Journaler.h"


/**
 * Descriptor of the work associated with purging a file.  We record
 * the minimal amount of information from the inode such as the size
 * and layout: all other un-needed inode metadata (times, permissions, etc)
 * has been discarded.
 */
class PurgeItem
{
public:
  enum Action : uint8_t {
    NONE = 0,
    PURGE_FILE = 1,
    TRUNCATE_FILE,
    PURGE_DIR
  };

  PurgeItem() {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &p);

  static Action str_to_type(std::string_view str) {
    return PurgeItem::actions.at(std::string(str));
  }

  void dump(Formatter *f) const
  {
    f->dump_int("action", action);
    f->dump_int("ino", ino);
    f->dump_int("size", size);
    f->open_object_section("layout");
    layout.dump(f);
    f->close_section();
    f->open_object_section("SnapContext");
    snapc.dump(f);
    f->close_section();
    f->open_object_section("fragtree");
    fragtree.dump(f);
    f->close_section();
  }

  std::string_view get_type_str() const;

  utime_t stamp;
  //None PurgeItem serves as NoOp for splicing out journal entries;
  //so there has to be a "pad_size" to specify the size of journal
  //space to be spliced.
  uint32_t pad_size = 0;
  Action action = NONE;
  inodeno_t ino = 0;
  uint64_t size = 0;
  file_layout_t layout;
  std::vector<int64_t> old_pools;
  SnapContext snapc;
  fragtree_t fragtree;
private:
  static const std::map<std::string, PurgeItem::Action> actions;
};
WRITE_CLASS_ENCODER(PurgeItem)

enum {
  l_pq_first = 3500,

  // How many items have been finished by PurgeQueue
  l_pq_executing_ops,
  l_pq_executing_ops_high_water,
  l_pq_executing,
  l_pq_executing_high_water,
  l_pq_executed_ops,
  l_pq_executed,
  l_pq_item_in_journal,
  l_pq_last
};

struct PurgeItemCommitOp {
public:
  enum PurgeType : uint8_t {
    PURGE_OP_RANGE = 0,
    PURGE_OP_REMOVE = 1,
    PURGE_OP_ZERO
  };

  PurgeItemCommitOp(PurgeItem _item, PurgeType _type, int _flags)
    : item(_item), type(_type), flags(_flags) {}

  PurgeItemCommitOp(PurgeItem _item, PurgeType _type, int _flags,
                    object_t _oid, object_locator_t _oloc)
    : item(_item), type(_type), flags(_flags), oid(_oid), oloc(_oloc) {}

  PurgeItem item;
  PurgeType type;
  int flags;
  object_t oid;
  object_locator_t oloc;
};

/**
 * A persistent queue of PurgeItems.  This class both writes and reads
 * to the queue.  There is one of these per MDS rank.
 *
 * Note that this class does not take a reference to MDSRank: we are
 * independent of all the metadata structures and do not need to
 * take mds_lock for anything.
 */
class PurgeQueue
{
public:
  PurgeQueue(
      CephContext *cct_,
      mds_rank_t rank_,
      const int64_t metadata_pool_,
      Objecter *objecter_,
      Context *on_error);
  ~PurgeQueue();

  void init();
  void activate();
  void shutdown();

  void create_logger();

  // Write an empty queue, use this during MDS rank creation
  void create(Context *completion);

  // Read the Journaler header for an existing queue and start consuming
  void open(Context *completion);

  void wait_for_recovery(Context *c);

  // Submit one entry to the work queue.  Call back when it is persisted
  // to the queue (there is no callback for when it is executed)
  void push(const PurgeItem &pi, Context *completion);

  void _commit_ops(int r, const std::vector<PurgeItemCommitOp>& ops_vec, uint64_t expire_to);

  // If the on-disk queue is empty and we are not currently processing
  // anything.
  bool is_idle() const;

  /**
   * Signal to the PurgeQueue that you would like it to hurry up and
   * finish consuming everything in the queue.  Provides progress
   * feedback.
   *
   * @param progress: bytes consumed since we started draining
   * @param progress_total: max bytes that were outstanding during purge
   * @param in_flight_count: number of file purges currently in flight
   *
   * @returns true if drain is complete
   */
  bool drain(
    uint64_t *progress,
    uint64_t *progress_total,
    size_t *in_flight_count);

  void update_op_limit(const MDSMap &mds_map);

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

private:
  uint32_t _calculate_ops(const PurgeItem &item) const;

  bool _can_consume();

  // recover the journal write_pos (drop any partial written entry)
  void _recover();

  /**
   * @return true if we were in a position to try and consume something:
   *         does not mean we necessarily did.
   */
  bool _consume();

  void _execute_item(const PurgeItem &item, uint64_t expire_to);
  void _execute_item_complete(uint64_t expire_to);

  void _go_readonly(int r);

  CephContext *cct;
  const mds_rank_t rank;
  ceph::mutex lock = ceph::make_mutex("PurgeQueue");
  bool readonly = false;

  int64_t metadata_pool;

  // Don't use the MDSDaemon's Finisher and Timer, because this class
  // operates outside of MDSDaemon::mds_lock
  Finisher finisher;
  SafeTimer timer;
  Filer filer;
  Objecter *objecter;
  std::unique_ptr<PerfCounters> logger;

  Journaler journaler;

  Context *on_error;

  // Map of Journaler offset to PurgeItem
  std::map<uint64_t, PurgeItem> in_flight;

  std::set<uint64_t> pending_expire;

  // Throttled allowances
  uint64_t ops_in_flight = 0;

  // Dynamic op limit per MDS based on PG count
  uint64_t max_purge_ops = 0;

  // How many bytes were remaining when drain() was first called,
  // used for indicating progress.
  uint64_t drain_initial = 0;

  // Has drain() ever been called on this instance?
  bool draining = false;

  // Do we currently have a flush timer event waiting?
  Context *delayed_flush = nullptr;

  bool recovered = false;
  std::vector<Context*> waiting_for_recovery;

  size_t purge_item_journal_size;

  uint64_t ops_high_water = 0;
  uint64_t files_high_water = 0;
};
#endif
