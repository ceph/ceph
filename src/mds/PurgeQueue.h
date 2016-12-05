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
  inodeno_t ino;
  uint64_t size;
  file_layout_t layout;
  compact_set<int64_t> old_pools;
  SnapContext snapc;

  PurgeItem()
   : ino(0), size(0)
  {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
};
WRITE_CLASS_ENCODER(PurgeItem)

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
protected:
  CephContext *cct;
  const mds_rank_t rank;
  Mutex lock;

  int64_t metadata_pool;

  // Don't use the MDSDaemon's Finisher and Timer, because this class
  // operates outside of MDSDaemon::mds_lock
  Finisher finisher;
  SafeTimer timer;
  Filer filer;
  Objecter *objecter;
  Journaler journaler;

  // Map of Journaler offset to PurgeItem
  std::map<uint64_t, PurgeItem> in_flight;

  // Throttled allowances
  uint64_t ops_in_flight;
  uint64_t files_purging;

  // Dynamic op limit per MDS based on PG count
  uint64_t max_purge_ops;

  //PerfCounters *logger;

  bool can_consume();

  void _consume();

  void _execute_item(
      const PurgeItem &item,
      uint64_t expire_to);
  void execute_item_complete(
      uint64_t expire_to);

public:
  void init();
  void shutdown();

  // Write an empty queue, use this during MDS rank creation
  void create(Context *completion);

  // Read the Journaler header for an existing queue and start consuming
  void open(Context *completion);

  // Submit one entry to the work queue.  Call back when it is persisted
  // to the queue (there is no callback for when it is executed)
  void push(const PurgeItem &pi, Context *completion);

  void update_op_limit(const MDSMap &mds_map);

  void handle_conf_change(const struct md_config_t *conf,
                          const std::set <std::string> &changed,
                          const MDSMap &mds_map);

  PurgeQueue(
      CephContext *cct_,
      mds_rank_t rank_,
      const int64_t metadata_pool_,
      Objecter *objecter_);
  ~PurgeQueue()
  {}
};


#endif

