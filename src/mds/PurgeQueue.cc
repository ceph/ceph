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

#include "common/debug.h"
#include "mds/mdstypes.h"
#include "mds/CInode.h"
#include "mds/MDCache.h"

#include "PurgeQueue.h"

#define dout_context cct
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, rank) << __func__ << ": "

using namespace std;

static ostream& _prefix(std::ostream *_dout, mds_rank_t rank) {
  return *_dout << "mds." << rank << ".purge_queue ";
}

const std::map<std::string, PurgeItem::Action> PurgeItem::actions = {
  {"NONE", PurgeItem::NONE},
  {"PURGE_FILE", PurgeItem::PURGE_FILE},
  {"TRUNCATE_FILE", PurgeItem::TRUNCATE_FILE},
  {"PURGE_DIR", PurgeItem::PURGE_DIR}
};

void PurgeItem::encode(bufferlist &bl) const
{
  ENCODE_START(2, 1, bl);
  encode((uint8_t)action, bl);
  encode(ino, bl);
  encode(size, bl);
  encode(layout, bl, CEPH_FEATURE_FS_FILE_LAYOUT_V2);
  encode(old_pools, bl);
  encode(snapc, bl);
  encode(fragtree, bl);
  encode(stamp, bl);
  uint8_t static const pad = 0xff;
  for (unsigned int i = 0; i<pad_size; i++) {
    encode(pad, bl);
  }
  ENCODE_FINISH(bl);
}

void PurgeItem::decode(bufferlist::const_iterator &p)
{
  DECODE_START(2, p);
  bool done = false;
  if (struct_v == 1) {
    auto p_start = p;
    try {
      // bad encoding introduced by v13.2.2
      decode(stamp, p);
      decode(pad_size, p);
      p += pad_size;
      uint8_t raw_action;
      decode(raw_action, p);
      action = (Action)raw_action;
      decode(ino, p);
      decode(size, p);
      decode(layout, p);
      decode(old_pools, p);
      decode(snapc, p);
      decode(fragtree, p);
      if (p.get_off() > struct_end)
	throw buffer::end_of_buffer();
      done = true;
    } catch (const buffer::error &e) {
      p = p_start;
    }
  }
  if (!done) {
    uint8_t raw_action;
    decode(raw_action, p);
    action = (Action)raw_action;
    decode(ino, p);
    decode(size, p);
    decode(layout, p);
    decode(old_pools, p);
    decode(snapc, p);
    decode(fragtree, p);
    if (struct_v >= 2) {
      decode(stamp, p);
    }
  }
  DECODE_FINISH(p);
}

// if Objecter has any slow requests, take that as a hint and
// slow down our rate of purging
PurgeQueue::PurgeQueue(
      CephContext *cct_,
      mds_rank_t rank_,
      const int64_t metadata_pool_,
      Objecter *objecter_,
      Context *on_error_)
  :
    cct(cct_),
    rank(rank_),
    metadata_pool(metadata_pool_),
    finisher(cct, "PurgeQueue", "PQ_Finisher"),
    timer(cct, lock),
    filer(objecter_, &finisher),
    objecter(objecter_),
    journaler("pq", MDS_INO_PURGE_QUEUE + rank, metadata_pool,
      CEPH_FS_ONDISK_MAGIC, objecter_, nullptr, 0,
      &finisher),
    on_error(on_error_)
{
  ceph_assert(cct != nullptr);
  ceph_assert(on_error != nullptr);
  ceph_assert(objecter != nullptr);
  journaler.set_write_error_handler(on_error);
}

PurgeQueue::~PurgeQueue()
{
  if (logger) {
    g_ceph_context->get_perfcounters_collection()->remove(logger.get());
  }
  delete on_error;
}

void PurgeQueue::create_logger()
{
  PerfCountersBuilder pcb(g_ceph_context, "purge_queue", l_pq_first, l_pq_last);

  pcb.add_u64_counter(l_pq_executed_ops, "pq_executed_ops", "Purge queue ops executed",
                      "puro", PerfCountersBuilder::PRIO_INTERESTING);
  pcb.add_u64_counter(l_pq_executed, "pq_executed", "Purge queue tasks executed",
                      "purg", PerfCountersBuilder::PRIO_INTERESTING);

  pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64(l_pq_executing_ops, "pq_executing_ops", "Purge queue ops in flight");
  pcb.add_u64(l_pq_executing_ops_high_water, "pq_executing_ops_high_water", "Maximum number of executing file purge ops");
  pcb.add_u64(l_pq_executing, "pq_executing", "Purge queue tasks in flight");
  pcb.add_u64(l_pq_executing_high_water, "pq_executing_high_water", "Maximum number of executing file purges");
  pcb.add_u64(l_pq_item_in_journal, "pq_item_in_journal", "Purge item left in journal");

  logger.reset(pcb.create_perf_counters());
  g_ceph_context->get_perfcounters_collection()->add(logger.get());
}

void PurgeQueue::init()
{
  std::lock_guard l(lock);

  ceph_assert(logger != nullptr);

  finisher.start();
  timer.init();
}

void PurgeQueue::activate()
{
  std::lock_guard l(lock);

  {
    PurgeItem item;
    bufferlist bl;

    // calculate purge item serialized size stored in journal
    // used to count how many items still left in journal later
    ::encode(item, bl);
    purge_item_journal_size = bl.length() + journaler.get_journal_envelope_size(); 
  }

  if (readonly) {
    dout(10) << "skipping activate: PurgeQueue is readonly" << dendl;
    return;
  }

  if (journaler.get_read_pos() == journaler.get_write_pos())
    return;

  if (in_flight.empty()) {
    dout(4) << "start work (by drain)" << dendl;
    finisher.queue(new LambdaContext([this](int r) {
	  std::lock_guard l(lock);
	  _consume();
	  }));
  }
}

void PurgeQueue::shutdown()
{
  std::lock_guard l(lock);

  journaler.shutdown();
  timer.shutdown();
  finisher.stop();
}

void PurgeQueue::open(Context *completion)
{
  dout(4) << "opening" << dendl;

  std::lock_guard l(lock);

  if (completion)
    waiting_for_recovery.push_back(completion);

  journaler.recover(new LambdaContext([this](int r){
    if (r == -CEPHFS_ENOENT) {
      dout(1) << "Purge Queue not found, assuming this is an upgrade and "
                 "creating it." << dendl;
      create(NULL);
    } else if (r == 0) {
      std::lock_guard l(lock);
      dout(4) << "open complete" << dendl;

      // Journaler only guarantees entries before head write_pos have been
      // fully flushed. Before appending new entries, we need to find and
      // drop any partial written entry.
      if (journaler.last_committed.write_pos < journaler.get_write_pos()) {
	dout(4) << "recovering write_pos" << dendl;
	journaler.set_read_pos(journaler.last_committed.write_pos);
	_recover();
	return;
      }

      journaler.set_writeable();
      recovered = true;
      finish_contexts(g_ceph_context, waiting_for_recovery);
    } else {
      derr << "Error " << r << " loading Journaler" << dendl;
      _go_readonly(r);
    }
  }));
}

void PurgeQueue::wait_for_recovery(Context* c)
{
  std::lock_guard l(lock);
  if (recovered) {
    c->complete(0);
  } else if (readonly) {
    dout(10) << "cannot wait for recovery: PurgeQueue is readonly" << dendl;
    c->complete(-CEPHFS_EROFS);
  } else {
    waiting_for_recovery.push_back(c);
  }
}

void PurgeQueue::_recover()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  // Journaler::is_readable() adjusts write_pos if partial entry is encountered
  while (1) {
    if (!journaler.is_readable() &&
	!journaler.get_error() &&
	journaler.get_read_pos() < journaler.get_write_pos()) {
      journaler.wait_for_readable(new LambdaContext([this](int r) {
        std::lock_guard l(lock);
	_recover();
      }));
      return;
    }

    if (journaler.get_error()) {
      int r = journaler.get_error();
      derr << "Error " << r << " recovering write_pos" << dendl;
      _go_readonly(r);
      return;
    }

    if (journaler.get_read_pos() == journaler.get_write_pos()) {
      dout(4) << "write_pos recovered" << dendl;
      // restore original read_pos
      journaler.set_read_pos(journaler.last_committed.expire_pos);
      journaler.set_writeable();
      recovered = true;
      finish_contexts(g_ceph_context, waiting_for_recovery);
      return;
    }

    bufferlist bl;
    bool readable = journaler.try_read_entry(bl);
    ceph_assert(readable);  // we checked earlier
  }
}

void PurgeQueue::create(Context *fin)
{
  dout(4) << "creating" << dendl;
  std::lock_guard l(lock);

  if (fin)
    waiting_for_recovery.push_back(fin);

  file_layout_t layout = file_layout_t::get_default();
  layout.pool_id = metadata_pool;
  journaler.set_writeable();
  journaler.create(&layout, JOURNAL_FORMAT_RESILIENT);
  journaler.write_head(new LambdaContext([this](int r) {
    std::lock_guard l(lock);
    if (r) {
      _go_readonly(r);
    } else {
      recovered = true;
      finish_contexts(g_ceph_context, waiting_for_recovery);
    }
  }));
}

/**
 * The `completion` context will always be called back via a Finisher
 */
void PurgeQueue::push(const PurgeItem &pi, Context *completion)
{
  dout(4) << "pushing inode " << pi.ino << dendl;
  std::lock_guard l(lock);

  if (readonly) {
    dout(10) << "cannot push inode: PurgeQueue is readonly" << dendl;
    completion->complete(-CEPHFS_EROFS);
    return;
  }

  // Callers should have waited for open() before using us
  ceph_assert(!journaler.is_readonly());

  bufferlist bl;

  encode(pi, bl);
  journaler.append_entry(bl);
  journaler.wait_for_flush(completion);

  // Maybe go ahead and do something with it right away
  bool could_consume = _consume();
  if (!could_consume) {
    // Usually, it is not necessary to explicitly flush here, because the reader
    // will get flushes generated inside Journaler::is_readable.  However,
    // if we remain in a _can_consume()==false state for a long period then
    // we should flush in order to allow MDCache to drop its strays rather
    // than having them wait for purgequeue to progress.
    if (!delayed_flush) {
      delayed_flush = new LambdaContext([this](int r){
            delayed_flush = nullptr;
            journaler.flush();
          });

      timer.add_event_after(
	  g_conf()->mds_purge_queue_busy_flush_period,
          delayed_flush);
    }
  }
}

uint32_t PurgeQueue::_calculate_ops(const PurgeItem &item) const
{
  uint32_t ops_required = 0;
  if (item.action == PurgeItem::PURGE_DIR) {
    // Directory, count dirfrags to be deleted
    frag_vec_t leaves;
    if (!item.fragtree.is_leaf(frag_t())) {
      item.fragtree.get_leaves(leaves);
    }
    // One for the root, plus any leaves
    ops_required = 1 + leaves.size();
  } else {
    // File, work out concurrent Filer::purge deletes
    // Account for removing (or zeroing) backtrace
    const uint64_t num = (item.size > 0) ?
      Striper::get_num_objects(item.layout, item.size) : 1;

    ops_required = num;

    // Account for deletions for old pools
    if (item.action != PurgeItem::TRUNCATE_FILE) {
      ops_required += item.old_pools.size();
    }
  }

  return ops_required;
}

bool PurgeQueue::_can_consume()
{
  if (readonly) {
    dout(10) << "can't consume: PurgeQueue is readonly" << dendl;
    return false;
  }

  dout(20) << ops_in_flight << "/" << max_purge_ops << " ops, "
           << in_flight.size() << "/" << g_conf()->mds_max_purge_files
           << " files" << dendl;

  if (in_flight.size() == 0 && cct->_conf->mds_max_purge_files > 0) {
    // Always permit consumption if nothing is in flight, so that the ops
    // limit can never be so low as to forbid all progress (unless
    // administrator has deliberately paused purging by setting max
    // purge files to zero).
    return true;
  }

  if (ops_in_flight >= max_purge_ops) {
    dout(20) << "Throttling on op limit " << ops_in_flight << "/"
             << max_purge_ops << dendl;
    return false;
  }

  if (in_flight.size() >= cct->_conf->mds_max_purge_files) {
    dout(20) << "Throttling on item limit " << in_flight.size()
             << "/" << cct->_conf->mds_max_purge_files << dendl;
    return false;
  } else {
    return true;
  }
}

void PurgeQueue::_go_readonly(int r)
{
  if (readonly) return;
  dout(1) << "going readonly because internal IO failed: " << strerror(-r) << dendl;
  readonly = true;
  finisher.queue(on_error, r);
  on_error = nullptr;
  journaler.set_readonly();
  finish_contexts(g_ceph_context, waiting_for_recovery, r);
}

bool PurgeQueue::_consume()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  bool could_consume = false;
  while(_can_consume()) {

    if (delayed_flush) {
      // We are now going to read from the journal, so any proactive
      // flush is no longer necessary.  This is not functionally necessary
      // but it can avoid generating extra fragmented flush IOs.
      timer.cancel_event(delayed_flush);
      delayed_flush = nullptr;
    }

    if (int r = journaler.get_error()) {
      derr << "Error " << r << " recovering write_pos" << dendl;
      _go_readonly(r);
      return could_consume;
    }

    if (!journaler.is_readable()) {
      dout(10) << " not readable right now" << dendl;
      // Because we are the writer and the reader of the journal
      // via the same Journaler instance, we never need to reread_head
      if (!journaler.have_waiter()) {
        journaler.wait_for_readable(new LambdaContext([this](int r) {
          std::lock_guard l(lock);
          if (r == 0) {
            _consume();
          } else if (r != -CEPHFS_EAGAIN) {
            _go_readonly(r);
          }
        }));
      }

      return could_consume;
    }

    could_consume = true;
    // The journaler is readable: consume an entry
    bufferlist bl;
    bool readable = journaler.try_read_entry(bl);
    ceph_assert(readable);  // we checked earlier

    dout(20) << " decoding entry" << dendl;
    PurgeItem item;
    auto q = bl.cbegin();
    try {
      decode(item, q);
    } catch (const buffer::error &err) {
      derr << "Decode error at read_pos=0x" << std::hex
           << journaler.get_read_pos() << dendl;
      _go_readonly(CEPHFS_EIO);
    }
    dout(20) << " executing item (" << item.ino << ")" << dendl;
    _execute_item(item, journaler.get_read_pos());
  }

  dout(10) << " cannot consume right now" << dendl;

  return could_consume;
}

class C_IO_PurgeItem_Commit : public Context {
public:
  C_IO_PurgeItem_Commit(PurgeQueue *pq, std::vector<PurgeItemCommitOp> ops, uint64_t expire_to)
    : purge_queue(pq), ops_vec(std::move(ops)), expire_to(expire_to) {
  }

  void finish(int r) override {
    purge_queue->_commit_ops(r, ops_vec, expire_to);
  }

private:
  PurgeQueue *purge_queue;
  std::vector<PurgeItemCommitOp> ops_vec;
  uint64_t expire_to;
};

void PurgeQueue::_commit_ops(int r, const std::vector<PurgeItemCommitOp>& ops_vec, uint64_t expire_to)
{
  if (r < 0) {
    derr << " r = " << r << dendl;
    return;
  }

  SnapContext nullsnapc;
  C_GatherBuilder gather(cct);

  for (auto &op : ops_vec) {
    dout(10) << op.item.get_type_str() << dendl;
    if (op.type == PurgeItemCommitOp::PURGE_OP_RANGE) {
      uint64_t first_obj = 0, num_obj = 0;
      uint64_t num = Striper::get_num_objects(op.item.layout, op.item.size);
      num_obj = num;

      if (op.item.action == PurgeItem::TRUNCATE_FILE) {
        first_obj = 1;
        if (num > 1)
          num_obj = num - 1;
        else
          continue;
      }

      filer.purge_range(op.item.ino, &op.item.layout, op.item.snapc,
                        first_obj, num_obj, ceph::real_clock::now(), op.flags,
                        gather.new_sub());
    } else if (op.type == PurgeItemCommitOp::PURGE_OP_REMOVE) {
      if (op.item.action == PurgeItem::PURGE_DIR) {
        objecter->remove(op.oid, op.oloc, nullsnapc,
                         ceph::real_clock::now(), op.flags,
                         gather.new_sub());
      } else {
        objecter->remove(op.oid, op.oloc, op.item.snapc,
                         ceph::real_clock::now(), op.flags,
                         gather.new_sub());
      }
    } else if (op.type == PurgeItemCommitOp::PURGE_OP_ZERO) {
      filer.zero(op.item.ino, &op.item.layout, op.item.snapc,
                 0, op.item.layout.object_size, ceph::real_clock::now(), 0, true,
                 gather.new_sub());
    } else {
      derr << "Invalid purge op: " << op.type << dendl;
      ceph_abort();
    }
  }

  ceph_assert(gather.has_subs());

  gather.set_finisher(new C_OnFinisher(
	              new LambdaContext([this, expire_to](int r) {
    std::lock_guard l(lock);

    if (r == -CEPHFS_EBLOCKLISTED) {
      finisher.queue(on_error, r);
      on_error = nullptr;
      return;
    }

    _execute_item_complete(expire_to);
    _consume();

    // Have we gone idle?  If so, do an extra write_head now instead of
    // waiting for next flush after journaler_write_head_interval.
    // Also do this periodically even if not idle, so that the persisted
    // expire_pos doesn't fall too far behind our progress when consuming
    // a very long queue.
    if (!readonly &&
        (in_flight.empty() || journaler.write_head_needed())) {
      journaler.write_head(nullptr);
    }
  }), &finisher));

  gather.activate();
}

void PurgeQueue::_execute_item(
    const PurgeItem &item,
    uint64_t expire_to)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  in_flight[expire_to] = item;
  logger->set(l_pq_executing, in_flight.size());
  files_high_water = std::max<uint64_t>(files_high_water,
                              in_flight.size());
  logger->set(l_pq_executing_high_water, files_high_water);
  auto ops = _calculate_ops(item);
  ops_in_flight += ops;
  logger->set(l_pq_executing_ops, ops_in_flight);
  ops_high_water = std::max(ops_high_water, ops_in_flight);
  logger->set(l_pq_executing_ops_high_water, ops_high_water);

  std::vector<PurgeItemCommitOp> ops_vec;
  auto submit_ops = [&]() {
    finisher.queue(new C_IO_PurgeItem_Commit(this, std::move(ops_vec), expire_to));
  };

  if (item.action == PurgeItem::PURGE_FILE) {
    if (item.size > 0) {
      uint64_t num = Striper::get_num_objects(item.layout, item.size);
      dout(10) << " 0~" << item.size << " objects 0~" << num
               << " snapc " << item.snapc << " on " << item.ino << dendl;
      ops_vec.emplace_back(item, PurgeItemCommitOp::PURGE_OP_RANGE, 0);
    }

    // remove the backtrace object if it was not purged
    object_t oid = CInode::get_object_name(item.ino, frag_t(), "");
    if (ops_vec.empty() || !item.layout.pool_ns.empty()) {
      object_locator_t oloc(item.layout.pool_id);
      dout(10) << " remove backtrace object " << oid
               << " pool " << oloc.pool << " snapc " << item.snapc << dendl;
      ops_vec.emplace_back(item, PurgeItemCommitOp::PURGE_OP_REMOVE, 0, oid, oloc);
    }

    // remove old backtrace objects
    for (const auto &p : item.old_pools) {
      object_locator_t oloc(p);
      dout(10) << " remove backtrace object " << oid
               << " old pool " << p << " snapc " << item.snapc << dendl;
      ops_vec.emplace_back(item, PurgeItemCommitOp::PURGE_OP_REMOVE, 0, oid, oloc);
    }
  } else if (item.action == PurgeItem::PURGE_DIR) {
    object_locator_t oloc(metadata_pool);
    frag_vec_t leaves;
    if (!item.fragtree.is_leaf(frag_t()))
      item.fragtree.get_leaves(leaves);
    leaves.push_back(frag_t());
    for (const auto &leaf : leaves) {
      object_t oid = CInode::get_object_name(item.ino, leaf, "");
      dout(10) << " remove dirfrag " << oid << dendl;
      ops_vec.emplace_back(item, PurgeItemCommitOp::PURGE_OP_REMOVE, 0, oid, oloc);
    }
  } else if (item.action == PurgeItem::TRUNCATE_FILE) {
    const uint64_t num = Striper::get_num_objects(item.layout, item.size);
    dout(10) << " 0~" << item.size << " objects 0~" << num
	     << " snapc " << item.snapc << " on " << item.ino << dendl;

    // keep backtrace object
    if (num > 1) {
      ops_vec.emplace_back(item, PurgeItemCommitOp::PURGE_OP_RANGE, 0);
    }
    ops_vec.emplace_back(item, PurgeItemCommitOp::PURGE_OP_ZERO, 0);
  } else {
    derr << "Invalid item (action=" << item.action << ") in purge queue, "
            "dropping it" << dendl;
    ops_in_flight -= ops;
    logger->set(l_pq_executing_ops, ops_in_flight);
    ops_high_water = std::max(ops_high_water, ops_in_flight);
    logger->set(l_pq_executing_ops_high_water, ops_high_water);
    in_flight.erase(expire_to);
    logger->set(l_pq_executing, in_flight.size());
    files_high_water = std::max<uint64_t>(files_high_water,
                                in_flight.size());
    logger->set(l_pq_executing_high_water, files_high_water);
    return;
  }

  submit_ops();
}

void PurgeQueue::_execute_item_complete(
    uint64_t expire_to)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  dout(10) << "complete at 0x" << std::hex << expire_to << std::dec << dendl;
  ceph_assert(in_flight.count(expire_to) == 1);

  auto iter = in_flight.find(expire_to);
  ceph_assert(iter != in_flight.end());
  if (iter == in_flight.begin()) {
    uint64_t pos = expire_to;
    if (!pending_expire.empty()) {
      auto n = iter;
      ++n;
      if (n == in_flight.end()) {
	pos = *pending_expire.rbegin();
	pending_expire.clear();
      } else {
	auto p = pending_expire.begin();
	do {
	  if (*p >= n->first)
	    break;
	  pos = *p;
	  pending_expire.erase(p++);
	} while (p != pending_expire.end());
      }
    }
    dout(10) << "expiring to 0x" << std::hex << pos << std::dec << dendl;
    journaler.set_expire_pos(pos);
  } else {
    // This is completely fine, we're not supposed to purge files in
    // order when doing them in parallel.
    dout(10) << "non-sequential completion, not expiring anything" << dendl;
    pending_expire.insert(expire_to);
  }

  auto executed_ops = _calculate_ops(iter->second);
  ops_in_flight -= executed_ops;
  logger->set(l_pq_executing_ops, ops_in_flight);
  ops_high_water = std::max(ops_high_water, ops_in_flight);
  logger->set(l_pq_executing_ops_high_water, ops_high_water);

  dout(10) << "completed item for ino " << iter->second.ino << dendl;

  in_flight.erase(iter);
  logger->set(l_pq_executing, in_flight.size());
  files_high_water = std::max<uint64_t>(files_high_water,
                              in_flight.size());
  logger->set(l_pq_executing_high_water, files_high_water);
  dout(10) << "in_flight.size() now " << in_flight.size() << dendl;

  uint64_t write_pos = journaler.get_write_pos(); 
  uint64_t read_pos = journaler.get_read_pos(); 
  uint64_t expire_pos = journaler.get_expire_pos(); 
  uint64_t item_num = (write_pos - (in_flight.size() ? expire_pos : read_pos)) 
		      / purge_item_journal_size;
  dout(10) << "left purge items in journal: " << item_num 
    << " (purge_item_journal_size/write_pos/read_pos/expire_pos) now at " 
    << "(" << purge_item_journal_size << "/" << write_pos << "/" << read_pos 
    << "/" << expire_pos << ")" << dendl;

  logger->set(l_pq_item_in_journal, item_num);
  logger->inc(l_pq_executed_ops, executed_ops);
  logger->inc(l_pq_executed);
}

void PurgeQueue::update_op_limit(const MDSMap &mds_map)
{
  std::lock_guard l(lock);

  if (readonly) {
    dout(10) << "skipping; PurgeQueue is readonly" << dendl;
    return;
  }

  uint64_t pg_count = 0;
  objecter->with_osdmap([&](const OSDMap& o) {
    // Number of PGs across all data pools
    const std::vector<int64_t> &data_pools = mds_map.get_data_pools();
    for (const auto dp : data_pools) {
      if (o.get_pg_pool(dp) == NULL) {
        // It is possible that we have an older OSDMap than MDSMap,
        // because we don't start watching every OSDMap until after
        // MDSRank is initialized
        dout(4) << " data pool " << dp << " not found in OSDMap" << dendl;
        continue;
      }
      pg_count += o.get_pg_num(dp);
    }
  });

  // Work out a limit based on n_pgs / n_mdss, multiplied by the user's
  // preference for how many ops per PG
  max_purge_ops = uint64_t(((double)pg_count / (double)mds_map.get_max_mds()) *
			   cct->_conf->mds_max_purge_ops_per_pg);

  // User may also specify a hard limit, apply this if so.
  if (cct->_conf->mds_max_purge_ops) {
    max_purge_ops = std::min(max_purge_ops, cct->_conf->mds_max_purge_ops);
  }
}

void PurgeQueue::handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map)
{
  if (changed.count("mds_max_purge_ops")
      || changed.count("mds_max_purge_ops_per_pg")) {
    update_op_limit(mds_map);
  } else if (changed.count("mds_max_purge_files")) {
    std::lock_guard l(lock);
    if (in_flight.empty()) {
      // We might have gone from zero to a finite limit, so
      // might need to kick off consume.
      dout(4) << "maybe start work again (max_purge_files="
              << g_conf()->mds_max_purge_files << dendl;
      finisher.queue(new LambdaContext([this](int r){
        std::lock_guard l(lock);
        _consume();
      }));
    }
  }
}

bool PurgeQueue::drain(
    uint64_t *progress,
    uint64_t *progress_total,
    size_t *in_flight_count
    )
{
  std::lock_guard l(lock);

  if (readonly) {
    dout(10) << "skipping drain; PurgeQueue is readonly" << dendl;
    return true;
  }

  ceph_assert(progress != nullptr);
  ceph_assert(progress_total != nullptr);
  ceph_assert(in_flight_count != nullptr);

  const bool done = in_flight.empty() && (
      journaler.get_read_pos() == journaler.get_write_pos());
  if (done) {
    return true;
  }

  const uint64_t bytes_remaining = journaler.get_write_pos()
                                   - journaler.get_read_pos();

  if (!draining) {
    // Start of draining: remember how much there was outstanding at
    // this point so that we can give a progress percentage later
    draining = true;

    // Life the op throttle as this daemon now has nothing to do but
    // drain the purge queue, so do it as fast as we can.
    max_purge_ops = 0xffff;
  }

  drain_initial = std::max(bytes_remaining, drain_initial);

  *progress = drain_initial - bytes_remaining;
  *progress_total = drain_initial;
  *in_flight_count = in_flight.size();

  return false;
}

std::string_view PurgeItem::get_type_str() const
{
  switch(action) {
  case PurgeItem::NONE: return "NONE";
  case PurgeItem::PURGE_FILE: return "PURGE_FILE";
  case PurgeItem::PURGE_DIR: return "PURGE_DIR";
  case PurgeItem::TRUNCATE_FILE: return "TRUNCATE_FILE";
  default:
    return "UNKNOWN";
  }
}
