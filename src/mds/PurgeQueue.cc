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
static ostream& _prefix(std::ostream *_dout, mds_rank_t rank) {
  return *_dout << "mds." << rank << ".purge_queue ";
}

void PurgeItem::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode((uint8_t)action, bl);
  ::encode(ino, bl);
  ::encode(size, bl);
  ::encode(layout, bl, CEPH_FEATURE_FS_FILE_LAYOUT_V2);
  ::encode(old_pools, bl);
  ::encode(snapc, bl);
  ::encode(fragtree, bl);
  ENCODE_FINISH(bl);
}

void PurgeItem::decode(bufferlist::iterator &p)
{
  DECODE_START(1, p);
  ::decode((uint8_t&)action, p);
  ::decode(ino, p);
  ::decode(size, p);
  ::decode(layout, p);
  ::decode(old_pools, p);
  ::decode(snapc, p);
  ::decode(fragtree, p);
  DECODE_FINISH(p);
}

// TODO: if Objecter has any slow requests, take that as a hint and
// slow down our rate of purging (keep accepting pushes though)
PurgeQueue::PurgeQueue(
      CephContext *cct_,
      mds_rank_t rank_,
      const int64_t metadata_pool_,
      Objecter *objecter_,
      Context *on_error_)
  :
    cct(cct_),
    rank(rank_),
    lock("PurgeQueue"),
    metadata_pool(metadata_pool_),
    finisher(cct, "PurgeQueue", "PQ_Finisher"),
    timer(cct, lock),
    filer(objecter_, &finisher),
    objecter(objecter_),
    journaler("pq", MDS_INO_PURGE_QUEUE + rank, metadata_pool,
      CEPH_FS_ONDISK_MAGIC, objecter_, nullptr, 0,
      &finisher),
    on_error(on_error_),
    ops_in_flight(0),
    max_purge_ops(0),
    drain_initial(0),
    draining(false),
    delayed_flush(nullptr),
    recovered(false)
{
  assert(cct != nullptr);
  assert(on_error != nullptr);
  assert(objecter != nullptr);
  journaler.set_write_error_handler(on_error);
}

PurgeQueue::~PurgeQueue()
{
  if (logger) {
    g_ceph_context->get_perfcounters_collection()->remove(logger.get());
  }
}

void PurgeQueue::create_logger()
{
  PerfCountersBuilder pcb(g_ceph_context,
          "purge_queue", l_pq_first, l_pq_last);
  pcb.add_u64(l_pq_executing_ops, "pq_executing_ops", "Purge queue ops in flight");
  pcb.add_u64(l_pq_executing, "pq_executing", "Purge queue tasks in flight");
  pcb.add_u64_counter(l_pq_executed, "pq_executed", "Purge queue tasks executed", "purg",
      PerfCountersBuilder::PRIO_INTERESTING);

  logger.reset(pcb.create_perf_counters());
  g_ceph_context->get_perfcounters_collection()->add(logger.get());
}

void PurgeQueue::init()
{
  Mutex::Locker l(lock);

  assert(logger != nullptr);

  finisher.start();
  timer.init();
}

void PurgeQueue::activate()
{
  Mutex::Locker l(lock);
  if (journaler.get_read_pos() == journaler.get_write_pos())
    return;

  if (in_flight.empty()) {
    dout(4) << "start work (by drain)" << dendl;
    finisher.queue(new FunctionContext([this](int r) {
	  Mutex::Locker l(lock);
	  _consume();
	  }));
  }
}

void PurgeQueue::shutdown()
{
  Mutex::Locker l(lock);

  journaler.shutdown();
  timer.shutdown();
  finisher.stop();
}

void PurgeQueue::open(Context *completion)
{
  dout(4) << "opening" << dendl;

  Mutex::Locker l(lock);

  if (completion)
    waiting_for_recovery.push_back(completion);

  journaler.recover(new FunctionContext([this](int r){
    if (r == -ENOENT) {
      dout(1) << "Purge Queue not found, assuming this is an upgrade and "
                 "creating it." << dendl;
      create(NULL);
    } else if (r == 0) {
      Mutex::Locker l(lock);
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
      on_error->complete(r);
    }
  }));
}

void PurgeQueue::wait_for_recovery(Context* c)
{
  Mutex::Locker l(lock);
  if (recovered)
    c->complete(0);
  else
    waiting_for_recovery.push_back(c);
}

void PurgeQueue::_recover()
{
  assert(lock.is_locked_by_me());

  // Journaler::is_readable() adjusts write_pos if partial entry is encountered
  while (1) {
    if (!journaler.is_readable() &&
	!journaler.get_error() &&
	journaler.get_read_pos() < journaler.get_write_pos()) {
      journaler.wait_for_readable(new FunctionContext([this](int r) {
        Mutex::Locker l(lock);
	_recover();
      }));
      return;
    }

    if (journaler.get_error()) {
      int r = journaler.get_error();
      derr << "Error " << r << " recovering write_pos" << dendl;
      on_error->complete(r);
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
    assert(readable);  // we checked earlier
  }
}

void PurgeQueue::create(Context *fin)
{
  dout(4) << "creating" << dendl;
  Mutex::Locker l(lock);

  if (fin)
    waiting_for_recovery.push_back(fin);

  file_layout_t layout = file_layout_t::get_default();
  layout.pool_id = metadata_pool;
  journaler.set_writeable();
  journaler.create(&layout, JOURNAL_FORMAT_RESILIENT);
  journaler.write_head(new FunctionContext([this](int r) {
    Mutex::Locker l(lock);
    recovered = true;
    finish_contexts(g_ceph_context, waiting_for_recovery);
  }));
}

/**
 * The `completion` context will always be called back via a Finisher
 */
void PurgeQueue::push(const PurgeItem &pi, Context *completion)
{
  dout(4) << "pushing inode 0x" << std::hex << pi.ino << std::dec << dendl;
  Mutex::Locker l(lock);

  // Callers should have waited for open() before using us
  assert(!journaler.is_readonly());

  bufferlist bl;

  ::encode(pi, bl);
  journaler.append_entry(bl);
  journaler.wait_for_flush(completion);

  // Maybe go ahead and do something with it right away
  bool could_consume = _consume();
  if (!could_consume) {
    // Usually, it is not necessary to explicitly flush here, because the reader
    // will get flushes generated inside Journaler::is_readable.  However,
    // if we remain in a can_consume()==false state for a long period then
    // we should flush in order to allow MDCache to drop its strays rather
    // than having them wait for purgequeue to progress.
    if (!delayed_flush) {
      delayed_flush = new FunctionContext([this](int r){
            delayed_flush = nullptr;
            journaler.flush();
          });

      timer.add_event_after(
          g_conf->mds_purge_queue_busy_flush_period,
          delayed_flush);
    }
  }
}

uint32_t PurgeQueue::_calculate_ops(const PurgeItem &item) const
{
  uint32_t ops_required = 0;
  if (item.action == PurgeItem::PURGE_DIR) {
    // Directory, count dirfrags to be deleted
    std::list<frag_t> ls;
    if (!item.fragtree.is_leaf(frag_t())) {
      item.fragtree.get_leaves(ls);
    }
    // One for the root, plus any leaves
    ops_required = 1 + ls.size();
  } else {
    // File, work out concurrent Filer::purge deletes
    const uint64_t num = (item.size > 0) ?
      Striper::get_num_objects(item.layout, item.size) : 1;

    ops_required = MIN(num, g_conf->filer_max_purge_ops);

    // Account for removing (or zeroing) backtrace
    ops_required += 1;

    // Account for deletions for old pools
    if (item.action != PurgeItem::TRUNCATE_FILE) {
      ops_required += item.old_pools.size();
    }
  }

  return ops_required;
}

bool PurgeQueue::can_consume()
{
  dout(20) << ops_in_flight << "/" << max_purge_ops << " ops, "
           << in_flight.size() << "/" << g_conf->mds_max_purge_files
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

bool PurgeQueue::_consume()
{
  assert(lock.is_locked_by_me());

  bool could_consume = false;
  while(can_consume()) {
    could_consume = true;

    if (delayed_flush) {
      // We are now going to read from the journal, so any proactive
      // flush is no longer necessary.  This is not functionally necessary
      // but it can avoid generating extra fragmented flush IOs.
      timer.cancel_event(delayed_flush);
      delayed_flush = nullptr;
    }

    if (!journaler.is_readable()) {
      dout(10) << " not readable right now" << dendl;
      // Because we are the writer and the reader of the journal
      // via the same Journaler instance, we never need to reread_head
      if (!journaler.have_waiter()) {
        journaler.wait_for_readable(new FunctionContext([this](int r) {
          Mutex::Locker l(lock);
          if (r == 0) {
            _consume();
          }
        }));
      }

      return could_consume;
    }

    // The journaler is readable: consume an entry
    bufferlist bl;
    bool readable = journaler.try_read_entry(bl);
    assert(readable);  // we checked earlier

    dout(20) << " decoding entry" << dendl;
    PurgeItem item;
    bufferlist::iterator q = bl.begin();
    try {
      ::decode(item, q);
    } catch (const buffer::error &err) {
      derr << "Decode error at read_pos=0x" << std::hex
           << journaler.get_read_pos() << dendl;
      on_error->complete(0);
    }
    dout(20) << " executing item (0x" << std::hex << item.ino
             << std::dec << ")" << dendl;
    _execute_item(item, journaler.get_read_pos());
  }

  dout(10) << " cannot consume right now" << dendl;

  return could_consume;
}

void PurgeQueue::_execute_item(
    const PurgeItem &item,
    uint64_t expire_to)
{
  assert(lock.is_locked_by_me());

  in_flight[expire_to] = item;
  logger->set(l_pq_executing, in_flight.size());
  ops_in_flight += _calculate_ops(item);
  logger->set(l_pq_executing_ops, ops_in_flight);

  SnapContext nullsnapc;

  C_GatherBuilder gather(cct);
  if (item.action == PurgeItem::PURGE_FILE) {
    if (item.size > 0) {
      uint64_t num = Striper::get_num_objects(item.layout, item.size);
      dout(10) << " 0~" << item.size << " objects 0~" << num
               << " snapc " << item.snapc << " on " << item.ino << dendl;
      filer.purge_range(item.ino, &item.layout, item.snapc,
                        0, num, ceph::real_clock::now(), 0,
                        gather.new_sub());
    }

    // remove the backtrace object if it was not purged
    object_t oid = CInode::get_object_name(item.ino, frag_t(), "");
    if (!gather.has_subs() || !item.layout.pool_ns.empty()) {
      object_locator_t oloc(item.layout.pool_id);
      dout(10) << " remove backtrace object " << oid
               << " pool " << oloc.pool << " snapc " << item.snapc << dendl;
      objecter->remove(oid, oloc, item.snapc,
                            ceph::real_clock::now(), 0,
                            gather.new_sub());
    }

    // remove old backtrace objects
    for (const auto &p : item.old_pools) {
      object_locator_t oloc(p);
      dout(10) << " remove backtrace object " << oid
               << " old pool " << p << " snapc " << item.snapc << dendl;
      objecter->remove(oid, oloc, item.snapc,
                            ceph::real_clock::now(), 0,
                            gather.new_sub());
    }
  } else if (item.action == PurgeItem::PURGE_DIR) {
    object_locator_t oloc(metadata_pool);
    std::list<frag_t> frags;
    if (!item.fragtree.is_leaf(frag_t()))
      item.fragtree.get_leaves(frags);
    frags.push_back(frag_t());
    for (const auto &frag : frags) {
      object_t oid = CInode::get_object_name(item.ino, frag, "");
      dout(10) << " remove dirfrag " << oid << dendl;
      objecter->remove(oid, oloc, nullsnapc,
                       ceph::real_clock::now(),
                       0, gather.new_sub());
    }
  } else if (item.action == PurgeItem::TRUNCATE_FILE) {
    const uint64_t num = Striper::get_num_objects(item.layout, item.size);
    dout(10) << " 0~" << item.size << " objects 0~" << num
	     << " snapc " << item.snapc << " on " << item.ino << dendl;

    // keep backtrace object
    if (num > 1) {
      filer.purge_range(item.ino, &item.layout, item.snapc,
			1, num - 1, ceph::real_clock::now(),
			0, gather.new_sub());
    }
    filer.zero(item.ino, &item.layout, item.snapc,
	       0, item.layout.object_size,
	       ceph::real_clock::now(),
	       0, true, gather.new_sub());
  } else {
    derr << "Invalid item (action=" << item.action << ") in purge queue, "
            "dropping it" << dendl;
    in_flight.erase(expire_to);
    logger->set(l_pq_executing, in_flight.size());
    return;
  }
  assert(gather.has_subs());

  gather.set_finisher(new C_OnFinisher(
                      new FunctionContext([this, expire_to](int r){
    Mutex::Locker l(lock);
    _execute_item_complete(expire_to);

    _consume();

    // Have we gone idle?  If so, do an extra write_head now instead of
    // waiting for next flush after journaler_write_head_interval.
    // Also do this periodically even if not idle, so that the persisted
    // expire_pos doesn't fall too far behind our progress when consuming
    // a very long queue.
    if (in_flight.empty() || journaler.write_head_needed()) {
      journaler.write_head(new FunctionContext([this](int r){
            journaler.trim();
            }));
    }
  }), &finisher));

  gather.activate();
}

void PurgeQueue::_execute_item_complete(
    uint64_t expire_to)
{
  assert(lock.is_locked_by_me());
  dout(10) << "complete at 0x" << std::hex << expire_to << std::dec << dendl;
  assert(in_flight.count(expire_to) == 1);

  auto iter = in_flight.find(expire_to);
  assert(iter != in_flight.end());
  if (iter == in_flight.begin()) {
    // This was the lowest journal position in flight, so we can now
    // safely expire the journal up to here.
    dout(10) << "expiring to 0x" << std::hex << expire_to << std::dec << dendl;
    journaler.set_expire_pos(expire_to);
  } else {
    // This is completely fine, we're not supposed to purge files in
    // order when doing them in parallel.
    dout(10) << "non-sequential completion, not expiring anything" << dendl;
  }

  ops_in_flight -= _calculate_ops(iter->second);
  logger->set(l_pq_executing_ops, ops_in_flight);

  dout(10) << "completed item for ino 0x" << std::hex << iter->second.ino
           << std::dec << dendl;

  in_flight.erase(iter);
  logger->set(l_pq_executing, in_flight.size());
  dout(10) << "in_flight.size() now " << in_flight.size() << dendl;

  logger->inc(l_pq_executed);
}

void PurgeQueue::update_op_limit(const MDSMap &mds_map)
{
  Mutex::Locker l(lock);

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
    max_purge_ops = MIN(max_purge_ops, cct->_conf->mds_max_purge_ops);
  }
}

void PurgeQueue::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed,
                             const MDSMap &mds_map)
{
  if (changed.count("mds_max_purge_ops")
      || changed.count("mds_max_purge_ops_per_pg")) {
    update_op_limit(mds_map);
  } else if (changed.count("mds_max_purge_files")) {
    Mutex::Locker l(lock);

    if (in_flight.empty()) {
      // We might have gone from zero to a finite limit, so
      // might need to kick off consume.
      dout(4) << "maybe start work again (max_purge_files="
              << conf->mds_max_purge_files << dendl;
      finisher.queue(new FunctionContext([this](int r){
        Mutex::Locker l(lock);
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
  assert(progress != nullptr);
  assert(progress_total != nullptr);
  assert(in_flight_count != nullptr);

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

  drain_initial = max(bytes_remaining, drain_initial);

  *progress = drain_initial - bytes_remaining;
  *progress_total = drain_initial;
  *in_flight_count = in_flight.size();

  return false;
}

