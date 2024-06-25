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
 */

#include "MDSRank.h"
#include "MDLog.h"
#include "MDCache.h"
#include "LogEvent.h"
#include "MDSContext.h"

#include "osdc/Journaler.h"
#include "mds/JournalPointer.h"

#include "common/entity_name.h"
#include "common/perf_counters.h"
#include "common/Cond.h"
#include "common/ceph_time.h"

#include "events/ESubtreeMap.h"
#include "events/ESegment.h"
#include "events/ELid.h"

#include "common/config.h"
#include "common/errno.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".log "

using namespace std;

const AutoSharedLogSegment MDLog::_null_segment = nullptr;

MDLog::MDLog(MDSRankBase* m)
  :
    mds(m),
    replay_thread(this),
    recovery_thread(this),
    submit_thread(this),
    log_trim_counter(DecayCounter(g_conf().get_val<double>("mds_log_trim_decay_rate")))
{
  debug_subtrees = g_conf().get_val<bool>("mds_debug_subtrees");
  event_large_threshold = g_conf().get_val<uint64_t>("mds_log_event_large_threshold");
  events_per_segment = g_conf().get_val<uint64_t>("mds_log_events_per_segment");
  pause = g_conf().get_val<bool>("mds_log_pause");
  major_segment_event_ratio = g_conf().get_val<uint64_t>("mds_log_major_segment_event_ratio");
  max_live_segments = g_conf().get_val<uint64_t>("mds_log_max_segments");
  max_live_events = g_conf().get_val<int64_t>("mds_log_max_events");
  skip_corrupt_events = g_conf().get_val<bool>("mds_log_skip_corrupt_events");
  skip_unbounded_events = g_conf().get_val<bool>("mds_log_skip_unbounded_events");
  upkeep_thread = std::thread(&MDLog::log_trim_upkeep, this);
}

MDLog::~MDLog()
{
  if (journaler) { delete journaler; journaler = 0; }
  if (logger) {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = 0;
  }
}

void MDLog::create_logger()
{
  PerfCountersBuilder plb(g_ceph_context, "mds_log", l_mdl_first, l_mdl_last);

  plb.add_u64_counter(l_mdl_evadd, "evadd", "Events submitted", "subm",
                      PerfCountersBuilder::PRIO_INTERESTING);
  plb.add_u64(l_mdl_ev, "ev", "Events", "evts",
              PerfCountersBuilder::PRIO_INTERESTING);
  plb.add_u64(l_mdl_seg, "seg", "Segments", "segs",
              PerfCountersBuilder::PRIO_INTERESTING);

  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  plb.add_u64(l_mdl_evlrg, "evlrg", "Large events");
  plb.add_u64(l_mdl_evexg, "evexg", "Expiring events");
  plb.add_u64(l_mdl_evexd, "evexd", "Current expired events");
  plb.add_u64(l_mdl_segexg, "segexg", "Expiring segments");
  plb.add_u64(l_mdl_segexd, "segexd", "Current expired segments");
  plb.add_u64(l_mdl_segmjr, "segmjr", "Major Segments");
  plb.add_u64_counter(l_mdl_replayed, "replayed", "Events replayed",
		      "repl", PerfCountersBuilder::PRIO_INTERESTING);
  plb.add_time_avg(l_mdl_jlat, "jlat", "Journaler flush latency");
  plb.add_u64_counter(l_mdl_evex, "evex", "Total expired events");
  plb.add_u64_counter(l_mdl_evtrm, "evtrm", "Trimmed events");
  plb.add_u64_counter(l_mdl_segadd, "segadd", "Segments added");
  plb.add_u64_counter(l_mdl_segex, "segex", "Total expired segments");
  plb.add_u64_counter(l_mdl_segtrm, "segtrm", "Trimmed segments");

  plb.set_prio_default(PerfCountersBuilder::PRIO_DEBUGONLY);
  plb.add_u64(l_mdl_expos, "expos", "Journaler xpire position");
  plb.add_u64(l_mdl_wrpos, "wrpos", "Journaler  write position");
  plb.add_u64(l_mdl_rdpos, "rdpos", "Journaler  read position");

  // logger
  logger = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void MDLog::set_write_iohint(unsigned iohint_flags)
{
  journaler->set_write_iohint(iohint_flags);
}

class C_MDL_WriteError : public MDSIOContextBase {
  protected:
  MDLog *mdlog;
  MDSRankBase *get_mds() override { return mdlog->mds; }

  void finish(int r) override {
    MDSRankBase *mds = get_mds();
    // assume journal is reliable, so don't choose action based on
    // g_conf()->mds_action_on_write_error.
    if (r == -CEPHFS_EBLOCKLISTED) {
      derr << "we have been blocklisted (fenced), respawning..." << dendl;
      mds->respawn();
    } else {
      derr << "unhandled error " << cpp_strerror(r) << ", shutting down..." << dendl;
      // Although it's possible that this could be something transient,
      // it's severe and scary, so disable this rank until an administrator
      // intervenes.
      mds->get_clog_ref()->error() << "Unhandled journal write error on MDS rank "
        << mds->get_nodeid() << ": " << cpp_strerror(r) << ", shutting down.";
      mds->damaged();
      ceph_abort();  // damaged should never return
    }
  }

  public:
  explicit C_MDL_WriteError(MDLog *m) :
    MDSIOContextBase(false), mdlog(m) {}
  void print(ostream& out) const override {
    out << "mdlog_write_error";
  }
};


void MDLog::write_head(MDSContext *c) 
{
  Context *fin = NULL;
  if (c != NULL) {
    fin = new C_IO_Wrapper(mds, c);
  }
  journaler->write_head(fin);
}

uint64_t MDLog::get_read_pos() const
{
  return journaler->get_read_pos(); 
}

uint64_t MDLog::get_write_pos() const
{
  return journaler->get_write_pos(); 
}

uint64_t MDLog::get_safe_pos() const
{
  return journaler->get_write_safe_pos(); 
}



void MDLog::create(MDSContext *c)
{
  dout(5) << "create empty log" << dendl;

  C_GatherBuilder gather(g_ceph_context);
  // This requires an OnFinisher wrapper because Journaler will call back the completion for write_head inside its own lock
  // XXX but should maybe that be handled inside Journaler?
  gather.set_finisher(new C_IO_Wrapper(mds, c));

  // The inode of the default Journaler we will create
  ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();

  // Instantiate Journaler and start async write to RADOS
  ceph_assert(journaler == NULL);
  journaler = mds->make_journaler("mdlog", ino, CEPH_FS_ONDISK_MAGIC, logger, l_mdl_jlat);
  ceph_assert(journaler->is_readonly());
  journaler->set_write_error_handler(new C_MDL_WriteError(this));
  journaler->set_writeable();
  journaler->create(mds->get_cache_log_proxy()->get_default_log_layout(), g_conf()->mds_journal_format);
  journaler->write_head(gather.new_sub());

  // Async write JournalPointer to RADOS
  JournalPointerStore* jps = mds->get_journal_pointer_store();
  jps->pointer.front = ino;
  jps->pointer.back = 0;
  jps->save(gather.new_sub());

  gather.activate();

  logger->set(l_mdl_expos, journaler->get_expire_pos());
  logger->set(l_mdl_wrpos, journaler->get_write_pos());

  submit_thread.create("md_submit");
}

void MDLog::open(MDSContext *c)
{
  dout(5) << "open discovering log bounds" << dendl;

  ceph_assert(!recovery_thread.is_started());
  recovery_thread.set_completion(c);
  recovery_thread.create("md_recov_open");

  submit_thread.create("md_submit");
  // either append() or replay() will follow.
}

/**
 * Final part of reopen() procedure, after recovery_thread
 * has done its thing we call append()
 */
class C_ReopenComplete : public MDSInternalContext {
  MDLog *mdlog;
  MDSContext *on_complete;
public:
  C_ReopenComplete(MDLog *mdlog_, MDSContext *on_complete_) : MDSInternalContext(mdlog_->mds), mdlog(mdlog_), on_complete(on_complete_) {}
  void finish(int r) override {
    mdlog->append();
    on_complete->complete(r);
  }
};

/**
 * Given that open() has been called in the past, go through the journal
 * recovery procedure again, potentially reformatting the journal if it
 * was in an old format.
 */
void MDLog::reopen(MDSContext *c)
{
  dout(5) << "reopen" << dendl;

  // Because we will call append() at the completion of this, check that we have already
  // read the whole journal.
  ceph_assert(journaler != NULL);
  ceph_assert(journaler->get_read_pos() == journaler->get_write_pos());

  delete journaler;
  journaler = NULL;

  // recovery_thread was started at some point in the past.  Although
  // it has called it's completion if we made it back here, it might
  // still not have been cleaned up: join it.
  recovery_thread.join();

  recovery_thread.set_completion(new C_ReopenComplete(this, c));
  recovery_thread.create("md_recov_reopen");
}

void MDLog::append()
{
  dout(5) << "append positioning at end and marking writeable" << dendl;
  journaler->set_read_pos(journaler->get_write_pos());
  journaler->set_expire_pos(journaler->get_write_pos());
  
  journaler->set_writeable();

  logger->set(l_mdl_expos, journaler->get_write_pos());
}



// -------------------------------------------------

const AutoSharedLogSegment& MDLog::_start_new_segment(SegmentBoundary* sb)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  auto ls = LogSegment::create(event_seq);
  auto [iter, _] = unexpired_segments.insert_or_assign(event_seq, ls);
  logger->inc(l_mdl_segadd);
  logger->set(l_mdl_seg, count_total_segments());
  sb->set_seq(event_seq);

  // Adjust to next stray dir
  if (!mds->is_stopping()) {
    mds->get_cache_log_proxy()->advance_stray();
  }
  return iter->second;;
}

void MDLog::submit_entry(LogEvent *le, MDSLogContextBase* c)
{
  dout(20) << __func__ << " " << *le << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));
  ceph_assert(!mds->is_any_replay());
  ceph_assert(!mds_is_shutting_down);

  event_seq++;
  events_since_last_major_segment++;

  if (auto sb = dynamic_cast<SegmentBoundary*>(le); sb) {
    auto ls = _start_new_segment(sb);
    if (sb->is_major_segment_boundary()) {
      major_segment_seqs.insert(ls->seq);
      logger->set(l_mdl_segmjr, major_segment_seqs.size());
      events_since_last_major_segment = 0;
    }
  }

  EMetaBlob *metablob = le->get_metablob();
  if (metablob) {
    for (auto& in : metablob->get_touched_inodes()) {
      in->last_journaled = event_seq;
    }
  }

  // let the event register itself in the segment
  decltype(auto) ls = get_current_segment();
  ls->num_events++;

  le->_segment = ls;
  le->update_segment();
  le->set_stamp(ceph_clock_now());

  mdsmap_up_features = mds->get_mds_map()->get_up_features();
  num_live_events++;
  if (logger) {
    logger->inc(l_mdl_evadd);
    logger->set(l_mdl_ev, get_num_events());
  }

  std::lock_guard l(submit_mutex);
  pending_events[ls->seq].push_back(PendingEvent(le, c));
  submit_cond.notify_all();
}

void MDLog::segment_upkeep()
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));
  uint64_t period = journaler->get_layout_period();
  auto ls = get_current_segment();
  // start a new segment?
  if (events_since_last_major_segment > events_per_segment*major_segment_event_ratio) {
    dout(10) << __func__ << ": starting new major segment, current " << *ls << dendl;
    auto sle = mds->get_cache_log_proxy()->create_subtree_map();
    submit_entry(sle, NULL);
  } else if (ls->get_end()/period != ls->get_offset()/period || ls->num_events >= events_per_segment) {
    dout(10) << __func__ << ": starting new segment, current " << *ls << dendl;
    auto sb = new ESegment();
    submit_entry(sb, nullptr);
  } else if (debug_subtrees && ls->num_events > 1) {
    // debug: journal this every time to catch subtree replay bugs.
    // use a different event id so it doesn't get interpreted as a
    // LogSegment boundary on replay.
    dout(10) << __func__ << ": creating test subtree map" << dendl;
    auto sle = mds->get_cache_log_proxy()->create_subtree_map();
    sle->set_type(EVENT_SUBTREEMAP_TEST);
    submit_entry(sle, NULL);
  }
}

/**
 * Invoked on the flush after each entry submitted
 */
class C_MDL_Flushed : public MDSLogContextBase {
protected:
  MDLog *mdlog;
  MDSRankBase *get_mds() override {return mdlog->mds;}
  Context *wrapped;

  void finish(int r) override {
    if (wrapped)
      wrapped->complete(r);
  }

public:
  C_MDL_Flushed(MDLog *m, Context *w)
    : mdlog(m), wrapped(w) {}
  C_MDL_Flushed(MDLog *m, const AutoSharedLogSegment& ls, uint64_t start_pos, uint64_t end_pos) : mdlog(m), wrapped(NULL) {
    set_event_bounds(ls, start_pos, end_pos);
  }
};

void MDLog::_submit_thread()
{
  dout(10) << "_submit_thread start" << dendl;

  std::unique_lock locker{submit_mutex};

  while (!mds->is_daemon_stopping()) {
    if (pause) {
      submit_cond.wait(locker);
      continue;
    }

    map<uint64_t,list<PendingEvent> >::iterator it = pending_events.begin();
    if (it == pending_events.end()) {
      submit_cond.wait(locker);
      continue;
    }

    if (it->second.empty()) {
      pending_events.erase(it);
      continue;
    }

    int64_t features = mdsmap_up_features;
    PendingEvent data = it->second.front();
    it->second.pop_front();

    locker.unlock();

    if (data.le) {
      // will be destroyed when this scope ends.
      std::unique_ptr<LogEvent> le{data.le};
      LogSegment *ls = le->_segment;

      const uint64_t start_offset = journaler->get_write_pos();
      le->set_start_off(start_offset);

      // encode it, with event type
      bufferlist bl;
      le->encode_with_header(bl, features);

      if (bl.length() >= event_large_threshold.load()) {
        dout(5) << "large event detected!" << dendl;
        logger->inc(l_mdl_evlrg);
      }

      dout(5) << "_submit_thread " << start_offset << "~" << bl.length()
	      << " : " << *le << dendl;

      // journal it.
      const uint64_t end_offset = journaler->append_entry(bl); // bl is destroyed.

      if (data.fin) {
	MDSLogContextBase* mds_fin = dynamic_cast<MDSLogContextBase*>(data.fin);
        ceph_assert(mds_fin);
        mds_fin->set_event_bounds(ls, start_offset, end_offset);
      } else {
        // we should always have an MDSLogContextBase
        // completion handler for serialized events
        // because we update the safe_pos when it's finished
        data.fin = new C_MDL_Flushed(this, ls, start_offset, end_offset);
      }

      if (logger) {
	logger->set(l_mdl_wrpos, end_offset);
      }
    } else if (data.fin) {
      // let the locking wrapper handle the locking :)
      data.fin = new MDSLockingWrapper(data.fin, mds);
    }

    if (data.fin) {
      journaler->wait_for_flush(data.fin);
    }

    if (data.flush) {
      journaler->flush();
    }

    locker.lock();
    if (data.flush) {
      ceph_assert(queued_flushes > 0);
      --queued_flushes;
    }
  }
}

void MDLog::wait_for_safe(Context* c)
{
  ceph_assert(c);
  std::unique_lock locker { submit_mutex };

  bool have_pending = !pending_events.empty();
  if (have_pending) {
    pending_events.rbegin()->second.push_back(PendingEvent(NULL, c));
    submit_cond.notify_all();
  }

  locker.unlock();

  if (!have_pending) {
    journaler->wait_for_flush(new C_IO_Wrapper(mds, c));
  }
}

void MDLog::flush()
{
  std::unique_lock locker { submit_mutex };

  if (journaler->is_readonly()) {
    return;
  }

  if (!pending_events.empty()) {
    pending_events.rbegin()->second.push_back(PendingEvent(NULL, NULL, true));
    ++queued_flushes;
    submit_cond.notify_all();
  }

  bool flush_queued = queued_flushes > 0;
  locker.unlock();

  if (!flush_queued)
    journaler->flush();
}

void MDLog::kick_submitter()
{
  std::lock_guard l(submit_mutex);
  submit_cond.notify_all();
}

void MDLog::cap()
{
  dout(5) << "mark mds is shutting down" << dendl;
  mds_is_shutting_down = true;
}

void MDLog::shutdown()
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  dout(5) << "shutdown" << dendl;
  if (!submit_thread.am_self()) {
    submit_mutex.lock();
  }
  if (submit_thread.is_started()) {
    ceph_assert(mds->is_daemon_stopping());

    if (submit_thread.am_self()) {
      // Called suicide from the thread: trust it to do no work after
      // returning from suicide, and subsequently respect mds->is_daemon_stopping()
      // and fall out of its loop.
    } else {
      mds->get_lock().unlock();
      // Because MDS::stopping is true, it's safe to drop mds_lock: nobody else
      // picking it up will do anything with it.

      submit_cond.notify_all();
      submit_mutex.unlock();

      mds->get_lock().lock();

      submit_thread.join();
    }
  } else if (!submit_thread.am_self()) {
    submit_mutex.unlock();
  }

  upkeep_log_trim_shutdown = true;
  cond.notify_one();

  mds->get_lock().unlock();
  upkeep_thread.join();
  mds->get_lock().lock();

  // Replay thread can be stuck inside e.g. Journaler::wait_for_readable,
  // so we need to shutdown the journaler first.
  if (journaler) {
    journaler->shutdown();
  }

  if (replay_thread.is_started() && !replay_thread.am_self()) {
    mds->get_lock().unlock();
    replay_thread.join();
    mds->get_lock().lock();
  }

  if (recovery_thread.is_started() && !recovery_thread.am_self()) {
    mds->get_lock().unlock();
    recovery_thread.join();
    mds->get_lock().lock();
  }
}

class C_OFT_Committed : public MDSInternalContext {
  MDLog *mdlog;
  uint64_t seq;
public:
  C_OFT_Committed(MDLog *l, uint64_t s) :
    MDSInternalContext(l->mds), mdlog(l), seq(s) {}
  void finish(int ret) override {
    mdlog->trim_expired_segments();
  }
};

void MDLog::try_to_commit_open_file_table(uint64_t last_seq)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  if (mds_is_shutting_down) // shutting down the MDS
    return;

  auto c = new C_OFT_Committed(this, last_seq);
  if (!mds->get_cache_log_proxy()->oft_try_to_commit(
    c,
    last_seq,
    CEPH_MSG_PRIO_HIGH
  )) {
    delete c;
  }
}

bool MDLog::await_expiring_segments(Context *c)
{
  // code in this function assumes protection by the mds_lock
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  if (last_expiring_segment_seq.has_value()) {
    // if unexpired_segments contans sequences <= last_expiring
    // this iterator will be in the range (begin(), end())
    dout(10) << __func__ << ": last_expiring_seq=" << *last_expiring_segment_seq << dendl;
    auto it = unexpired_segments.upper_bound(*last_expiring_segment_seq);
    if (it != unexpired_segments.end() && it != unexpired_segments.begin()) {
      ceph_assert(unexpired_segments.size() > 1); //we shouldn't ever expire the last segment
      expiry_waiters[*last_expiring_segment_seq].push_back(c);
      return true;
    }
  }
  dout(10) << __func__ << ": nothing is expiring currently" << dendl;
  dout(20) << __func__ << ": unexpired segments: " << unexpired_segments << dendl;
  return false;
}

void MDLog::log_trim_upkeep(void) {
  dout(10) << dendl;

  std::unique_lock mds_lock(mds->get_lock());
  while (!upkeep_log_trim_shutdown.load()) {
    cond.wait_for(mds_lock, g_conf().get_val<std::chrono::milliseconds>("mds_log_trim_upkeep_interval"));

    if (mds->is_active() || mds->is_stopping()) {
      trim();
    }
  }
  dout(10) << __func__ << ": finished" << dendl;
}

void MDLog::trim()
{
  // code in this function assumes protection by the mds_lock
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));
  int max_ev = max_live_events;

  if (mds->get_cache_log_proxy()->is_readonly()) {
    dout(10) << __func__ << ": ignoring read-only FS" << dendl;
    return;
  }

  // Clamp max_ev to not be smaller than events per segment
  if (max_ev > 0 && (uint64_t)max_ev <= events_per_segment) {
    max_ev = events_per_segment + 1;
  }

  auto log_trim_counter_start = log_trim_counter.get();
  auto log_trim_threshold = g_conf().get_val<Option::size_t>("mds_log_trim_threshold");

  // trim!
  dout(10) << __func__ << ": "
           << count_live_segments() << " / " << max_live_segments << " segments, "
           << log_trim_counter_start << " / " << log_trim_threshold << " segments decay threshold, "
           << num_live_events << " / " << max_ev << " events"
           << ", " << num_expiring_segments << " (" << num_expiring_events << ") expiring"
           << ", " << expired_segments.size() << " (" << num_expired_events << ") expired"
           << dendl;

  // we never trim the last segment,
  // so we should have at least two unexpired to proceed.
  if (unexpired_segments.size() < 2) {
    dout(10) << __func__ << ": less than two unexpired segments, bailing out" << dendl;
    return;
  }

  if (log_trim_counter_start >= log_trim_threshold) {
    dout(10) << __func__ << ": decay counter is above threshold, can't proceed yet" << dendl;
    return;
  }

  int op_prio = CEPH_MSG_PRIO_LOW +
		(CEPH_MSG_PRIO_HIGH - CEPH_MSG_PRIO_LOW) *
		num_expiring_segments / max_live_segments;
  if (op_prio > CEPH_MSG_PRIO_HIGH)
    op_prio = CEPH_MSG_PRIO_HIGH;


  if (num_replayed_segments > 0){
    uint64_t num_total_segments = count_total_segments();
    ceph_assert(num_total_segments >= num_replayed_segments);
  }

  auto p = unexpired_segments.begin();
  if (last_expiring_segment_seq.has_value()) {
    // pick the first segment that hasn't been marked for expiry
    p = unexpired_segments.upper_bound(last_expiring_segment_seq.value());
  }

  auto trim_start = ceph::coarse_mono_clock::now();

  while (p != unexpired_segments.end()) {
    if ((count_live_segments() <= max_live_segments) &&
	(max_ev < 0 || num_live_events <= (uint64_t)max_ev)) {
      dout(10) << __func__ << ": breaking out of trim loop - segments/events fell below ceiling"
	       << " max_segments/max_ev" << dendl;
      break;
    }

    // take note of the current segment and peek the next one
    const AutoSharedLogSegment& ls = p->second;
    ceph_assert(ls);
    ++p;

    if (p == unexpired_segments.end()) {
      // no next segment, i.e. `ls` is the last segment, we shouldn't expire it
      dout(10) << __func__ << ": stopping at the last segment" << dendl;
      break;
    }

    if (!ls->has_bounds()) {
      dout(5) << __func__ << ": " << *ls << ": events are still pending" << dendl;
      break;
    }

    // We detect that `ls` has no more pending events by looking
    // at the next segment `p->second` and checking that it has bounds,
    // i.e. it had its first event serialized.
    // This is a little lazy but then we don't have to grab the submit mutex
    // to inspect the `pending_events`.
    if (!ls->end_is_safe(safe_pos) || !p->second->has_bounds()) {
      dout(5) << __func__ << ": " << *ls << " is not fully flushed yet: safe "
              << journaler->get_write_safe_pos() << " < end " << ls->get_end() << dendl;
      break;
    }

    _mark_for_expiry(ls, op_prio);

    if (log_trim_counter.hit() > log_trim_threshold) {
      dout(10) << __func__ << ": reached trim threshold of " << log_trim_threshold << dendl;
    }
  }

  auto trim_end = ceph::coarse_mono_clock::now();
  dout(10) << __func__ << ": processed "
           << log_trim_counter.get() - log_trim_counter_start << " segment(s) in " 
           << std::chrono::duration_cast<std::chrono::duration<double>>(trim_end - trim_start).count()
           << "s" << dendl;

  try_to_commit_open_file_table(get_last_segment_seq());

  // discard expired segments
  trim_expired_segments();
}

class C_MaybeExpiredSegment : public MDSInternalContext {
  MDLog *mdlog;
  AutoSharedLogSegment ls;
  int op_prio;
  public:
  C_MaybeExpiredSegment(MDLog *mdl, const AutoSharedLogSegment &s, int p) :
    MDSInternalContext(mdl->mds), mdlog(mdl), ls(s), op_prio(p) {}
  void finish(int res) override {
    if (res < 0)
      mdlog->mds->handle_write_error(res);
    mdlog->_maybe_expired(ls, op_prio);
  }
};

/**
 * Like MDLog::trim, but instead of trimming to max_segments, trim all but the latest
 * segment.
 */
int MDLog::trim_all()
{
  // code in this function assumes protection by the mds_lock
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  dout(10) << __func__ << ": "
	   << count_live_segments()
           << "/" << num_expiring_segments
           << "/" << expired_segments.size() << dendl;

  uint64_t last_seq = 0;
  if (!unexpired_segments.empty()) {
    last_seq = get_last_segment_seq();
    try_to_commit_open_file_table(last_seq);
  }

  auto p = unexpired_segments.begin();
  if (last_expiring_segment_seq.has_value()) {
    // pick the first segment that hasn't been marked for expiry
    p = unexpired_segments.upper_bound(last_expiring_segment_seq.value());
  }
  while (p != unexpired_segments.end() && p->first < last_seq && p->second->end_is_safe(safe_pos)) { // next segment should have been started
    const AutoSharedLogSegment &ls = p->second;
    ++p; // there should always be at least one more due to `p->first < last_seq`

    // Caller should have flushed journaler before calling this
    if (!p->second->has_bounds()) {
      dout(5) << __func__ << ": " << *ls << " may have pending events" << dendl;
      return -CEPHFS_EAGAIN;
    }

    _mark_for_expiry(ls, CEPH_MSG_PRIO_DEFAULT);
  }

  trim_expired_segments();

  return 0;
}

void MDLog::_mark_for_expiry(const AutoSharedLogSegment& ls, int op_prio)
{
  ceph_assert(!last_expiring_segment_seq.has_value() || last_expiring_segment_seq.value() < ls->seq);

  dout(15) << __func__ << " " << *ls << dendl;
  last_expiring_segment_seq = ls->seq;
  num_expiring_events += ls->num_events;
  num_expiring_segments += 1;

  ceph_assert(num_live_events >= ls->num_events);
  num_live_events -= ls->num_events;

  try_expire(ls, op_prio);
}

void MDLog::try_expire(const AutoSharedLogSegment& ls, int op_prio)
{
  // code in this function assumes protection by the mds_lock
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  MDSGatherBuilder gather_bld(g_ceph_context);
  dout(5) << __func__ << " " << *ls << dendl;
  ls->try_to_expire(mds, gather_bld, op_prio);

  if (gather_bld.has_subs()) {
    gather_bld.set_finisher(new C_MaybeExpiredSegment(this, ls, op_prio));
    gather_bld.activate();
  } else {
    ceph_assert(last_expiring_segment_seq >= ls->seq);
    _expired(ls);
  }

  logger->set(l_mdl_segexg, num_expiring_segments);
  logger->set(l_mdl_evexg, num_expiring_events);
}

void MDLog::_maybe_expired(const AutoSharedLogSegment& ls, int op_prio)
{
  if (mds->get_cache_log_proxy()->is_readonly()) {
    dout(10) << "_maybe_expired, ignoring read-only FS" <<  dendl;
    return;
  }

  try_expire(ls, op_prio);
}

void MDLog::trim_expired_segments()
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  // since we are trimming up to a major segment, exclusive,
  // there's nothing to do if we have less than 1 major known
  if (major_segment_seqs.size() < 1) {
    dout(5) << __func__ << ": not enough major segments" << dendl;
    return;
  }

  uint64_t const oft_committed_seq = mds->get_cache_log_proxy()->oft_get_committed_log_seq();

  // trim expired segments?
  bool trimmed = false;
  LogSegment::seq_t first_unexpired =
    unexpired_segments.size()
    ? unexpired_segments.begin()->first
    : LogSegment::SEQ_MAX;

  if (!mds_is_shutting_down && first_unexpired > oft_committed_seq) {
    dout(10) << __func__ << ": prevent trimming beyond file table committedseq " << oft_committed_seq
             << " (first_unexpired = " << first_unexpired << ")" << dendl;
    first_unexpired = oft_committed_seq;
  }

  // The major segment we can trim up to is the one that
  // has the highest seq less than or equal to first_unexpired,
  // or is the last major segment known.
  // The "one before upper_bound(unexpired)" points to such a member.
  auto trim_up_to_major_seq_it = major_segment_seqs.upper_bound(first_unexpired);
  if (trim_up_to_major_seq_it == major_segment_seqs.begin()) {
    dout(10) << __func__ << ": no major segment matching the required expiration condition" << dendl;
    return;
  }
  trim_up_to_major_seq_it = std::prev(trim_up_to_major_seq_it);
  dout(10) << __func__ << ": will trim up to major seq " << *trim_up_to_major_seq_it << dendl;

  uint64_t num_trimmed_segments = 0;
  uint64_t num_trimmed_events = 0;
  const bool ignore_zombies = !g_conf().get_val<bool>("mds_debug_zombie_log_segments");

  // Scan through the expired segments to keep accounting
  // NB: we know the offset of the next major segment
  //     so we could just trim up to there, but we want to
  //     be sure that all expired segments are accounted for
  uint64_t trim_up_to_pos = UINT64_MAX;
  LogSegment::seq_t last_exp_seq = 0;
  auto exp_it = expired_segments.begin();
  while (exp_it != expired_segments.end() && exp_it->first < *trim_up_to_major_seq_it) {
    auto& [exp_seq, exp_info] = *exp_it;
    dout(20) << __func__ << ": examining expired seq=" << exp_seq << dendl;
    num_trimmed_segments += 1;
    num_trimmed_events += exp_info.num_events;
    trim_up_to_pos = exp_info.end;
    last_exp_seq = exp_seq;
    if (auto segment = exp_info.segment.lock()) {
      dout(10) << __func__ << " found a zombie: " << *segment << dendl;
      ceph_assert(ignore_zombies);
    }
    ++exp_it;
  }

  logger->inc(l_mdl_evtrm, num_trimmed_events);
  logger->inc(l_mdl_segtrm, num_trimmed_segments);

  ceph_assert(last_exp_seq < *trim_up_to_major_seq_it);
  major_segment_seqs.erase(major_segment_seqs.begin(), major_segment_seqs.upper_bound(last_exp_seq));
  logger->set(l_mdl_segmjr, major_segment_seqs.size());

  expired_segments.erase(expired_segments.begin(), exp_it);
  ceph_assert(num_expired_events >= num_trimmed_events);
  num_expired_events -= num_trimmed_events;

  logger->set(l_mdl_evexd, num_expired_events);
  logger->set(l_mdl_segexd, expired_segments.size());

  auto jexpire_pos = journaler->get_expire_pos();
  // only consider `trim_up_to_pos` if we found at least one segment
  if (num_trimmed_events && jexpire_pos < trim_up_to_pos) {
    ceph_assert(trim_up_to_pos < UINT64_MAX);
    journaler->set_expire_pos(trim_up_to_pos);
    logger->set(l_mdl_expos, trim_up_to_pos);
    trimmed = true;
  } else {
    logger->set(l_mdl_expos, jexpire_pos);
  }

  num_replayed_segments -= std::min(num_replayed_segments, num_trimmed_segments);

  if (trimmed)
    journaler->write_head(0);
}

// we want this to take a strong reference to avoid races
// when removing the segment from unexpired
void MDLog::_expired(AutoSharedLogSegment ls)
{
  ceph_assert(ceph_mutex_is_locked_by_me(mds->get_lock()));

  dout(5) << __func__ << *ls << dendl;

  if (!mds_is_shutting_down && ls == peek_current_segment()) {
    ceph_abort_msg("unexpected expiration of the last segment");
  }
  // expired.
  expired_segments[ls->seq] = ExpiredSegmentInfo(ls);
  num_expired_events += ls->num_events;
  num_expiring_segments -= 1;
  num_expiring_events -= ls->num_events;

  ceph_assert(unexpired_segments.contains(ls->seq));
  unexpired_segments.erase(ls->seq);

  // now that the segment is no longer in unexpired
  // see if we can make any waiters happy
  auto it = expiry_waiters.begin();
  auto first_unexpired = unexpired_segments.size() ? unexpired_segments.begin()->first : LogSegment::SEQ_MAX;
  while (it != expiry_waiters.end() && it->first < first_unexpired) {
    for(auto c: it->second) {
      c->complete(0);
    }
    ++it;
  }
  expiry_waiters.erase(expiry_waiters.begin(), it);

  logger->inc(l_mdl_evex, ls->num_events);
  logger->inc(l_mdl_segex);
  logger->set(l_mdl_ev, num_live_events);
  logger->set(l_mdl_evexd, num_expired_events);
  logger->set(l_mdl_segexd, expired_segments.size());
}

void MDLog::replay(MDSContext *c)
{
  ceph_assert(journaler->is_active());
  ceph_assert(journaler->is_readonly());

  // empty?
  if (journaler->get_read_pos() == journaler->get_write_pos()) {
    dout(10) << "replay - journal empty, done." << dendl;
    mds->get_cache_log_proxy()->trim();
    if (mds->is_standby_replay())
      mds->update_mlogger();
    if (c) {
      c->complete(0);
    }
    return;
  }

  // add waiter
  if (c)
    waitfor_replay.push_back(c);

  // go!
  dout(10) << "replay start, from " << journaler->get_read_pos()
	   << " to " << journaler->get_write_pos() << dendl;

  ceph_assert(count_total_events() == 0 || already_replayed);
  if (already_replayed) {
    // Ensure previous instance of ReplayThread is joined before
    // we create another one
    replay_thread.join();
  }
  already_replayed = true;

  replay_thread.create("md_log_replay");
}


/**
 * Resolve the JournalPointer object to a journal file, and
 * instantiate a Journaler object.  This may re-write the journal
 * if the journal in RADOS appears to be in an old format.
 *
 * This is a separate thread because of the way it is initialized from inside
 * the mds lock, which is also the global objecter lock -- rather than split
 * it up into hard-to-read async operations linked up by contexts, 
 *
 * When this function completes, the `journaler` attribute will be set to
 * a Journaler instance using the latest available serialization format.
 */
void MDLog::_recovery_thread(MDSContext *completion)
{
  ceph_assert(journaler == NULL);
  if (g_conf()->mds_journal_format > JOURNAL_FORMAT_MAX) {
      dout(0) << "Configuration value for mds_journal_format is out of bounds, max is "
              << JOURNAL_FORMAT_MAX << dendl;

      // Oh dear, something unreadable in the store for this rank: require
      // operator intervention.
      mds->damaged_unlocked();
      ceph_abort();  // damaged should not return
  }

  // First, read the pointer object.
  // If the pointer object is not present, then create it with
  // front = default ino and back = null
  JournalPointerStore *jps = mds->get_journal_pointer_store();
  const int read_result = jps->load();
  if (read_result == -CEPHFS_ENOENT) {
    inodeno_t const default_log_ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
    jps->pointer.front = default_log_ino;
    int write_result = jps->save();
    if (write_result < 0) {
      std::lock_guard l(mds->get_lock());
      if (mds->is_daemon_stopping()) {
        return;
      }
      mds->damaged();
      ceph_abort();  // damaged should never return
    }
  } else if (read_result == -CEPHFS_EBLOCKLISTED) {
    derr << "Blocklisted during JournalPointer read!  Respawning..." << dendl;
    mds->respawn();
    ceph_abort(); // Should be unreachable because respawn calls execv
  } else if (read_result != 0) {
    mds->get_clog_ref()->error() << "failed to read JournalPointer: " << read_result
                       << " (" << cpp_strerror(read_result) << ")";
    mds->damaged_unlocked();
    ceph_abort();  // Should be unreachable because damaged() calls respawn()
  }

  // If the back pointer is non-null, that means that a journal
  // rewrite failed part way through.  Erase the back journal
  // to clean up.
  if (jps->pointer.back) {
    if (mds->is_standby_replay()) {
      dout(1) << "Journal " << jps->pointer.front << " is being rewritten, "
              << "cannot replay in standby until an active MDS completes rewrite" << dendl;
      std::lock_guard l(mds->get_lock());
      if (mds->is_daemon_stopping()) {
        return;
      }
      completion->complete(-CEPHFS_EAGAIN);
      return;
    }
    dout(1) << "Erasing journal " << jps->pointer.back << dendl;
    C_SaferCond erase_waiter;
    std::unique_ptr<Journaler> back(mds->make_journaler("mdlog", jps->pointer.back, CEPH_FS_ONDISK_MAGIC, logger, l_mdl_jlat));

    // Read all about this journal (header + extents)
    C_SaferCond recover_wait;
    back->recover(&recover_wait);
    int recovery_result = recover_wait.wait();
    if (recovery_result == -CEPHFS_EBLOCKLISTED) {
      derr << "Blocklisted during journal recovery!  Respawning..." << dendl;
      mds->respawn();
      ceph_abort(); // Should be unreachable because respawn calls execv
    } else if (recovery_result != 0) {
      // Journaler.recover succeeds if no journal objects are present: an error
      // means something worse like a corrupt header, which we can't handle here.
      mds->get_clog_ref()->error() << "Error recovering journal " << jps->pointer.front << ": "
        << cpp_strerror(recovery_result);
      mds->damaged_unlocked();
      ceph_assert(recovery_result == 0); // Unreachable because damaged() calls respawn()
    }

    // We could read journal, so we can erase it.
    back->erase(&erase_waiter);
    int erase_result = erase_waiter.wait();

    // If we are successful, or find no data, we can update the JournalPointer to
    // reflect that the back journal is gone.
    if (erase_result != 0 && erase_result != -CEPHFS_ENOENT) {
      derr << "Failed to erase journal " << jps->pointer.back << ": " << cpp_strerror(erase_result) << dendl;
    } else {
      dout(1) << "Successfully erased journal, updating journal pointer" << dendl;
      jps->pointer.back = 0;
      int write_result = jps->save();
      // Nothing graceful we can do for this
      ceph_assert(write_result >= 0);
    }
  }

  /* Read the header from the front journal */
  Journaler* front_journal = mds->make_journaler("mdlog", jps->pointer.front, CEPH_FS_ONDISK_MAGIC, logger, l_mdl_jlat);

  // Assign to ::journaler so that we can be aborted by ::shutdown while
  // waiting for journaler recovery
  {
    std::lock_guard l(mds->get_lock());
    journaler = front_journal;
  }

  C_SaferCond recover_wait;
  front_journal->recover(&recover_wait);
  dout(4) << "Waiting for journal " << jps->pointer.front << " to recover..." << dendl;
  int recovery_result = recover_wait.wait();
  if (recovery_result == -CEPHFS_EBLOCKLISTED) {
    derr << "Blocklisted during journal recovery!  Respawning..." << dendl;
    mds->respawn();
    ceph_abort(); // Should be unreachable because respawn calls execv
  } else if (recovery_result != 0) {
    mds->get_clog_ref()->error() << "Error recovering journal " << jps->pointer.front << ": "
                                 << cpp_strerror(recovery_result);
    mds->damaged_unlocked();
    ceph_assert(recovery_result == 0); // Unreachable because damaged() calls respawn()
  }
  dout(4) << "Journal " << jps->pointer.front << " recovered." << dendl;

  /* Check whether the front journal format is acceptable or needs re-write */
  if (front_journal->get_stream_format() > JOURNAL_FORMAT_MAX) {
    dout(0) << "Journal " << jps->pointer.front << " is in unknown format " << front_journal->get_stream_format()
            << ", does this MDS daemon require upgrade?" << dendl;
    {
      std::lock_guard l(mds->get_lock());
      if (mds->is_daemon_stopping()) {
        journaler = NULL;
        delete front_journal;
        return;
      }
      completion->complete(-CEPHFS_EINVAL);
    }
  } else if (mds->is_standby_replay() || front_journal->get_stream_format() >= g_conf()->mds_journal_format) {
    /* The journal is of configured format, or we are in standbyreplay and will
     * tolerate replaying old journals until we have to go active. Use front_journal as
     * our journaler attribute and complete */
    dout(4) << "Recovered journal " << jps->pointer.front << " in format " << front_journal->get_stream_format() << dendl;
    {
      std::lock_guard l(mds->get_lock());
      journaler->set_write_error_handler(new C_MDL_WriteError(this));
      if (mds->is_daemon_stopping()) {
        return;
      }
      completion->complete(0);
    }
  } else {
    /* Hand off to reformat routine, which will ultimately set the
     * completion when it has done its thing */
    dout(1) << "Journal " << jps->pointer.front << " has old format "
            << front_journal->get_stream_format() << ", it will now be updated" << dendl;
    _reformat_journal(jps, front_journal, completion);
  }
}

/**
 * Blocking rewrite of the journal to a new file, followed by
 * swap of journal pointer to point to the new one.
 *
 * We write the new journal to the 'back' journal from the JournalPointer,
 * swapping pointers to make that one the front journal only when we have
 * safely completed.
 */
void MDLog::_reformat_journal(JournalPointerStore *jps, Journaler *old_journal, MDSContext *completion)
{
  ceph_assert(jps != NULL);
  ceph_assert(completion != NULL);
  ceph_assert(old_journal != NULL);

  /* Set JournalPointer.back to the location we will write the new journal */
  inodeno_t primary_ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
  inodeno_t secondary_ino = MDS_INO_LOG_BACKUP_OFFSET + mds->get_nodeid();
  jps->pointer.back = (jps->pointer.front == primary_ino ? secondary_ino : primary_ino);
  int write_result = jps->save();
  ceph_assert(write_result == 0);

  /* Create the new Journaler file */
  Journaler *new_journal = mds->make_journaler("mdlog", jps->pointer.back, CEPH_FS_ONDISK_MAGIC, logger, l_mdl_jlat);
  dout(4) << "Writing new journal header " << jps->pointer.back << dendl;
  file_layout_t new_layout = old_journal->get_layout();
  new_journal->set_writeable();
  new_journal->create(&new_layout, g_conf()->mds_journal_format);

  /* Write the new journal header to RADOS */
  C_SaferCond write_head_wait;
  new_journal->write_head(&write_head_wait);
  write_head_wait.wait();

  // Read in the old journal, and whenever we have readable events,
  // write them to the new journal.
  int r = 0;

  // In old format journals before event_seq was introduced, the serialized
  // offset of a SubtreeMap message in the log is used as the unique ID for
  // a log segment.  Because we change serialization, this will end up changing
  // for us, so we have to explicitly update the fields that point back to that
  // log segment.
  std::map<LogSegment::seq_t, LogSegment::seq_t> segment_pos_rewrite;

  // The logic in here borrowed from replay_thread expects mds_lock to be held,
  // e.g. between checking readable and doing wait_for_readable so that journaler
  // state doesn't change in between.
  uint32_t events_transcribed = 0;
  while (1) {
    old_journal->poll();
    if (old_journal->get_error()) {
      r = old_journal->get_error();
      dout(0) << "_replay journaler got error " << r << ", aborting" << dendl;
      break;
    }

    if (!old_journal->is_readable() &&
	old_journal->get_read_pos() == old_journal->get_write_pos())
      break;

    // Read one serialized LogEvent
    ceph_assert(old_journal->is_readable());
    bufferlist bl;
    uint64_t le_pos = old_journal->get_read_pos();
    bool r = old_journal->try_read_entry(bl);
    if (!r && old_journal->get_error())
      continue;
    ceph_assert(r);

    // Update segment_pos_rewrite
    auto le = LogEvent::decode_event(bl.cbegin());
    if (le) {
      bool modified = false;

      if (auto sb = dynamic_cast<SegmentBoundary*>(le.get()); sb) {
        if (sb->get_seq() == 0) {
          // A non-explicit event seq: the effective sequence number 
          // of this segment is it's position in the old journal and
          // the new effective sequence number will be its position
          // in the new journal.
          segment_pos_rewrite[le_pos] = new_journal->get_write_pos();
          dout(20) << __func__ << " discovered segment seq mapping "
            << le_pos << " -> " << new_journal->get_write_pos() << dendl;
        }
      } else {
        event_seq++;
      }

      // Rewrite segment references if necessary
      EMetaBlob *blob = le->get_metablob();
      if (blob) {
        modified = blob->rewrite_truncate_finish(mds, segment_pos_rewrite);
      }

      // Zero-out expire_pos in subtreemap because offsets have changed
      // (expire_pos is just an optimization so it's safe to eliminate it)
      if (le->get_type() == EVENT_SUBTREEMAP
          || le->get_type() == EVENT_SUBTREEMAP_TEST) {
        auto& sle = dynamic_cast<ESubtreeMap&>(*le);
        dout(20) << __func__ << " zeroing expire_pos in subtreemap event at "
          << le_pos << " seq=" << sle.get_seq() << dendl;
        sle.expire_pos = 0;
        modified = true;
      }

      if (modified) {
        bl.clear();
        le->encode_with_header(bl, mds->get_mds_map()->get_up_features());
      }
    } else {
      // Failure from LogEvent::decode, our job is to change the journal wrapper,
      // not validate the contents, so pass it through.
      dout(1) << __func__ << " transcribing un-decodable LogEvent at old position "
        << old_journal->get_read_pos() << ", new position " << new_journal->get_write_pos()
        << dendl;
    }

    // Write (buffered, synchronous) one serialized LogEvent
    events_transcribed += 1;
    new_journal->append_entry(bl);
  }

  dout(1) << "Transcribed " << events_transcribed << " events, flushing new journal" << dendl;
  C_SaferCond flush_waiter;
  new_journal->flush(&flush_waiter);
  flush_waiter.wait();

  // If failed to rewrite journal, leave the part written journal
  // as garbage to be cleaned up next startup.
  ceph_assert(r == 0);

  /* Now that the new journal is safe, we can flip the pointers */
  jps->pointer.rotate();
  write_result = jps->save();
  ceph_assert(write_result == 0);

  /* Delete the old journal to free space */
  dout(1) << "New journal flushed, erasing old journal" << dendl;
  C_SaferCond erase_waiter;
  old_journal->erase(&erase_waiter);
  int erase_result = erase_waiter.wait();
  ceph_assert(erase_result == 0);
  {
    std::lock_guard l(mds->get_lock());
    if (mds->is_daemon_stopping()) {
      delete new_journal;
      return;
    }
    ceph_assert(journaler == old_journal);
    journaler = NULL;
    delete old_journal;

    /* Update the pointer to reflect we're back in clean single journal state. */
    jps->pointer.back = 0;
    write_result = jps->save();
    ceph_assert(write_result == 0);

    /* Reset the Journaler object to its default state */
    dout(1) << "Journal rewrite complete, continuing with normal startup" << dendl;
    if (mds->is_daemon_stopping()) {
      delete new_journal;
      return;
    }
    journaler = new_journal;
    journaler->set_readonly();
    journaler->set_write_error_handler(new C_MDL_WriteError(this));

    /* Trigger completion */
    if (mds->is_daemon_stopping()) {
      return;
    }
    completion->complete(0);
  }
}


// i am a separate thread
void MDLog::_replay_thread()
{
  dout(10) << "_replay_thread start" << dendl;

  // loop
  int r = 0;
  while (1) {
    // wait for read?
    journaler->poll();
    if (journaler->get_error()) {
      r = journaler->get_error();
      dout(0) << "_replay journaler got error " << r << ", aborting" << dendl;
      if (r == -CEPHFS_ENOENT) {
        if (mds->is_standby_replay()) {
          // journal has been trimmed by somebody else
          r = -CEPHFS_EAGAIN;
        } else {
          mds->get_clog_ref()->error() << "missing journal object";
          mds->damaged_unlocked();
          ceph_abort();  // Should be unreachable because damaged() calls respawn()
        }
      } else if (r == -CEPHFS_EINVAL) {
        if (journaler->get_read_pos() < journaler->get_expire_pos()) {
          // this should only happen if you're following somebody else
          if(journaler->is_readonly()) {
            dout(0) << "expire_pos is higher than read_pos, returning CEPHFS_EAGAIN" << dendl;
            r = -CEPHFS_EAGAIN;
          } else {
            mds->get_clog_ref()->error() << "invalid journaler offsets";
            mds->damaged_unlocked();
            ceph_abort();  // Should be unreachable because damaged() calls respawn()
          }
        } else {
          /* re-read head and check it
           * Given that replay happens in a separate thread and
           * the MDS is going to either shut down or restart when
           * we return this error, doing it synchronously is fine
           * -- as long as we drop the main mds lock--. */
          C_SaferCond reread_fin;
          journaler->reread_head(&reread_fin);
          int err = reread_fin.wait();
          if (err) {
            if (err == -CEPHFS_ENOENT && mds->is_standby_replay()) {
              r = -CEPHFS_EAGAIN;
              dout(1) << "Journal header went away while in standby replay, journal rewritten?"
                      << dendl;
              break;
            } else {
                dout(0) << "got error while reading head: " << cpp_strerror(err)
                        << dendl;

                mds->get_clog_ref()->error() << "error reading journal header";
                mds->damaged_unlocked();
                ceph_abort();  // Should be unreachable because damaged() calls
                            // respawn()
            }
          }
	  standby_cleanup_trimmed_segments();
          if (journaler->get_read_pos() < journaler->get_expire_pos()) {
            dout(0) << "expire_pos is higher than read_pos, returning CEPHFS_EAGAIN" << dendl;
            r = -CEPHFS_EAGAIN;
          }
        }
      }
      break;
    }

    if (journaler->get_read_pos() == journaler->get_write_pos()) {
      dout(10) << "_replay: read_pos == write_pos" << dendl;
      break;
    }
    
    // read it
    uint64_t pos = journaler->get_read_pos();
    bufferlist bl;
    bool r = journaler->try_read_entry(bl);
    if (!r && journaler->get_error())
      continue;
    ceph_assert(r);
    
    // unpack event
    auto le = LogEvent::decode_event(bl.cbegin());
    if (!le) {
      dout(0) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos() 
	      << " -- unable to decode event" << dendl;
      dout(0) << "dump of unknown or corrupt event:\n";
      bl.hexdump(*_dout);
      *_dout << dendl;

      mds->get_clog_ref()->error() << "corrupt journal event at " << pos << "~"
                         << bl.length() << " / "
                         << journaler->get_write_pos();
      if (skip_corrupt_events) {
        continue;
      } else {
        mds->damaged_unlocked();
        ceph_abort();  // Should be unreachable because damaged() calls
                    // respawn()
      }
    } else if (!unexpired_segments.empty() && dynamic_cast<ELid*>(le.get())) {
      /* This can reasonably happen when a up:stopping MDS restarts after
       * writing ELid. We will merge with the previous segment.
       * We are enforcing the constraint that ESubtreeMap should begin
       * the journal.
       */
      dout(20) << "found ELid not at the start of the journal" << dendl;
      continue;
    }
    le->set_start_off(pos);

    events_since_last_major_segment++;
    if (auto sb = dynamic_cast<SegmentBoundary*>(le.get()); sb) {
      auto seq = sb->get_seq();
      if (seq > 0) {
        event_seq = seq;
      } else {
        event_seq = pos;
      }
      unexpired_segments[event_seq] = LogSegment::create(event_seq, pos);
      logger->set(l_mdl_seg, count_total_segments());
      if (sb->is_major_segment_boundary()) {
        major_segment_seqs.insert(event_seq);
        logger->set(l_mdl_segmjr, major_segment_seqs.size());
        events_since_last_major_segment = 0;
      }
    } else {
      event_seq++;
    }

    if (major_segment_seqs.empty()) {
      dout(0) << __func__ << " " << pos << "~" << bl.length() << " / "
              << journaler->get_write_pos() << " " << le->get_stamp()
              << " -- waiting for major segment."
              << dendl;
      dout(0) << " Log event is " << *le << dendl;
      if (skip_unbounded_events) {
        dout(5) << __func__ << " skipping!" << dendl;
        continue;
      } else {
        mds->damaged_unlocked();
        ceph_abort();  // Should be unreachable because damaged() calls
                       // respawn()
      }
    }

    dout(10) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos()
             << " " << le->get_stamp() << ": " << *le << dendl;
    le->_segment = get_current_segment();    // replay may need this
    le->_segment->num_events++;
    le->_segment->bounds_upkeep(journaler->get_read_pos());
    num_live_events++;
    logger->set(l_mdl_ev, count_total_events());

    {
      std::lock_guard l(mds->get_lock());
      if (mds->is_daemon_stopping()) {
        return;
      }
      logger->inc(l_mdl_replayed);
      le->replay(mds);
    }

    logger->set(l_mdl_rdpos, pos);
    logger->set(l_mdl_expos, journaler->get_expire_pos());
    logger->set(l_mdl_wrpos, journaler->get_write_pos());
  }

  // done!
  if (r == 0) {
    ceph_assert(journaler->get_read_pos() == journaler->get_write_pos());
    dout(10) << "_replay - complete, " << count_total_events()
	     << " events" << dendl;

    logger->set(l_mdl_expos, journaler->get_expire_pos());
  }

  safe_pos = journaler->get_write_safe_pos();

  dout(10) << "_replay_thread kicking waiters" << dendl;
  {
    std::lock_guard l(mds->get_lock());
    if (mds->is_daemon_stopping()) {
      return;
    }
    num_replayed_segments = count_total_segments();  // get num of logs when replay is finished
    finish_contexts(g_ceph_context, waitfor_replay, r);  
  }

  dout(10) << "_replay_thread finish" << dendl;
}

void MDLog::standby_cleanup_trimmed_segments()
{
  uint64_t expire_pos = journaler->get_expire_pos();
  dout(10) << __func__ << " expire_pos=" << expire_pos << dendl;

  mds->get_cache_log_proxy()->oft_trim_destroyed_inos(expire_pos);

  uint64_t removed_segments = 0;
  while (unexpired_segments.size()) {
    auto it = unexpired_segments.begin();
    auto ls = it->second;
    if (!ls->has_bounds()) {
      dout(10) << __func__ << " won't remove "
               << " segment seq=" << ls->seq
               << ", not flushed !" << dendl;
      break;
    }
    dout(10) << __func__ << " segment " << *ls << dendl;

    if (!ls->end_is_safe(expire_pos)) {
      dout(10) << __func__ << " won't remove, not expired!" << dendl;
      break;
    }

    if (unexpired_segments.size() == 1) {
      dout(10) << __func__ << " won't remove, last segment!" << dendl;
      break;
    }

    dout(10) << " removing segment" << dendl;
    mds->get_cache_log_proxy()->standby_trim_segment(ls);
    unexpired_segments.erase(it);
    ++removed_segments;
  }

  num_replayed_segments -= std::min(num_replayed_segments, removed_segments);
  if (removed_segments) {
    dout(20) << " calling mdcache->trim!" << dendl;
    mds->get_cache_log_proxy()->trim();
  } else {
    dout(20) << " removed no segments!" << dendl;
  }
}

void MDLog::dump_replay_status(Formatter *f) const
{
  f->open_object_section("replay_status");
  f->dump_unsigned("journal_read_pos", journaler ? journaler->get_read_pos() : 0);
  f->dump_unsigned("journal_write_pos", journaler ? journaler->get_write_pos() : 0);
  f->dump_unsigned("journal_expire_pos", journaler ? journaler->get_expire_pos() : 0);
  f->dump_unsigned("num_events", get_num_events());
  f->dump_unsigned("num_segments", get_num_segments());
  f->close_section();
}


void MDLog::handle_conf_change(const std::set<std::string>& changed, const MDSMap& mdsmap)
{
  if (changed.count("mds_debug_subtrees")) {
    debug_subtrees = g_conf().get_val<bool>("mds_debug_subtrees");
  }
  if (changed.count("mds_log_event_large_threshold")) {
    event_large_threshold = g_conf().get_val<uint64_t>("mds_log_event_large_threshold");
  }
  if (changed.count("mds_log_events_per_segment")) {
    events_per_segment = g_conf().get_val<uint64_t>("mds_log_events_per_segment");
  }
  if (changed.count("mds_log_major_segment_event_ratio")) {
    major_segment_event_ratio = g_conf().get_val<uint64_t>("mds_log_major_segment_event_ratio");
  }
  if (changed.count("mds_log_max_events")) {
    max_live_events = g_conf().get_val<int64_t>("mds_log_max_events");
  }
  if (changed.count("mds_log_max_segments")) {
    max_live_segments = g_conf().get_val<uint64_t>("mds_log_max_segments");
  }
  if (changed.count("mds_log_pause")) {
    pause = g_conf().get_val<bool>("mds_log_pause");
    if (!pause) {
      kick_submitter();
    }
  }
  if (changed.count("mds_log_skip_corrupt_events")) {
    skip_corrupt_events = g_conf().get_val<bool>("mds_log_skip_corrupt_events");
  }
  if (changed.count("mds_log_skip_unbounded_events")) {
    skip_unbounded_events = g_conf().get_val<bool>("mds_log_skip_unbounded_events");
  }
  if (changed.count("mds_log_trim_decay_rate")) {
    log_trim_counter = DecayCounter(g_conf().get_val<double>("mds_log_trim_decay_rate"));
  }
}