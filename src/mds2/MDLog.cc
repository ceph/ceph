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
#include "JournalPointer.h"

#include "common/entity_name.h"
#include "common/perf_counters.h"
#include "common/Cond.h"

#include "events/ESubtreeMap.h"

#include "common/config.h"
#include "common/errno.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".log "

// cons/des
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

  plb.add_u64_counter(l_mdl_evadd, "evadd",
      "Events submitted", "subm");
  plb.add_u64_counter(l_mdl_evex, "evex", "Total expired events");
  plb.add_u64_counter(l_mdl_evtrm, "evtrm", "Trimmed events");
  plb.add_u64(l_mdl_ev, "ev",
      "Events", "evts");
  plb.add_u64(l_mdl_evexg, "evexg", "Expiring events");
  plb.add_u64(l_mdl_evexd, "evexd", "Current expired events");

  plb.add_u64_counter(l_mdl_segadd, "segadd", "Segments added");
  plb.add_u64_counter(l_mdl_segex, "segex", "Total expired segments");
  plb.add_u64_counter(l_mdl_segtrm, "segtrm", "Trimmed segments");
  plb.add_u64(l_mdl_seg, "seg",
      "Segments", "segs");
  plb.add_u64(l_mdl_segexg, "segexg", "Expiring segments");
  plb.add_u64(l_mdl_segexd, "segexd", "Current expired segments");

  plb.add_u64(l_mdl_expos, "expos", "Journaler xpire position");
  plb.add_u64(l_mdl_wrpos, "wrpos", "Journaler  write position");
  plb.add_u64(l_mdl_rdpos, "rdpos", "Journaler  read position");
  plb.add_u64(l_mdl_jlat, "jlat", "Journaler flush latency");

  // logger
  logger = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void MDLog::set_write_iohint(unsigned iohint_flags)
{
  journaler->set_write_iohint(iohint_flags);
}

class C_MDL_WriteError : public MDSContextBase {
  protected:
  MDLog *mdlog;
  MDSRank *get_mds() {return mdlog->mds;}

  void finish(int r) {
    MDSRank *mds = get_mds();
    // assume journal is reliable, so don't choose action based on
    // g_conf->mds_action_on_write_error.
    if (r == -EBLACKLISTED) {
      derr << "we have been blacklisted (fenced), respawning..." << dendl;
      mds->respawn();
    } else {
      derr << "unhandled error " << cpp_strerror(r) << ", shutting down..." << dendl;
      // Although it's possible that this could be something transient,
      // it's severe and scary, so disable this rank until an administrator
      // intervenes.
      mds->clog->error() << "Unhandled journal write error on MDS rank " <<
        mds->get_nodeid() << ": " << cpp_strerror(r) << ", shutting down.";
      mds->damaged();
      assert(0);  // damaged should never return
    }
  }

  public:
  explicit C_MDL_WriteError(MDLog *m) : mdlog(m) {}
};

uint64_t MDLog::get_read_pos()
{
  return journaler->get_read_pos(); 
}

uint64_t MDLog::get_write_pos()
{
  return journaler->get_write_pos(); 
}

uint64_t MDLog::get_safe_pos()
{
  return journaler->get_write_safe_pos(); 
}


void MDLog::create(MDSContextBase *c)
{
  dout(5) << "create empty log" << dendl;

  C_GatherBuilder gather(g_ceph_context, c);

  // The inode of the default Journaler we will create
  ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();

  // Instantiate Journaler and start async write to RADOS
  assert(journaler == NULL);
  journaler = new Journaler(ino, mds->mdcache->get_metadata_pool(), CEPH_FS_ONDISK_MAGIC, mds->objecter,
			    logger, l_mdl_jlat,
			    &mds->timer,
                            mds->get_log_finisher());
  assert(journaler->is_readonly());
  journaler->set_write_error_handler(new C_MDL_WriteError(this));
  journaler->set_writeable();
  journaler->create(&mds->mdcache->get_default_log_layout(), g_conf->mds_journal_format);
  journaler->write_head(gather.new_sub());

  // Async write JournalPointer to RADOS
  JournalPointer jp(mds->get_nodeid(), mds->mdcache->get_metadata_pool());
  jp.front = ino;
  jp.back = 0;
  jp.save(mds->objecter, gather.new_sub());

  gather.activate();

  logger->set(l_mdl_expos, journaler->get_expire_pos());
  logger->set(l_mdl_wrpos, journaler->get_write_pos());

  submit_thread.create("md_submit");
}

void MDLog::open(MDSContextBase *c)
{
  dout(5) << "open discovering log bounds" << dendl;

  assert(!recovery_thread.is_started());
  recovery_thread.set_completion(c);
  recovery_thread.create("md_recov_open");

  submit_thread.create("md_submit");
  // either append() or replay() will follow.
}

/**
 * Final part of reopen() procedure, after recovery_thread
 * has done its thing we call append()
 */
class C_ReopenComplete : public MDSContext {
  MDLog *mdlog;
  MDSContextBase *on_complete;
public:
  C_ReopenComplete(MDLog *mdlog_, MDSContextBase *on_complete_) : MDSContext(mdlog_->mds), mdlog(mdlog_), on_complete(on_complete_) {}
  void finish(int r) {
    mdlog->append();
    on_complete->complete(r);
  }
};

/**
 * Given that open() has been called in the past, go through the journal
 * recovery procedure again, potentially reformatting the journal if it
 * was in an old format.
 */
void MDLog::reopen(MDSContextBase *c)
{
  dout(5) << "reopen" << dendl;

  // Because we will call append() at the completion of this, check that we have already
  // read the whole journal.
  assert(journaler != NULL);
  assert(journaler->get_read_pos() == journaler->get_write_pos());

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

void MDLog::_start_entry(LogEvent *e)
{
  assert(submit_mutex.is_locked_by_me());

  assert(cur_event == NULL);
  cur_event = e;

  event_seq++;

  EMetaBlob *metablob = e->get_metablob();
  if (metablob) {
    metablob->event_seq = event_seq;
    metablob->last_subtree_map = get_last_segment_seq();
  }
}

void MDLog::cancel_entry(LogEvent *le)
{
  assert(le == cur_event);
  cur_event = NULL;
  delete le;
}

void MDLog::_submit_entry(LogEvent *le, MDSLogContextBase *c, bool flush)
{
  assert(submit_mutex.is_locked_by_me());
  assert(!mds->is_any_replay());
  assert(!capped);

  assert(le == cur_event);
  cur_event = NULL;

  if (!g_conf->mds_log) {
    // hack: log is disabled.
    if (c) {
      c->complete(0);
    }
    return;
  }

  // let the event register itself in the segment
  assert(!segments.empty());
  LogSegment *ls = segments.rbegin()->second;
  ls->num_events++;

  le->_segment = ls;
  le->update_segment();
  le->set_stamp(ceph_clock_now(g_ceph_context));

  pending_events[ls->seq].push_back(PendingEvent(le, c, flush));
  num_events++;

  if (logger) {
    logger->inc(l_mdl_evadd);
    logger->set(l_mdl_ev, num_events);
  }

  unflushed++;
  
  uint64_t period = journaler->get_layout_period();
  // start a new segment?
  if (le->get_type() == EVENT_SUBTREEMAP ||
      (le->get_type() == EVENT_IMPORTFINISH && mds->is_resolve())) {
    // avoid infinite loop when ESubtreeMap is very large.
    // do not insert ESubtreeMap among EImportFinish events that finish
    // disambiguate imports. Because the ESubtreeMap reflects the subtree
    // state when all EImportFinish events are replayed.
  } else if (ls->end/period != ls->offset/period ||
	     ls->num_events >= g_conf->mds_log_events_per_segment) {
    dout(10) << "submit_entry also starting new segment: last = "
	     << ls->seq  << "/" << ls->offset << ", event seq = " << event_seq << dendl;
    _start_new_segment();
  } else if (g_conf->mds_debug_subtrees &&
	     le->get_type() != EVENT_SUBTREEMAP_TEST) {
    // debug: journal this every time to catch subtree replay bugs.
    // use a different event id so it doesn't get interpreted as a
    // LogSegment boundary on replay.
    LogEvent *sle = mds->mdcache->create_subtree_map();
    sle->set_type(EVENT_SUBTREEMAP_TEST);
    _submit_entry(sle, NULL, false);
  }
}

void MDLog::set_safe_pos(uint64_t pos)
{
  assert(pos >= safe_pos.read());
  safe_pos.set(pos);
}

class C_MDL_Flushed : public MDSLogContextBase {
protected:
  MDLog *mdlog;
  MDSRank *get_mds() {return mdlog->mds; }

  MDSContextBase *wrapped;
  void finish(int r) {
    if (wrapped)
      wrapped->complete(r);
  }
public:
  C_MDL_Flushed(MDLog *l, MDSContextBase* c) : mdlog(l), wrapped(c) {}
  C_MDL_Flushed(MDLog *l, uint64_t pos) :
    mdlog(l), wrapped(NULL) {
    set_write_pos(pos);
  }
};

void MDLog::_submit_thread()
{
  dout(10) << "_submit_thread start" << dendl;

  submit_mutex.Lock();

  while (!mds->is_daemon_stopping()) {
    if (g_conf->mds_log_pause) {
      submit_cond.Wait(submit_mutex);
      continue;
    }

    map<uint64_t,list<PendingEvent> >::iterator it = pending_events.begin();
    if (it == pending_events.end()) {
      submit_cond.Wait(submit_mutex);
      continue;
    }

    if (it->second.empty()) {
      pending_events.erase(it);
      continue;
    }

    PendingEvent data = it->second.front();
    it->second.pop_front();

    submit_mutex.Unlock();

    if (data.le) {
      LogEvent *le = data.le;
      LogSegment *ls = le->_segment;
      // encode it, with event type
      bufferlist bl;
      le->encode_with_header(bl, mds->mdsmap->get_up_features());

      uint64_t write_pos = journaler->get_write_pos();

      le->set_start_off(write_pos);
      if (le->get_type() == EVENT_SUBTREEMAP)
	ls->offset = write_pos;

      dout(5) << "_submit_thread " << write_pos << "~" << bl.length()
	      << " : " << *le << dendl;

      // journal it.
      const uint64_t new_write_pos = journaler->append_entry(bl);  // bl is destroyed.
      ls->end = new_write_pos;

      if (data.fin) {
	MDSLogContextBase *fin = dynamic_cast<MDSLogContextBase*>(data.fin);
	assert(fin);
	fin->set_write_pos(new_write_pos);
      } else {
	data.fin = new C_MDL_Flushed(this, new_write_pos);
      }
  
      journaler->wait_for_flush(data.fin);
      if (data.flush)
	journaler->flush();

      if (logger)
	logger->set(l_mdl_wrpos, ls->end);

      delete le;
    } else {
      if (data.fin) {
	MDSAsyncContextBase *fin = dynamic_cast<MDSAsyncContextBase*>(data.fin);
	assert(fin);
	journaler->wait_for_flush(data.fin);
      }
      if (data.flush)
	journaler->flush();
    }

    submit_mutex.Lock();
    if (data.flush)
      unflushed = 0;
    else if (data.le)
      unflushed++;
  }

  submit_mutex.Unlock();
}

void MDLog::wait_for_safe(MDSAsyncContextBase *c)
{
  if (!g_conf->mds_log) {
    // hack: bypass.
    c->complete(0);
    return;
  }
    

  submit_mutex.Lock();

  bool no_pending = true;
  if (!pending_events.empty()) {
    pending_events.rbegin()->second.push_back(PendingEvent(NULL, c));
    no_pending = false;
    submit_cond.Signal();
  }

  submit_mutex.Unlock();

  if (no_pending && c)
    journaler->wait_for_flush(c);
}

void MDLog::flush()
{
  submit_mutex.Lock();

  bool do_flush = unflushed > 0;
  unflushed = 0;
  if (!pending_events.empty()) {
    auto &ls = pending_events.rbegin()->second;
    if (ls.empty())
      ls.push_back(PendingEvent(NULL, NULL, true));
    else
      ls.back().flush = true;
    do_flush = false;
    submit_cond.Signal();
  }

  submit_mutex.Unlock();

  if (do_flush)
    journaler->flush();
}

void MDLog::kick_submitter()
{
  Mutex::Locker l(submit_mutex);
  submit_cond.Signal();
}

void MDLog::cap()
{ 
  dout(5) << "cap" << dendl;
  capped = true;
}

void MDLog::shutdown()
{
  assert(mds->mds_lock.is_locked_by_me());

  dout(5) << "shutdown" << dendl;
  if (submit_thread.is_started()) {
    assert(mds->is_daemon_stopping());

    if (submit_thread.am_self()) {
      // Called suicide from the thread: trust it to do no work after
      // returning from suicide, and subsequently respect mds->is_daemon_stopping()
      // and fall out of its loop.
    } else {
      mds->mds_lock.Unlock();
      // Because MDS::stopping is true, it's safe to drop mds_lock: nobody else
      // picking it up will do anything with it.
   
      submit_mutex.Lock();
      submit_cond.Signal();
      submit_mutex.Unlock();

      mds->mds_lock.Lock();

      submit_thread.join();
    }
  }

  // Replay thread can be stuck inside e.g. Journaler::wait_for_readable,
  // so we need to shutdown the journaler first.
  if (journaler) {
    journaler->shutdown();
  }

  if (replay_thread.is_started() && !replay_thread.am_self()) {
    mds->mds_lock.Unlock();
    replay_thread.join();
    mds->mds_lock.Lock();
  }

  if (recovery_thread.is_started() && !recovery_thread.am_self()) {
    mds->mds_lock.Unlock();
    recovery_thread.join();
    mds->mds_lock.Lock();
  }
}


// -----------------------------
// segments

void MDLog::_start_new_segment()
{
  _prepare_new_segment();
  _journal_segment_subtree_map(NULL);
}

void MDLog::_prepare_new_segment()
{
  assert(submit_mutex.is_locked_by_me());

  uint64_t seq = event_seq + 1;
  dout(7) << __func__ << " seq " << seq << dendl;

  segments[seq] = new LogSegment(seq);

  logger->inc(l_mdl_segadd);
  logger->set(l_mdl_seg, segments.size());

  // Adjust to next stray dir
  dout(10) << "Advancing to next stray directory on mds " << mds->get_nodeid() 
	   << dendl;
  mds->mdcache->advance_stray();
}

void MDLog::_journal_segment_subtree_map(MDSLogContextBase *onsync)
{
  assert(submit_mutex.is_locked_by_me());

  dout(7) << __func__ << dendl;
  ESubtreeMap *sle = mds->mdcache->create_subtree_map();
  sle->event_seq = get_last_segment_seq();
  _submit_entry(sle, onsync, !!onsync);
}

void MDLog::journal_segment_subtree_map(MDSContextBase *onsync) {
  submit_mutex.Lock();
  _journal_segment_subtree_map(new C_MDL_Flushed(this, onsync));
  submit_mutex.Unlock();
  if (onsync)
    flush();
}

void MDLog::trim(int m)
{
  unsigned max_segments = g_conf->mds_log_max_segments;
  int max_events = g_conf->mds_log_max_events;
  if (m >= 0)
    max_events = m;

  if (mds->mdcache->is_readonly()) {
    dout(10) << "trim, ignoring read-only FS" <<  dendl;
    return;
  }

  // Clamp max_events to not be smaller than events per segment
  if (max_events > 0 && max_events <= g_conf->mds_log_events_per_segment) {
    max_events = g_conf->mds_log_events_per_segment + 1;
  }

  submit_mutex.Lock();

  // trim!
  dout(10) << "trim " 
	   << segments.size() << " / " << max_segments << " segments, " 
	   << num_events << " / " << max_events << " events"
	   << ", " << expiring_segments.size() << " (" << expiring_events << ") expiring"
	   << ", " << expired_segments.size() << " (" << expired_events << ") expired"
	   << dendl;

  if (segments.empty()) {
    submit_mutex.Unlock();
    return;
  }

  // hack: only trim for a few seconds at a time
  utime_t stop = ceph_clock_now(g_ceph_context);
  stop += 2.0;

  map<uint64_t,LogSegment*>::iterator p = segments.begin();
  while (p != segments.end() &&
	 ((max_events >= 0 &&
	   num_events - expiring_events - expired_events > max_events) ||
	  (segments.size() - expiring_segments.size() - expired_segments.size() > max_segments))) {
    
    if (stop < ceph_clock_now(g_ceph_context))
      break;

    int num_expiring_segments = (int)expiring_segments.size();
    if (num_expiring_segments >= g_conf->mds_log_max_expiring)
      break;

    int op_prio = CEPH_MSG_PRIO_LOW +
		  (CEPH_MSG_PRIO_HIGH - CEPH_MSG_PRIO_LOW) *
		  num_expiring_segments / g_conf->mds_log_max_expiring;

    // look at first segment
    LogSegment *ls = p->second;
    assert(ls);
    ++p;
    
    if (pending_events.count(ls->seq) ||
	ls->end > safe_pos.read()) {
      dout(5) << "trim segment " << ls->seq << "/" << ls->offset << ", not fully flushed yet, safe "
	      << journaler->get_write_safe_pos() << " < end " << ls->end << dendl;
      break;
    }
    if (expiring_segments.count(ls)) {
      dout(5) << "trim already expiring segment " << ls->seq << "/" << ls->offset
	      << ", " << ls->num_events << " events" << dendl;
    } else if (expired_segments.count(ls)) {
      dout(5) << "trim already expired segment " << ls->seq << "/" << ls->offset
	      << ", " << ls->num_events << " events" << dendl;
    } else {
      assert(expiring_segments.count(ls) == 0);
      expiring_segments.insert(ls);
      expiring_events += ls->num_events;
      submit_mutex.Unlock();

      uint64_t last_seq = ls->seq;
      try_expire(ls, op_prio);

      submit_mutex.Lock();
      p = segments.lower_bound(last_seq + 1);
    }
  }

  // discard expired segments and unlock submit_mutex
  _trim_expired_segments();
}

class C_MaybeExpiredSegment : public MDSContext {
  MDLog *mdlog;
  LogSegment *ls;
  int op_prio;
  public:
  C_MaybeExpiredSegment(MDLog *mdl, LogSegment *s, int p) :
    MDSContext(mdl->mds), mdlog(mdl), ls(s), op_prio(p) {}
  void finish(int res) {
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
  submit_mutex.Lock();

  dout(10) << __func__ << ": "
	   << segments.size()
           << "/" << expiring_segments.size()
           << "/" << expired_segments.size() << dendl;

  uint64_t last_seq = 0;
  if (!segments.empty())
    last_seq = get_last_segment_seq();

  map<uint64_t,LogSegment*>::iterator p = segments.begin();
  while (p != segments.end() &&
	 p->first < last_seq && p->second->end <= safe_pos.read()) {
    LogSegment *ls = p->second;
    ++p;

    // Caller should have flushed journaler before calling this
    if (pending_events.count(ls->seq)) {
      dout(5) << __func__ << ": segment " << ls->seq << " has pending events" << dendl;
      submit_mutex.Unlock();
      return -EAGAIN;
    }

    if (expiring_segments.count(ls)) {
      dout(5) << "trim already expiring segment " << ls->seq << "/" << ls->offset
	      << ", " << ls->num_events << " events" << dendl;
    } else if (expired_segments.count(ls)) {
      dout(5) << "trim already expired segment " << ls->seq << "/" << ls->offset
	      << ", " << ls->num_events << " events" << dendl;
    } else {
      assert(expiring_segments.count(ls) == 0);
      expiring_segments.insert(ls);
      expiring_events += ls->num_events;
      submit_mutex.Unlock();

      uint64_t next_seq = ls->seq + 1;
      try_expire(ls, CEPH_MSG_PRIO_DEFAULT);

      submit_mutex.Lock();
      p = segments.lower_bound(next_seq);
    }
  }

  _trim_expired_segments();

  return 0;
}


void MDLog::try_expire(LogSegment *ls, int op_prio)
{
  MDSGatherBuilder gather_bld(g_ceph_context);
  ls->try_to_expire(mds, gather_bld, op_prio);

  if (gather_bld.has_subs()) {
    dout(5) << "try_expire expiring segment " << ls->seq << "/" << ls->offset << dendl;
    gather_bld.set_finisher(new C_MaybeExpiredSegment(this, ls, op_prio));
    gather_bld.activate();
  } else {
    dout(10) << "try_expire expired segment " << ls->seq << "/" << ls->offset << dendl;
    submit_mutex.Lock();
    assert(expiring_segments.count(ls));
    expiring_segments.erase(ls);
    expiring_events -= ls->num_events;
    _expired(ls);
    submit_mutex.Unlock();
  }
  
  logger->set(l_mdl_segexg, expiring_segments.size());
  logger->set(l_mdl_evexg, expiring_events);
}

void MDLog::_maybe_expired(LogSegment *ls, int op_prio)
{
  if (mds->mdcache->is_readonly()) {
    dout(10) << "_maybe_expired, ignoring read-only FS" <<  dendl;
    return;
  }

  dout(10) << "_maybe_expired segment " << ls->seq << "/" << ls->offset
	   << ", " << ls->num_events << " events" << dendl;
  try_expire(ls, op_prio);
}

void MDLog::_trim_expired_segments()
{
  assert(submit_mutex.is_locked_by_me());

  // trim expired segments?
  bool trimmed = false;
  while (!segments.empty()) {
    LogSegment *ls = segments.begin()->second;
    if (!expired_segments.count(ls)) {
      dout(10) << "_trim_expired_segments waiting for " << ls->seq << "/" << ls->offset
	       << " to expire" << dendl;
      break;
    }
    
    dout(10) << "_trim_expired_segments trimming expired "
	     << ls->seq << "/0x" << std::hex << ls->offset << std::dec << dendl;
    expired_events -= ls->num_events;
    expired_segments.erase(ls);
    num_events -= ls->num_events;
      
    // this was the oldest segment, adjust expire pos
    if (journaler->get_expire_pos() < ls->end) {
      journaler->set_expire_pos(ls->end);
    }
    
    logger->set(l_mdl_expos, ls->offset);
    logger->inc(l_mdl_segtrm);
    logger->inc(l_mdl_evtrm, ls->num_events);
    
    segments.erase(ls->seq);
    delete ls;
    trimmed = true;
  }

  submit_mutex.Unlock();

  if (trimmed)
    journaler->write_head(0);
}

void MDLog::trim_expired_segments()
{
  submit_mutex.Lock();
  _trim_expired_segments();
}

void MDLog::_expired(LogSegment *ls)
{
  assert(submit_mutex.is_locked_by_me());

  dout(5) << "_expired segment " << ls->seq << "/" << ls->offset
	  << ", " << ls->num_events << " events" << dendl;

  if (!capped && ls == _peek_current_segment()) {
    dout(5) << "_expired not expiring " << ls->seq << "/" << ls->offset
	    << ", last one and !capped" << dendl;
  } else {
    // expired.
    expired_segments.insert(ls);
    expired_events += ls->num_events;

    // Trigger all waiters
    for (std::list<MDSContextBase*>::iterator i = ls->expiry_waiters.begin();
        i != ls->expiry_waiters.end(); ++i) {
      (*i)->complete(0);
    }
    ls->expiry_waiters.clear();
    
    logger->inc(l_mdl_evex, ls->num_events);
    logger->inc(l_mdl_segex);
  }

  logger->set(l_mdl_ev, num_events);
  logger->set(l_mdl_evexd, expired_events);
  logger->set(l_mdl_seg, segments.size());
  logger->set(l_mdl_segexd, expired_segments.size());
}



void MDLog::replay(MDSContextBase *c)
{
  assert(journaler->is_active());
  assert(journaler->is_readonly());

  // empty?
  if (journaler->get_read_pos() == journaler->get_write_pos()) {
    dout(10) << "replay - journal empty, done." << dendl;
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

  assert(num_events == 0 || already_replayed);
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
void MDLog::_recovery_thread(MDSContextBase *completion)
{
  assert(journaler == NULL);
  if (g_conf->mds_journal_format > JOURNAL_FORMAT_MAX) {
      dout(0) << "Configuration value for mds_journal_format is out of bounds, max is "
              << JOURNAL_FORMAT_MAX << dendl;

      // Oh dear, something unreadable in the store for this rank: require
      // operator intervention.
      mds->damaged();
      assert(0);  // damaged should not return
  }

  // First, read the pointer object.
  // If the pointer object is not present, then create it with
  // front = default ino and back = null
  JournalPointer jp(mds->get_nodeid(), mds->mdcache->get_metadata_pool());
  int const read_result = jp.load(mds->objecter);
  if (read_result == -ENOENT) {
    inodeno_t const default_log_ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
    jp.front = default_log_ino;
    int write_result = jp.save(mds->objecter);
    // Nothing graceful we can do for this
    assert(write_result >= 0);
  } else if (read_result != 0) {
    mds->clog->error() << "failed to read JournalPointer: " << read_result
                       << " (" << cpp_strerror(read_result) << ")";
    mds->damaged_unlocked();
    assert(0);  // Should be unreachable because damaged() calls respawn()
  }

  // If the back pointer is non-null, that means that a journal
  // rewrite failed part way through.  Erase the back journal
  // to clean up.
  if (jp.back) {
    if (mds->is_standby_replay()) {
      dout(1) << "Journal " << jp.front << " is being rewritten, "
        << "cannot replay in standby until an active MDS completes rewrite" << dendl;
      Mutex::Locker l(mds->mds_lock);
      if (mds->is_daemon_stopping()) {
        return;
      }
      completion->complete(-EAGAIN);
      return;
    }
    dout(1) << "Erasing journal " << jp.back << dendl;
    C_SaferCond erase_waiter;
    Journaler back(jp.back, mds->mdcache->get_metadata_pool(), CEPH_FS_ONDISK_MAGIC,
        mds->objecter, logger, l_mdl_jlat, &mds->timer, mds->get_log_finisher());

    // Read all about this journal (header + extents)
    C_SaferCond recover_wait;
    back.recover(&recover_wait);
    int recovery_result = recover_wait.wait();
    if (recovery_result != 0) {
      // Journaler.recover succeeds if no journal objects are present: an error
      // means something worse like a corrupt header, which we can't handle here.
      mds->clog->error() << "Error recovering journal " << jp.front << ": "
        << cpp_strerror(recovery_result);
      mds->damaged_unlocked();
      assert(recovery_result == 0); // Unreachable because damaged() calls respawn()
    }

    // We could read journal, so we can erase it.
    back.erase(&erase_waiter);
    int erase_result = erase_waiter.wait();

    // If we are successful, or find no data, we can update the JournalPointer to
    // reflect that the back journal is gone.
    if (erase_result != 0 && erase_result != -ENOENT) {
      derr << "Failed to erase journal " << jp.back << ": " << cpp_strerror(erase_result) << dendl;
    } else {
      dout(1) << "Successfully erased journal, updating journal pointer" << dendl;
      jp.back = 0;
      int write_result = jp.save(mds->objecter);
      // Nothing graceful we can do for this
      assert(write_result >= 0);
    }
  }

  /* Read the header from the front journal */
  Journaler *front_journal = new Journaler(jp.front, mds->mdcache->get_metadata_pool(),
      CEPH_FS_ONDISK_MAGIC, mds->objecter, logger, l_mdl_jlat, &mds->timer,
      mds->get_log_finisher());

  // Assign to ::journaler so that we can be aborted by ::shutdown while
  // waiting for journaler recovery
  {
    Mutex::Locker l(mds->mds_lock);
    journaler = front_journal;
  }

  C_SaferCond recover_wait;
  front_journal->recover(&recover_wait);
  dout(4) << "Waiting for journal " << jp.front << " to recover..." << dendl;
  int recovery_result = recover_wait.wait();
  dout(4) << "Journal " << jp.front << " recovered." << dendl;

  if (recovery_result != 0) {
    mds->clog->error() << "Error recovering journal " << jp.front << ": "
      << cpp_strerror(recovery_result);
    mds->damaged_unlocked();
    assert(recovery_result == 0); // Unreachable because damaged() calls respawn()
  }

  /* Check whether the front journal format is acceptable or needs re-write */
  if (front_journal->get_stream_format() > JOURNAL_FORMAT_MAX) {
    dout(0) << "Journal " << jp.front << " is in unknown format " << front_journal->get_stream_format()
            << ", does this MDS daemon require upgrade?" << dendl;
    {
      Mutex::Locker l(mds->mds_lock);
      if (mds->is_daemon_stopping()) {
        journaler = NULL;
        delete front_journal;
        return;
      }
      completion->complete(-EINVAL);
    }
  } else if (mds->is_standby_replay() || front_journal->get_stream_format() >= g_conf->mds_journal_format) {
    /* The journal is of configured format, or we are in standbyreplay and will
     * tolerate replaying old journals until we have to go active. Use front_journal as
     * our journaler attribute and complete */
    dout(4) << "Recovered journal " << jp.front << " in format " << front_journal->get_stream_format() << dendl;
    journaler->set_write_error_handler(new C_MDL_WriteError(this));
    {
      Mutex::Locker l(mds->mds_lock);
      if (mds->is_daemon_stopping()) {
        return;
      }
      completion->complete(0);
    }
  } else {
    /* Hand off to reformat routine, which will ultimately set the
     * completion when it has done its thing */
    dout(1) << "Journal " << jp.front << " has old format "
      << front_journal->get_stream_format() << ", it will now be updated" << dendl;
    _reformat_journal(jp, front_journal, completion);
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
void MDLog::_reformat_journal(JournalPointer const &jp_in, Journaler *old_journal, MDSContextBase *completion)
{
  assert(!jp_in.is_null());
  assert(completion != NULL);
  assert(old_journal != NULL);

  JournalPointer jp = jp_in;

  /* Set JournalPointer.back to the location we will write the new journal */
  inodeno_t primary_ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
  inodeno_t secondary_ino = MDS_INO_LOG_BACKUP_OFFSET + mds->get_nodeid();
  jp.back = (jp.front == primary_ino ? secondary_ino : primary_ino);
  int write_result = jp.save(mds->objecter);
  assert(write_result == 0);

  /* Create the new Journaler file */
  Journaler *new_journal = new Journaler(jp.back, mds->mdcache->get_metadata_pool(),
      CEPH_FS_ONDISK_MAGIC, mds->objecter, logger, l_mdl_jlat, &mds->timer,
      mds->get_log_finisher());
  dout(4) << "Writing new journal header " << jp.back << dendl;
  file_layout_t new_layout = old_journal->get_layout();
  new_journal->set_writeable();
  new_journal->create(&new_layout, g_conf->mds_journal_format);

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
  std::map<log_segment_seq_t, log_segment_seq_t> segment_pos_rewrite;

  // The logic in here borrowed from replay_thread expects mds_lock to be held,
  // e.g. between checking readable and doing wait_for_readable so that journaler
  // state doesn't change in between.
  uint32_t events_transcribed = 0;
  while (1) {
    while (!old_journal->is_readable() &&
	   old_journal->get_read_pos() < old_journal->get_write_pos() &&
	   !old_journal->get_error()) {

      // Issue a journal prefetch
      C_SaferCond readable_waiter;
      old_journal->wait_for_readable(&readable_waiter);

      // Wait for a journal prefetch to complete
      readable_waiter.wait();
    }
    if (old_journal->get_error()) {
      r = old_journal->get_error();
      dout(0) << "_replay journaler got error " << r << ", aborting" << dendl;
      break;
    }

    if (!old_journal->is_readable() &&
	old_journal->get_read_pos() == old_journal->get_write_pos())
      break;

    // Read one serialized LogEvent
    assert(old_journal->is_readable());
    bufferlist bl;
    uint64_t le_pos = old_journal->get_read_pos();
    bool r = old_journal->try_read_entry(bl);
    if (!r && old_journal->get_error())
      continue;
    assert(r);

    // Update segment_pos_rewrite
    LogEvent *le = LogEvent::decode(bl);
    if (le) {
      bool modified = false;

      if (le->get_type() == EVENT_SUBTREEMAP ||
          le->get_type() == EVENT_RESETJOURNAL) {
        ESubtreeMap *sle = dynamic_cast<ESubtreeMap*>(le);
        if (sle == NULL || sle->event_seq == 0) {
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
        ESubtreeMap *sle = dynamic_cast<ESubtreeMap*>(le);
        dout(20) << __func__ << " zeroing expire_pos in subtreemap event at "
          << le_pos << " seq=" << sle->event_seq << dendl;
        assert(sle != NULL);
        sle->expire_pos = 0;
        modified = true;
      }

      if (modified) {
        bl.clear();
        le->encode_with_header(bl, mds->mdsmap->get_up_features());
      }

      delete le;
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
  assert(r == 0);

  /* Now that the new journal is safe, we can flip the pointers */
  inodeno_t const tmp = jp.front;
  jp.front = jp.back;
  jp.back = tmp;
  write_result = jp.save(mds->objecter);
  assert(write_result == 0);

  /* Delete the old journal to free space */
  dout(1) << "New journal flushed, erasing old journal" << dendl;
  C_SaferCond erase_waiter;
  old_journal->erase(&erase_waiter);
  int erase_result = erase_waiter.wait();
  assert(erase_result == 0);
  {
    Mutex::Locker l(mds->mds_lock);
    if (mds->is_daemon_stopping()) {
      delete new_journal;
      return;
    }
    assert(journaler == old_journal);
    journaler = NULL;
    delete old_journal;
  }

  /* Update the pointer to reflect we're back in clean single journal state. */
  jp.back = 0;
  write_result = jp.save(mds->objecter);
  assert(write_result == 0);

  /* Reset the Journaler object to its default state */
  dout(1) << "Journal rewrite complete, continuing with normal startup" << dendl;
  {
    Mutex::Locker l(mds->mds_lock);
    if (mds->is_daemon_stopping()) {
      delete new_journal;
      return;
    }
    journaler = new_journal;
    journaler->set_readonly();
    journaler->set_write_error_handler(new C_MDL_WriteError(this));
  }

  /* Trigger completion */
  {
    Mutex::Locker l(mds->mds_lock);
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
    while (!journaler->is_readable() &&
	   journaler->get_read_pos() < journaler->get_write_pos() &&
	   !journaler->get_error()) {
      C_SaferCond readable_waiter;
      journaler->wait_for_readable(&readable_waiter);
      r = readable_waiter.wait();
    }
    if (journaler->get_error()) {
      r = journaler->get_error();
      dout(0) << "_replay journaler got error " << r << ", aborting" << dendl;
      if (r == -ENOENT) {
        if (mds->is_standby_replay()) {
          // journal has been trimmed by somebody else
          r = -EAGAIN;
        } else {
          mds->clog->error() << "missing journal object";
          mds->damaged_unlocked();
          assert(0);  // Should be unreachable because damaged() calls respawn()
        }
      } else if (r == -EINVAL) {
        if (journaler->get_read_pos() < journaler->get_expire_pos()) {
          // this should only happen if you're following somebody else
          if(journaler->is_readonly()) {
            dout(0) << "expire_pos is higher than read_pos, returning EAGAIN" << dendl;
            r = -EAGAIN;
          } else {
            mds->clog->error() << "invalid journaler offsets";
            mds->damaged_unlocked();
            assert(0);  // Should be unreachable because damaged() calls respawn()
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
            if (err == -ENOENT && mds->is_standby_replay()) {
              r = -EAGAIN;
              dout(1) << "Journal header went away while in standby replay, journal rewritten?"
                      << dendl;
              break;
            } else {
                dout(0) << "got error while reading head: " << cpp_strerror(err)
                        << dendl;

                mds->clog->error() << "error reading journal header";
                mds->damaged_unlocked();
                assert(0);  // Should be unreachable because damaged() calls
                            // respawn()
            }
          }
	  standby_trim_segments();
          if (journaler->get_read_pos() < journaler->get_expire_pos()) {
            dout(0) << "expire_pos is higher than read_pos, returning EAGAIN" << dendl;
            r = -EAGAIN;
          }
        }
      }
      break;
    }

    if (!journaler->is_readable() &&
	journaler->get_read_pos() == journaler->get_write_pos())
      break;
    
    assert(journaler->is_readable() || mds->is_daemon_stopping());
    
    // read it
    uint64_t pos = journaler->get_read_pos();
    bufferlist bl;
    bool r = journaler->try_read_entry(bl);
    if (!r && journaler->get_error())
      continue;
    assert(r);
    
    // unpack event
    LogEvent *le = LogEvent::decode(bl);
    if (!le) {
      dout(0) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos() 
	      << " -- unable to decode event" << dendl;
      dout(0) << "dump of unknown or corrupt event:\n";
      bl.hexdump(*_dout);
      *_dout << dendl;

      mds->clog->error() << "corrupt journal event at " << pos << "~"
                         << bl.length() << " / "
                         << journaler->get_write_pos();
      if (g_conf->mds_log_skip_corrupt_events) {
        continue;
      } else {
        mds->damaged_unlocked();
        assert(0);  // Should be unreachable because damaged() calls
                    // respawn()
      }

    }
    le->set_start_off(pos);

    // new segment?
    if (le->get_type() == EVENT_SUBTREEMAP ||
	le->get_type() == EVENT_RESETJOURNAL) {
      ESubtreeMap *sle = dynamic_cast<ESubtreeMap*>(le);
      if (sle && sle->event_seq > 0)
	event_seq = sle->event_seq;
      else
	event_seq = pos;
      segments[event_seq] = new LogSegment(event_seq, pos);
      logger->set(l_mdl_seg, segments.size());
    } else {
      event_seq++;
    }

    // have we seen an import map yet?
    if (segments.empty()) {
      dout(10) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos() 
	       << " " << le->get_stamp() << " -- waiting for subtree_map.  (skipping " << *le << ")" << dendl;
    } else {
      dout(10) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos() 
	       << " " << le->get_stamp() << ": " << *le << dendl;
      le->_segment = _get_current_segment();    // replay may need this
      le->_segment->num_events++;
      le->_segment->end = journaler->get_read_pos();
      num_events++;

      {
        Mutex::Locker l(mds->mds_lock);
        if (mds->is_daemon_stopping()) {
          return;
        }
        le->replay(mds);
      }
    }
    delete le;

    logger->set(l_mdl_rdpos, pos);
  }

  // done!
  if (r == 0) {
    assert(journaler->get_read_pos() == journaler->get_write_pos());
    dout(10) << "_replay - complete, " << num_events
	     << " events" << dendl;

    logger->set(l_mdl_expos, journaler->get_expire_pos());
  }

  set_safe_pos(journaler->get_write_safe_pos());

  dout(10) << "_replay_thread kicking waiters" << dendl;
  {
    Mutex::Locker l(mds->mds_lock);
    if (mds->is_daemon_stopping()) {
      return;
    }
    finish_contexts(g_ceph_context, waitfor_replay, r);  
  }

  dout(10) << "_replay_thread finish" << dendl;
}

void MDLog::standby_trim_segments()
{
  dout(10) << "standby_trim_segments" << dendl;
  uint64_t expire_pos = journaler->get_expire_pos();
  dout(10) << " expire_pos=" << expire_pos << dendl;
  bool removed_segment = false;
  while (have_any_segments()) {
    LogSegment *seg = get_oldest_segment();
    if (seg->end > expire_pos)
      break;
    dout(10) << " removing segment " << seg->seq << "/" << seg->offset << dendl;
    mds->mdcache->standby_trim_segment(seg);
    remove_oldest_segment();
    removed_segment = true;
  }

  if (removed_segment) {
    dout(20) << " calling mdcache->trim!" << dendl;
    mds->mdcache->trim(-1);
  } else
    dout(20) << " removed no segments!" << dendl;
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
