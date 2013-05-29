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

#include "MDLog.h"
#include "MDS.h"
#include "MDCache.h"
#include "LogEvent.h"

#include "osdc/Journaler.h"

#include "common/entity_name.h"
#include "common/perf_counters.h"

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

  plb.add_u64_counter(l_mdl_evadd, "evadd");
  plb.add_u64_counter(l_mdl_evex, "evex");
  plb.add_u64_counter(l_mdl_evtrm, "evtrm");
  plb.add_u64(l_mdl_ev, "ev");
  plb.add_u64(l_mdl_evexg, "evexg");
  plb.add_u64(l_mdl_evexd, "evexd");

  plb.add_u64_counter(l_mdl_segadd, "segadd");
  plb.add_u64_counter(l_mdl_segex, "segex");
  plb.add_u64_counter(l_mdl_segtrm, "segtrm");
  plb.add_u64(l_mdl_seg, "seg");
  plb.add_u64(l_mdl_segexg, "segexg");
  plb.add_u64(l_mdl_segexd, "segexd");

  plb.add_u64(l_mdl_expos, "expos");
  plb.add_u64(l_mdl_wrpos, "wrpos");
  plb.add_u64(l_mdl_rdpos, "rdpos");
  plb.add_u64(l_mdl_jlat, "jlat");

  // logger
  logger = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void MDLog::init_journaler()
{
  // inode
  ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
  
  // log streamer
  if (journaler) delete journaler;
  journaler = new Journaler(ino, mds->mdsmap->get_metadata_pool(), CEPH_FS_ONDISK_MAGIC, mds->objecter,
			    logger, l_mdl_jlat,
			    &mds->timer);
  assert(journaler->is_readonly());
  journaler->set_write_error_handler(new C_MDL_WriteError(this));
}

void MDLog::handle_journaler_write_error(int r)
{
  if (r == -EBLACKLISTED) {
    derr << "we have been blacklisted (fenced), respawning..." << dendl;
    mds->respawn();
  } else {
    derr << "unhandled error " << cpp_strerror(r) << ", shutting down..." << dendl;
    mds->suicide();
  }
}

void MDLog::write_head(Context *c) 
{
  journaler->write_head(c);
}

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



void MDLog::create(Context *c)
{
  dout(5) << "create empty log" << dendl;
  init_journaler();
  journaler->set_writeable();
  journaler->create(&mds->mdcache->default_log_layout);
  journaler->write_head(c);

  logger->set(l_mdl_expos, journaler->get_expire_pos());
  logger->set(l_mdl_wrpos, journaler->get_write_pos());
}

void MDLog::open(Context *c)
{
  dout(5) << "open discovering log bounds" << dendl;
  init_journaler();
  journaler->recover(c);

  // either append() or replay() will follow.
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

void MDLog::start_entry(LogEvent *e)
{
  assert(cur_event == NULL);
  cur_event = e;
  e->set_start_off(get_write_pos());
}

void MDLog::submit_entry(LogEvent *le, Context *c) 
{
  assert(!mds->is_any_replay());
  assert(le == cur_event);
  cur_event = NULL;

  if (!g_conf->mds_log) {
    // hack: log is disabled.
    if (c) {
      c->finish(0);
      delete c;
    }
    return;
  }

  // let the event register itself in the segment
  assert(!segments.empty());
  le->_segment = segments.rbegin()->second;
  le->_segment->num_events++;
  le->update_segment();

  le->set_stamp(ceph_clock_now(g_ceph_context));
  
  num_events++;
  assert(!capped);
  
  // encode it, with event type
  {
    bufferlist bl;
    le->encode_with_header(bl);

    dout(5) << "submit_entry " << journaler->get_write_pos() << "~" << bl.length()
	    << " : " << *le << dendl;
      
    // journal it.
    journaler->append_entry(bl);  // bl is destroyed.
  }

  le->_segment->end = journaler->get_write_pos();

  if (logger) {
    logger->inc(l_mdl_evadd);
    logger->set(l_mdl_ev, num_events);
    logger->set(l_mdl_wrpos, journaler->get_write_pos());
  }

  unflushed++;

  if (c)
    journaler->wait_for_flush(c);
  
  // start a new segment?
  //  FIXME: should this go elsewhere?
  uint64_t last_seg = get_last_segment_offset();
  uint64_t period = journaler->get_layout_period();
  // start a new segment if there are none or if we reach end of last segment
  if (journaler->get_write_pos()/period != last_seg/period) {
    dout(10) << "submit_entry also starting new segment: last = " << last_seg
	     << ", cur pos = " << journaler->get_write_pos() << dendl;
    start_new_segment();
  } else if (g_conf->mds_debug_subtrees &&
	     le->get_type() != EVENT_SUBTREEMAP_TEST &&
	     le->get_type() != EVENT_SUBTREEMAP) {
    // debug: journal this every time to catch subtree replay bugs.
    // use a different event id so it doesn't get interpreted as a
    // LogSegment boundary on replay.
    LogEvent *sle = mds->mdcache->create_subtree_map();
    sle->set_type(EVENT_SUBTREEMAP_TEST);
    submit_entry(sle);
  }

  delete le;
}

void MDLog::wait_for_safe(Context *c)
{
  if (g_conf->mds_log) {
    // wait
    journaler->wait_for_flush(c);
  } else {
    // hack: bypass.
    c->finish(0);
    delete c;
  }
}

void MDLog::flush()
{
  if (unflushed)
    journaler->flush();
  unflushed = 0;
}

void MDLog::cap()
{ 
  dout(5) << "cap" << dendl;
  capped = true;
}


// -----------------------------
// segments

void MDLog::start_new_segment(Context *onsync)
{
  dout(7) << "start_new_segment at " << journaler->get_write_pos() << dendl;

  segments[journaler->get_write_pos()] = new LogSegment(journaler->get_write_pos());

  ESubtreeMap *le = mds->mdcache->create_subtree_map();
  submit_entry(le);
  if (onsync) {
    wait_for_safe(onsync);  
    flush();
  }

  logger->inc(l_mdl_segadd);
  logger->set(l_mdl_seg, segments.size());

  // Adjust to next stray dir
  dout(10) << "Advancing to next stray directory on mds " << mds->get_nodeid() 
	   << dendl;
  mds->mdcache->advance_stray();
}

void MDLog::trim(int m)
{
  int max_segments = g_conf->mds_log_max_segments;
  int max_events = g_conf->mds_log_max_events;
  if (m >= 0)
    max_events = m;

  // trim!
  dout(10) << "trim " 
	   << segments.size() << " / " << max_segments << " segments, " 
	   << num_events << " / " << max_events << " events"
	   << ", " << expiring_segments.size() << " (" << expiring_events << ") expiring"
	   << ", " << expired_segments.size() << " (" << expired_events << ") expired"
	   << dendl;

  if (segments.empty())
    return;

  // hack: only trim for a few seconds at a time
  utime_t stop = ceph_clock_now(g_ceph_context);
  stop += 2.0;

  map<uint64_t,LogSegment*>::iterator p = segments.begin();
  while (p != segments.end() && 
	 ((max_events >= 0 &&
	   num_events - expiring_events - expired_events > max_events) ||
	  (max_segments >= 0 &&
	   segments.size() - expiring_segments.size() - expired_segments.size() > (unsigned)max_segments))) {
    
    if (stop < ceph_clock_now(g_ceph_context))
      break;

    if ((int)expiring_segments.size() >= g_conf->mds_log_max_expiring)
      break;

    // look at first segment
    LogSegment *ls = p->second;
    assert(ls);
    ++p;
    
    if (ls->end > journaler->get_write_safe_pos()) {
      dout(5) << "trim segment " << ls->offset << ", not fully flushed yet, safe "
	      << journaler->get_write_safe_pos() << " < end " << ls->end << dendl;
      break;
    }
    if (expiring_segments.count(ls)) {
      dout(5) << "trim already expiring segment " << ls->offset << ", " << ls->num_events << " events" << dendl;
    } else if (expired_segments.count(ls)) {
      dout(5) << "trim already expired segment " << ls->offset << ", " << ls->num_events << " events" << dendl;
    } else {
      try_expire(ls);
    }
  }

  // discard expired segments
  _trim_expired_segments();
}


void MDLog::try_expire(LogSegment *ls)
{
  C_GatherBuilder gather_bld(g_ceph_context);
  ls->try_to_expire(mds, gather_bld);
  if (gather_bld.has_subs()) {
    assert(expiring_segments.count(ls) == 0);
    expiring_segments.insert(ls);
    expiring_events += ls->num_events;
    dout(5) << "try_expire expiring segment " << ls->offset << dendl;
    gather_bld.set_finisher(new C_MaybeExpiredSegment(this, ls));
    gather_bld.activate();
  } else {
    dout(10) << "try_expire expired segment " << ls->offset << dendl;
    _expired(ls);
  }
  
  logger->set(l_mdl_segexg, expiring_segments.size());
  logger->set(l_mdl_evexg, expiring_events);
}

void MDLog::_maybe_expired(LogSegment *ls) 
{
  dout(10) << "_maybe_expired segment " << ls->offset << " " << ls->num_events << " events" << dendl;
  assert(expiring_segments.count(ls));
  expiring_segments.erase(ls);
  expiring_events -= ls->num_events;
  try_expire(ls);
}

void MDLog::_trim_expired_segments()
{
  // trim expired segments?
  bool trimmed = false;
  while (!segments.empty()) {
    LogSegment *ls = segments.begin()->second;
    if (!expired_segments.count(ls)) {
      dout(10) << "_trim_expired_segments waiting for " << ls->offset << " to expire" << dendl;
      break;
    }
    
    dout(10) << "_trim_expired_segments trimming expired " << ls->offset << dendl;
    expired_events -= ls->num_events;
    expired_segments.erase(ls);
    num_events -= ls->num_events;
      
    // this was the oldest segment, adjust expire pos
    if (journaler->get_expire_pos() < ls->offset)
      journaler->set_expire_pos(ls->offset);
    
    logger->set(l_mdl_expos, ls->offset);
    logger->inc(l_mdl_segtrm);
    logger->inc(l_mdl_evtrm, ls->num_events);
    
    segments.erase(ls->offset);
    delete ls;
    trimmed = true;
  }
  
  if (trimmed)
    journaler->write_head(0);
}

void MDLog::_expired(LogSegment *ls)
{
  dout(5) << "_expired segment " << ls->offset << " " << ls->num_events << " events" << dendl;

  if (!capped && ls == peek_current_segment()) {
    dout(5) << "_expired not expiring " << ls->offset << ", last one and !capped" << dendl;
  } else {
    // expired.
    expired_segments.insert(ls);
    expired_events += ls->num_events;
    
    logger->inc(l_mdl_evex, ls->num_events);
    logger->inc(l_mdl_segex);
  }

  logger->set(l_mdl_ev, num_events);
  logger->set(l_mdl_evexd, expired_events);
  logger->set(l_mdl_seg, segments.size());
  logger->set(l_mdl_segexd, expired_segments.size());
}



void MDLog::replay(Context *c)
{
  assert(journaler->is_active());
  assert(journaler->is_readonly());

  // empty?
  if (journaler->get_read_pos() == journaler->get_write_pos()) {
    dout(10) << "replay - journal empty, done." << dendl;
    if (c) {
      c->finish(0);
      delete c;
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
  already_replayed = true;

  replay_thread.create();
  replay_thread.detach();
}

class C_MDL_Replay : public Context {
  MDLog *mdlog;
public:
  C_MDL_Replay(MDLog *l) : mdlog(l) {}
  void finish(int r) { 
    mdlog->replay_cond.Signal();
  }
};



// i am a separate thread
void MDLog::_replay_thread()
{
  mds->mds_lock.Lock();
  dout(10) << "_replay_thread start" << dendl;

  // loop
  int r = 0;
  while (1) {
    // wait for read?
    while (!journaler->is_readable() &&
	   journaler->get_read_pos() < journaler->get_write_pos() &&
	   !journaler->get_error()) {
      journaler->wait_for_readable(new C_MDL_Replay(this));
      replay_cond.Wait(mds->mds_lock);
    }
    if (journaler->get_error()) {
      r = journaler->get_error();
      dout(0) << "_replay journaler got error " << r << ", aborting" << dendl;
      if (r == -EINVAL) {
        if (journaler->get_read_pos() < journaler->get_expire_pos()) {
          // this should only happen if you're following somebody else
          assert(journaler->is_readonly());
          dout(0) << "expire_pos is higher than read_pos, returning EAGAIN" << dendl;
          r = -EAGAIN;
        } else {
          /* re-read head and check it
           * Given that replay happens in a separate thread and
           * the MDS is going to either shut down or restart when
           * we return this error, doing it synchronously is fine
           * -- as long as we drop the main mds lock--. */
          Mutex mylock("MDLog::_replay_thread lock");
          Cond cond;
          bool done = false;
          int err = 0;
          journaler->reread_head(new C_SafeCond(&mylock, &cond, &done, &err));
          mds->mds_lock.Unlock();
	  mylock.Lock();
          while (!done)
            cond.Wait(mylock);
	  mylock.Unlock();
          if (err) { // well, crap
            dout(0) << "got error while reading head: " << cpp_strerror(err)
                    << dendl;
            mds->suicide();
          }
          mds->mds_lock.Lock();
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
    
    assert(journaler->is_readable());
    
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

      assert(!!"corrupt log event" == g_conf->mds_log_skip_corrupt_events);
      continue;
    }
    le->set_start_off(pos);

    // new segment?
    if (le->get_type() == EVENT_SUBTREEMAP ||
	le->get_type() == EVENT_RESETJOURNAL) {
      segments[pos] = new LogSegment(pos);
      logger->set(l_mdl_seg, segments.size());
    }

    // have we seen an import map yet?
    if (segments.empty()) {
      dout(10) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos() 
	       << " " << le->get_stamp() << " -- waiting for subtree_map.  (skipping " << *le << ")" << dendl;
    } else {
      dout(10) << "_replay " << pos << "~" << bl.length() << " / " << journaler->get_write_pos() 
	       << " " << le->get_stamp() << ": " << *le << dendl;
      le->_segment = get_current_segment();    // replay may need this
      le->_segment->num_events++;
      le->_segment->end = journaler->get_read_pos();
      num_events++;

      le->replay(mds);
    }
    delete le;

    logger->set(l_mdl_rdpos, pos);

    // drop lock for a second, so other events/messages (e.g. beacon timer!) can go off
    mds->mds_lock.Unlock();
    mds->mds_lock.Lock();
  }

  // done!
  if (r == 0) {
    assert(journaler->get_read_pos() == journaler->get_write_pos());
    dout(10) << "_replay - complete, " << num_events
	     << " events" << dendl;

    logger->set(l_mdl_expos, journaler->get_expire_pos());
  }

  dout(10) << "_replay_thread kicking waiters" << dendl;
  finish_contexts(g_ceph_context, waitfor_replay, 0);  

  dout(10) << "_replay_thread finish" << dendl;
  mds->mds_lock.Unlock();
}

void MDLog::standby_trim_segments()
{
  dout(10) << "standby_trim_segments" << dendl;
  uint64_t expire_pos = journaler->get_expire_pos();
  dout(10) << " expire_pos=" << expire_pos << dendl;
  LogSegment *seg = NULL;
  bool removed_segment = false;
  while ((seg = get_oldest_segment())->end <= expire_pos) {
    dout(10) << " removing segment " << seg->offset << dendl;
    seg->dirty_dirfrags.clear_list();
    seg->new_dirfrags.clear_list();
    seg->dirty_inodes.clear_list();
    seg->dirty_dentries.clear_list();
    seg->open_files.clear_list();
    seg->dirty_parent_inodes.clear_list();
    seg->dirty_dirfrag_dir.clear_list();
    seg->dirty_dirfrag_nest.clear_list();
    seg->dirty_dirfrag_dirfragtree.clear_list();
    remove_oldest_segment();
    removed_segment = true;
  }

  if (removed_segment) {
    dout(20) << " calling mdcache->trim!" << dendl;
    mds->mdcache->trim(-1);
  } else
    dout(20) << " removed no segments!" << dendl;
}
