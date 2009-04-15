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

#include "common/LogType.h"
#include "common/Logger.h"

#include "events/ESubtreeMap.h"

#include "config.h"

#define DOUT_SUBSYS mds
#undef DOUT_COND
#define DOUT_COND(l) l<=g_conf.debug_mds || l <= g_conf.debug_mds_log
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "mds" << mds->get_nodeid() << ".log "

// cons/des

LogType mdlog_logtype(l_mdl_first, l_mdl_last);


MDLog::~MDLog()
{
  if (journaler) { delete journaler; journaler = 0; }
  if (logger) { delete logger; logger = 0; }
}


void MDLog::reopen_logger(utime_t start, bool append)
{
  // logger
  char name[80];
  sprintf(name, "mds%d.log", mds->get_nodeid());
  logger = new Logger(name, &mdlog_logtype, append);
  logger->set_start(start);

  static bool didit = false;
  if (!didit) {
    didit = true;
    mdlog_logtype.add_inc(l_mdl_evadd, "evadd");
    mdlog_logtype.add_inc(l_mdl_evex, "evex");
    mdlog_logtype.add_inc(l_mdl_evtrm, "evtrm");
    mdlog_logtype.add_set(l_mdl_ev, "ev");
    mdlog_logtype.add_set(l_mdl_evexg, "evexg");
    mdlog_logtype.add_set(l_mdl_evexd, "evexd");

    mdlog_logtype.add_inc(l_mdl_segadd, "segadd");
    mdlog_logtype.add_inc(l_mdl_segex, "segex");
    mdlog_logtype.add_inc(l_mdl_segtrm, "segtrm");    
    mdlog_logtype.add_set(l_mdl_seg, "seg");
    mdlog_logtype.add_set(l_mdl_segexg, "segexg");
    mdlog_logtype.add_set(l_mdl_segexd, "segexd");

    mdlog_logtype.add_set(l_mdl_expos, "expos");
    mdlog_logtype.add_set(l_mdl_wrpos, "wrpos");
    mdlog_logtype.add_set(l_mdl_rdpos, "rdpos");
    mdlog_logtype.add_avg(l_mdl_jlat, "jlat");
    mdlog_logtype.validate();
  }

}

void MDLog::init_journaler()
{
  // inode
  memset(&log_inode, 0, sizeof(log_inode));
  log_inode.ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
  log_inode.layout = g_default_mds_log_layout;
  
  if (g_conf.mds_local_osd) 
    log_inode.layout.fl_pg_preferred = mds->get_nodeid() + g_conf.num_osd;  // hack
  
  // log streamer
  if (journaler) delete journaler;
  journaler = new Journaler(log_inode.ino, &log_inode.layout, CEPH_FS_ONDISK_MAGIC, mds->objecter, 
			    logger, l_mdl_jlat,
			    &mds->mds_lock);
}

void MDLog::write_head(Context *c) 
{
  journaler->write_head(c);
}

loff_t MDLog::get_read_pos() 
{
  return journaler->get_read_pos(); 
}

loff_t MDLog::get_write_pos() 
{
  return journaler->get_write_pos(); 
}

loff_t MDLog::get_safe_pos() 
{
  return journaler->get_write_safe_pos(); 
}



void MDLog::create(Context *c)
{
  dout(5) << "create empty log" << dendl;
  init_journaler();
  journaler->reset();
  write_head(c);

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
  dout(5) << "append positioning at end" << dendl;
  journaler->set_read_pos(journaler->get_write_pos());
  journaler->set_expire_pos(journaler->get_write_pos());

  logger->set(l_mdl_expos, journaler->get_write_pos());
}



// -------------------------------------------------

void MDLog::submit_entry( LogEvent *le, Context *c, bool wait_safe ) 
{
  if (!g_conf.mds_log) {
    // hack: log is disabled.
    if (c) {
      c->finish(0);
      delete c;
    }
    return;
  }

  dout(5) << "submit_entry " << journaler->get_write_pos() << " : " << *le << dendl;
  
  // let the event register itself in the segment
  assert(!segments.empty());
  le->_segment = segments.rbegin()->second;
  le->_segment->num_events++;
  le->update_segment();
  
  num_events++;
  assert(!capped);
  
  // encode it, with event type
  {
    bufferlist bl;
    ::encode(le->_type, bl);
    le->encode(bl);
    
    // journal it.
    journaler->append_entry(bl);  // bl is destroyed.
  }

  le->_segment->end = journaler->get_write_pos();

  delete le;

  if (logger) {
    logger->inc(l_mdl_evadd);
    logger->set(l_mdl_ev, num_events);
    logger->set(l_mdl_wrpos, journaler->get_write_pos());
  }

  unflushed++;

  if (c) {
    
    if (!g_conf.mds_log_unsafe)
      wait_safe = true;

    if (0) {
      unflushed = 0;
      journaler->flush();
    }

    if (wait_safe)
      journaler->wait_for_flush(0, c);
    else
      journaler->wait_for_flush(c, 0);      
  }
  
  // start a new segment?
  //  FIXME: should this go elsewhere?
  loff_t last_seg = get_last_segment_offset();
  if (!segments.empty() && 
      !writing_subtree_map &&
      (journaler->get_write_pos() / ceph_file_layout_period(log_inode.layout) != (last_seg / ceph_file_layout_period(log_inode.layout)) &&
       (journaler->get_write_pos() - last_seg > ceph_file_layout_period(log_inode.layout)/2))) {
    dout(10) << "submit_entry also starting new segment: last = " << last_seg
	     << ", cur pos = " << journaler->get_write_pos() << dendl;
    start_new_segment();
  }
}

void MDLog::wait_for_sync( Context *c )
{
  if (!g_conf.mds_log_unsafe)
    return wait_for_safe(c);

  if (g_conf.mds_log) {
    // wait
    journaler->wait_for_flush(c, 0);
  } else {
    // hack: bypass.
    c->finish(0);
    delete c;
  }
}
void MDLog::wait_for_safe( Context *c )
{
  if (g_conf.mds_log) {
    // wait
    journaler->wait_for_flush(0, c);
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
  assert(!writing_subtree_map);

  segments[journaler->get_write_pos()] = new LogSegment(journaler->get_write_pos());

  writing_subtree_map = true;

  ESubtreeMap *le = mds->mdcache->create_subtree_map();
  submit_entry(le, new C_MDL_WroteSubtreeMap(this, mds->mdlog->get_write_pos()));
  if (onsync) {
    wait_for_sync(onsync);  
    flush();
  }

  logger->inc(l_mdl_segadd);
  logger->set(l_mdl_seg, segments.size());
}

void MDLog::_logged_subtree_map(loff_t off)
{
  dout(10) << "_logged_subtree_map at " << off << dendl;
  writing_subtree_map = false;

  /*
  list<Context*> ls;
  take_subtree_map_expire_waiters(ls);
  mds->queue_waiters(ls);
  */
}



void MDLog::trim()
{
  // trim!
  dout(10) << "trim " 
	   << segments.size() << " / " << max_segments << " segments, " 
	   << num_events << " / " << max_events << " events"
	   << ", " << expiring_segments.size() << " (" << expiring_events << ") expiring"
	   << ", " << expired_segments.size() << " (" << expired_events << ") expired"
	   << dendl;

  if (segments.empty()) return;

  // hack: only trim for a few seconds at a time
  utime_t stop = g_clock.now();
  stop += 2.0;

  map<loff_t,LogSegment*>::iterator p = segments.begin();
  int left = num_events;
  while (p != segments.end() && 
	 ((max_events >= 0 && left-expiring_events-expired_events > max_events) ||
	  (max_segments >= 0 && (int)(segments.size()-expiring_segments.size()-expired_segments.size()) > max_segments))) {

    if (stop < g_clock.now())
      break;

    if ((int)expiring_segments.size() >= g_conf.mds_log_max_expiring)
      break;

    // look at first segment
    LogSegment *ls = p->second;
    assert(ls);

    p++;
    
    left -= ls->num_events;

    if (expiring_segments.count(ls)) {
      dout(5) << "trim already expiring segment " << ls->offset << ", " << ls->num_events << " events" << dendl;
    } else if (expired_segments.count(ls)) {
      dout(5) << "trim already expired segment " << ls->offset << ", " << ls->num_events << " events" << dendl;
    } else {
      try_expire(ls);
    }
  }
}


void MDLog::try_expire(LogSegment *ls)
{
  C_Gather *exp = ls->try_to_expire(mds);
  if (exp) {
    assert(expiring_segments.count(ls) == 0);
    expiring_segments.insert(ls);
    expiring_events += ls->num_events;
    dout(5) << "try_expire expiring segment " << ls->offset << dendl;
    exp->set_finisher(new C_MaybeExpiredSegment(this, ls));
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

void MDLog::_expired(LogSegment *ls)
{
  dout(5) << "_expired segment " << ls->offset << " " << ls->num_events << " events" << dendl;

  if (!capped && ls == get_current_segment()) {
    dout(5) << "_expired not expiring " << ls->offset << ", last one and !capped" << dendl;
  } else if (ls->end > journaler->get_write_ack_pos()) {
    dout(5) << "_expired not expiring " << ls->offset << ", not fully flushed yet, ack "
	    << journaler->get_write_ack_pos() << " < end " << ls->end << dendl;
  } else {
    // expired.
    expired_segments.insert(ls);
    expired_events += ls->num_events;
    
    logger->inc(l_mdl_evex, ls->num_events);
    logger->inc(l_mdl_segex);
    
    // trim expired segments?
    while (!segments.empty()) {
      ls = segments.begin()->second;
      if (!expired_segments.count(ls)) {
	dout(10) << "_expired  waiting for " << ls->offset << " to expire first" << dendl;
	break;
      }
      
      expired_events -= ls->num_events;
      expired_segments.erase(ls);
      num_events -= ls->num_events;
      
      journaler->set_expire_pos(ls->offset);  // this was the oldest segment, adjust expire pos
      journaler->write_head(0);
      
      logger->set(l_mdl_expos, ls->offset);
      logger->inc(l_mdl_segtrm);
      logger->inc(l_mdl_evtrm, ls->num_events);
      
      segments.erase(ls->offset);
      delete ls;
    }
  }

  logger->set(l_mdl_ev, num_events);
  logger->set(l_mdl_evexd, expired_events);
  logger->set(l_mdl_seg, segments.size());
  logger->set(l_mdl_segexd, expired_segments.size());
}



void MDLog::replay(Context *c)
{
  assert(journaler->is_active());

  // start reading at the last known expire point.
  journaler->set_read_pos( journaler->get_expire_pos() );

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

  assert(num_events == 0);

  replay_thread.create();
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
  loff_t new_expire_pos = journaler->get_expire_pos();
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
      break;
    }
    
    if (!journaler->is_readable() &&
	journaler->get_read_pos() == journaler->get_write_pos())
      break;
    
    assert(journaler->is_readable());
    
    // read it
    loff_t pos = journaler->get_read_pos();
    bufferlist bl;
    bool r = journaler->try_read_entry(bl);
    if (!r && journaler->get_error())
      continue;
    assert(r);
    
    // unpack event
    LogEvent *le = LogEvent::decode(bl);

    // new segment?
    if (le->get_type() == EVENT_SUBTREEMAP) {
      segments[pos] = new LogSegment(pos);
      logger->set(l_mdl_seg, segments.size());
    }

    // have we seen an import map yet?
    if (segments.empty()) {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " -- waiting for subtree_map.  (skipping " << *le << ")" << dendl;
    } else {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " : " << *le << dendl;
      le->_segment = get_current_segment();    // replay may need this
      le->_segment->num_events++;
      le->_segment->end = journaler->get_read_pos();
      num_events++;

      le->replay(mds);

      if (!new_expire_pos) 
	new_expire_pos = pos;
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
    dout(10) << "_replay - complete, " << num_events << " events, new read/expire pos is " << new_expire_pos << dendl;
    
    // move read pointer _back_ to first subtree map we saw, for eventual trimming
    journaler->set_read_pos(new_expire_pos);
    journaler->set_expire_pos(new_expire_pos);
    logger->set(l_mdl_expos, new_expire_pos);
  }

  // kick waiter(s)
  list<Context*> ls;
  ls.swap(waitfor_replay);
  finish_contexts(ls, r);  
  
  dout(10) << "_replay_thread finish" << dendl;
  mds->mds_lock.Unlock();
}



