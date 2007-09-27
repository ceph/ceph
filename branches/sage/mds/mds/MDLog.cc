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
#undef dout
#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".log "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".log "

// cons/des

LogType mdlog_logtype;


MDLog::~MDLog()
{
  if (journaler) { delete journaler; journaler = 0; }
  if (logger) { delete logger; logger = 0; }
}


void MDLog::init_journaler()
{
  // logger
  char name[80];
  sprintf(name, "mds%d.log", mds->get_nodeid());
  logger = new Logger(name, &mdlog_logtype);

  static bool didit = false;
  if (!didit) {
    didit = true;
    mdlog_logtype.add_inc("add");
    mdlog_logtype.add_inc("expire");    
    mdlog_logtype.add_inc("obs");    
    mdlog_logtype.add_inc("trim");    
    mdlog_logtype.add_set("size");
    mdlog_logtype.add_set("read");
    mdlog_logtype.add_set("append");
    mdlog_logtype.add_inc("lsum");
    mdlog_logtype.add_inc("lnum");
  }

  // inode
  memset(&log_inode, 0, sizeof(log_inode));
  log_inode.ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();
  log_inode.layout = g_OSD_MDLogLayout;
  
  if (g_conf.mds_local_osd) 
    log_inode.layout.preferred = mds->get_nodeid() + g_conf.mds_local_osd_offset;  // hack
  
  // log streamer
  if (journaler) delete journaler;
  journaler = new Journaler(log_inode, mds->objecter, logger);
}

void MDLog::flush_logger()
{
  if (logger)
    logger->flush(true);
}



void MDLog::reset()
{
  dout(5) << "reset to empty log" << endl;
  init_journaler();
  journaler->reset();
}

void MDLog::open(Context *c)
{
  dout(5) << "open discovering log bounds" << endl;
  init_journaler();
  journaler->recover(c);
}

void MDLog::write_head(Context *c) 
{
  journaler->write_head(c);
}


off_t MDLog::get_read_pos() 
{
  return journaler->get_read_pos(); 
}

off_t MDLog::get_write_pos() 
{
  return journaler->get_write_pos(); 
}



void MDLog::submit_entry( LogEvent *le, Context *c ) 
{
  if (g_conf.mds_log) {
    dout(5) << "submit_entry " << journaler->get_write_pos() << " : " << *le << endl;

    // let the event register itself in the segment
    assert(!segments.empty());
    le->_segment = segments.rbegin()->second;
    le->_segment->num_events++;
    le->update_segment();

    // encode it, with event type
    {
      bufferlist bl;
      bl.append((char*)&le->_type, sizeof(le->_type));
      le->encode_payload(bl);
      
      // journal it.
      journaler->append_entry(bl);  // bl is destroyed.
    }

    assert(!capped);

    delete le;
    num_events++;

    logger->inc("add");
    logger->set("size", num_events);
    logger->set("append", journaler->get_write_pos());

    if (c) {
      unflushed = 0;
      journaler->flush(c);
    }
    else
      unflushed++;

    // start a new segment?
    //  FIXME: should this go elsewhere?
    if (!segments.empty() && !writing_subtree_map &&
	(journaler->get_write_pos() / log_inode.layout.period()) !=
	(last_subtree_map / log_inode.layout.period()) &&
	(journaler->get_write_pos() - last_subtree_map > log_inode.layout.period()/2)) {
      dout(10) << "submit_entry also starting new segment: last = " << last_subtree_map
	       << ", cur pos = " << journaler->get_write_pos() << endl;
      start_new_segment();
    }

  } else {
    // hack: log is disabled.
    if (c) {
      c->finish(0);
      delete c;
    }
  }
}

void MDLog::wait_for_sync( Context *c )
{
  if (g_conf.mds_log) {
    // wait
    journaler->flush(c);
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

  // trim
  trim();
}



// -----------------------------
// segments

void MDLog::start_new_segment(Context *onsync)
{
  dout(7) << "start_new_segment at " << journaler->get_write_pos() << endl;
  assert(!writing_subtree_map);

  segments[journaler->get_write_pos()] = new LogSegment(journaler->get_write_pos());
  num_segments++;

  writing_subtree_map = true;

  ESubtreeMap *le = mds->mdcache->create_subtree_map();
  submit_entry(le, new C_MDL_WroteSubtreeMap(this, mds->mdlog->get_write_pos()));
  if (onsync)
    wait_for_sync(onsync);  
}

void MDLog::_logged_subtree_map(off_t off)
{
  dout(10) << "_logged_subtree_map at " << off << endl;
  last_subtree_map = off;
  writing_subtree_map = false;

  list<Context*> ls;
  take_subtree_map_expire_waiters(ls);
  mds->queue_waiters(ls);
}



class C_MDL_TrimmedSegment : public Context {
  MDLog *mdlog;
  LogSegment *ls;
public:
  C_MDL_TrimmedSegment(MDLog *mdl, LogSegment *s) : mdlog(mdl), ls(s) {}
  void finish(int res) {
    mdlog->_trimmed(ls);
  }
};

void MDLog::trim()
{
  // trim!
  dout(10) << "trim " << segments.size() << " segments, " 
	   << num_events << " events / " << max_events << " max" << endl;

  if (segments.empty()) return;

  // hack: only trim for a few seconds at a time
  utime_t stop = g_clock.now();
  stop += 2.0;

  map<off_t,LogSegment*>::iterator p = segments.begin();
  int left = num_events;
  while (left > max_events) {
    if (stop < g_clock.now())
      break;

    // look at first segment
    LogSegment *ls = p->second;
    
    if (trimming_segments.count(ls)) {
      dout(5) << "trim already trimming segment " << ls->offset << ", " << ls->num_events << " events" << endl;
    } else {
      C_Gather *exp = ls->try_to_expire(mds);
      if (exp) {
	trimming_segments.insert(ls);
	dout(5) << "trim trimming segment " << ls->offset << endl;
	exp->set_finisher(new C_MDL_TrimmedSegment(this, ls));
      } else {
	dout(5) << "trim trimmed segment " << ls->offset << endl;
	_trimmed(ls);
      }
    }

    left -= ls->num_events;
    p++;
  }
}

void MDLog::_trimmed(LogSegment *ls)
{
  if (ls == segments.begin()->second) {
    dout(5) << "_trimmed segment " << ls->offset << " " << ls->num_events << " events, dropping" << endl;
    num_events -= ls->num_events;
    segments.erase(segments.begin());
    delete ls;
  } else {
    dout(5) << "_trimmed segment " << ls->offset << " " << ls->num_events << " events, remembering" << endl;
  }
}



void MDLog::replay(Context *c)
{
  assert(journaler->is_active());

  // start reading at the last known expire point.
  journaler->set_read_pos( journaler->get_expire_pos() );

  // empty?
  if (journaler->get_read_pos() == journaler->get_write_pos()) {
    dout(10) << "replay - journal empty, done." << endl;
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
	   << " to " << journaler->get_write_pos() << endl;

  assert(num_events == 0);

  replay_thread.create();
  //_replay(); 
}

class C_MDL_Replay : public Context {
  MDLog *mdlog;
public:
  C_MDL_Replay(MDLog *l) : mdlog(l) {}
  void finish(int r) { 
    mdlog->replay_cond.Signal();
    //mdlog->_replay(); 
  }
};



// i am a separate thread
void MDLog::_replay_thread()
{
  mds->mds_lock.Lock();
  dout(10) << "_replay_thread start" << endl;

  // loop
  while (1) {
    // wait for read?
    while (!journaler->is_readable() &&
	   journaler->get_read_pos() < journaler->get_write_pos()) {
      journaler->wait_for_readable(new C_MDL_Replay(this));
      replay_cond.Wait(mds->mds_lock);
    }
    
    if (!journaler->is_readable() &&
	journaler->get_read_pos() == journaler->get_write_pos())
      break;
    
    assert(journaler->is_readable());
    
    // read it
    off_t pos = journaler->get_read_pos();
    bufferlist bl;
    bool r = journaler->try_read_entry(bl);
    assert(r);
    
    // unpack event
    LogEvent *le = LogEvent::decode(bl);
    num_events++;

    // new segment?
    if (le->get_type() == EVENT_SUBTREEMAP) {
      segments[pos] = new LogSegment(pos);
      num_segments++;
    }

    le->_segment = get_current_segment();    // replay may need this

    // have we seen an import map yet?
    if (segments.empty()) {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " -- waiting for subtree_map.  (skipping " << *le << ")" << endl;
    } else {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " : " << *le << endl;
      le->replay(mds);
    }
    delete le;

    // drop lock for a second, so other events/messages (e.g. beacon timer!) can go off
    mds->mds_lock.Unlock();
    mds->mds_lock.Lock();
  }

  // done!
  assert(journaler->get_read_pos() == journaler->get_write_pos());
  dout(10) << "_replay - complete" << endl;
  
  // move read pointer _back_ to expire pos, for eventual trimming
  journaler->set_read_pos(journaler->get_expire_pos());
  
  // kick waiter(s)
  list<Context*> ls;
  ls.swap(waitfor_replay);
  finish_contexts(ls,0);  
  
  dout(10) << "_replay_thread finish" << endl;
  mds->mds_lock.Unlock();
}



