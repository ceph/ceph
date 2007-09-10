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

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) *_dout << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".log "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) *_derr << dbeginl << g_clock.now() << " mds" << mds->get_nodeid() << ".log "

// cons/des

LogType mdlog_logtype;


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
    mdlog_logtype.add_inc("add");
    mdlog_logtype.add_inc("obs");    
    mdlog_logtype.add_inc("trims");
    mdlog_logtype.add_inc("trimf");
    mdlog_logtype.add_inc("trimng");    
    mdlog_logtype.add_set("size");
    mdlog_logtype.add_set("rdpos");
    mdlog_logtype.add_set("wrpos");
    mdlog_logtype.add_avg("jlat");
  }

}

void MDLog::init_journaler()
{
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



void MDLog::reset()
{
  dout(5) << "reset to empty log" << dendl;
  init_journaler();
  journaler->reset();
}

void MDLog::open(Context *c)
{
  dout(5) << "open discovering log bounds" << dendl;
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
    dout(5) << "submit_entry " << journaler->get_write_pos() << " : " << *le << dendl;

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

    if (logger) {
      logger->inc("add");
      logger->set("size", num_events);
      logger->set("wrpos", journaler->get_write_pos());
    }

    if (c) {
      unflushed = 0;
      journaler->flush(c);
    }
    else
      unflushed++;

    // should we log a new import_map?
    // FIXME: should this go elsewhere?
    if (!writing_subtree_map &&
	(journaler->get_write_pos() / log_inode.layout.period()) !=
	(get_last_subtree_map_offset() / log_inode.layout.period()) &&
	(journaler->get_write_pos() - get_last_subtree_map_offset() > log_inode.layout.period()/2)) {
      // log import map
      dout(10) << "submit_entry also logging subtree map: last = " << get_last_subtree_map_offset()
	       << ", cur pos = " << journaler->get_write_pos() << dendl;
      mds->mdcache->log_subtree_map();
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
  trim(NULL);
}

void MDLog::cap()
{ 
  dout(5) << "cap" << dendl;
  capped = true;
  kick_subtree_map();
}


// trim

class C_MDL_Trimmed : public Context {
public:
  MDLog *mdl;
  LogEvent *le;

  C_MDL_Trimmed(MDLog *mdl, LogEvent *le) {
    this->mdl = mdl; 
    this->le = le;
  }
  void finish(int res) {
    mdl->_trimmed(le);
  }
};

class C_MDL_Reading : public Context {
public:
  MDLog *mdl;
  C_MDL_Reading(MDLog *m) {
    mdl = m; 
  }
  void finish(int res) {
    mdl->_did_read();
  }
};


void MDLog::_did_read() 
{
  dout(5) << "_did_read()" << dendl;
  waiting_for_read = false;
  trim(0);
}

void MDLog::_trimmed(LogEvent *le) 
{
  // successful trim?
  if (!le->has_expired(mds)) {
    dout(7) << "retrimming : " << le->get_start_off() << " : " << *le << dendl;
    le->expire(mds, new C_MDL_Trimmed(this, le));
    return;
  }

  dout(7) << "trimmed : " << le->get_start_off() << " : " << *le << dendl;

  bool kick = false;

  map<off_t,LogEvent*>::iterator p = trimming.begin();
  if (p->first == le->_start_off) {
    // we trimmed off the front!  it must have been a segment head.
    assert(!subtree_maps.empty());
    assert(p->first == *subtree_maps.begin());
    subtree_maps.erase(subtree_maps.begin());

    // we can expire the log a bit.
    off_t to = get_trimmed_to();
    journaler->set_expire_pos(to);
    journaler->trim();

    kick = true;
  } else {
    p++;
    
    // is the next one us?
    if (le->_start_off == p->first) {
      p++;

      // did we empty a segment?
      if (subtree_maps.size() >= 2) {
	set<off_t>::iterator segp = subtree_maps.begin();
	assert(*segp < le->_end_off);
	segp++;
	dout(20) << "i ended at " << le->get_end_off() 
		 << ", next seg starts at " << *segp
		 << ", next trimming is " << (p == trimming.end() ? 0:p->first)
		 << dendl;
	if (*segp >= le->_end_off &&
	    (p == trimming.end() ||
	     p->first >= *segp)) {
	  dout(10) << "_trimmed segment looks empty" << dendl;
	  kick = true;
	}
      } else if (capped && trimming.size() < 3) {
	kick = true;   // blech, imprecise
      }
    }
  }

  trimming.erase(le->_start_off);
  delete le;

  if (kick) 
    kick_subtree_map();
  
  if (logger) {
    logger->inc("trimf");
    logger->set("trimng", trimming.size());
    logger->set("rdpos", journaler->get_read_pos());
  }

  trim(0);
}



void MDLog::trim(Context *c)
{
  // add waiter
  if (c) 
    trim_waiters.push_back(c);

  // trim!
  dout(10) << "trim " <<  num_events << " events / " << max_events << " max" << dendl;

  // hack: only trim for a few seconds at a time
  utime_t stop = g_clock.now();
  stop += 2.0;

  while (num_events > max_events) {
    // don't check the clock on _every_ event, here!
    if (num_events % 100 == 0 &&
	stop < g_clock.now())
	break;
    
    off_t gap = journaler->get_write_pos() - journaler->get_read_pos();
    dout(5) << "trim num_events " << num_events << " > max " << max_events
	    << ", trimming " << trimming.size()
	    << ", byte gap " << gap
	    << dendl;

    if ((int)trimming.size() >= g_conf.mds_log_max_trimming) {
      dout(7) << "trim  already trimming max, waiting" << dendl;
      return;
    }
    
    bufferlist bl;
    off_t so = journaler->get_read_pos();
    if (journaler->try_read_entry(bl)) {
      // decode logevent
      LogEvent *le = LogEvent::decode(bl);
      le->_start_off = so;
      le->_end_off = journaler->get_read_pos();
      num_events--;

      // we just read an event.
      if (le->has_expired(mds)) {
        // obsolete
 	dout(7) << "trim  obsolete : " << le->get_start_off() << " : " << *le << dendl;
        delete le;
        if (logger) logger->inc("obs");
      } else {
        assert ((int)trimming.size() < g_conf.mds_log_max_trimming);

        // trim!
	dout(7) << "trim  expiring : " << le->get_start_off() << " : " << *le << dendl;
        trimming[le->_start_off] = le;
        le->expire(mds, new C_MDL_Trimmed(this, le));
	if (logger) {
	  logger->inc("trims");
	  logger->set("trimng", trimming.size());
	}
      }
      if (logger) {
	logger->set("rdpos", journaler->get_read_pos());
	logger->set("size", num_events);
      }
    } else {
      // need to read!
      if (!waiting_for_read) {
        waiting_for_read = true;
        dout(7) << "trim  waiting for read" << dendl;
        journaler->wait_for_readable(new C_MDL_Reading(this));
      } else {
        dout(7) << "trim  already waiting for read" << dendl;
      }
      return;
    }
  }

  dout(10) << "trim num_events " << num_events << " <= max " << max_events
	  << ", trimming " << trimming.size()
	  << ", done for now."
	  << dendl;
  
  // trimmed!
  std::list<Context*> finished;
  finished.swap(trim_waiters);
  finish_contexts(finished, 0);
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
  dout(10) << "_replay_thread start" << dendl;

  // loop
  off_t new_expire_pos = journaler->get_expire_pos();
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

    // have we seen an import map yet?
    if (!seen_subtree_map &&
	le->get_type() != EVENT_SUBTREEMAP) {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " -- waiting for subtree_map.  (skipping " << *le << ")" << dendl;
    } else {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " : " << *le << dendl;
      le->replay(mds);

      num_events++;
      if (!new_expire_pos) 
	new_expire_pos = pos;

      if (le->get_type() == EVENT_SUBTREEMAP)
	seen_subtree_map = true;
    }
    delete le;

    // drop lock for a second, so other events/messages (e.g. beacon timer!) can go off
    mds->mds_lock.Unlock();
    mds->mds_lock.Lock();
  }

  // done!
  assert(journaler->get_read_pos() == journaler->get_write_pos());
  dout(10) << "_replay - complete, " << num_events << " events, new read/expire pos is " << new_expire_pos << dendl;
  
  // move read pointer _back_ to first subtree map we saw, for eventual trimming
  journaler->set_read_pos(new_expire_pos);
  journaler->set_expire_pos(new_expire_pos);
  
  // kick waiter(s)
  list<Context*> ls;
  ls.swap(waitfor_replay);
  finish_contexts(ls,0);  
  
  dout(10) << "_replay_thread finish" << dendl;
  mds->mds_lock.Unlock();
}



