// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#undef dout
#define  dout(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".log "
#define  derr(l)    if (l<=g_conf.debug_mds || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".log "

// cons/des

LogType mdlog_logtype;

MDLog::MDLog(MDS *m) 
{
  mds = m;
  num_events = 0;
  waiting_for_read = false;

  last_import_map = 0;
  writing_import_map = false;
  seen_import_map = false;

  max_events = g_conf.mds_log_max_len;

  capped = false;

  unflushed = 0;

  journaler = 0;
  logger = 0;
}


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
  
  if (g_conf.mds_local_osd) {
    log_inode.layout.object_layout = OBJECT_LAYOUT_STARTOSD;
    log_inode.layout.osd = mds->get_nodeid() + 10000;   // hack
  }
  
  // log streamer
  if (journaler) delete journaler;
  journaler = new Journaler(log_inode, mds->objecter, logger);
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



void MDLog::submit_entry( LogEvent *le,
			  Context *c ) 
{
  if (g_conf.mds_log) {
    dout(5) << "submit_entry " << journaler->get_write_pos() << " : " << *le << endl;

    // encode it, with event type
    bufferlist bl;
    bl.append((char*)&le->_type, sizeof(le->_type));
    le->encode_payload(bl);

    // journal it.
    journaler->append_entry(bl);

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

    // should we log a new import_map?
    // FIXME: should this go elsewhere?
    if (last_import_map && !writing_import_map &&
	journaler->get_write_pos() - last_import_map >= g_conf.mds_log_import_map_interval) {
      // log import map
      mds->mdcache->log_import_map();
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
  dout(5) << "_did_read()" << endl;
  waiting_for_read = false;
  trim(0);
}

void MDLog::_trimmed(LogEvent *le) 
{
  dout(7) << "trimmed : " << le->get_start_off() << " : " << *le << endl;
  assert(le->has_expired(mds));

  if (trimming.begin()->first == le->_end_off) {
    // we trimmed off the front!  
    // we can expire the log a bit.
    journaler->set_expire_pos(le->_end_off);
  }

  trimming.erase(le->_end_off);
  delete le;
 
  logger->set("trim", trimming.size());
  logger->set("read", journaler->get_read_pos());
 
  trim(0);
}



void MDLog::trim(Context *c)
{
  // add waiter
  if (c) 
    trim_waiters.push_back(c);

  // trim!
  dout(10) << "trim " <<  num_events << " events / " << max_events << " max" << endl;

  while (num_events > max_events) {
    
    off_t gap = journaler->get_write_pos() - journaler->get_read_pos();
    dout(5) << "trim num_events " << num_events << " > max " << max_events
	    << ", trimming " << trimming.size()
	    << ", byte gap " << gap
	    << endl;

    if ((int)trimming.size() >= g_conf.mds_log_max_trimming) {
      dout(7) << "trim  already trimming max, waiting" << endl;
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
 	dout(7) << "trim  obsolete : " << le->get_start_off() << " : " << *le << endl;
        delete le;
        logger->inc("obs");
      } else {
        assert ((int)trimming.size() < g_conf.mds_log_max_trimming);

        // trim!
	dout(7) << "trim  expiring : " << le->get_start_off() << " : " << *le << endl;
        trimming[le->_end_off] = le;
        le->expire(mds, new C_MDL_Trimmed(this, le));
        logger->inc("expire");
        logger->set("trim", trimming.size());
      }
      logger->set("read", journaler->get_read_pos());
      logger->set("size", num_events);
    } else {
      // need to read!
      if (!waiting_for_read) {
        waiting_for_read = true;
        dout(7) << "trim  waiting for read" << endl;
        journaler->wait_for_readable(new C_MDL_Reading(this));
      } else {
        dout(7) << "trim  already waiting for read" << endl;
      }
      return;
    }
  }

  dout(5) << "trim num_events " << num_events << " <= max " << max_events
	  << ", trimming " << trimming.size()
	  << ", done for now."
	  << endl;
  
  // trimmed!
  std::list<Context*> finished;
  finished.swap(trim_waiters);
  finish_contexts(finished, 0);

  // hmm, are we at the end?
  /*
  if (journaler->get_read_pos() == journaler->get_write_pos() &&
      trimming.size() == import_map_expire_waiters.size()) {
    dout(5) << "trim log is empty, allowing import_map to expire" << endl;
    list<Context*> ls;
    ls.swap(import_map_expire_waiters);
    finish_contexts(ls);
  }
  */
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

  _replay(); 
}

class C_MDL_Replay : public Context {
  MDLog *mdlog;
public:
  C_MDL_Replay(MDLog *l) : mdlog(l) {}
  void finish(int r) { mdlog->_replay(); }
};

void MDLog::_replay()
{
  // read what's buffered
  while (journaler->is_readable() &&
	 journaler->get_read_pos() < journaler->get_write_pos()) {
    // read it
    off_t pos = journaler->get_read_pos();
    bufferlist bl;
    bool r = journaler->try_read_entry(bl);
    assert(r);
    
    // unpack event
    LogEvent *le = LogEvent::decode(bl);
    num_events++;

    // have we seen an import map yet?
    if (!seen_import_map &&
	le->get_type() != EVENT_IMPORTMAP) {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " -- waiting for import_map.  (skipping " << *le << ")" << endl;
    } else {
      dout(10) << "_replay " << pos << " / " << journaler->get_write_pos() 
	       << " : " << *le << endl;
      le->replay(mds);

      if (le->get_type() == EVENT_IMPORTMAP)
	seen_import_map = true;
    }
    delete le;
  }

  // wait for read?
  if (journaler->get_read_pos() < journaler->get_write_pos()) {
    journaler->wait_for_readable(new C_MDL_Replay(this));
    return;    
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
}


