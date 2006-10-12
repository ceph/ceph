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
#include "LogEvent.h"

#include "osdc/LogStreamer.h"

#include "common/LogType.h"
#include "common/Logger.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".log "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds_log) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".log "

// cons/des

LogType mdlog_logtype;

MDLog::MDLog(MDS *m) 
{
  mds = m;
  num_events = 0;
  waiting_for_read = false;

  max_events = g_conf.mds_log_max_len;

  // logger
  char name[80];
  sprintf(name, "mds%d.log", mds->get_nodeid());
  logger = new Logger(name, &mdlog_logtype);

  static bool didit = false;
  if (!didit) {
    mdlog_logtype.add_inc("add");
    mdlog_logtype.add_inc("retire");    
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
  logstreamer = new LogStreamer(log_inode, mds->objecter, logger);

}


MDLog::~MDLog()
{
  if (logstreamer) { delete logstreamer; logstreamer = 0; }
  if (logger) { delete logger; logger = 0; }
}



void MDLog::submit_entry( LogEvent *e,
			  Context *c ) 
{
  dout(5) << "submit_entry at " << num_events << endl;

  if (g_conf.mds_log) {
    // encode and append
    bufferlist bl;
    e->encode(bl);
    logstreamer->append_entry(bl);

    delete e;
    num_events++;

    logger->inc("add");
    logger->set("size", num_events);
    logger->set("append", logstreamer->get_write_pos());

    if (c) 
      logstreamer->flush(c);
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
    logstreamer->flush(c);
  } else {
    // hack: bypass.
    c->finish(0);
    delete c;
  }
}

void MDLog::flush()
{
  logstreamer->flush();

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
  dout(7) << "  trimmed " << le << endl;
  trimming.erase(le);
  delete le;
 
  logger->set("trim", trimming.size());
  logger->set("read", logstreamer->get_read_pos());
 
  trim(0);
}



void MDLog::trim(Context *c)
{
  // add waiter
  if (c) 
    trim_waiters.push_back(c);

  // trim!
  while (num_events > max_events) {
    
    off_t gap = logstreamer->get_write_pos() - logstreamer->get_read_pos();
    dout(5) << "trim num_events " << num_events << " > max " << max_events
	    << ", trimming " << trimming.size()
	    << ", byte gap " << gap
	    << endl;

    if ((int)trimming.size() >= g_conf.mds_log_max_trimming) {
      dout(7) << "trim  already trimming max, waiting" << endl;
      return;
    }
    
    bufferlist bl;
    if (logstreamer->try_read_entry(bl)) {
      // decode logevent
      LogEvent *le = LogEvent::decode(bl);
      num_events--;

      // we just read an event.
      if (le->obsolete(mds) == true) {
        // obsolete
        dout(7) << "trim  obsolete " << le << endl;
        delete le;
        logger->inc("obs");
      } else {
        assert ((int)trimming.size() < g_conf.mds_log_max_trimming);

        // trim!
        dout(7) << "trim  trimming " << le << endl;
        trimming.insert(le);
        le->retire(mds, new C_MDL_Trimmed(this, le));
        logger->inc("retire");
        logger->set("trim", trimming.size());
      }
      logger->set("read", logstreamer->get_read_pos());
      logger->set("size", num_events);
    } else {
      // need to read!
      if (!waiting_for_read) {
        waiting_for_read = true;
        dout(7) << "trim  waiting for read" << endl;
        logstreamer->wait_for_readable(new C_MDL_Reading(this));
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
}


