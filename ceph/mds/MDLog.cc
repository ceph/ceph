
#include "include/types.h"
#include "MDLog.h"
#include "MDS.h"
#include "MDCluster.h"
#include "LogStream.h"
#include "LogEvent.h"

#include "common/LogType.h"
#include "common/Logger.h"
#include "msg/Message.h"

LogType mdlog_logtype;

#include "include/config.h"
#undef dout
#define  dout(l)    if (mds->get_nodeid() == 0 && (l<=g_conf.debug || l<=g_conf.debug_mds_log)) cout << "mds" << mds->get_nodeid() << ".log "

// cons/des

MDLog::MDLog(MDS *m) 
{
  mds = m;
  num_events = 0;
  max_events = 0;

  waiting_for_read = false;

  logstream = new LogStream(mds, mds->filer, MDS_INO_LOG_OFFSET + mds->get_nodeid());

  char name[80];
  sprintf(name, "mds%d.log", mds->get_nodeid());
  logger = new Logger(name, (LogType*)&mdlog_logtype);
}


MDLog::~MDLog()
{
  if (logstream) { delete logstream; logstream = 0; }
  if (logger) { delete logger; logger = 0; }
}



int MDLog::submit_entry( LogEvent *e,
						 Context *c ) 
{
  dout(5) << "submit_entry at " << num_events << endl;

  if (g_conf.mds_log) {
	off_t off = logstream->append(e);
	delete e;
	num_events++;

	logger->inc("add");
	logger->set("size", num_events);

	if (c) 
	  logstream->wait_for_sync(c, off);
  } else {
	// hack: log is disabled..
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
	logstream->wait_for_sync(c);
  } else {
	// hack: bypass
	c->finish(0);
	delete c;
  }
}

void MDLog::flush()
{
  logstream->flush();

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
	mdl->waiting_for_read = false;
	mdl->trim(0);
  }
};


void MDLog::trim(Context *c)
{
  // add waiter
  if (c) trim_waiters.push_back(c);

  // trim!
  while (num_events > max_events) {
	off_t gap = logstream->get_append_pos() - logstream->get_read_pos();
	dout(5) << "trim: num_events " << num_events << " - trimming " << trimming.size() << " > max " << max_events << " .. gap " << gap << endl;
	
	LogEvent *le = logstream->get_next_event();

	if (le) {
	  num_events--;

	  // we just read an event.
	  if (le->obsolete(mds) == true) {
		// obsolete
		dout(7) << "  obsolete " << le << endl;
		delete le;
		logger->inc("obs");
	  } else {
		if (trimming.size() < g_conf.mds_log_max_trimming) {
		  // trim!
		  dout(7) << "  trimming " << le << endl;
		  trimming.insert(le);
		  le->retire(mds, new C_MDL_Trimmed(this, le));
		  logger->inc("retire");
		  logger->set("trim", trimming.size());
		} else {
		  dout(7) << "  already trimming max, waiting" << endl;
		  return;
		}
	  }
	} else {
	  // need to read!
	  if (!waiting_for_read) {
		waiting_for_read = true;
		dout(7) << "  waiting for read" << endl;
		logstream->wait_for_next_event(new C_MDL_Reading(this));
	  } else {
		dout(7) << "  already waiting for read" << endl;
	  }
	  return;
	}
  }
  
  // trimmed!
  list<Context*> finished;
  finished.splice(finished.begin(), trim_waiters);
  
  finish_contexts(finished, 0);
}

void MDLog::_trimmed(LogEvent *le) 
{
  dout(7) << "  trimmed " << le << endl;
  trimming.erase(le);
  delete le;
 
  logger->set("trim", trimming.size());
 
  trim(0);
}


