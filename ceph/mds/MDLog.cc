
#include "MDLog.h"
#include "MDS.h"
#include "MDCluster.h"
#include "LogStream.h"
#include "LogEvent.h"

#define MAX_TRIMMING   4    // max events to be retiring simultaneously

#include "include/LogType.h"
#include "include/Logger.h"
#include "include/Message.h"

LogType mdlog_logtype;


#define  dout(l)    if (l<=DEBUG_LEVEL) cout << "mds" << mds->get_nodeid() << ".log "
#define  dout2(l)    if (1<=DEBUG_LEVEL) cout

// cons/des

MDLog::MDLog(MDS *m) 
{
  mds = m;
  num_events = 0;
  max_events = 0;
  trim_reading = false;
  reader = new LogStream(mds, 
						 mds->get_cluster()->get_log_osd(mds->get_nodeid()),
						 mds->get_cluster()->get_log_oid(mds->get_nodeid()));
  writer = new LogStream(mds,
						 mds->get_cluster()->get_log_osd(mds->get_nodeid()),
						 mds->get_cluster()->get_log_oid(mds->get_nodeid()));

  string name;
  name = "log.mds";
  int w = mds->get_nodeid();
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));
  logger = new Logger(name, (LogType*)&mdlog_logtype);
}


MDLog::~MDLog()
{
  if (reader) { delete reader; reader = 0; }
  if (writer) { delete writer; writer = 0; }
  if (logger) { delete logger; logger = 0; }
}


class C_MDL_SubmitEntry : public Context {
protected:
  MDLog *mdl;
public:
  Context *c;
  LogEvent *le;

  C_MDL_SubmitEntry(MDLog *m, LogEvent *e, Context *c) {
	mdl = m; 
	le = e;
	this->c = c;
  }
  void finish(int res) {
	mdl->submit_entry_2(le,c);
  }
};

int MDLog::submit_entry( LogEvent *e,
						 Context *c ) 
{
  // write it
  writer->append(e, new C_MDL_SubmitEntry(this, e, c));
  logger->inc("add");
}

int MDLog::submit_entry_2( LogEvent *e,
						   Context *c ) 
{
  // written!
  num_events++;
  delete e;

  if (c) {
	c->finish(0);
	delete c;
  }
  
  // trim
  trim(NULL);    // FIXME probably not every time?
}

class C_MDL_Trim : public Context {
protected:
  MDLog *mdl;
public:
  LogEvent *le;
  int step;

  C_MDL_Trim(MDLog *m, LogEvent *e = 0, int s=2) {
	mdl = m; 
	step = s; le = e;
  }
  void finish(int res) {
	if (step == 2) 
	  mdl->trim_2_didread(le);
	else if (step == 3)
	  mdl->trim_3_didretire(le);
  }
};


int MDLog::trim(Context *c)
{
  if (num_events - trimming.size() > max_events) {
	// add this waiter
	if (c) trim_waiters.push_back(c);
	
	trim_readnext();   // read next event off end of log.
	return 0;
  } 

  // no trimming to be done.
  if (c) {
	c->finish(0);
	delete c;
  }
}

void MDLog::trim_readnext()
{
  trim_reading = true;
  C_MDL_Trim *readfin = new C_MDL_Trim(this);
  reader->read_next(&readfin->le, readfin);
  logger->inc("read");
}


// trim_2 : just read an event
int MDLog::trim_2_didread(LogEvent *le)
{
  trim_reading = false;
  
  // we just read an event.
  if (le->obsolete(mds) == true) {
	trim_3_didretire(le);    // we can discard this event and be done.
	logger->inc("obs");
  } else {
	trimming.push_back(le);	 // add to limbo list
	le->retire(mds, new C_MDL_Trim(this, le, 3)); 	// retire entry
	logger->inc("retire");
  }

  // read another event?      FIXME: max_trimming maybe?  would need to restructure this again.
  if (num_events - trimming.size() > max_events &&
	  trimming.size() < MAX_TRIMMING) {
	trim_readnext();
  }
}


int MDLog::trim_3_didretire(LogEvent *le)
{
  // done with this le.
  if (le) {
	num_events--;
	trimming.remove(le);
	delete le;
  }  

  // read more?
  if (trim_reading == false &&
	  num_events - trimming.size() > max_events) {
	trim_readnext();
  }

  // last one?
  if (trimming.size() == 0 &&       // none mid-retire,
	  trim_reading == false) {      // and not mid-read
	
	// we're done.
	list<Context*> finished = trim_waiters;
	trim_waiters.clear();

	list<Context*>::iterator it = finished.begin();
	while (it != finished.end()) {
	  Context *c = *it;
	  if (c) {
		c->finish(0);
		delete c;
	  }
	}
  }
}

