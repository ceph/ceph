
#include "MDLog.h"
#include "MDS.h"
#include "LogStream.h"
#include "LogEvent.h"

#define MAX_TRIMMING   4    // max events to be retiring simultaneously


// cons/des

MDLog::MDLog(MDS *m) 
{
  mds = m;
  num_events = 0;
  max_events = 0;
  trim_reading = false;
  reader = new LogStream(mds, 666, mds->get_nodeid());
  writer = new LogStream(mds, 666, mds->get_nodeid());
}


MDLog::~MDLog()
{
  if (reader) { delete reader; reader = 0; }
  if (writer) { delete writer; writer = 0; }
}


int MDLog::submit_entry( LogEvent *e,
						 Context *c ) 
{
  // write it
  writer->append(e, c);

  // trim
  trim(NULL);
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
}


// trim_2 : just read an event
int MDLog::trim_2_didread(LogEvent *le)
{
  trim_reading = false;
  
  // we just read an event.
  if (le->obsolete(mds) == true) {
	trim_3_didretire(le);    // we can discard this event and be done.
  } else {
	trimming.push_back(le);	 // add to limbo list
	le->retire(mds, new C_MDL_Trim(this, le, 3)); 	// retire entry
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

