#ifndef __MDLOG_H
#define __MDLOG_H

/*

hmm, some things that go in the MDS log:


prepare + commit versions of many of these?

- inode update
 ???  entry will include mdloc_t of dir it resides in... 

- directory operation
  unlink,
  rename= atomic link+unlink (across multiple dirs, possibly...)

- import
- export


*/

#include "../include/Context.h"

#include <list>

using namespace std;

class LogStream;
class LogEvent;
class MDS;

class Logger;

class MDLog {
 protected:
  MDS *mds;
  size_t num_events; // in events
  size_t max_events;

  LogStream *logstream;
  
  set<LogEvent*>  trimming;     // events currently being trimmed
  list<Context*>  trim_waiters; // contexts waiting for trim
  bool            trim_reading;

  bool waiting_for_read;
  friend class C_MDL_Reading;

  Logger *logger;
  
 public:
  MDLog(MDS *m);
  ~MDLog();
  
  void set_max_events(size_t max) {
	max_events = max;
  }
  size_t get_max_events() {
	return max_events;
  }
  size_t get_num_events() {
	return num_events;
  }

  int submit_entry( LogEvent *e,
					Context *c = 0 );
  void wait_for_sync( Context *c );
  void flush();

  void trim(Context *c);
  void _trimmed(LogEvent *le);
};

#endif
