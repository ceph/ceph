// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

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

#include <ext/hash_set>
using namespace __gnu_cxx;

class LogStream;
class LogEvent;
class MDS;

class Logger;


namespace __gnu_cxx {
  template<> struct hash<LogEvent*> {
	size_t operator()(const LogEvent *p) const { 
	  static hash<unsigned long> H;
	  return H((unsigned long)p); 
	}
  };
}

class MDLog {
 protected:
  MDS *mds;
  size_t num_events; // in events
  size_t max_events;

  LogStream *logstream;
  
  hash_set<LogEvent*>  trimming;     // events currently being trimmed
  list<Context*>  trim_waiters;   // contexts waiting for trim
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
	return num_events + trimming.size();
  }

  void submit_entry( LogEvent *e,
					 Context *c = 0 );
  void wait_for_sync( Context *c );
  void flush();

  void trim(Context *c);
  void _did_read();
  void _trimmed(LogEvent *le);
};

#endif
