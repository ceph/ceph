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

#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#define EVENT_STRING       1

#define EVENT_SESSION      7
#define EVENT_SUBTREEMAP   2
#define EVENT_EXPORT       30
#define EVENT_IMPORTSTART  31
#define EVENT_IMPORTFINISH 32
#define EVENT_FRAGMENT     33

#define EVENT_UPDATE       3
#define EVENT_SLAVEUPDATE  4
#define EVENT_OPEN         5

#define EVENT_PURGEFINISH  22

#define EVENT_ANCHOR       40
#define EVENT_ANCHORCLIENT 41




#include <string>
using namespace std;

#include "include/buffer.h"
#include "include/Context.h"

class MDS;
class LogSegment;

// generic log event
class LogEvent {
 private:
  int _type;
  off_t _start_off,_end_off;

  friend class MDLog;

 public:
  LogSegment *_segment;

  LogEvent(int t) : 
    _type(t), _start_off(0), _end_off(0), _segment(0) { }
  virtual ~LogEvent() { }

  int get_type() { return _type; }
  off_t get_start_off() { return _start_off; }
  off_t get_end_off() { return _end_off; }

  // encoding
  virtual void encode_payload(bufferlist& bl) = 0;
  virtual void decode_payload(bufferlist& bl, int& off) = 0;
  static LogEvent *decode(bufferlist &bl);


  virtual void print(ostream& out) { 
    out << "event(" << _type << ")";
  }

  /*** live journal ***/

  virtual void update_segment() { }
    

  /* obsolete() - is this entry committed to primary store, such that
   *   we can expire it from the journal?
   */
  virtual bool has_expired(MDS *m) {
    return true;
  }
  
  /* expire() - prod MDS into committing the relevant state so that this
   *   entry can be expired from the jorunal.
   */
  virtual void expire(MDS *m, Context *c) {
    assert(0);
    c->finish(0);
    delete c;
  }

  
  /*** recovery ***/
  /* replay() - replay given event.  this is idempotent.
   */
  virtual void replay(MDS *m) { assert(0); }


};

inline ostream& operator<<(ostream& out, LogEvent& le) {
  le.print(out);
  return out;
}

#endif
