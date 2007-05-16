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


#ifndef __ESTRING_H
#define __ESTRING_H

#include <stdlib.h>
#include <string>
using namespace std;

#include "../LogEvent.h"

// generic log event
class EString : public LogEvent {
 protected:
  string event;

 public:
  EString(string e) :
    LogEvent(EVENT_STRING) {
    event = e;
  }
  EString() :
    LogEvent(EVENT_STRING) {
  }

  void decode_payload(bufferlist& bl, int& off) {
    event = bl.c_str() + off;
    off += event.length() + 1;
  }
  void encode_payload(bufferlist& bl) {
    bl.append(event.c_str(), event.length()+1);
  }

  void print(ostream& out) {
    out << '"' << event << '"';
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);

};

#endif
