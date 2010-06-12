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


#ifndef CEPH_ESTRING_H
#define CEPH_ESTRING_H

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

  void encode(bufferlist& bl) const {
    ::encode(event, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(event, bl);
  }

  void print(ostream& out) {
    out << '"' << event << '"';
  }

  void replay(MDS *mds);

};

#endif
