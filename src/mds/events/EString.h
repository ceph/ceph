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
    ::_decode(event, bl, off);
  }
  void encode_payload(bufferlist& bl) {
    ::_encode(event, bl);
  }

  void print(ostream& out) {
    out << '"' << event << '"';
  }

  void replay(MDS *mds);

};

#endif
