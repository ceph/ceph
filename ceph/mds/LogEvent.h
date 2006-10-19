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



#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#define EVENT_STRING       1
#define EVENT_INODEUPDATE  2
#define EVENT_DIRUPDATE    3
#define EVENT_UNLINK       4
#define EVENT_ALLOC        5


#include <string>
using namespace std;

#include "include/buffer.h"
#include "include/Context.h"

class MDS;

// generic log event
class LogEvent {
 private:
  int _type;

 public:
  LogEvent(int t) : _type(t) { }
  virtual ~LogEvent() { }
  
  int get_type() { return _type; }

  virtual void encode_payload(bufferlist& bl) = 0;
  virtual void decode_payload(bufferlist& bl, int& off) = 0;

  void encode(bufferlist& bl) {
    // type
    assert(_type > 0);
    bl.append((char*)&_type, sizeof(_type));

    // payload
    encode_payload(bl);

    /*// HACK: pad payload to match md log layout?
    int elen = bl.length() - off + sizeof(_type);
    if (elen % g_conf.mds_log_pad_entry > 0) {
      int add = g_conf.mds_log_pad_entry - (elen % g_conf.mds_log_pad_entry);
      //cout << "elen " << elen << "  adding " << add << endl;
      buffer *b = new buffer(add);
      memset(b->c_str(), 0, add);
      bufferptr bp(b);
      bl.append(bp);
    } 

    len = bl.length() - off - sizeof(len);
    bl.copy_in(off, sizeof(len), (char*)&len);
    */
  }

  static LogEvent *decode(bufferlist &bl);
  
  // ...
  virtual bool obsolete(MDS *m) {
    return true;
  }

  virtual void retire(MDS *m, Context *c) {
    c->finish(0);
    delete c;
  }

};

#endif
