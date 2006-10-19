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


#ifndef __CAPABILITY_H
#define __CAPABILITY_H

#include "include/buffer.h"

#include <map>
using namespace std;

#include "config.h"


// definite caps
#define CAP_FILE_RDCACHE   1    // client can safely cache reads
#define CAP_FILE_RD        2    // client can read
#define CAP_FILE_WR        4    // client can write
#define CAP_FILE_WREXTEND  8    // client can extend file
#define CAP_FILE_WRBUFFER  16   // client can safely buffer writes
#define CAP_FILE_LAZYIO    32   // client can perform lazy io


// heuristics
//#define CAP_FILE_DELAYFLUSH  32

inline string cap_string(int cap)
{
  string s;
  s = "[";
  if (cap & CAP_FILE_RDCACHE) s += " rdcache";
  if (cap & CAP_FILE_RD) s += " rd";
  if (cap & CAP_FILE_WR) s += " wr";
  if (cap & CAP_FILE_WRBUFFER) s += " wrbuffer";
  if (cap & CAP_FILE_WRBUFFER) s += " wrextend";
  if (cap & CAP_FILE_LAZYIO) s += " lazyio";
  s += " ]";
  return s;
}


class Capability {
  int wanted_caps;     // what the client wants (ideally)

  map<long, int>  cap_history;  // seq -> cap
  long last_sent, last_recv;
    
  bool suppress;

public:
  Capability(int want=0) :
    wanted_caps(want),
    last_sent(0),
    last_recv(0),
    suppress(false) { 
    //cap_history[last_sent] = 0;
  }

  
  bool is_suppress() { return suppress; }
  void set_suppress(bool b) { suppress = b; }

  bool is_null() { return cap_history.empty(); }

  // most recently issued caps.
  int pending()   { 
    if (cap_history.count(last_sent))
      return cap_history[ last_sent ];
    return 0;
  }
  
  // caps client has confirmed receipt of
  int confirmed() { 
    if (cap_history.count(last_recv))
      return cap_history[ last_recv ];
    return 0;
  }

  // caps potentially issued
  int issued() { 
    int c = 0;
    for (long seq = last_recv; seq <= last_sent; seq++) {
      if (cap_history.count(seq)) {
        c |= cap_history[seq];
        dout(10) << " cap issued: seq " << seq << " " << cap_string(cap_history[seq]) << " -> " << cap_string(c) << endl;
      }
    }
    return c;
  }

  // caps this client wants to hold
  int wanted() { return wanted_caps; }
  void set_wanted(int w) {
    wanted_caps = w;
  }

  // needed
  static int needed(int from) {
    // strip out wrbuffer, rdcache
    return from & (CAP_FILE_WR|CAP_FILE_RD);
  }
  int needed() { return needed(wanted_caps); }

  // conflicts
  static int conflicts(int from) {
    int c = 0;
    if (from & CAP_FILE_WRBUFFER) c |= CAP_FILE_RDCACHE|CAP_FILE_RD;
    if (from & CAP_FILE_WR) c |= CAP_FILE_RDCACHE;
    if (from & CAP_FILE_RD) c |= CAP_FILE_WRBUFFER;
    if (from & CAP_FILE_RDCACHE) c |= CAP_FILE_WRBUFFER|CAP_FILE_WR;
    return c;
  }
  int wanted_conflicts() { return conflicts(wanted()); }
  int needed_conflicts() { return conflicts(needed()); }
  int issued_conflicts() { return conflicts(issued()); }

  // issue caps; return seq number.
  long issue(int c) {
    //int was = pending();
    //no!  if (c == was && last_sent) return -1;  // repeat of previous?
    
    ++last_sent;
    cap_history[last_sent] = c;

    /* no!
    // not recalling, just adding?
    if (c & ~was &&
        cap_history.count(last_sent-1)) { 
      cap_history.erase(last_sent-1);
    }
    */
    return last_sent;
  }
  long get_last_seq() { return last_sent; }

  void merge(Capability& other) {
    // issued + pending
    int newpending = other.pending() | pending();
    if (other.issued() & ~newpending)
      issue(other.issued() | newpending);
    issue(newpending);

    // wanted
    wanted_caps = wanted_caps | other.wanted();
  }

  // confirm receipt of a previous sent/issued seq.
  int confirm_receipt(long seq, int caps) {
    int r = 0;

    // old seqs
    while (last_recv < seq) {
      dout(10) << " cap.confirm_receipt forgetting seq " << last_recv << " " << cap_string(cap_history[last_recv]) << endl;
      r |= cap_history[last_recv];
      cap_history.erase(last_recv);
      ++last_recv;
    }
    
    // release current?
    if (cap_history.count(seq) &&
        cap_history[seq] != caps) {
      dout(10) << " cap.confirm_receipt revising seq " << seq << " " << cap_string(cap_history[seq]) << " -> " << cap_string(caps) << endl;
      // note what we're releasing..
      assert(cap_history[seq] & ~caps);
      r |= cap_history[seq] & ~caps; 

      cap_history[seq] = caps; // confirmed() now less than before..
    }

    // null?
    if (caps == 0 && 
        cap_history.size() == 1 &&
        cap_history.count(seq)) {
      cap_history.clear();  // viola, null!
    }

    return r;
  }

  // serializers
  void _encode(bufferlist& bl) {
    bl.append((char*)&wanted_caps, sizeof(wanted_caps));
    bl.append((char*)&last_sent, sizeof(last_sent));
    bl.append((char*)&last_recv, sizeof(last_recv));
    ::_encode(cap_history, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(wanted_caps), (char*)&wanted_caps);
    off += sizeof(wanted_caps);
    bl.copy(off, sizeof(last_sent), (char*)&last_sent);
    off += sizeof(last_sent);
    bl.copy(off, sizeof(last_recv), (char*)&last_recv);
    off += sizeof(last_recv);
    ::_decode(cap_history, bl, off);
  }
  
};





#endif
