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


#ifndef __CAPABILITY_H
#define __CAPABILITY_H

#include "include/buffer.h"
#include "include/xlist.h"

#include <map>
using namespace std;

#include "config.h"


// heuristics
//#define CEPH_CAP_DELAYFLUSH  32

inline string cap_string(int cap)
{
  string s;
  s = "[";
  if (cap & CEPH_CAP_PIN) s += " pin";
  if (cap & CEPH_CAP_RDCACHE) s += " rdcache";
  if (cap & CEPH_CAP_RD) s += " rd";
  if (cap & CEPH_CAP_WR) s += " wr";
  if (cap & CEPH_CAP_WRBUFFER) s += " wrbuffer";
  if (cap & CEPH_CAP_WRBUFFER) s += " wrextend";
  if (cap & CEPH_CAP_LAZYIO) s += " lazyio";
  s += " ]";
  return s;
}

typedef uint32_t capseq_t;

class CInode;

class Capability {
public:
  struct Export {
    int wanted;
    int issued;
    int pending;
    Export() {}
    Export(int w, int i, int p) : wanted(w), issued(i), pending(p) {}
  };

private:
  CInode *inode;
  int wanted_caps;     // what the client wants (ideally)
  
  map<capseq_t, int>  cap_history;  // seq -> cap
  capseq_t last_sent, last_recv;
  capseq_t last_open;
  
  bool suppress;
  bool stale;
public:
  xlist<Capability*>::item session_caps_item;

  Capability(CInode *i=0, int want=0, capseq_t s=0) :
    inode(i),
    wanted_caps(want),
    last_sent(s),
    last_recv(s),
    last_open(0),
    suppress(false), stale(false),
    session_caps_item(this) { 
  }
  
  capseq_t get_last_open() { return last_open; }
  void set_last_open() { last_open = last_sent; }

  bool is_suppress() { return suppress; }
  void set_suppress(bool b) { suppress = b; }

  bool is_stale() { return stale; }
  void set_stale(bool b) { stale = b; }

  CInode *get_inode() { return inode; }
  void set_inode(CInode *i) { inode = i; }
  void add_to_cap_list(xlist<Capability*>& ls) {
    ls.push_back(&session_caps_item);
  }

  bool is_null() { return cap_history.empty() && wanted_caps == 0; }

  // most recently issued caps.
  int pending() { 
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
    for (capseq_t seq = last_recv; seq <= last_sent; seq++) {
      if (cap_history.count(seq)) {
        c |= cap_history[seq];
        generic_dout(10) << " cap issued: seq " << seq << " " << cap_string(cap_history[seq]) << " -> " << cap_string(c) << dendl;
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
    return from & (CEPH_CAP_WR|CEPH_CAP_RD);
  }
  int needed() { return needed(wanted_caps); }

  // conflicts
  static int conflicts(int from) {
    int c = 0;
    if (from & CEPH_CAP_WRBUFFER) c |= CEPH_CAP_RDCACHE|CEPH_CAP_RD;
    if (from & CEPH_CAP_WR) c |= CEPH_CAP_RDCACHE;
    if (from & CEPH_CAP_RD) c |= CEPH_CAP_WRBUFFER;
    if (from & CEPH_CAP_RDCACHE) c |= CEPH_CAP_WRBUFFER|CEPH_CAP_WR;
    return c;
  }
  int wanted_conflicts() { return conflicts(wanted()); }
  int needed_conflicts() { return conflicts(needed()); }
  int issued_conflicts() { return conflicts(issued()); }

  // issue caps; return seq number.
  capseq_t issue(int c) {
    ++last_sent;
    cap_history[last_sent] = c;
    return last_sent;
  }
  capseq_t get_last_seq() { return last_sent; }

  Export make_export() {
    return Export(wanted_caps, issued(), pending());
  }
  void merge(Export& other) {
    // issued + pending
    int newpending = other.pending | pending();
    if (other.issued & ~newpending)
      issue(other.issued | newpending);
    issue(newpending);

    // wanted
    wanted_caps = wanted_caps | other.wanted;
  }
  void merge(int otherwanted, int otherissued) {
    // issued + pending
    int newpending = pending();
    if (otherissued & ~newpending)
      issue(otherissued | newpending);
    issue(newpending);

    // wanted
    wanted_caps = wanted_caps | otherwanted;
  }

  // confirm receipt of a previous sent/issued seq.
  int confirm_receipt(capseq_t seq, int caps) {
    int r = 0;

    // old seqs
    while (last_recv < seq) {
      generic_dout(10) << " cap.confirm_receipt forgetting seq " << last_recv << " " << cap_string(cap_history[last_recv]) << dendl;
      r |= cap_history[last_recv];
      cap_history.erase(last_recv);
      ++last_recv;
    }
    
    // release current?
    if (cap_history.count(seq) &&
        cap_history[seq] != caps) {
      generic_dout(10) << " cap.confirm_receipt revising seq " << seq << " " << cap_string(cap_history[seq]) << " -> " << cap_string(caps) << dendl;
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

  void revoke() {
    if (pending())
      issue(0);
    confirm_receipt(last_sent, 0);
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
