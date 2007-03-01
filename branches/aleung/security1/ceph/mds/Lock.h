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


#ifndef __LOCK_H
#define __LOCK_H

#include <assert.h>
#include <set>
using namespace std;

#include "include/buffer.h"

#include "Capability.h"

// states and such.
//  C = cache reads, R = read, W = write, A = append, B = buffer writes, L = lazyio

// basic lock                    -----auth--------   ---replica-------
#define LOCK_SYNC     0  // AR   R . / C R . . . L   R . / C R . . . L   stat()
#define LOCK_LOCK     1  // AR   R W / C . . . . .   . . / C . . . . .   truncate()
#define LOCK_GLOCKR   2  // AR   R . / C . . . . .   . . / C . . . . .

// file lock states
#define LOCK_GLOCKL   3  // A    . . / . . . . . .                       loner -> lock
#define LOCK_GLOCKM   4  // A    . . / . . . . . .
#define LOCK_MIXED    5  // AR   . . / . R W A . L   . . / . R . . . L
#define LOCK_GMIXEDR  6  // AR   R . / . R . . . L   . . / . R . . . L 
#define LOCK_GMIXEDL  7  // A    . . / . . . . . L                       loner -> mixed

#define LOCK_LONER    8  // A    . . / C R W A B L        (lock)      
#define LOCK_GLONERR  9  // A    . . / . R . . . L
#define LOCK_GLONERM  10 // A    . . / . R W A . L

#define LOCK_GSYNCL   11 // A    . . / C ? . . . L                       loner -> sync    (*) FIXME: let old loner keep R, somehow...
#define LOCK_GSYNCM   12 // A    . . / . R . . . L

//   4 stable
//  +9 transition
//  13 total

/* no append scenarios:

loner + truncate():
  - loner needs to lose A (?unless it's the loner doing the truncate?)
loner + statlite(size):
  - loner needs to lose A

any + statlite(size)
  - all lose A

any + statlite(mtime)
  - all lose W

-> we need to add lonerfixed and mixedfixed states (and associated transitions)
 in order to efficiently support statlite(size) and truncate().  until then,
 we have to LOCK.

 */

// -- lock... hard or file

class Message;

class CLock {
 protected:
  // lock state
  char     state;
  set<int> gather_set;  // auth

  // local state
  int      nread;
  Message *wrlock_by;

  
 public:
  CLock() : 
    state(LOCK_SYNC), 
    nread(0), 
    wrlock_by(0) {
  }
  
  // encode/decode
  void encode_state(bufferlist& bl) {
    bl.append((char*)&state, sizeof(state));
    _encode(gather_set, bl);

    //bl.append((char*)&nread, sizeof(nread));
    //bl.append((char*)&nwrite, sizeof(nwrite));
  }
  void decode_state(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(state), (char*)&state);
    off += sizeof(state);
    _decode(gather_set, bl, off);

    //bl.copy(off, sizeof(nread), (char*)&nread);
    //off += sizeof(nread);
    //bl.copy(off, sizeof(nwrite), (char*)&nwrite);
    //off += sizeof(nwrite);
  }

  char get_state() { return state; }
  char set_state(char s) { 
    state = s; 
    assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
    return s;
  };

  char get_replica_state() {
    switch (state) {
    case LOCK_LOCK:
    case LOCK_GLOCKM:
    case LOCK_GLOCKL:
    case LOCK_GLOCKR: 
    case LOCK_LONER:
    case LOCK_GLONERR:
    case LOCK_GLONERM:
      return LOCK_LOCK;
    case LOCK_MIXED:
    case LOCK_GMIXEDR:
      return LOCK_MIXED;
    case LOCK_SYNC:
      return LOCK_SYNC;

      // after gather auth will bc LOCK_AC_MIXED or whatever
    case LOCK_GSYNCM:
      return LOCK_MIXED;
    case LOCK_GSYNCL:
    case LOCK_GMIXEDL:     // ** LOCK isn't exact right state, but works.
      return LOCK_LOCK;

    default: 
      assert(0);
    }
    return 0;
  }

  // gather set
  set<int>& get_gather_set() { return gather_set; }
  void init_gather(const map<int,int>& i) {
    for (map<int,int>::const_iterator p = i.begin(); p != i.end(); ++p)
      gather_set.insert(p->first);
  }
  bool is_gathering(int i) {
    return gather_set.count(i);
  }
  void clear_gather() {
    gather_set.clear();
  }

  // ref counting
  int get_read() { return ++nread; }
  int put_read() {
    assert(nread>0);
    return --nread;
  }
  int get_nread() { return nread; }

  void get_write(Message *who) { 
    assert(wrlock_by == 0);
    wrlock_by = who; 
  }
  void put_write() {
    assert(wrlock_by);
    wrlock_by = 0;
  }
  bool is_wrlocked() { return wrlock_by ? true:false; }
  Message *get_wrlocked_by() { return wrlock_by; }
  bool is_used() {
    return (is_wrlocked() || (nread>0)) ? true:false;
  }
  
  
  // stable
  bool is_stable() {
    return (state == LOCK_SYNC) || 
      (state == LOCK_LOCK) || 
      (state == LOCK_MIXED) || 
      (state == LOCK_LONER);
  }

  // read/write access
  bool can_read(bool auth) {
    if (auth)
      return (state == LOCK_SYNC) || (state == LOCK_GMIXEDR) 
        || (state == LOCK_GLOCKR) || (state == LOCK_LOCK);
    else
      return (state == LOCK_SYNC);
  }
  bool can_read_soon(bool auth) {
    if (auth)
      return (state == LOCK_GLOCKL);
    else
      return false;
  }

  bool can_write(bool auth) {
    if (auth) 
      return (state == LOCK_LOCK) && !is_wrlocked();
    else
      return false;
  }
  bool can_write_soon(bool auth) {
    if (auth)
      return (state == LOCK_GLOCKR) || (state == LOCK_GLOCKL)
        || (state == LOCK_GLOCKM);
    else
      return false;
  }

  // client caps allowed
  int caps_allowed_ever(bool auth) {
    if (auth)
      return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_WRBUFFER | CAP_FILE_LAZYIO;
    else
      return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_LAZYIO;
  }
  int caps_allowed(bool auth) {
    if (auth)
      switch (state) {
      case LOCK_SYNC:
        return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_LAZYIO;
      case LOCK_LOCK:
      case LOCK_GLOCKR:
        return CAP_FILE_RDCACHE;

      case LOCK_GLOCKL:
      case LOCK_GLOCKM:
        return 0;

      case LOCK_MIXED:
        return CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_LAZYIO;
      case LOCK_GMIXEDR:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      case LOCK_GMIXEDL:
        return 0;

      case LOCK_LONER:  // single client writer, of course.
        return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_WRBUFFER | CAP_FILE_LAZYIO;
      case LOCK_GLONERR:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      case LOCK_GLONERM:
        return CAP_FILE_RD | CAP_FILE_WR | CAP_FILE_WREXTEND | CAP_FILE_LAZYIO;

      case LOCK_GSYNCL:
        return CAP_FILE_RDCACHE | CAP_FILE_LAZYIO;
      case LOCK_GSYNCM:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      }
    else
      switch (state) {
      case LOCK_SYNC:
        return CAP_FILE_RDCACHE | CAP_FILE_RD | CAP_FILE_LAZYIO;
      case LOCK_LOCK:
      case LOCK_GLOCKR:
        return CAP_FILE_RDCACHE;
      case LOCK_GMIXEDR:
      case LOCK_MIXED:
        return CAP_FILE_RD | CAP_FILE_LAZYIO;
      }
    assert(0);
    return 0;
  }

  friend class MDCache;
  friend class Locker;
  friend class Migrator;
};

//ostream& operator<<(ostream& out, CLock& l);
inline ostream& operator<<(ostream& out, CLock& l) 
{
  static char* __lock_states[] = {
    "sync",
    "lock",
    "glockr",
    "glockl",
    "glockm",
    "mixed",
    "gmixedr",
    "gmixedl",
    "loner",
    "glonerr",
    "glonerm",
    "gsyncl",
    "gsyncm"
  }; 

  out << "(" << __lock_states[(int)l.get_state()];

  if (!l.get_gather_set().empty()) out << " g=" << l.get_gather_set();

  if (l.get_nread()) 
    out << " r=" << l.get_nread();
  if (l.is_wrlocked())
    out << " w=" << l.get_wrlocked_by();

  // rw?
  /*
  out << " ";
  if (l.can_read(true)) out << "r[" << l.get_nread() << "]";
  if (l.can_write(true)) out << "w[" << l.get_nwrite() << "]";
  out << "/";
  if (l.can_read(false)) out << "r[" << l.get_nread() << "]";
  if (l.can_write(false)) out << "w[" << l.get_nwrite() << "]";  
  */
  out << ")";
  return out;
}

#endif
