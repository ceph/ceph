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


#ifndef __MLOCK_H
#define __MLOCK_H

#include "msg/Message.h"

#define LOCK_OTYPE_IHARD  1
#define LOCK_OTYPE_IFILE  2
#define LOCK_OTYPE_DIR    3
#define LOCK_OTYPE_DN     4

// for replicas
#define LOCK_AC_SYNC          0
#define LOCK_AC_MIXED         1
#define LOCK_AC_LOCK          2

#define LOCK_AC_REQXLOCKACK   9  // req dentry xlock
#define LOCK_AC_REQXLOCKNAK  10  // req dentry xlock
#define LOCK_AC_LOCKNAK      12  // for dentry xlock


#define LOCK_AC_FOR_REPLICA(a)  ((a) <= 10)
#define LOCK_AC_FOR_AUTH(a)     ((a) >= 11)

// for auth

#define LOCK_AC_SYNCACK      13
#define LOCK_AC_MIXEDACK     14
#define LOCK_AC_LOCKACK      15


#define LOCK_AC_REQREAD      19
#define LOCK_AC_REQWRITE     20

#define LOCK_AC_REQXLOCK     21
#define LOCK_AC_REQXLOCKC    22 // create if necessary
#define LOCK_AC_UNXLOCK      23

#define lock_ac_name(x)      


class MLock : public Message {
  int       asker;  // who is initiating this request
  int       action;  // action type

  char      otype;  // lock object type
  inodeno_t ino;    // ino ref, or possibly
  string    dn;     // dentry name
  bufferlist data;   // and possibly some data
  string    path;   // possibly a path too (for dentry lock discovers)

 public:
  inodeno_t get_ino() { return ino; }
  string& get_dn() { return dn; }
  bufferlist& get_data() { return data; }
  int get_asker() { return asker; }
  int get_action() { return action; }
  int get_otype() { return otype; }
  string& get_path() { return path; }

  MLock() {}
  MLock(int action, int asker) :
    Message(MSG_MDS_LOCK) {
    this->action = action;
    this->asker = asker;
  }
  virtual char *get_type_name() { return "ILock"; }
  
  void set_ino(inodeno_t ino, char ot) {
    otype = ot;
    this->ino = ino;
  }
  void set_dirino(inodeno_t dirino) {
    otype = LOCK_OTYPE_DIR;
    this->ino = ino;
  }
  void set_dn(inodeno_t dirino, string& dn) {
    otype = LOCK_OTYPE_DN;
    this->ino = dirino;
    this->dn = dn;
  }
  void set_data(bufferlist& data) {
    this->data.claim( data );
  }
  void set_path(const string& p) {
    path = p;
  }
  
  void decode_payload() {
    int off = 0;
    payload.copy(off,sizeof(action), (char*)&action);
    off += sizeof(action);
    payload.copy(off,sizeof(asker), (char*)&asker);
    off += sizeof(asker);
    payload.copy(off,sizeof(otype), (char*)&otype);
    off += sizeof(otype);
    payload.copy(off,sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    ::_decode(dn, payload, off);
    ::_decode(path, payload, off);
    ::_decode(data, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&action, sizeof(action));
    payload.append((char*)&asker, sizeof(asker));
    payload.append((char*)&otype, sizeof(otype));
    payload.append((char*)&ino, sizeof(inodeno_t));
    ::_encode(dn, payload);
    ::_encode(path, payload);
    ::_encode(data, payload);
  }

};

#endif
