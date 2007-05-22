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


// for replicas
#define LOCK_AC_SYNC        -1
#define LOCK_AC_MIXED       -2
#define LOCK_AC_LOCK        -3

#define LOCK_AC_REQXLOCKACK -4  // req dentry xlock
#define LOCK_AC_REQXLOCKNAK -5  // req dentry xlock

#define LOCK_AC_SCATTER     -6

// for auth
#define LOCK_AC_SYNCACK      1
#define LOCK_AC_MIXEDACK     2
#define LOCK_AC_LOCKACK      3

#define LOCK_AC_REQREAD      4
#define LOCK_AC_REQWRITE     5

#define LOCK_AC_REQXLOCK     6
#define LOCK_AC_UNXLOCK      7
#define LOCK_AC_FINISH       8


#define LOCK_AC_FOR_REPLICA(a)  ((a) < 0)
#define LOCK_AC_FOR_AUTH(a)     ((a) > 0)


class MLock : public Message {
  int       asker;  // who is initiating this request
  int       action;  // action type

  char      otype;  // lock object type
  inodeno_t ino;    // ino ref, or possibly
  dirfrag_t dirfrag;
  string    dn;     // dentry name
  
  metareqid_t reqid;  // for remote lock requests
  
  bufferlist data;  // and possibly some data

 public:
  inodeno_t get_ino() { return ino; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  string& get_dn() { return dn; }
  bufferlist& get_data() { return data; }
  int get_asker() { return asker; }
  int get_action() { return action; }
  int get_otype() { return otype; }
  metareqid_t get_reqid() { return reqid; }

  MLock() {}
  MLock(int action, int asker) :
    Message(MSG_MDS_LOCK) {
    this->action = action;
    this->asker = asker;
  }
  MLock(SimpleLock *lock, int action, int asker) :
    Message(MSG_MDS_LOCK) {
    this->otype = lock->get_type();
    lock->get_parent()->set_mlock_info(this);
    this->action = action;
    this->asker = asker;
  }
  MLock(SimpleLock *lock, int action, int asker, bufferlist& bl) :
    Message(MSG_MDS_LOCK) {
    this->otype = lock->get_type();
    lock->get_parent()->set_mlock_info(this);
    this->action = action;
    this->asker = asker;
    data.claim(bl);
  }
  virtual char *get_type_name() { return "ILock"; }
  
  void set_ino(inodeno_t ino, char ot) {
    otype = ot;
    this->ino = ino;
  }
  void set_ino(inodeno_t ino) {
    this->ino = ino;
  }
  /*
  void set_dirfrag(dirfrag_t df) {
    otype = LOCK_OTYPE_DIR;
    this->dirfrag = df;
  }
  */
  void set_dn(dirfrag_t df, const string& dn) {
    otype = LOCK_OTYPE_DN;
    this->dirfrag = df;
    this->dn = dn;
  }
  void set_reqid(metareqid_t ri) { reqid = ri; }
  void set_data(bufferlist& data) {
    this->data.claim( data );
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
    payload.copy(off,sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    ::_decode(reqid, payload, off);
    ::_decode(dn, payload, off);
    ::_decode(data, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&action, sizeof(action));
    payload.append((char*)&asker, sizeof(asker));
    payload.append((char*)&otype, sizeof(otype));
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&dirfrag, sizeof(dirfrag));
    ::_encode(reqid, payload);
    ::_encode(dn, payload);
    ::_encode(data, payload);
  }

};

#endif
