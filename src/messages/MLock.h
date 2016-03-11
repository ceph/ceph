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


#ifndef CEPH_MLOCK_H
#define CEPH_MLOCK_H

#include "msg/Message.h"
#include "mds/locks.h"
#include "mds/SimpleLock.h"

class MLock : public Message {
  int32_t     action;  // action type
  int32_t     asker;  // who is initiating this request
  metareqid_t reqid;  // for remote lock requests
  
  __u16      lock_type;  // lock object type
  MDSCacheObjectInfo object_info;  
  
  bufferlist lockdata;  // and possibly some data
  
public:
  bufferlist& get_data() { return lockdata; }
  int get_asker() { return asker; }
  int get_action() { return action; }
  metareqid_t get_reqid() { return reqid; }
  
  int get_lock_type() { return lock_type; }
  MDSCacheObjectInfo &get_object_info() { return object_info; }
  
  MLock() : Message(MSG_MDS_LOCK) {}
  MLock(int ac, int as) :
    Message(MSG_MDS_LOCK),
    action(ac), asker(as),
    lock_type(0) { }
  MLock(SimpleLock *lock, int ac, int as) :
    Message(MSG_MDS_LOCK),
    action(ac), asker(as),
    lock_type(lock->get_type()) {
    lock->get_parent()->set_object_info(object_info);
  }
  MLock(SimpleLock *lock, int ac, int as, bufferlist& bl) :
    Message(MSG_MDS_LOCK),
    action(ac), asker(as), lock_type(lock->get_type()) {
    lock->get_parent()->set_object_info(object_info);
    lockdata.claim(bl);
  }
private:
  ~MLock() {}
  
public:
  const char *get_type_name() const { return "ILock"; }
  void print(ostream& out) const {
    out << "lock(a=" << get_lock_action_name(action)
	<< " " << get_lock_type_name(lock_type)
	<< " " << object_info
	<< ")";
  }
  
  void set_reqid(metareqid_t ri) { reqid = ri; }
  void set_data(const bufferlist& lockdata) {
    this->lockdata = lockdata;
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(asker, p);
    ::decode(action, p);
    ::decode(reqid, p);
    ::decode(lock_type, p);
    ::decode(object_info, p);
    ::decode(lockdata, p);
  }
  virtual void encode_payload(uint64_t features) {
    ::encode(asker, payload);
    ::encode(action, payload);
    ::encode(reqid, payload);
    ::encode(lock_type, payload);
    ::encode(object_info, payload);
    ::encode(lockdata, payload);
  }

};
REGISTER_MESSAGE(MLock, MSG_MDS_LOCK);
#endif
