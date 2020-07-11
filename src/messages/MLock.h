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

#include "mds/locks.h"
#include "mds/SimpleLock.h"
#include "messages/MMDSOp.h"

class MLock : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  int32_t     action = 0;  // action type
  mds_rank_t  asker = 0;  // who is initiating this request
  metareqid_t reqid;  // for remote lock requests
  
  __u16      lock_type = 0;  // lock object type
  MDSCacheObjectInfo object_info;  
  
  ceph::buffer::list lockdata;  // and possibly some data
  
public:
  ceph::buffer::list& get_data() { return lockdata; }
  const ceph::buffer::list& get_data() const { return lockdata; }
  int get_asker() const { return asker; }
  int get_action() const { return action; }
  metareqid_t get_reqid() const { return reqid; }
  
  int get_lock_type() const { return lock_type; }
  const MDSCacheObjectInfo &get_object_info() const { return object_info; }
  MDSCacheObjectInfo &get_object_info() { return object_info; }

protected:
  MLock() : MMDSOp{MSG_MDS_LOCK, HEAD_VERSION, COMPAT_VERSION} {}
  MLock(int ac, mds_rank_t as) :
    MMDSOp{MSG_MDS_LOCK, HEAD_VERSION, COMPAT_VERSION},
    action(ac), asker(as),
    lock_type(0) { }
  MLock(SimpleLock *lock, int ac, mds_rank_t as) :
    MMDSOp{MSG_MDS_LOCK, HEAD_VERSION, COMPAT_VERSION},
    action(ac), asker(as),
    lock_type(lock->get_type()) {
    lock->get_parent()->set_object_info(object_info);
  }
  MLock(SimpleLock *lock, int ac, mds_rank_t as, ceph::buffer::list& bl) :
    MMDSOp{MSG_MDS_LOCK, HEAD_VERSION, COMPAT_VERSION},
    action(ac), asker(as), lock_type(lock->get_type()) {
    lock->get_parent()->set_object_info(object_info);
    lockdata = std::move(bl);
  }
  ~MLock() override {}
  
public:
  std::string_view get_type_name() const override { return "ILock"; }
  void print(std::ostream& out) const override {
    out << "lock(a=" << SimpleLock::get_lock_action_name(action)
	<< " " << SimpleLock::get_lock_type_name(lock_type)
	<< " " << object_info
	<< ")";
  }
  
  void set_reqid(metareqid_t ri) { reqid = ri; }
  void set_data(const ceph::buffer::list& lockdata) {
    this->lockdata = lockdata;
  }
  
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(asker, p);
    decode(action, p);
    decode(reqid, p);
    decode(lock_type, p);
    decode(object_info, p);
    decode(lockdata, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(asker, payload);
    encode(action, payload);
    encode(reqid, payload);
    encode(lock_type, payload);
    encode(object_info, payload);
    encode(lockdata, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
