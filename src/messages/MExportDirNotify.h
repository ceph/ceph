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

#ifndef CEPH_MEXPORTDIRNOTIFY_H
#define CEPH_MEXPORTDIRNOTIFY_H

#include "messages/MMDSOp.h"

class MExportDirNotify : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t base;
  bool ack;
  std::pair<__s32,__s32> old_auth, new_auth;
  std::list<dirfrag_t> bounds;  // bounds; these dirs are _not_ included (tho the dirfragdes are)

 public:
  dirfrag_t get_dirfrag() const { return base; }
  std::pair<__s32,__s32> get_old_auth() const { return old_auth; }
  std::pair<__s32,__s32> get_new_auth() const { return new_auth; }
  bool wants_ack() const { return ack; }
  const std::list<dirfrag_t>& get_bounds() const { return bounds; }
  std::list<dirfrag_t>& get_bounds() { return bounds; }

protected:
  MExportDirNotify() :
    MMDSOp{MSG_MDS_EXPORTDIRNOTIFY, HEAD_VERSION, COMPAT_VERSION} {}
  MExportDirNotify(dirfrag_t i, uint64_t tid, bool a, std::pair<__s32,__s32> oa,
		   std::pair<__s32,__s32> na) :
    MMDSOp{MSG_MDS_EXPORTDIRNOTIFY, HEAD_VERSION, COMPAT_VERSION},
    base(i), ack(a), old_auth(oa), new_auth(na) {
    set_tid(tid);
  }
  ~MExportDirNotify() override {}

public:
  std::string_view get_type_name() const override { return "ExNot"; }
  void print(std::ostream& o) const override {
    o << "export_notify(" << base;
    o << " " << old_auth << " -> " << new_auth;
    if (ack) 
      o << " ack)";
    else
      o << " no ack)";
  }
  
  void copy_bounds(std::list<dirfrag_t>& ex) {
    this->bounds = ex;
  }
  void copy_bounds(std::set<dirfrag_t>& ex) {
    for (auto i = ex.begin(); i != ex.end(); ++i)
      bounds.push_back(*i);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(base, payload);
    encode(ack, payload);
    encode(old_auth, payload);
    encode(new_auth, payload);
    encode(bounds, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(base, p);
    decode(ack, p);
    decode(old_auth, p);
    decode(new_auth, p);
    decode(bounds, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
