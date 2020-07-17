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

#ifndef CEPH_MMDSFRAGMENTNOTIFY_H
#define CEPH_MMDSFRAGMENTNOTIFY_H

#include "messages/MMDSOp.h"

class MMDSFragmentNotify : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t base_dirfrag;
  int8_t bits = 0;
  bool ack_wanted = false;

 public:
  inodeno_t get_ino() const { return base_dirfrag.ino; }
  frag_t get_basefrag() const { return base_dirfrag.frag; }
  dirfrag_t get_base_dirfrag() const { return base_dirfrag; }
  int get_bits() const { return bits; }
  bool is_ack_wanted() const { return ack_wanted; }
  void mark_ack_wanted() { ack_wanted = true; }

  ceph::buffer::list basebl;

protected:
  MMDSFragmentNotify() :
    MMDSOp{MSG_MDS_FRAGMENTNOTIFY, HEAD_VERSION, COMPAT_VERSION} {}
  MMDSFragmentNotify(dirfrag_t df, int b, uint64_t tid) :
    MMDSOp{MSG_MDS_FRAGMENTNOTIFY, HEAD_VERSION, COMPAT_VERSION},
    base_dirfrag(df), bits(b) {
    set_tid(tid);
  }
  ~MMDSFragmentNotify() override {}

public: 
  std::string_view get_type_name() const override { return "fragment_notify"; }
  void print(std::ostream& o) const override {
    o << "fragment_notify(" << base_dirfrag << " " << (int)bits << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(base_dirfrag, payload);
    encode(bits, payload);
    encode(basebl, payload);
    encode(ack_wanted, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(base_dirfrag, p);
    decode(bits, p);
    decode(basebl, p);
    if (header.version >= 2)
      decode(ack_wanted, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);  
};

#endif
