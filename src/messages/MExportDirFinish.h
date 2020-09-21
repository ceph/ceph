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

#ifndef CEPH_MEXPORTDIRFINISH_H
#define CEPH_MEXPORTDIRFINISH_H

#include "messages/MMDSOp.h"

class MExportDirFinish : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t dirfrag;
  bool last;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  bool is_last() const { return last; }
  
protected:
  MExportDirFinish() :
    MMDSOp{MSG_MDS_EXPORTDIRFINISH, HEAD_VERSION, COMPAT_VERSION}, last(false) {}
  MExportDirFinish(dirfrag_t df, bool l, uint64_t tid) :
    MMDSOp{MSG_MDS_EXPORTDIRFINISH, HEAD_VERSION, COMPAT_VERSION}, dirfrag(df), last(l) {
    set_tid(tid);
  }
  ~MExportDirFinish() override {}

public:
  std::string_view get_type_name() const override { return "ExFin"; }
  void print(std::ostream& o) const override {
    o << "export_finish(" << dirfrag << (last ? " last" : "") << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(last, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(last, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
