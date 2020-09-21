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

#ifndef CEPH_MEXPORTDIRPREPACK_H
#define CEPH_MEXPORTDIRPREPACK_H

#include "include/types.h"
#include "messages/MMDSOp.h"

class MExportDirPrepAck : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t dirfrag;
  bool success = false;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }

protected:
  MExportDirPrepAck() :
    MMDSOp{MSG_MDS_EXPORTDIRPREPACK, HEAD_VERSION, COMPAT_VERSION} {}
  MExportDirPrepAck(dirfrag_t df, bool s, uint64_t tid) :
    MMDSOp{MSG_MDS_EXPORTDIRPREPACK, HEAD_VERSION, COMPAT_VERSION}, dirfrag(df), success(s) {
    set_tid(tid);
  }
  ~MExportDirPrepAck() override {}

public:  
  bool is_success() const { return success; }
  std::string_view get_type_name() const override { return "ExPAck"; }
  void print(std::ostream& o) const override {
    o << "export_prep_ack(" << dirfrag << (success ? " success)" : " fail)");
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(success, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(success, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
