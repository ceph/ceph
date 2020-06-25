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

#ifndef CEPH_MEXPORTDIRNOTIFYACK_H
#define CEPH_MEXPORTDIRNOTIFYACK_H

#include "messages/MMDSOp.h"

class MExportDirNotifyAck : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t dirfrag;
  std::pair<__s32,__s32> new_auth;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  std::pair<__s32,__s32> get_new_auth() const { return new_auth; }
  
protected:
  MExportDirNotifyAck() :
    MMDSOp{MSG_MDS_EXPORTDIRNOTIFYACK, HEAD_VERSION, COMPAT_VERSION} {}
  MExportDirNotifyAck(dirfrag_t df, uint64_t tid, std::pair<__s32,__s32> na) :
    MMDSOp{MSG_MDS_EXPORTDIRNOTIFYACK, HEAD_VERSION, COMPAT_VERSION}, dirfrag(df), new_auth(na) {
    set_tid(tid);
  }
  ~MExportDirNotifyAck() override {}

public:
  std::string_view get_type_name() const override { return "ExNotA"; }
  void print(std::ostream& o) const override {
    o << "export_notify_ack(" << dirfrag << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(new_auth, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(new_auth, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);  
};

#endif
