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

#ifndef CEPH_MEXPORTDIRACK_H
#define CEPH_MEXPORTDIRACK_H

#include "MExportDir.h"
#include "messages/MMDSOp.h"

class MExportDirAck final : public MMDSOp {
public:
  dirfrag_t dirfrag;
  ceph::buffer::list imported_caps;

  dirfrag_t get_dirfrag() const { return dirfrag; }
  
protected:
  MExportDirAck() : MMDSOp{MSG_MDS_EXPORTDIRACK} {}
  MExportDirAck(dirfrag_t df, uint64_t tid) :
    MMDSOp{MSG_MDS_EXPORTDIRACK}, dirfrag(df) {
    set_tid(tid);
  }
  ~MExportDirAck() final {}

public:
  std::string_view get_type_name() const override { return "ExAck"; }
  void print(std::ostream& o) const override {
    o << "export_ack(" << dirfrag << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(imported_caps, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(imported_caps, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
