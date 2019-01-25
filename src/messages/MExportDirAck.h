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
#include "msg/Message.h"

class MExportDirAck : public MessageInstance<MExportDirAck> {
public:
  friend factory;

  dirfrag_t dirfrag;
  bufferlist imported_caps;

  dirfrag_t get_dirfrag() const { return dirfrag; }
  
protected:
  MExportDirAck() : MessageInstance(MSG_MDS_EXPORTDIRACK) {}
  MExportDirAck(dirfrag_t df, uint64_t tid) :
    MessageInstance(MSG_MDS_EXPORTDIRACK), dirfrag(df) {
    set_tid(tid);
  }
  ~MExportDirAck() override {}

public:
  std::string_view get_type_name() const override { return "ExAck"; }
    void print(ostream& o) const override {
    o << "export_ack(" << dirfrag << ")";
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(imported_caps, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(imported_caps, payload);
  }

};

#endif
