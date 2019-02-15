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

#ifndef CEPH_MEXPORTDIRCANCEL_H
#define CEPH_MEXPORTDIRCANCEL_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirCancel : public MessageInstance<MExportDirCancel> {
public:
  friend factory;
private:
  dirfrag_t dirfrag;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }

protected:
  MExportDirCancel() : MessageInstance(MSG_MDS_EXPORTDIRCANCEL) {}
  MExportDirCancel(dirfrag_t df, uint64_t tid) :
    MessageInstance(MSG_MDS_EXPORTDIRCANCEL), dirfrag(df) {
    set_tid(tid);
  }
  ~MExportDirCancel() override {}

public:
  std::string_view get_type_name() const override { return "ExCancel"; }
  void print(ostream& o) const override {
    o << "export_cancel(" << dirfrag << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(dirfrag, p);
  }
};

#endif
