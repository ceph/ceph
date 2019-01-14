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


#ifndef CEPH_MEXPORTCAPSACK_H
#define CEPH_MEXPORTCAPSACK_H

#include "msg/Message.h"


class MExportCapsAck : public MessageInstance<MExportCapsAck> {
public:  
  friend factory;

  inodeno_t ino;
  bufferlist cap_bl;

protected:
  MExportCapsAck() :
    MessageInstance(MSG_MDS_EXPORTCAPSACK) {}
  MExportCapsAck(inodeno_t i) :
    MessageInstance(MSG_MDS_EXPORTCAPSACK), ino(i) {}
  ~MExportCapsAck() override {}

public:
  std::string_view get_type_name() const override { return "export_caps_ack"; }
  void print(ostream& o) const override {
    o << "export_caps_ack(" << ino << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(cap_bl, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(ino, p);
    decode(cap_bl, p);
  }
};

#endif
