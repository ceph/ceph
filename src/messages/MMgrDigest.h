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


#ifndef CEPH_MMGRDIGEST_H
#define CEPH_MMGRDIGEST_H

#include "msg/Message.h"

/**
 * The mgr digest is a way for the mgr to subscribe to things
 * other than the cluster maps, which are needed by 
 */
class MMgrDigest : public MessageInstance<MMgrDigest> {
public:
  friend factory;

  bufferlist mon_status_json;
  bufferlist health_json;

  MMgrDigest() : 
    MessageInstance(MSG_MGR_DIGEST) {}

  std::string_view get_type_name() const override { return "mgrdigest"; }
  void print(ostream& out) const override {
    out << get_type_name();
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(mon_status_json, p);
    decode(health_json, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(mon_status_json, payload);
    encode(health_json, payload);
  }

private:
  ~MMgrDigest() override {}

};

#endif
