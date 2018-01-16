// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMONMETADATA_H
#define CEPH_MMONMETADATA_H

#include "mon/mon_types.h"
#include "msg/Message.h"

class MMonMetadata : public Message {
public:
  Metadata data;

private:
  static const int HEAD_VERSION = 1;
  ~MMonMetadata() override {}

public:
  MMonMetadata() :
    Message(CEPH_MSG_MON_METADATA)
  {}
  MMonMetadata(const Metadata& metadata) :
    Message(CEPH_MSG_MON_METADATA, HEAD_VERSION),
    data(metadata)
  {}

  const char *get_type_name() const override {
    return "mon_metadata";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(data, payload);
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    decode(data, p);
  }
};

#endif
