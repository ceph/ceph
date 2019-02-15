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

class MMonMetadata : public MessageInstance<MMonMetadata> {
public:
  friend factory;

  Metadata data;

private:
  static constexpr int HEAD_VERSION = 1;
  ~MMonMetadata() override {}

public:
  MMonMetadata() :
    MessageInstance(CEPH_MSG_MON_METADATA)
  {}
  MMonMetadata(const Metadata& metadata) :
    MessageInstance(CEPH_MSG_MON_METADATA, HEAD_VERSION),
    data(metadata)
  {}

  std::string_view get_type_name() const override {
    return "mon_metadata";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(data, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(data, p);
  }
};

#endif
