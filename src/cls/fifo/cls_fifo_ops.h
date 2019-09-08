// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 * Copyright (C) 2019 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "include/types.h"
#include "include/utime.h"
#include "cls/fifo/cls_fifo_types.h"

struct cls_fifo_create_op
{
  string id;
  struct {
    string name;
    string ns;
  } pool;
  std::optional<string> oid_prefix;

  uint64_t max_obj_size{0};
  uint64_t max_entry_size{0};

  bool exclusive{false};

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(pool.name, bl);
    encode(pool.ns, bl);
    encode(oid_prefix, bl);
    encode(max_obj_size, bl);
    encode(max_entry_size, bl);
    encode(exclusive, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(pool.name, bl);
    decode(pool.ns, bl);
    decode(oid_prefix, bl);
    decode(max_obj_size, bl);
    decode(max_entry_size, bl);
    decode(exclusive, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_fifo_create_op)

