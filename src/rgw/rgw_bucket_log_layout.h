// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <optional>
#include <string>
#include "include/encoding.h"

namespace rgw {

enum class BucketLogType : uint8_t {
  InIndex,
  FIFO,
};

enum class BucketLogHashType : uint8_t {
  Mod, // rjenkins hash of object name, modulo num_shards
};

inline std::ostream& operator<<(std::ostream& out, const BucketLogType &log_type)
{
  switch (log_type) {
    case BucketLogType::InIndex:
      return out << "InIndex";
    case BucketLogType::FIFO:
      return out << "FIFO";
    default:
      return out << "Unknown";
  }
}

struct bucket_log_index_layout {
  uint32_t num_shards = 1;

  BucketLogHashType log_hash_type = BucketLogHashType::Mod;
};

void encode(const bucket_log_index_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_index_layout& l, bufferlist::const_iterator& bl);


struct bucket_log_layout {
  BucketLogType type = BucketLogType::InIndex;

  bucket_log_index_layout index_log;
};

void encode(const bucket_log_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout& l, bufferlist::const_iterator& bl);


struct bucket_log_layout_generation {
  uint64_t log_gen = 0;
  bucket_log_layout log_layout;

};

void encode(const bucket_log_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_log_layout_generation& l, bufferlist::const_iterator& bl);

} // namespace rgw
