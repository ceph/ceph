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

enum class BucketIndexType : uint8_t {
  Normal, // normal hash-based sharded index layout
  Indexless, // no bucket index, so listing is unsupported
};

enum class BucketHashType : uint8_t {
  Mod, // rjenkins hash of object name, modulo num_shards
};

inline std::ostream& operator<<(std::ostream& out, const BucketIndexType &index_type)
{
  switch (index_type) {
    case BucketIndexType::Normal:
      return out << "Normal";
    case BucketIndexType::Indexless:
      return out << "Indexless";
    default:
      return out << "Unknown";
  }
}

struct bucket_index_normal_layout {
  uint32_t num_shards = 1;

  BucketHashType hash_type = BucketHashType::Mod;
};

void encode(const bucket_index_normal_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_normal_layout& l, bufferlist::const_iterator& bl);


struct bucket_index_layout {
  BucketIndexType type = BucketIndexType::Normal;

  // TODO: variant of layout types?
  bucket_index_normal_layout normal;
};

void encode(const bucket_index_layout& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout& l, bufferlist::const_iterator& bl);


struct bucket_index_layout_generation {
  uint64_t gen = 0;
  bucket_index_layout layout;
};

void encode(const bucket_index_layout_generation& l, bufferlist& bl, uint64_t f=0);
void decode(bucket_index_layout_generation& l, bufferlist::const_iterator& bl);


enum class BucketReshardState : uint8_t {
  NOT_RESHARDING,
  IN_PROGRESS,
  DONE,
};

// describes the layout of bucket index objects
struct BucketLayout {
  BucketReshardState resharding = BucketReshardState::NOT_RESHARDING;

  // current bucket index layout
  bucket_index_layout_generation current_index;

  // target index layout of a resharding operation
  bucket_index_layout_generation target_index;
};

void encode(const BucketLayout& l, bufferlist& bl, uint64_t f=0);
void decode(BucketLayout& l, bufferlist::const_iterator& bl);

} // namespace rgw
