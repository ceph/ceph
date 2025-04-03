// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "include/types.h"
#include "common/mini_flat_map.h"

struct ec_align_t {
  uint64_t offset;
  uint64_t size;
  uint32_t flags;
  ec_align_t(std::pair<uint64_t, uint64_t> p, uint32_t flags)
    : offset(p.first), size(p.second), flags(flags) {}
  ec_align_t(uint64_t offset, uint64_t size, uint32_t flags)
    : offset(offset), size(size), flags(flags) {}
  bool operator==(const ec_align_t &other) const;
  void print(std::ostream &os) const {
    os << offset << "," << size << "," << flags;
  }
};

struct raw_shard_id_t {
  int8_t id;

  raw_shard_id_t() : id(0) {}
  explicit constexpr raw_shard_id_t(int8_t _id) : id(_id) {}

  explicit constexpr operator int8_t() const { return id; }
  // For convenient use in comparisons
  explicit constexpr operator int() const { return id; }
  explicit constexpr operator uint64_t() const { return id; }

  const static raw_shard_id_t NO_SHARD;

  void dump(ceph::Formatter *f) const {
    f->dump_int("id", id);
  }
  static void generate_test_instances(std::list<raw_shard_id_t*>& ls) {
    ls.push_back(new raw_shard_id_t(1));
    ls.push_back(new raw_shard_id_t(2));
  }
  raw_shard_id_t& operator++() { ++id; return *this; }
  friend constexpr std::strong_ordering operator<=>(const raw_shard_id_t &lhs, const raw_shard_id_t &rhs) { return lhs.id <=> rhs.id; }
  friend constexpr std::strong_ordering operator<=>(int lhs, const raw_shard_id_t &rhs) { return lhs <=> rhs.id; }
  friend constexpr std::strong_ordering operator<=>(const raw_shard_id_t &lhs, int rhs) { return lhs.id <=> rhs; }

  raw_shard_id_t& operator=(int other) { id = other; return *this; }
  bool operator==(const raw_shard_id_t &other) const { return id == other.id; }
};

template <typename T>
using shard_id_map = mini_flat_map<shard_id_t, T>;
