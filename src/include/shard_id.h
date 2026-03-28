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
#ifndef CEPH_SHARD_ID_H
#define CEPH_SHARD_ID_H

#include <compare> // for std::strong_ordering
#include <cstdint>
#include <list>
#include <iosfwd>

#include "buffer.h"
#include "encoding.h"

namespace ceph {
  class Formatter;
}

struct shard_id_t {
  int8_t id;

  shard_id_t() : id(0) {}
  explicit constexpr shard_id_t(int8_t _id) : id(_id) {}

  explicit constexpr operator int8_t() const { return id; }
  explicit constexpr operator int64_t() const { return id; }
  explicit constexpr operator int() const { return id; }
  explicit constexpr operator unsigned() const { return id; }

  const static shard_id_t NO_SHARD;

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    encode(id, bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(id, bl);
  }
  void dump(ceph::Formatter *f) const;
  static std::list<shard_id_t> generate_test_instances();
  shard_id_t& operator++() { ++id; return *this; }
  friend constexpr std::strong_ordering operator<=>(const shard_id_t &lhs,
                                                    const shard_id_t &rhs) {
    return lhs.id <=> rhs.id;
  }

  friend constexpr std::strong_ordering operator<=>(int lhs,
                                                    const shard_id_t &rhs) {
    return lhs <=> rhs.id;
  }
  friend constexpr std::strong_ordering operator<=>(const shard_id_t &lhs,
                                                    int rhs) {
    return lhs.id <=> rhs;
  }

  shard_id_t& operator=(int other) { id = other; return *this; }
  bool operator==(const shard_id_t &other) const { return id == other.id; }

  shard_id_t operator+(int other) const { return shard_id_t(id + other); }
  shard_id_t operator-(int other) const { return shard_id_t(id - other); }
};
WRITE_CLASS_ENCODER(shard_id_t)
std::ostream &operator<<(std::ostream &lhs, const shard_id_t &rhs);

#endif
