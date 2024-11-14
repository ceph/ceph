// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <unordered_set>

#include "HitSet.h"
#include "include/encoding_unordered_set.h"

/**
 * explicitly enumerate objects in the set
 */
class ExplicitObjectHitSet : public HitSet::Impl {
  uint64_t count;
  std::unordered_set<hobject_t> hits;
public:
  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_EXPLICIT_OBJECT;
    }
    HitSet::Impl *get_new_impl() const override {
      return new ExplicitObjectHitSet;
    }
    static std::list<Params> generate_test_instances() {
      std::list<Params> o;
      o.emplace_back();
      return o;
    }
  };

  ExplicitObjectHitSet() : count(0) {}
  explicit ExplicitObjectHitSet(const ExplicitObjectHitSet::Params *p) : count(0) {}
  ExplicitObjectHitSet(const ExplicitObjectHitSet &o) : count(o.count),
      hits(o.hits) {}

  HitSet::Impl *clone() const override {
    return new ExplicitObjectHitSet(*this);
  }

  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_EXPLICIT_OBJECT;
  }
  bool is_full() const override {
    return false;
  }
  void insert(const hobject_t& o) override {
    hits.insert(o);
    ++count;
  }
  bool contains(const hobject_t& o) const override {
    return hits.count(o);
  }
  unsigned insert_count() const override {
    return count;
  }
  unsigned approx_unique_insert_count() const override {
    return hits.size();
  }
  void encode(ceph::buffer::list &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) override {
    DECODE_START(1, bl);
    decode(count, bl);
    decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const override;
  static std::list<ExplicitObjectHitSet> generate_test_instances() {
    std::list<ExplicitObjectHitSet> o;
    o.emplace_back();
    o.emplace_back();
    o.back().insert(hobject_t());
    o.back().insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back().insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
    return o;
  }
};
WRITE_CLASS_ENCODER(ExplicitObjectHitSet)
