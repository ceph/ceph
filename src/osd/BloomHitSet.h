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

#include "HitSet.h"
#include "common/bloom_filter.hpp"

/**
 * use a bloom_filter to track hits to the set
 */
class BloomHitSet : public HitSet::Impl {
  compressible_bloom_filter bloom;

public:
  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_BLOOM;
  }

  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_BLOOM;
    }
    HitSet::Impl *get_new_impl() const override {
      return new BloomHitSet;
    }

    uint32_t fpp_micro;    ///< false positive probability / 1M
    uint64_t target_size;  ///< number of unique insertions we expect to this HitSet
    uint64_t seed;         ///< seed to use when initializing the bloom filter

    Params()
      : fpp_micro(0), target_size(0), seed(0) {}
    Params(double fpp, uint64_t t, uint64_t s)
      : fpp_micro(fpp * 1000000.0), target_size(t), seed(s) {}
    Params(const Params &o)
      : fpp_micro(o.fpp_micro),
	target_size(o.target_size),
	seed(o.seed) {}
    ~Params() override {}

    double get_fpp() const {
      return (double)fpp_micro / 1000000.0;
    }
    void set_fpp(double f) {
      fpp_micro = (unsigned)(llrintl(f * 1000000.0));
    }

    void encode(ceph::buffer::list& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(fpp_micro, bl);
      encode(target_size, bl);
      encode(seed, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) override {
      DECODE_START(1, bl);
      decode(fpp_micro, bl);
      decode(target_size, bl);
      decode(seed, bl);
      DECODE_FINISH(bl);
    }
    void dump(ceph::Formatter *f) const override;
    void dump_stream(std::ostream& o) const override {
      o << "false_positive_probability: "
	<< get_fpp() << ", target_size: " << target_size
	<< ", seed: " << seed;
    }
    static std::list<Params> generate_test_instances() {
      std::list<Params> o;
      o.emplace_back();
      o.emplace_back();
      o.back().fpp_micro = 123456;
      o.back().target_size = 300;
      o.back().seed = 99;
      return o;
    }
  };

  BloomHitSet() {}
  BloomHitSet(unsigned inserts, double fpp, int seed)
    : bloom(inserts, fpp, seed)
  {}
  explicit BloomHitSet(const BloomHitSet::Params *p) : bloom(p->target_size,
                                                    p->get_fpp(),
                                                    p->seed)
  {}

  BloomHitSet(const BloomHitSet &o) {
    // oh god
    ceph::buffer::list bl;
    o.encode(bl);
    auto bli = std::cbegin(bl);
    this->decode(bli);
  }

  HitSet::Impl *clone() const override {
    return new BloomHitSet(*this);
  }

  bool is_full() const override {
    return bloom.is_full();
  }

  void insert(const hobject_t& o) override {
    bloom.insert(o.get_hash());
  }
  bool contains(const hobject_t& o) const override {
    return bloom.contains(o.get_hash());
  }
  unsigned insert_count() const override {
    return bloom.element_count();
  }
  unsigned approx_unique_insert_count() const override {
    return bloom.approx_unique_element_count();
  }
  void seal() override {
    // aim for a density of .5 (50% of bit set)
    double pc = bloom.density() * 2.0;
    if (pc < 1.0)
      bloom.compress(pc);
  }

  void encode(ceph::buffer::list &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(bloom, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) override {
    DECODE_START(1, bl);
    decode(bloom, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const override;
  static std::list<BloomHitSet> generate_test_instances() {
    std::list<BloomHitSet> o;
    o.emplace_back();
    o.push_back(BloomHitSet(10, .1, 1));
    o.back().insert(hobject_t());
    o.back().insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back().insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
    return o;
  }
};
WRITE_CLASS_ENCODER(BloomHitSet)
