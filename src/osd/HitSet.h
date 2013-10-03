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

#ifndef CEPH_OSD_HITSET_H
#define CEPH_OSD_HITSET_H

#include <boost/scoped_ptr.hpp>

#include "include/encoding.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "common/Formatter.h"

/**
 * generic container for a HitSet
 *
 * Encapsulate a HitSetImpl of any type.  Expose a generic interface
 * to users and wrap the encoded object with a type so that it can be
 * safely decoded later.
 */
class HitSet {
public:
  typedef enum {
    TYPE_NONE = 0,
    TYPE_EXPLICIT_HASH = 1,
    TYPE_EXPLICIT_OBJECT = 2,
    TYPE_BLOOM = 3
  } type_t;

  static const char *get_type_name(type_t t) {
    switch (t) {
    case TYPE_NONE: return "none";
    case TYPE_EXPLICIT_HASH: return "explicit_hash";
    case TYPE_EXPLICIT_OBJECT: return "explicit_object";
    case TYPE_BLOOM: return "bloom";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    if (impl)
      return get_type_name(impl->get_type());
    return get_type_name(TYPE_NONE);
  }

  /// abstract interface for a HitSet implementation
  class Impl {
  public:
    virtual type_t get_type() const = 0;
    virtual void insert(const hobject_t& o) = 0;
    virtual bool contains(const hobject_t& o) const = 0;
    virtual unsigned insert_count() const = 0;
    virtual unsigned approx_unique_insert_count() const = 0;
    virtual void encode(bufferlist &bl) const = 0;
    virtual void decode(bufferlist::iterator& p) = 0;
    virtual void dump(Formatter *f) const = 0;
    /// optimize structure for a desired false positive probability
    virtual void optimize() {}
    virtual ~Impl() {}
  };

  boost::scoped_ptr<Impl> impl;

  HitSet() : impl(NULL) {}
  HitSet(Impl *i) : impl(i) {}
  HitSet(type_t type, unsigned target_size, double fpp, unsigned seed);

  HitSet(const HitSet& o) {
    // only allow copying empty instances... FIXME
    assert(!o.impl);
  }

  /// insert a hash into the set
  void insert(const hobject_t& o) {
    impl->insert(o);
  }

  /// query whether a hash is in the set
  bool contains(const hobject_t& o) const {
    return impl->contains(o);
  }

  unsigned insert_count() const {
    return impl->insert_count();
  }
  unsigned approx_unique_insert_count() const {
    return impl->approx_unique_insert_count();
  }
  void optimize() {
    impl->optimize();
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HitSet*>& o);
};
WRITE_CLASS_ENCODER(HitSet);

/**
 * explicitly enumerate hash hits in the set
 */
class ExplicitHashHitSet : public HitSet::Impl {
  uint64_t count;
  hash_set<uint32_t> hits;
public:
  ExplicitHashHitSet() : count(0) {}

  HitSet::type_t get_type() const {
    return HitSet::TYPE_EXPLICIT_HASH;
  }
  void insert(const hobject_t& o) {
    hits.insert(o.hash);
    ++count;
  }
  bool contains(const hobject_t& o) const {
    return hits.count(o.hash);
  }
  unsigned insert_count() const {
    return count;
  }
  unsigned approx_unique_insert_count() const {
    return hits.size();
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(count, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(count, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("insert_count", count);
    f->open_array_section("hash_set");
    for (hash_set<uint32_t>::const_iterator p = hits.begin(); p != hits.end(); ++p)
      f->dump_unsigned("hash", *p);
    f->close_section();
  }
  static void generate_test_instances(list<ExplicitHashHitSet*>& o) {
    o.push_back(new ExplicitHashHitSet);
    o.push_back(new ExplicitHashHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitHashHitSet)

/**
 * explicitly enumerate objects in the set
 */
class ExplicitObjectHitSet : public HitSet::Impl {
  uint64_t count;
  hash_set<hobject_t> hits;
public:
  ExplicitObjectHitSet() : count(0) {}

  HitSet::type_t get_type() const {
    return HitSet::TYPE_EXPLICIT_OBJECT;
  }
  void insert(const hobject_t& o) {
    hits.insert(o);
    ++count;
  }
  bool contains(const hobject_t& o) const {
    return hits.count(o);
  }
  unsigned insert_count() const {
    return count;
  }
  unsigned approx_unique_insert_count() const {
    return hits.size();
  }
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(count, bl);
    ::encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(count, bl);
    ::decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_unsigned("insert_count", count);
    f->open_array_section("set");
    for (hash_set<hobject_t>::const_iterator p = hits.begin(); p != hits.end(); ++p) {
      f->open_object_section("object");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<ExplicitObjectHitSet*>& o) {
    o.push_back(new ExplicitObjectHitSet);
    o.push_back(new ExplicitObjectHitSet);
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(ExplicitObjectHitSet)

/**
 * use a bloom_filter to track hits to the set
 */
class BloomHitSet : public HitSet::Impl {
  compressible_bloom_filter bloom;

public:
  HitSet::type_t get_type() const {
    return HitSet::TYPE_BLOOM;
  }

  BloomHitSet() {}
  BloomHitSet(unsigned inserts, double fpp, int seed)
    : bloom(inserts, fpp, seed)
  {}

  void insert(const hobject_t& o) {
    bloom.insert(o.hash);
  }
  bool contains(const hobject_t& o) const {
    return bloom.contains(o.hash);
  }
  unsigned insert_count() const {
    return bloom.element_count();
  }
  unsigned approx_unique_insert_count() const {
    return bloom.approx_unique_element_count();
  }
  void optimize() {
    // aim for a density of .5 (50% of bit set)
    double pc = (double)bloom.density() * 2.0 * 100.0;
    bloom.compress(pc);
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(bloom, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(bloom, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_object_section("bloom_filter");
    bloom.dump(f);
    f->close_section();
  }
  static void generate_test_instances(list<BloomHitSet*>& o) {
    o.push_back(new BloomHitSet);
    o.push_back(new BloomHitSet(10, .1, 1));
    o.back()->insert(hobject_t());
    o.back()->insert(hobject_t("asdf", "", CEPH_NOSNAP, 123, 1, ""));
    o.back()->insert(hobject_t("qwer", "", CEPH_NOSNAP, 456, 1, ""));
  }
};
WRITE_CLASS_ENCODER(BloomHitSet)

#endif
