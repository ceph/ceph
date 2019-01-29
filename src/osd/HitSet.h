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

#include <string_view>

#include <boost/scoped_ptr.hpp>

#include "include/encoding.h"
#include "include/unordered_set.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"

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
  } impl_type_t;

  static std::string_view get_type_name(impl_type_t t) {
    switch (t) {
    case TYPE_NONE: return "none";
    case TYPE_EXPLICIT_HASH: return "explicit_hash";
    case TYPE_EXPLICIT_OBJECT: return "explicit_object";
    case TYPE_BLOOM: return "bloom";
    default: return "???";
    }
  }
  std::string_view get_type_name() const {
    if (impl)
      return get_type_name(impl->get_type());
    return get_type_name(TYPE_NONE);
  }

  /// abstract interface for a HitSet implementation
  class Impl {
  public:
    virtual impl_type_t get_type() const = 0;
    virtual bool is_full() const = 0;
    virtual void insert(const hobject_t& o) = 0;
    virtual bool contains(const hobject_t& o) const = 0;
    virtual unsigned insert_count() const = 0;
    virtual unsigned approx_unique_insert_count() const = 0;
    virtual void encode(bufferlist &bl) const = 0;
    virtual void decode(bufferlist::const_iterator& p) = 0;
    virtual void dump(Formatter *f) const = 0;
    virtual Impl* clone() const = 0;
    virtual void seal() {}
    virtual ~Impl() {}
  };

  boost::scoped_ptr<Impl> impl;
  bool sealed;

  class Params {
    /// create an Impl* of the given type
    bool create_impl(impl_type_t t);

  public:
    class Impl {
    public:
      virtual impl_type_t get_type() const = 0;
      virtual HitSet::Impl *get_new_impl() const = 0;
      virtual void encode(bufferlist &bl) const {}
      virtual void decode(bufferlist::const_iterator& p) {}
      virtual void dump(Formatter *f) const {}
      virtual void dump_stream(ostream& o) const {}
      virtual ~Impl() {}
    };

    Params()  {}
    explicit Params(Impl *i) : impl(i) {}
    virtual ~Params() {}

    boost::scoped_ptr<Params::Impl> impl;

    impl_type_t get_type() const {
      if (impl)
	return impl->get_type();
      return TYPE_NONE;
    }

    Params(const Params& o) noexcept;
    const Params& operator=(const Params& o);

    void encode(bufferlist &bl) const;
    void decode(bufferlist::const_iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<HitSet::Params*>& o);

    friend ostream& operator<<(ostream& out, const HitSet::Params& p);
  };

  HitSet() : impl(NULL), sealed(false) {}
  explicit HitSet(Impl *i) : impl(i), sealed(false) {}
  explicit HitSet(const HitSet::Params& params);

  HitSet(const HitSet& o) {
    sealed = o.sealed;
    if (o.impl)
      impl.reset(o.impl->clone());
    else
      impl.reset(NULL);
  }
  const HitSet& operator=(const HitSet& o) {
    sealed = o.sealed;
    if (o.impl)
      impl.reset(o.impl->clone());
    else
      impl.reset(NULL);
    return *this;
  }


  bool is_full() const {
    return impl->is_full();
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
  void seal() {
    ceph_assert(!sealed);
    sealed = true;
    impl->seal();
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HitSet*>& o);

private:
  void reset_to_type(impl_type_t type);
};
WRITE_CLASS_ENCODER(HitSet)
WRITE_CLASS_ENCODER(HitSet::Params)

typedef boost::shared_ptr<HitSet> HitSetRef;

ostream& operator<<(ostream& out, const HitSet::Params& p);

/**
 * explicitly enumerate hash hits in the set
 */
class ExplicitHashHitSet : public HitSet::Impl {
  uint64_t count;
  ceph::unordered_set<uint32_t> hits;
public:
  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_EXPLICIT_HASH;
    }
    HitSet::Impl *get_new_impl() const override {
      return new ExplicitHashHitSet;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
    }
  };

  ExplicitHashHitSet() : count(0) {}
  explicit ExplicitHashHitSet(const ExplicitHashHitSet::Params *p) : count(0) {}
  ExplicitHashHitSet(const ExplicitHashHitSet &o) : count(o.count),
      hits(o.hits) {}

  HitSet::Impl *clone() const override {
    return new ExplicitHashHitSet(*this);
  }

  HitSet::impl_type_t get_type() const override {
    return HitSet::TYPE_EXPLICIT_HASH;
  }
  bool is_full() const override {
    return false;
  }
  void insert(const hobject_t& o) override {
    hits.insert(o.get_hash());
    ++count;
  }
  bool contains(const hobject_t& o) const override {
    return hits.count(o.get_hash());
  }
  unsigned insert_count() const override {
    return count;
  }
  unsigned approx_unique_insert_count() const override {
    return hits.size();
  }
  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &bl) override {
    DECODE_START(1, bl);
    decode(count, bl);
    decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
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
  ceph::unordered_set<hobject_t> hits;
public:
  class Params : public HitSet::Params::Impl {
  public:
    HitSet::impl_type_t get_type() const override {
      return HitSet::TYPE_EXPLICIT_OBJECT;
    }
    HitSet::Impl *get_new_impl() const override {
      return new ExplicitObjectHitSet;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
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
  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    encode(hits, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(1, bl);
    decode(count, bl);
    decode(hits, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
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

    void encode(bufferlist& bl) const override {
      ENCODE_START(1, 1, bl);
      encode(fpp_micro, bl);
      encode(target_size, bl);
      encode(seed, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& bl) override {
      DECODE_START(1, bl);
      decode(fpp_micro, bl);
      decode(target_size, bl);
      decode(seed, bl);
      DECODE_FINISH(bl);
    }
    void dump(Formatter *f) const override;
    void dump_stream(ostream& o) const override {
      o << "false_positive_probability: "
	<< get_fpp() << ", target_size: " << target_size
	<< ", seed: " << seed;
    }
    static void generate_test_instances(list<Params*>& o) {
      o.push_back(new Params);
      o.push_back(new Params);
      (*o.rbegin())->fpp_micro = 123456;
      (*o.rbegin())->target_size = 300;
      (*o.rbegin())->seed = 99;
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
    bufferlist bl;
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

  void encode(bufferlist &bl) const override {
    ENCODE_START(1, 1, bl);
    encode(bloom, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) override {
    DECODE_START(1, bl);
    decode(bloom, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const override;
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
