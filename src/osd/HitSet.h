// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
    virtual void encode(ceph::buffer::list &bl) const = 0;
    virtual void decode(ceph::buffer::list::const_iterator& p) = 0;
    virtual void dump(ceph::Formatter *f) const = 0;
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
      virtual void encode(ceph::buffer::list &bl) const {}
      virtual void decode(ceph::buffer::list::const_iterator& p) {}
      virtual void dump(ceph::Formatter *f) const {}
      virtual void dump_stream(std::ostream& o) const {}
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

    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static std::list<HitSet::Params> generate_test_instances();

    friend std::ostream& operator<<(std::ostream& out, const HitSet::Params& p);
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

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static std::list<HitSet> generate_test_instances();

private:
  void reset_to_type(impl_type_t type);
};
WRITE_CLASS_ENCODER(HitSet)
WRITE_CLASS_ENCODER(HitSet::Params)

typedef boost::shared_ptr<HitSet> HitSetRef;

std::ostream& operator<<(std::ostream& out, const HitSet::Params& p);

#endif
