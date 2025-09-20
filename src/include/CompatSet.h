// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_COMPATSET_H
#define CEPH_COMPATSET_H

#include <iosfwd>
#include <map>
#include <string>

#include "include/buffer.h"
#include "include/encoding.h"

namespace ceph { class Formatter; }

struct CompatSet {

  struct Feature {
    uint64_t id;
    std::string name;

    Feature(uint64_t _id, const std::string& _name) : id(_id), name(_name) {}
  };

  class FeatureSet {
    uint64_t mask;
    std::map<uint64_t, std::string> names;

  public:
    friend struct CompatSet;
    friend class CephCompatSet_AllSet_Test;
    friend class CephCompatSet_other_Test;
    friend class CephCompatSet_merge_Test;
    friend std::ostream& operator<<(std::ostream& out, const CompatSet::FeatureSet& fs);
    friend std::ostream& operator<<(std::ostream& out, const CompatSet& compat);
    FeatureSet() : mask(1), names() {}
    void insert(const Feature& f) {
      ceph_assert(f.id > 0);
      ceph_assert(f.id < 64);
      mask |= ((uint64_t)1<<f.id);
      names[f.id] = f.name;
    }

    bool contains(const Feature& f) const {
      return names.count(f.id);
    }
    bool contains(uint64_t f) const {
      return names.count(f);
    }
    /**
     * Getter instead of using name[] to be const safe
     */
    std::string get_name(uint64_t const f) const {
      std::map<uint64_t, std::string>::const_iterator i = names.find(f);
      ceph_assert(i != names.end());
      return i->second;
    }

    void remove(uint64_t f) {
      if (names.count(f)) {
	names.erase(f);
	mask &= ~((uint64_t)1<<f);
      }
    }
    void remove(const Feature& f) {
      remove(f.id);
    }

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);

    void dump(ceph::Formatter *f) const;
  };

  // These features have no impact on the read / write status
  FeatureSet compat;
  // If any of these features are missing, read is possible ( as long
  // as no incompat feature is missing ) but it is not possible to write
  FeatureSet ro_compat;
  // If any of these features are missing, read or write is not possible
  FeatureSet incompat;

  CompatSet(FeatureSet& _compat, FeatureSet& _ro_compat, FeatureSet& _incompat) :
    compat(_compat), ro_compat(_ro_compat), incompat(_incompat) {}

  CompatSet() : compat(), ro_compat(), incompat() { }


  /* does this filesystem implementation have the
     features required to read the other? */
  bool readable(CompatSet const& other) const {
    return !((other.incompat.mask ^ incompat.mask) & other.incompat.mask);
  }

  /* does this filesystem implementation have the
     features required to write the other? */
  bool writeable(CompatSet const& other) const {
    return readable(other) &&
      !((other.ro_compat.mask ^ ro_compat.mask) & other.ro_compat.mask);
  }

  /* Compare this CompatSet to another.
   * CAREFULLY NOTE: This operation is NOT commutative.
   * a > b DOES NOT imply that b < a.
   * If returns:
   * 0: The CompatSets have the same feature set.
   * 1: This CompatSet's features are a strict superset of the other's.
   * -1: This CompatSet is missing at least one feature
   *     described in the other. It may still have more features, though.
   */
  int compare(const CompatSet& other) const {
    if ((other.compat.mask == compat.mask) &&
	(other.ro_compat.mask == ro_compat.mask) &&
	(other.incompat.mask == incompat.mask)) return 0;
    //okay, they're not the same

    //if we're writeable we have a superset of theirs on incompat and ro_compat
    if (writeable(other) && !((other.compat.mask ^ compat.mask)
			      & other.compat.mask)) return 1;
    //if we make it here, we weren't writeable or had a difference compat set
    return -1;
  }

  /* Get the features supported by other CompatSet but not this one,
   * as a CompatSet.
   */
  CompatSet unsupported(const CompatSet& other) const {
    CompatSet diff;
    uint64_t other_compat =
      ((other.compat.mask ^ compat.mask) & other.compat.mask);
    uint64_t other_ro_compat =
      ((other.ro_compat.mask ^ ro_compat.mask) & other.ro_compat.mask);
    uint64_t other_incompat =
      ((other.incompat.mask ^ incompat.mask) & other.incompat.mask);
    for (int id = 1; id < 64; ++id) {
      uint64_t mask = (uint64_t)1 << id;
      if (mask & other_compat) {
	diff.compat.insert( Feature(id, other.compat.names.at(id)));
      }
      if (mask & other_ro_compat) {
	diff.ro_compat.insert(Feature(id, other.ro_compat.names.at(id)));
      }
      if (mask & other_incompat) {
	diff.incompat.insert( Feature(id, other.incompat.names.at(id)));
      }
    }
    return diff;
  }
  
  /* Merge features supported by other CompatSet into this one.
   * Return: true if some features were merged
   */
  bool merge(CompatSet const & other) {
    uint64_t other_compat =
      ((other.compat.mask ^ compat.mask) & other.compat.mask);
    uint64_t other_ro_compat =
      ((other.ro_compat.mask ^ ro_compat.mask) & other.ro_compat.mask);
    uint64_t other_incompat =
      ((other.incompat.mask ^ incompat.mask) & other.incompat.mask);
    if (!other_compat && !other_ro_compat && !other_incompat)
      return false;
    for (int id = 1; id < 64; ++id) {
      uint64_t mask = (uint64_t)1 << id;
      if (mask & other_compat) {
	compat.insert( Feature(id, other.compat.get_name(id)));
      }
      if (mask & other_ro_compat) {
	ro_compat.insert(Feature(id, other.ro_compat.get_name(id)));
      }
      if (mask & other_incompat) {
	incompat.insert( Feature(id, other.incompat.get_name(id)));
      }
    }
    return true;
  }

  std::ostream& printlite(std::ostream& o) const;

  void encode(ceph::buffer::list& bl) const {
    compat.encode(bl);
    ro_compat.encode(bl);
    incompat.encode(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    compat.decode(bl);
    ro_compat.decode(bl);
    incompat.decode(bl);
  }

  void dump(ceph::Formatter *f) const;

  static std::list<CompatSet> generate_test_instances();
};
WRITE_CLASS_ENCODER(CompatSet)

std::ostream& operator<<(std::ostream& out, const CompatSet::Feature& f);
std::ostream& operator<<(std::ostream& out, const CompatSet::FeatureSet& fs);
std::ostream& operator<<(std::ostream& out, const CompatSet& compat);

#endif
