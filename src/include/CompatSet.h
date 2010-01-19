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

#ifndef __CEPH_COMPATSET_H
#define __CEPH_COMPATSET_H
#include "include/buffer.h"
#include <vector>

#define END_FEATURE CompatSet::Feature(0, "")

struct CompatSet {

  struct Feature {
    __u64 id;
    string name;

    Feature(__u64 _id, const char *_name) : id(_id), name(_name) {}
    Feature(__u64 _id, string& _name) : id(_id), name(_name) {}
  };

  struct FeatureSet {
    __u64 mask;
    map <__u64,string> names;

    FeatureSet() : mask(0), names() {}
    void insert(Feature f) {
      mask |= f.id;
      names[f.id] = f.name;
    }

    void encode(bufferlist& bl) const {
      ::encode(mask, bl);
      ::encode(names, bl);
    }

    void decode(bufferlist::iterator& bl) {
      ::decode(mask, bl);
      ::decode(names, bl);
    }
  };

  FeatureSet compat, ro_compat, incompat;

  CompatSet(FeatureSet& _compat, FeatureSet& _ro_compat, FeatureSet& _incompat) :
    compat(_compat), ro_compat(_ro_compat), incompat(_incompat) {}

  CompatSet(const Feature _compat[], const Feature _ro_compat[], const Feature _incompat[]) :
    compat(), ro_compat(), incompat()
  {
    for (int i = 0; _compat[i].id != 0; ++i) compat.insert(_compat[i]);
    for (int j = 0; _ro_compat[j].id !=0; ++j) ro_compat.insert(_ro_compat[j]);
    for (int k = 0; _incompat[k].id != 0; ++k) incompat.insert(_incompat[k]);
  }

  CompatSet() : compat(), ro_compat(), incompat() { }


  /* does this filesystem implementation have the
     features required to read the other? */
  bool readable(CompatSet& other) {
    return !((other.incompat.mask ^ incompat.mask) & other.incompat.mask);
  }

  /* does this filesystem implementation have the
     features required to write the other? */
  bool writeable(CompatSet& other) {
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
  int compare(CompatSet& other) {
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
  CompatSet unsupported(CompatSet& other) {
    CompatSet diff;
    __u64 other_compat =
      ((other.compat.mask ^ compat.mask) & other.compat.mask);
    __u64 other_ro_compat =
      ((other.ro_compat.mask ^ ro_compat.mask) & other.ro_compat.mask);
    __u64 other_incompat =
      ((other.incompat.mask ^ incompat.mask) & other.incompat.mask);
    for (int i = 0; i < 64; ++i) {
      int mask = 1 << i;
      if (mask & other_compat) {
	diff.compat.insert( Feature(mask & other_compat,
				    other.compat.names[mask&other_compat]));
      }
      if (mask & other_ro_compat) {
	diff.ro_compat.insert(Feature(mask & other_ro_compat,
				      other.compat.names[mask&other_ro_compat]));
      }
      if (mask & other_incompat) {
	diff.incompat.insert( Feature(mask & other_incompat,
				      other.incompat.names[mask&other_incompat]));
      }
    }
    return diff;
  }
  
  void encode(bufferlist& bl) const {
    compat.encode(bl);
    ro_compat.encode(bl);
    incompat.encode(bl);
  }
  
  void decode(bufferlist::iterator& bl) {
    compat.decode(bl);
    ro_compat.decode(bl);
    incompat.decode(bl);
  }
};
#endif
