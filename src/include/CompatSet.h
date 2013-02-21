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
#include "include/buffer.h"
#include <vector>

#include "common/Formatter.h"

struct CompatSet {

  struct Feature {
    uint64_t id;
    string name;

    Feature(uint64_t _id, const char *_name) : id(_id), name(_name) {}
    Feature(uint64_t _id, string& _name) : id(_id), name(_name) {}
  };

  struct FeatureSet {
    uint64_t mask;
    map <uint64_t,string> names;

    FeatureSet() : mask(1), names() {}
    void insert(Feature f) {
      assert(f.id > 0);
      assert(f.id < 63);
      mask |= (1<<f.id);
      names[f.id] = f.name;
    }

    bool contains(Feature f) const {
      return names.count(f.id);
    }
    bool contains(uint64_t f) const {
      return names.count(f);
    }
    void remove(uint64_t f) {
      if (names.count(f)) {
	names.erase(f);
	mask &= ~(1<<f);
      }
    }
    void remove(Feature f) {
      remove(f.id);
    }

    void encode(bufferlist& bl) const {
      /* See below, mask always has the lowest bit set in memory, but
       * unset in the encoding */
      ::encode(mask & (~(uint64_t)1), bl);
      ::encode(names, bl);
    }

    void decode(bufferlist::iterator& bl) {
      ::decode(mask, bl);
      ::decode(names, bl);
      /**
       * Previously, there was a bug where insert did
       * mask |= f.id rather than mask |= (1 << f.id).
       * In FeatureSets from those version, mask always
       * has the lowest bit set.  Since then, masks always
       * have the lowest bit unset.
       *
       * When we encounter such a FeatureSet, we have to
       * reconstruct the mask from the names map.
       */
      if (mask & 1) {
	mask = 1;
	map<uint64_t, string> temp_names;
	temp_names.swap(names);
	for (map<uint64_t, string>::iterator i = temp_names.begin();
	     i != temp_names.end();
	     ++i) {
	  insert(Feature(i->first, i->second));
	}
      } else {
	mask |= 1;
      }
    }

    void dump(Formatter *f) const {
      for (map<uint64_t,string>::const_iterator p = names.begin();
	   p != names.end();
	   ++p) {
	char s[10];
	snprintf(s, sizeof(s), "%lld", (unsigned long long)p->first);
	f->dump_string(s, p->second);
      }
    }
  };

  FeatureSet compat, ro_compat, incompat;

  CompatSet(FeatureSet& _compat, FeatureSet& _ro_compat, FeatureSet& _incompat) :
    compat(_compat), ro_compat(_ro_compat), incompat(_incompat) {}

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
    uint64_t other_compat =
      ((other.compat.mask ^ compat.mask) & other.compat.mask);
    uint64_t other_ro_compat =
      ((other.ro_compat.mask ^ ro_compat.mask) & other.ro_compat.mask);
    uint64_t other_incompat =
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

  void dump(Formatter *f) const {
    f->open_object_section("compat");
    compat.dump(f);
    f->close_section();
    f->open_object_section("ro_compat");
    ro_compat.dump(f);
    f->close_section();
    f->open_object_section("incompat");
    incompat.dump(f);
    f->close_section();
  }

  static void generate_test_instances(list<CompatSet*>& o) {
    o.push_back(new CompatSet);
    o.push_back(new CompatSet);
    o.back()->compat.insert(Feature(1, "one"));
    o.back()->compat.insert(Feature(2, "two"));
    o.back()->ro_compat.insert(Feature(4, "four"));
    o.back()->incompat.insert(Feature(3, "three"));
  }
};
WRITE_CLASS_ENCODER(CompatSet)

inline ostream& operator<<(ostream& out, const CompatSet::FeatureSet& fs)
{
  return out << fs.names;
}

inline ostream& operator<<(ostream& out, const CompatSet& compat)
{
  return out << "compat=" << compat.compat
	     << ",rocompat=" << compat.ro_compat
	     << ",incompat=" << compat.incompat;
}

#endif
