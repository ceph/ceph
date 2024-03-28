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

#ifndef CEPH_FRAG_H
#define CEPH_FRAG_H

#include <boost/container/small_vector.hpp>

#include <iostream>

#include <stdint.h>
#include <stdio.h>

#include "buffer.h"
#include "compact_map.h"

#include "ceph_frag.h"
#include "include/encoding.h"
#include "include/ceph_assert.h"

#include "common/dout.h"

/*
 * 
 * the goal here is to use a binary split strategy to partition a namespace.  
 * frag_t represents a particular fragment.  bits() tells you the size of the
 * fragment, and value() it's name.  this is roughly analogous to an ip address
 * and netmask.
 * 
 * fragtree_t represents an entire namespace and it's partition.  it essentially 
 * tells you where fragments are split into other fragments, and by how much 
 * (i.e. by how many bits, resulting in a power of 2 number of child fragments).
 * 
 * this vaguely resembles a btree, in that when a fragment becomes large or small
 * we can split or merge, except that there is no guarantee of being balanced.
 *
 * presumably we are partitioning the output of a (perhaps specialized) hash 
 * function.
 */

/**
 * frag_t
 *
 * description of an individual fragment.  that is, a particular piece
 * of the overall namespace.
 *
 * this is conceptually analogous to an ip address and netmask.
 *
 * a value v falls "within" fragment f iff (v & f.mask()) == f.value().
 *
 * we write it as v/b, where v is a value and b is the number of bits.
 * 0/0 (bits==0) corresponds to the entire namespace.  if we bisect that,
 * we get 0/1 and 1/1.  quartering gives us 0/2, 1/2, 2/2, 3/2.  and so on.
 *
 * this makes the right most bit of v the "most significant", which is the 
 * opposite of what we usually see.
 */

/*
 * TODO:
 *  - get_first_child(), next_sibling(int parent_bits) to make (possibly partial) 
 *    iteration efficient (see, e.g., try_assimilate_children()
 *  - rework frag_t so that we mask the left-most (most significant) bits instead of
 *    the right-most (least significant) bits.  just because it's more intuitive, and
 *    matches the network/netmask concept.
 */

class frag_t {
  /*
   * encoding is dictated by frag_* functions in ceph_fs.h.  use those
   * helpers _exclusively_.
   */
public:
  using _frag_t = uint32_t;
  
  frag_t() = default;
  frag_t(unsigned v, unsigned b) : _enc(ceph_frag_make(b, v)) { }
  frag_t(_frag_t e) : _enc(e) { }

  // constructors
  void from_unsigned(unsigned e) { _enc = e; }
  
  // accessors
  unsigned value() const { return ceph_frag_value(_enc); }
  unsigned bits() const { return ceph_frag_bits(_enc); }
  unsigned mask() const { return ceph_frag_mask(_enc); }
  unsigned mask_shift() const { return ceph_frag_mask_shift(_enc); }

  operator _frag_t() const { return _enc; }

  // tests
  bool contains(unsigned v) const { return ceph_frag_contains_value(_enc, v); }
  bool contains(frag_t sub) const { return ceph_frag_contains_frag(_enc, sub._enc); }
  bool is_root() const { return bits() == 0; }
  frag_t parent() const {
    ceph_assert(bits() > 0);
    return frag_t(ceph_frag_parent(_enc));
  }

  // splitting
  frag_t make_child(int i, int nb) const {
    ceph_assert(i < (1<<nb));
    return frag_t(ceph_frag_make_child(_enc, nb, i));
  }
  template<typename T>
  void split(int nb, T& fragments) const {
    ceph_assert(nb > 0);
    unsigned nway = 1 << nb;
    for (unsigned i=0; i<nway; i++) 
      fragments.push_back(make_child(i, nb));
  }

  // binary splitting
  frag_t left_child() const { return frag_t(ceph_frag_left_child(_enc)); }
  frag_t right_child() const { return frag_t(ceph_frag_right_child(_enc)); }

  bool is_left() const { return ceph_frag_is_left_child(_enc); }
  bool is_right() const { return ceph_frag_is_right_child(_enc); }
  frag_t get_sibling() const {
    ceph_assert(!is_root());
    return frag_t(ceph_frag_sibling(_enc));
  }

  // sequencing
  bool is_leftmost() const { return ceph_frag_is_leftmost(_enc); }
  bool is_rightmost() const { return ceph_frag_is_rightmost(_enc); }
  frag_t next() const {
    ceph_assert(!is_rightmost());
    return frag_t(ceph_frag_next(_enc));
  }

  // parse
  bool parse(const char *s) {
    int pvalue, pbits;
    int r = sscanf(s, "%x/%d", &pvalue, &pbits);
    if (r == 2) {
      *this = frag_t(pvalue, pbits);
      return true;
    }
    return false;
  }

  void encode(ceph::buffer::list& bl) const {
    ceph::encode_raw(_enc, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    __u32 v;
    ceph::decode_raw(v, p);
    _enc = v;
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("value", value());
    f->dump_unsigned("bits", bits());
  }
  static void generate_test_instances(std::list<frag_t*>& ls) {
    ls.push_back(new frag_t);
    ls.push_back(new frag_t(10, 2));
    ls.push_back(new frag_t(11, 3));
  }
  bool operator<(const frag_t& b) const
  {
    if (value() != b.value())
      return value() < b.value();
    else
      return bits() < b.bits();
  }
private:
  _frag_t _enc = 0;
};
WRITE_CLASS_ENCODER(frag_t)

inline std::ostream& operator<<(std::ostream& out, const frag_t& hb)
{
  //out << std::hex << hb.value() << std::dec << "/" << hb.bits() << '=';
  unsigned num = hb.bits();
  if (num) {
    unsigned val = hb.value();
    for (unsigned bit = 23; num; num--, bit--) 
      out << ((val & (1<<bit)) ? '1':'0');
  }
  return out << '*';
}


using frag_vec_t = boost::container::small_vector<frag_t, 4>;

/**
 * fragtree_t -- partition an entire namespace into one or more frag_t's. 
 */
class fragtree_t {
  // pairs <f, b>:
  //  frag_t f is split by b bits.
  //  if child frag_t does not appear, it is not split.
public:
  compact_map<frag_t,int32_t> _splits;

public:
  // -------------
  // basics
  void swap(fragtree_t& other) {
    _splits.swap(other._splits);
  }
  void clear() {
    _splits.clear();
  }

  // -------------
  // accessors
  bool empty() const { 
    return _splits.empty();
  }
  int get_split(const frag_t hb) const {
    compact_map<frag_t,int32_t>::const_iterator p = _splits.find(hb);
    if (p == _splits.end())
      return 0;
    else
      return p->second;
  }

  
  bool is_leaf(frag_t x) const {
    frag_vec_t s;
    get_leaves_under(x, s);
    //generic_dout(10) << "is_leaf(" << x << ") -> " << ls << dendl;
    return s.size() == 1 && s.front() == x;
  }

  /**
   * get_leaves -- list all leaves
   */
  template<typename T>
  void get_leaves(T& c) const {
    return get_leaves_under_split(frag_t(), c);
  }

  /**
   * get_leaves_under_split -- list all leaves under a known split point (or root)
   */
  template<typename T>
  void get_leaves_under_split(frag_t under, T& c) const {
    frag_vec_t s;
    s.push_back(under);
    while (!s.empty()) {
      frag_t t = s.back();
      s.pop_back();
      int nb = get_split(t);
      if (nb) 
	t.split(nb, s);   // queue up children
      else
	c.push_back(t);  // not spit, it's a leaf.
    }
  }

  /**
   * get_branch -- get branch point at OR above frag @a x
   *  - may be @a x itself, if @a x is a split
   *  - may be root (frag_t())
   */
  frag_t get_branch(frag_t x) const {
    while (1) {
      if (x == frag_t()) return x;  // root
      if (get_split(x)) return x;   // found it!
      x = x.parent();
    }
  }

  /**
   * get_branch_above -- get a branch point above frag @a x
   *  - may be root (frag_t())
   *  - may NOT be @a x, even if @a x is a split.
   */
  frag_t get_branch_above(frag_t x) const {
    while (1) {
      if (x == frag_t()) return x;  // root
      x = x.parent();
      if (get_split(x)) return x;   // found it!
    }
  }


  /**
   * get_branch_or_leaf -- get branch or leaf point parent for frag @a x
   *  - may be @a x itself, if @a x is a split or leaf
   *  - may be root (frag_t())
   */
  frag_t get_branch_or_leaf(frag_t x) const {
    frag_t branch = get_branch(x);
    int nb = get_split(branch);
    if (nb > 0 &&                                  // if branch is a split, and
	branch.bits() + nb <= x.bits())            // one of the children is or contains x 
      return frag_t(x.value(), branch.bits()+nb);  // then return that child (it's a leaf)
    else
      return branch;
  }

  /**
   * get_leaves_under(x, ls) -- search for any leaves fully contained by x
   */
  template<typename T>
  void get_leaves_under(frag_t x, T& c) const {
    frag_vec_t s;
    s.push_back(get_branch_or_leaf(x));
    while (!s.empty()) {
      frag_t t = s.back();
      s.pop_back();
      if (t.bits() >= x.bits() &&    // if t is more specific than x, and
	  !x.contains(t))            // x does not contain t,
	continue;         // then skip
      int nb = get_split(t);
      if (nb) 
	t.split(nb, s);   // queue up children
      else if (x.contains(t))
	c.push_back(t);  // not spit, it's a leaf.
    }
  }

  /**
   * contains(fg) -- does fragtree contain the specific frag @a x
   */
  bool contains(frag_t x) const {
    frag_vec_t s;
    s.push_back(get_branch(x));
    while (!s.empty()) {
      frag_t t = s.back();
      s.pop_back();
      if (t.bits() >= x.bits() &&  // if t is more specific than x, and
	  !x.contains(t))          // x does not contain t,
	continue;         // then skip 
      int nb = get_split(t);
      if (nb) {
	if (t == x) return false;  // it's split.
	t.split(nb, s);   // queue up children
      } else {
	if (t == x) return true;   // it's there.
      }
    }
    return false;
  }

  /** 
   * operator[] -- map a (hash?) value to a frag
   */
  frag_t operator[](unsigned v) const {
    frag_t t;
    while (1) {
      ceph_assert(t.contains(v));
      int nb = get_split(t);

      // is this a leaf?
      if (nb == 0) return t;  // done.
      
      // pick appropriate child fragment.
      unsigned nway = 1 << nb;
      unsigned i;
      for (i=0; i<nway; i++) {
	frag_t n = t.make_child(i, nb);
	if (n.contains(v)) {
	  t = n;
	  break;
	}
      }
      ceph_assert(i < nway);
    }
  }


  // ---------------
  // modifiers
  void split(frag_t x, int b, bool simplify=true) {
    ceph_assert(is_leaf(x));
    _splits[x] = b;
    
    if (simplify)
      try_assimilate_children(get_branch_above(x));
  }
  void merge(frag_t x, int b, bool simplify=true) {
    ceph_assert(!is_leaf(x));
    ceph_assert(_splits[x] == b);
    _splits.erase(x);

    if (simplify)
      try_assimilate_children(get_branch_above(x));
  }

  /*
   * if all of a given split's children are identically split,
   * then the children can be assimilated.
   */
  void try_assimilate_children(frag_t x) {
    int nb = get_split(x);
    if (!nb) return;
    frag_vec_t children;
    x.split(nb, children);
    int childbits = 0;
    for (auto& frag : children) {
      int cb = get_split(frag);
      if (!cb) return;  // nope.
      if (childbits && cb != childbits) return;  // not the same
      childbits = cb;
    }
    // all children are split with childbits!
    for (auto& frag : children)
      _splits.erase(frag);
    _splits[x] += childbits;
  }

  bool force_to_leaf(CephContext *cct, frag_t x) {
    if (is_leaf(x))
      return false;

    lgeneric_dout(cct, 10) << "force_to_leaf " << x << " on " << _splits << dendl;

    frag_t parent = get_branch_or_leaf(x);
    ceph_assert(parent.bits() <= x.bits());
    lgeneric_dout(cct, 10) << "parent is " << parent << dendl;

    // do we need to split from parent to x?
    if (parent.bits() < x.bits()) {
      int spread = x.bits() - parent.bits();
      int nb = get_split(parent);
      lgeneric_dout(cct, 10) << "spread " << spread << ", parent splits by " << nb << dendl;
      if (nb == 0) {
	// easy: split parent (a leaf) by the difference
	lgeneric_dout(cct, 10) << "splitting parent " << parent << " by spread " << spread << dendl;
	split(parent, spread);
	ceph_assert(is_leaf(x));
	return true;
      }
      ceph_assert(nb > spread);
      
      // add an intermediary split
      merge(parent, nb, false);
      split(parent, spread, false);

      frag_vec_t subs;
      parent.split(spread, subs);
      for (auto& frag : subs) {
	lgeneric_dout(cct, 10) << "splitting intermediate " << frag << " by " << (nb-spread) << dendl;
	split(frag, nb - spread, false);
      }
    }

    // x is now a leaf or split.  
    // hoover up any children.
    frag_vec_t s;
    s.push_back(x);
    while (!s.empty()) {
      frag_t t = s.back();
      s.pop_back();
      int nb = get_split(t);
      if (nb) {
	lgeneric_dout(cct, 10) << "merging child " << t << " by " << nb << dendl;
	merge(t, nb, false);    // merge this point, and
	t.split(nb, s);         // queue up children
      }
    }

    lgeneric_dout(cct, 10) << "force_to_leaf done" << dendl;
    ceph_assert(is_leaf(x));
    return true;
  }

  // encoding
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(_splits, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    decode(_splits, p);
  }
  void encode_nohead(ceph::buffer::list& bl) const {
    using ceph::encode;
    for (compact_map<frag_t,int32_t>::const_iterator p = _splits.begin();
	 p != _splits.end();
	 ++p) {
      encode(p->first, bl);
      encode(p->second, bl);
    }
  }
  void decode_nohead(int n, ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    _splits.clear();
    while (n-- > 0) {
      frag_t f;
      decode(f, p);
      decode(_splits[f], p);
    }
  }

  void print(std::ostream& out) {
    out << "fragtree_t(";
    frag_vec_t s;
    s.push_back(frag_t());
    while (!s.empty()) {
      frag_t t = s.back();
      s.pop_back();
      // newline + indent?
      if (t.bits()) {
	out << std::endl;
	for (unsigned i=0; i<t.bits(); i++) out << ' ';
      }
      int nb = get_split(t);
      if (nb) {
	out << t << " %" << nb;
	t.split(nb, s);   // queue up children
      } else {
	out << t;
      }
    }
    out << ")";
  }

  void dump(ceph::Formatter *f) const {
    f->open_array_section("splits");
    for (auto p = _splits.begin(); p != _splits.end(); ++p) {
      f->open_object_section("split");
      std::ostringstream frag_str;
      frag_str << p->first;
      f->dump_string("frag", frag_str.str());
      f->dump_int("children", p->second);
      f->close_section(); // split
    }
    f->close_section(); // splits
  }

  static void generate_test_instances(std::list<fragtree_t*>& ls) {
    ls.push_back(new fragtree_t);
    ls.push_back(new fragtree_t);
  }
};
WRITE_CLASS_ENCODER(fragtree_t)

inline bool operator==(const fragtree_t& l, const fragtree_t& r) {
  return l._splits == r._splits;
}
inline bool operator!=(const fragtree_t& l, const fragtree_t& r) {
  return l._splits != r._splits;
}

inline std::ostream& operator<<(std::ostream& out, const fragtree_t& ft)
{
  out << "fragtree_t(";
  
  for (compact_map<frag_t,int32_t>::const_iterator p = ft._splits.begin();
       p != ft._splits.end();
       ++p) {
    if (p != ft._splits.begin())
      out << " ";
    out << p->first << "^" << p->second;
  }
  return out << ")";
}

/**
 * fragset_t -- a set of fragments
 */
class fragset_t {
  std::set<frag_t> _set;

public:
  const std::set<frag_t> &get() const { return _set; }
  std::set<frag_t>::const_iterator begin() const { return _set.begin(); }
  std::set<frag_t>::const_iterator end() const { return _set.end(); }

  bool empty() const { return _set.empty(); }

  bool contains(frag_t f) const {
    while (1) {
      if (_set.count(f)) return true;
      if (f.bits() == 0) return false;
      f = f.parent();
    }
  }

  void clear() {
    _set.clear();
  }

  void insert_raw(frag_t f){
    _set.insert(f);
  }
  void insert(frag_t f) {
    _set.insert(f);
    simplify();
  }

  void simplify() {
    auto it = _set.begin();
    while (it != _set.end()) {
      if (!it->is_root() &&
	  _set.count(it->get_sibling())) {
	_set.erase(it->get_sibling());
	auto ret = _set.insert(it->parent());
	_set.erase(it);
	it = ret.first;
      } else {
	++it;
      }
    }
  }

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(_set, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    ceph::decode(_set, p);
  }
};
WRITE_CLASS_ENCODER(fragset_t)


inline std::ostream& operator<<(std::ostream& out, const fragset_t& fs) 
{
  return out << "fragset_t(" << fs.get() << ")";
}

#endif
