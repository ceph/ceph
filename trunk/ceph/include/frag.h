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

#ifndef __FRAG_H
#define __FRAG_H

#include <stdint.h>
#include <map>
#include <list>
#include <iostream>
#include "buffer.h"
#include "encodable.h"

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
 *    the right-most (least significant) bits.  just because it's more intutive, and
 *    matches the network/netmask concept.
 */

typedef uint32_t _frag_t;

class frag_t {
  /* encoded value.
   *  8 upper bits = "bits"
   * 24 lower bits = "value"
   */
  _frag_t _enc;  
  
 public:
  frag_t() : _enc(0) { }
  frag_t(unsigned v, unsigned b) : _enc((b << 24) + 
					(v & (0xffffffffULL >> (32-b)))) { }
  frag_t(_frag_t e) : _enc(e) { }

  // constructors
  void from_unsigned(unsigned e) { _enc = e; }
  
  // accessors
  unsigned value() const { return _enc & 0xffffff; }
  unsigned bits() const { return _enc >> 24; }
  unsigned mask() const { return 0xffffffffULL >> (32-bits()); }

  operator _frag_t() const { return _enc; }

  // tests
  bool contains(unsigned v) const {
    return (v & mask()) == value();
  }
  bool contains(frag_t sub) const {
    return (sub.bits() >= bits() &&             // they at least as specific as us,
	    (sub.value() & mask()) == value()); // and they are contained by us.
  }
  bool is_root() const { 
    return bits() == 0; 
  }
  frag_t parent() const {
    assert(bits() > 0);
    return frag_t(value() & (mask() >> 1), bits()-1);
  }

  // splitting
  void split(int nb, std::list<frag_t>& fragments) const {
    assert(nb > 0);
    unsigned nway = 1 << nb;
    for (unsigned i=0; i<nway; i++) 
      fragments.push_back( frag_t(value() | (i << bits()), 
				  bits()+nb) );
  }

  // binary splitting
  frag_t get_sibling() const {
    assert(!is_root());
    return frag_t(_enc ^ (1 << (bits()-1)));
  }
  bool is_left() const {
    return 
      bits() > 0 &&
      (value() & (1 << (bits()-1)) == 0);
  }
  bool is_right() const {
    return 
      bits() > 0 &&
      (value() & (1 << (bits()-1)) == 1);
  }
  frag_t left_child() const {
    return frag_t(value(), bits()+1);
  }
  frag_t right_child() const {
    return frag_t(value() | (1<<bits()), bits()+1);
  }

  // sequencing
  bool is_leftmost() const {
    return value() == 0;
  }
  bool is_rightmost() const {
    return value() == mask();
  }
  frag_t next() const {
    assert(!is_rightmost());
    return frag_t(value() + 1, bits());
  }
};

inline std::ostream& operator<<(std::ostream& out, frag_t hb)
{
  return out << std::hex << hb.value() << std::dec << "/" << hb.bits();
}


/**
 * fragtree_t -- partition an entire namespace into one or more frag_t's. 
 */
class fragtree_t {
  // pairs <f, b>:
  //  frag_t f is split by b bits.
  //  if child frag_t does not appear, it is not split.
public:
  std::map<frag_t,int32_t> _splits;  

public:
  // -------------
  // basics
  void swap(fragtree_t& other) {
    _splits.swap(other._splits);
  }

  // -------------
  // accessors
  bool empty() { 
    return _splits.empty();
  }
  int get_split(const frag_t hb) const {
    std::map<frag_t,int32_t>::const_iterator p = _splits.find(hb);
    if (p == _splits.end())
      return 0;
    else
      return p->second;
  }

  
  bool is_leaf(frag_t x) const {
    std::list<frag_t> ls;
    get_leaves_under(x, ls);
    //cout << "is_leaf(" << x << ") -> " << ls << std::endl;
    if (!ls.empty() &&
	ls.front() == x &&
	ls.size() == 1)
      return true;
    return false;
  }

  /**
   * get_leaves -- list all leaves
   */
  void get_leaves(std::list<frag_t>& ls) const {
    return get_leaves_under_split(frag_t(), ls);
  }

  /**
   * get_leaves_under_split -- list all leaves under a known split point (or root)
   */
  void get_leaves_under_split(frag_t under, std::list<frag_t>& ls) const {
    std::list<frag_t> q;
    q.push_back(under);
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      int nb = get_split(t);
      if (nb) 
	t.split(nb, q);   // queue up children
      else
	ls.push_back(t);  // not spit, it's a leaf.
    }
  }

  /**
   * get_branch -- get branch point at OR above frag @x
   *  - may be @x itself, if @x is a split
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
   * get_branch_above -- get a branch point above frag @x
   *  - may be root (frag_t())
   *  - may NOT be @x, even if @x is a split.
   */
  frag_t get_branch_above(frag_t x) const {
    while (1) {
      if (x == frag_t()) return x;  // root
      x = x.parent();
      if (get_split(x)) return x;   // found it!
    }
  }


  /**
   * get_branch_or_leaf -- get branch or leaf point parent for frag @x
   *  - may be @x itself, if @x is a split or leaf
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
  void get_leaves_under(frag_t x, std::list<frag_t>& ls) const {
    std::list<frag_t> q;
    q.push_back(get_branch(x));
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      if (t.bits() >= x.bits() &&    // if t is more specific than x, and
	  !x.contains(t))            // x does not contain t,
	continue;         // then skip
      int nb = get_split(t);
      if (nb) 
	t.split(nb, q);   // queue up children
      else 
	ls.push_back(t);  // not spit, it's a leaf.
    }
  }

  /**
   * contains(fg) -- does fragtree contain the specific frag @x
   */
  bool contains(frag_t x) const {
    std::list<frag_t> q;
    q.push_back(get_branch(x));
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      if (t.bits() >= x.bits() &&  // if t is more specific than x, and
	  !x.contains(t))          // x does not contain t,
	continue;         // then skip 
      int nb = get_split(t);
      if (nb) {
	if (t == x) return false;  // it's split.
	t.split(nb, q);   // queue up children
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
      assert(t.contains(v));
      int nb = get_split(t);

      // is this a leaf?
      if (nb == 0) return t;  // done.
      
      // pick appropriate child fragment.
      unsigned nway = 1 << nb;
      unsigned i;
      for (i=0; i<nway; i++) {
	frag_t n(t.value() | (i << t.bits()), 
		 t.bits()+nb);
	if (n.contains(v)) {
	  t = n;
	  break;
	}
      }
      assert(i < nway);
    }
  }


  // ---------------
  // modifiers
  void split(frag_t x, int b) {
    assert(is_leaf(x));
    _splits[x] = b;
    
    // simplify?
    try_assimilate_children(get_branch_above(x));
  }
  void merge(frag_t x, int b) {
    assert(!is_leaf(x));
    assert(_splits[x] == b);
    _splits.erase(x);

    // simplify?
    try_assimilate_children(get_branch_above(x));
  }

  /*
   * if all of a given split's children are identically split,
   * then the children can be assimilated.
   */
  void try_assimilate_children(frag_t x) {
    int nb = get_split(x);
    if (!nb) return;
    std::list<frag_t> children;
    x.split(nb, children);
    int childbits = 0;
    for (std::list<frag_t>::iterator p = children.begin();
	 p != children.end();
	 ++p) {
      int cb = get_split(*p);
      if (!cb) return;  // nope.
      if (childbits && cb != childbits) return;  // not the same
      childbits = cb;
    }
    // all children are split with childbits!
    for (std::list<frag_t>::iterator p = children.begin();
	 p != children.end();
	 ++p)
      _splits.erase(*p);
    _splits[x] += childbits;
  }

  bool force_to_leaf(frag_t x) {
    if (is_leaf(x))
      return false;

    cout << "force_to_leaf " << x << " on " << _splits << std::endl;

    frag_t parent = get_branch_or_leaf(x);
    assert(parent.bits() <= x.bits());
    cout << "parent is " << parent << std::endl;

    // do we need to split from parent to x?
    if (parent.bits() < x.bits()) {
      int spread = x.bits() - parent.bits();
      int nb = get_split(parent);
      cout << "spread " << spread << ", parent splits by " << nb << std::endl;
      if (nb == 0) {
	// easy: split parent (a leaf) by the difference
	cout << "splitting parent " << parent << " by spread " << spread << std::endl;
	split(parent, spread);
	assert(is_leaf(x));
	return true;
      }
      assert(nb > spread);
      
      // add an intermediary split
      merge(parent, nb);
      split(parent, spread);

      std::list<frag_t> subs;
      parent.split(spread, subs);
      for (std::list<frag_t>::iterator p = subs.begin();
	   p != subs.end();
	   ++p) {
	cout << "splitting intermediate " << *p << " by " << (nb-spread) << std::endl;
	split(*p, nb - spread);
      }
    }

    // x is now a leaf or split.  
    // hoover up any children.
    std::list<frag_t> q;
    q.push_back(x);
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      int nb = get_split(t);
      if (nb) {
	cout << "merging child " << t << " by " << nb << std::endl;
	merge(t, nb);         // merge this point, and
	t.split(nb, q);   // queue up children
      }
    }

    cout << "force_to_leaf done" << std::endl;
    assert(is_leaf(x));
    return true;
  }

  // verify that we describe a legal partition of the namespace.
  void verify() const {
    std::map<frag_t,int32_t> copy;
    std::list<frag_t> q;
    q.push_back(frag_t());
    
    while (1) {
      frag_t cur = q.front();
      q.pop_front();
      int b = get_split(cur);
      if (!b) continue;
      copy[cur] = b;
      cur.split(b, q);
    }
    
    assert(copy == _splits);	
  }
  
  // encoding
  void _encode(bufferlist& bl) const {
    ::_encode(_splits, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    ::_decode(_splits, bl, off);
  }
  void _decode(bufferlist::iterator& p) {
    ::_decode_simple(_splits, p);
  }

  void print(std::ostream& out) {
    out << "fragtree_t(";
    std::list<frag_t> q;
    q.push_back(frag_t());
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      // newline + indent?
      if (t.bits()) {
	out << std::endl;
	for (unsigned i=0; i<t.bits(); i++) out << ' ';
      }
      int nb = get_split(t);
      if (nb) {
	out << t << " %" << nb;
	t.split(nb, q);   // queue up children
      } else {
	out << t;
      }
    }
    out << ")";
  }
};

inline std::ostream& operator<<(std::ostream& out, fragtree_t& ft)
{
  out << "fragtree_t(";
  
  if (0) {
    std::list<frag_t> q;
    q.push_back(frag_t());
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      int nb = ft.get_split(t);
      if (nb) {
	if (t.bits()) out << ' ';
	out << t << '%' << nb;
	t.split(nb, q);   // queue up children
      }
    }
  }
  if (1) {
    std::list<frag_t> leaves;
    ft.get_leaves(leaves);
    out << leaves;
  }
  return out << ")";
}


/**
 * fragset_t -- a set of fragments
 */
class fragset_t {
  std::set<frag_t> _set;

public:
  std::set<frag_t> &get() { return _set; }
  std::set<frag_t>::iterator begin() { return _set.begin(); }
  std::set<frag_t>::iterator end() { return _set.end(); }

  bool empty() const { return _set.empty(); }

  bool contains(frag_t f) const {
    while (1) {
      if (_set.count(f)) return true;
      if (f.bits() == 0) return false;
      f = f.parent();
    }
  }
  
  void insert(frag_t f) {
    _set.insert(f);
    simplify();
  }

  void simplify() {
    while (1) {
      bool clean = true;
      std::set<frag_t>::iterator p = _set.begin();
      while (p != _set.end()) {
	if (!p->is_root() &&
	    _set.count(p->get_sibling())) {
	  _set.erase(p->get_sibling());
	  _set.insert(p->parent());
	  _set.erase(p++);
	  clean = false;
	} else {
	  p++;
	}
      }
      if (clean)
	break;
    }
  }
};

inline std::ostream& operator<<(std::ostream& out, fragset_t& fs) 
{
  return out << "fragset_t(" << fs.get() << ")";
}

#endif
