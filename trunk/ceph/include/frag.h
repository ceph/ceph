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
#include "buffer.h"

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
 * presumably we are partitioning the output of a (perhaps specialized) hash 
 * function.
 * 
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
  frag_t(unsigned v, unsigned b) : _enc((b << 24) + v) { }
  frag_t(_frag_t e) : _enc(e) { }

  // constructors
  void from_unsigned(unsigned e) { _enc = e; }
  
  // accessors
  unsigned value() const { return _enc & 0xffffff; }
  unsigned bits() const { return _enc >> 24; }
  unsigned mask() const { return 0xffffffff >> (32-bits()); }
  operator _frag_t() const { return _enc; }

  // tests
  bool contains(unsigned v) const {
    return (v & mask()) == value();
  }
  bool contains(frag_t sub) const {
    return (sub.bits() >= bits() &&             // they are more specific than us,
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
  frag_t left_half() const {
    return frag_t(value(), bits()+1);
  }
  frag_t right_half() const {
    return frag_t(value() | (1<<bits()), bits()+1);
  }
  void split(int nb, list<frag_t>& fragments) const {
    assert(nb > 0);
    unsigned nway = 1 << (nb-1);
    for (unsigned i=0; i<nway; i++) 
      fragments.push_back( frag_t(value() | (i << (bits()+nb-1)), bits()+nb) );
  }
};

inline ostream& operator<<(ostream& out, frag_t& hb)
{
  return out << hex << hb.value() << dec << "/" << hb.bits();
}


/**
 * fragtree_t
 *
 * partition for an entire namespace.
 */
class fragtree_t {
  // pairs <f, b>:
  //  frag_t f is split by b bits.
  //  if child frag_t does not appear, it is not split.
  std::map<frag_t,__int32_t> _splits;  

 public:
  // accessors
  bool empty() { 
    return _splits.empty();
  }
  int get_split(const frag_t hb) const {
    std::map<frag_t,__int32_t>::const_iterator p = _splits.find(hb);
    if (p == _splits.end())
      return 0;
    else
      return p->second;
  }
  void get_leaves(list<frag_t>& ls) const {
    get_leaves(frag_t(), ls);
  }
  void get_leaves(frag_t under, list<frag_t>& ls) const {
    list<frag_t> q;
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
  bool contains(frag_t fg) const {
    list<frag_t> q;
    q.push_back(frag_t());
    while (!q.empty()) {
      frag_t t = q.front();
      q.pop_front();
      int nb = get_split(t);
      if (nb) {
	if (t == fg) return false;  // it's split.
	t.split(nb, q);   // queue up children
      } else {
	if (t == fg) return true;   // it's there.
      }
    }
    return false;
  }

  frag_t operator[](unsigned v) const {
    frag_t t;
    while (1) {
      assert(t.contains(v));
      int nb = get_split(t);

      // is this a leaf?
      if (nb == 0) return t;  // done.
      
      // pick appropriate child fragment.
      unsigned nway = 1 << (nb-1);
      unsigned i;
      for (i=0; i<nway; i++) {
	frag_t n(t.value() | (i << (t.bits()+nb-1)), t.bits()+nb);
	if (n.contains(v)) {
	  t = n;
	  break;
	}
      }
      assert(i < nway);
    }
  }
  
  // modifiers
  void swap(fragtree_t& other) {
    _splits.swap(other._splits);
  }
  void split(frag_t hb, int b) {
    assert(_splits.count(hb) == 0);
    _splits[hb] = b;
  }
  void merge(frag_t hb, int b) {
    assert(_splits[hb] == b);
    _splits.erase(hb);
  }

  // verify that we describe a legal partition of the namespace.
  void verify() const {
    std::map<frag_t,__int32_t> copy;
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
  void _encode(bufferlist& bl) {
    ::_encode(_splits, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    ::_decode(_splits, bl, off);
  }
};

inline ostream& operator<<(ostream& out, fragtree_t& ft)
{
  out << "fragtree_t(";
  
  bool first = true;
  list<frag_t> q;
  q.push_back(frag_t());
  while (!q.empty()) {
    frag_t t = q.front();
    q.pop_front();
    int nb = ft.get_split(t);
    if (nb) {
      if (first) 
	first = false;
      else
	out << ' ';
      out << t << '%' << nb;
      t.split(nb, q);   // queue up children
    }
  }
  return out << ")";
}

#endif
