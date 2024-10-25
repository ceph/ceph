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

#include "include/frag.h"
#include "include/types.h" // for operator<<(std::set)
#include "common/debug.h"
#include "common/Formatter.h"

#include <iostream>
#include <sstream>

bool frag_t::parse(const char *s) {
  int pvalue, pbits;
  int r = sscanf(s, "%x/%d", &pvalue, &pbits);
  if (r == 2) {
    *this = frag_t(pvalue, pbits);
    return true;
  }
  return false;
}

void frag_t::encode(ceph::buffer::list& bl) const {
  ceph::encode_raw(_enc, bl);
}

void frag_t::decode(ceph::buffer::list::const_iterator& p) {
  __u32 v;
  ceph::decode_raw(v, p);
  _enc = v;
}

void frag_t::dump(ceph::Formatter *f) const {
  f->dump_unsigned("value", value());
  f->dump_unsigned("bits", bits());
}

void frag_t::generate_test_instances(std::list<frag_t*>& ls) {
  ls.push_back(new frag_t);
  ls.push_back(new frag_t(10, 2));
  ls.push_back(new frag_t(11, 3));
}

std::ostream& operator<<(std::ostream& out, const frag_t& hb)
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

bool fragtree_t::force_to_leaf(CephContext *cct, frag_t x) {
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

void fragtree_t::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  encode(_splits, bl);
}

void fragtree_t::decode(ceph::buffer::list::const_iterator& p) {
  using ceph::decode;
  decode(_splits, p);
}

void fragtree_t::encode_nohead(ceph::buffer::list& bl) const {
  using ceph::encode;
  for (compact_map<frag_t,int32_t>::const_iterator p = _splits.begin();
       p != _splits.end();
       ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}

void fragtree_t::decode_nohead(int n, ceph::buffer::list::const_iterator& p) {
  using ceph::decode;
  _splits.clear();
  while (n-- > 0) {
    frag_t f;
    decode(f, p);
    decode(_splits[f], p);
  }
}

void fragtree_t::print(std::ostream& out) {
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

void fragtree_t::dump(ceph::Formatter *f) const {
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

void fragtree_t::generate_test_instances(std::list<fragtree_t*>& ls) {
  ls.push_back(new fragtree_t);
  ls.push_back(new fragtree_t);
}

std::ostream& operator<<(std::ostream& out, const fragtree_t& ft)
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

void fragset_t::simplify() {
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

void fragset_t::encode(ceph::buffer::list& bl) const {
  ceph::encode(_set, bl);
}

void fragset_t::decode(ceph::buffer::list::const_iterator& p) {
  ceph::decode(_set, p);
}

std::ostream& operator<<(std::ostream& out, const fragset_t& fs) 
{
  return out << "fragset_t(" << fs.get() << ")";
}
