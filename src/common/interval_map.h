// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef INTERVAL_MAP_H
#define INTERVAL_MAP_H

#include "include/interval_set.h"

template <typename K, typename V, typename S>
/**
 * interval_map
 *
 * Maps intervals to values.  Erasing or inserting over an existing
 * range will use S::operator() to split any overlapping existing
 * values.
 *
 * Surprisingly, boost/icl/interval_map doesn't seem to be appropriate
 * for this use case.  The aggregation concept seems to assume
 * commutativity, which doesn't work if we want more recent insertions
 * to overwrite previous ones.
 */
class interval_map {
  S s;
  using map = std::map<K, std::pair<K, V> >;
  using mapiter = typename std::map<K, std::pair<K, V> >::iterator;
  using cmapiter = typename std::map<K, std::pair<K, V> >::const_iterator;
  map m;
  std::pair<mapiter, mapiter> get_range(K off, K len) {
    // fst is first iterator with end after off (may be end)
    auto fst = m.upper_bound(off);
    if (fst != m.begin())
      --fst;
    if (fst != m.end() && off >= (fst->first + fst->second.first))
      ++fst;

    // lst is first iterator with start after off + len (may be end)
    auto lst = m.lower_bound(off + len);
    return std::make_pair(fst, lst);
  }
  std::pair<cmapiter, cmapiter> get_range(K off, K len) const {
    // fst is first iterator with end after off (may be end)
    auto fst = m.upper_bound(off);
    if (fst != m.begin())
      --fst;
    if (fst != m.end() && off >= (fst->first + fst->second.first))
      ++fst;

    // lst is first iterator with start after off + len (may be end)
    auto lst = m.lower_bound(off + len);
    return std::make_pair(fst, lst);
  }
  void try_merge(mapiter niter) {
    if (niter != m.begin()) {
      auto prev = niter;
      prev--;
      if (prev->first + prev->second.first == niter->first &&
	  s.can_merge(prev->second.second, niter->second.second)) {
	V n = s.merge(
	  std::move(prev->second.second),
	  std::move(niter->second.second));
	K off = prev->first;
	K len = niter->first + niter->second.first - off;
	niter++;
	m.erase(prev, niter);
	auto p = m.insert(
	  std::make_pair(
	    off,
	    std::make_pair(len, std::move(n))));
	assert(p.second);
	niter = p.first;
      }
    }
    auto next = niter;
    next++;
    if (next != m.end() &&
	niter->first + niter->second.first == next->first &&
	s.can_merge(niter->second.second, next->second.second)) {
      V n = s.merge(
	std::move(niter->second.second),
	std::move(next->second.second));
      K off = niter->first;
      K len = next->first + next->second.first - off;
      next++;
      m.erase(niter, next);
      auto p = m.insert(
	std::make_pair(
	  off,
	  std::make_pair(len, std::move(n))));
      assert(p.second);
    }
  }
public:
  interval_map intersect(K off, K len) const {
    interval_map ret;
    auto limits = get_range(off, len);
    for (auto i = limits.first; i != limits.second; ++i) {
      K o = i->first;
      K l = i->second.first;
      V v = i->second.second;
      if (o < off) {
	V p = v;
	l -= (off - o);
	v = s.split(off - o, l, p);
	o = off;
      }
      if ((o + l) > (off + len)) {
	V p = v;
	l -= (o + l) - (off + len);
	v = s.split(0, l, p);
      }
      ret.insert(o, l, v);
    }
    return ret;
  }
  void clear() {
    m.clear();
  }
  void erase(K off, K len) {
    if (len == 0)
      return;
    auto range = get_range(off, len);
    std::vector<
      std::pair<
	K,
	std::pair<K, V>
	>> to_insert;
    for (auto i = range.first; i != range.second; ++i) {
      if (i->first < off) {
	to_insert.emplace_back(
	  std::make_pair(
	    i->first,
	    std::make_pair(
	      off - i->first,
	      s.split(0, off - i->first, i->second.second))));
      }
      if ((off + len) < (i->first + i->second.first)) {
	K nlen = (i->first + i->second.first) - (off + len);
	to_insert.emplace_back(
	  std::make_pair(
	    off + len,
	    std::make_pair(
	      nlen,
	      s.split(i->second.first - nlen, nlen, i->second.second))));
      }
    }
    m.erase(range.first, range.second);
    m.insert(to_insert.begin(), to_insert.end());
  }
  void insert(K off, K len, V &&v) {
    assert(len > 0);
    assert(len == s.length(v));
    erase(off, len);
    auto p = m.insert(make_pair(off, std::make_pair(len, std::forward<V>(v))));
    assert(p.second);
    try_merge(p.first);
  }
  void insert(interval_map &&other) {
    for (auto i = other.m.begin();
	 i != other.m.end();
	 other.m.erase(i++)) {
      insert(i->first, i->second.first, std::move(i->second.second));
    }
  }
  void insert(K off, K len, const V &v) {
    assert(len > 0);
    assert(len == s.length(v));
    erase(off, len);
    auto p = m.insert(make_pair(off, std::make_pair(len, v)));
    assert(p.second);
    try_merge(p.first);
  }
  void insert(const interval_map &other) {
    for (auto &&i: other) {
      insert(i.get_off(), i.get_len(), i.get_val());
    }
  }
  bool empty() const {
    return m.empty();
  }
  interval_set<K> get_interval_set() const {
    interval_set<K> ret;
    for (auto &&i: *this) {
      ret.insert(i.get_off(), i.get_len());
    }
    return ret;
  }
  class const_iterator {
    cmapiter it;
    const_iterator(cmapiter &&it) : it(std::move(it)) {}
    const_iterator(const cmapiter &it) : it(it) {}

    friend class interval_map;
  public:
    const_iterator(const const_iterator &) = default;
    const_iterator &operator=(const const_iterator &) = default;

    const_iterator &operator++() {
      ++it;
      return *this;
    }
    const_iterator operator++(int) {
      return const_iterator(it++);
    }
    const_iterator &operator--() {
      --it;
      return *this;
    }
    const_iterator operator--(int) {
      return const_iterator(it--);
    }
    bool operator==(const const_iterator &rhs) const {
      return it == rhs.it;
    }
    bool operator!=(const const_iterator &rhs) const {
      return it != rhs.it;
    }
    K get_off() const {
      return it->first;
    }
    K get_len() const {
      return it->second.first;
    }
    const V &get_val() const {
      return it->second.second;
    }
    const_iterator &operator*() {
      return *this;
    }
  };
  const_iterator begin() const {
    return const_iterator(m.begin());
  }
  const_iterator end() const {
    return const_iterator(m.end());
  }
  std::pair<const_iterator, const_iterator> get_containing_range(
    K off,
    K len) const {
    auto rng = get_range(off, len);
    return std::make_pair(const_iterator(rng.first), const_iterator(rng.second));
  }
  unsigned ext_count() const {
    return m.size();
  }
  bool operator==(const interval_map &rhs) const {
    return m == rhs.m;
  }

  std::ostream &print(std::ostream &out) const {
    bool first = true;
    out << "{";
    for (auto &&i: *this) {
      if (first) {
	first = false;
      } else {
	out << ",";
      }
      out << i.get_off() << "~" << i.get_len() << "("
	  << s.length(i.get_val()) << ")";
    }
    return out << "}";
  }
};

template <typename K, typename V, typename S>
std::ostream &operator<<(std::ostream &out, const interval_map<K, V, S> &m) {
  return m.print(out);
}

#endif
