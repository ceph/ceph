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
#include <initializer_list>

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
template <typename K, typename V, typename S, template<typename, typename, typename ...> class C = std::map>
class interval_map {
  S s;
  using Map = C<K, std::pair<K, V> >;
  using Mapiter = typename Map::iterator;
  using Cmapiter = typename Map::const_iterator;
  Map m;
  std::pair<Mapiter, Mapiter> get_range(K off, K len) {
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
  std::pair<Cmapiter, Cmapiter> get_range(K off, K len) const {
    // fst is first iterator with end after off (may be end)
    auto fst = get_range_fst(off);

    // lst is first iterator with start after off + len (may be end)
    auto lst = m.lower_bound(off + len);
    return std::make_pair(fst, lst);
  }
  Cmapiter get_range_fst(K off) const {
    // fst is first iterator with end after off (may be end)
    auto fst = m.upper_bound(off);
    if (fst != m.begin())
      --fst;
    if (fst != m.end() && off >= (fst->first + fst->second.first))
      ++fst;

    return fst;
  }
  void try_merge(Mapiter niter) {
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
	ceph_assert(p.second);
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
      ceph_assert(p.second);
    }
  }
public:
  interval_map() = default;
  interval_map(std::initializer_list<typename Map::value_type> l) {
    for (auto& v : l) {
      insert(v.first, v.second.first, v.second.second);
    }
  }

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
    ceph_assert(len > 0);
    ceph_assert(len == s.length(v));
    erase(off, len);
    auto p = m.insert(make_pair(off, std::make_pair(len, std::forward<V>(v))));
    ceph_assert(p.second);
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
    ceph_assert(len > 0);
    ceph_assert(len == s.length(v));
    erase(off, len);
    auto p = m.insert(make_pair(off, std::make_pair(len, v)));
    ceph_assert(p.second);
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
  interval_set<K, C> get_interval_set() const {
    interval_set<K, C> ret;
    for (auto &&i: *this) {
      ret.insert(i.get_off(), i.get_len());
    }
    return ret;
  }
  template<template<typename, typename, typename ...> class ISC = std::map, bool strict = true>
  void to_interval_set(interval_set<K, ISC, strict> &set) const {
    for (auto &&i: *this) {
      set.insert(i.get_off(), i.get_len());
    }
  }
  class const_iterator {
    Cmapiter it;
    const_iterator(Cmapiter &&it) : it(std::move(it)) {}
    const_iterator(const Cmapiter &it) : it(it) {}

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
    constexpr bool contains(K _off, K _len) const {
      K off = get_off();
      K len = get_len();
      return off <= _off && _off + _len <= off + len;
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

  const_iterator get_lower_range(
      K off,
      K len) const {
    return const_iterator(get_range_fst(off));
  }
  K get_start_off() const
  {
    auto i = m.begin();
    ceph_assert(i != m.end());
    return i->first;
  }
  K get_end_off() const
  {
    auto i = m.rbegin();
    ceph_assert(i != m.rend());
    return i->first + i->second.first;
  }
  bool contains(K off, K len) const {
    auto it = get_range_fst(off);
    if (it == m.end()) return false;

    K _off = it->first;
    K _len = it->second.first;
    return _off <= off && _off + _len >= off + len;
  }
  unsigned ext_count() const {
    return m.size();
  }
  bool operator==(const interval_map &rhs) const {
    return m == rhs.m;
  }

  void print(std::ostream &os) const {
    bool first = true;
    os << "{";
    for (auto &&i: *this) {
      if (first) {
	first = false;
      } else {
	os << ",";
      }
      os << i.get_off() << "~" << i.get_len() << "("
	  << s.length(i.get_val()) << ")";
    }
    os << "}";
  }

  std::string fmt_print() const
  requires fmt::formattable<K> {
    std::string str = "{";
    bool first = true;
    for (auto &&i: *this) {
      if (first) {
        first = false;
      } else {
        str += ",";
      }
      str += fmt::format("{}~{}({})", i.get_off(), i.get_len(),
        s.length(i.get_val()));
    }
    str += "}";
    return str;
  }
};

// make sure fmt::range would not try (and fail) to treat interval_map as a range
template <typename K, typename V, typename S, template<typename, typename, typename ...> class C>
struct fmt::is_range<interval_map<K, V, S, C>, char> : std::false_type {};

#endif
