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

#pragma once

#include <deque>
#include <list>
#include <set>
#include <span>
#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>
#include "boost/tuple/tuple.hpp"
#include <map>
#include <vector>
#include <optional>
#include <iomanip>
#include <iosfwd>
#include <unordered_map>
#include <unordered_set>

#include "common/convenience.h" // for ceph::for_each()

// -- io helpers --

// Forward declare all the I/O helpers so strict ADL can find them in
// the case of containers of containers. I'm tempted to abstract this
// stuff using template templates like I did for denc.

namespace std {
template<class A, class B>
inline std::ostream& operator<<(std::ostream&out, const std::pair<A,B>& v);
template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::vector<A,Alloc>& v);
template<class T, std::size_t Extent>
inline std::ostream& operator<<(std::ostream& out, const std::span<T, Extent>& s);
template<class A, std::size_t N, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::small_vector<A,N,Alloc>& v);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::deque<A,Alloc>& v);
template<typename... Ts>
inline std::ostream& operator<<(std::ostream& out, const std::tuple<Ts...> &t);
template<typename T>
inline std::ostream& operator<<(std::ostream& out, const std::optional<T> &t);
template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::list<A,Alloc>& ilist);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::set<A, Comp, Alloc>& iset);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::unordered_set<A, Comp, Alloc>& iset);
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multiset<A,Comp,Alloc>& iset);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::map<A,B,Comp,Alloc>& m);
template<class A, class B, class Hash, class KeyEqual>
inline std::ostream& operator<<(std::ostream& out, const std::unordered_map<A,B,Hash, KeyEqual>& m);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multimap<A,B,Comp,Alloc>& m);
}

namespace boost {
template<typename... Ts>
inline std::ostream& operator<<(std::ostream& out, const boost::tuple<Ts...> &t);

namespace container {
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_set<A, Comp, Alloc>& iset);
template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_map<A, B, Comp, Alloc>& iset);
}
}

namespace std {
template<class A, class B>
inline std::ostream& operator<<(std::ostream& out, const std::pair<A,B>& v) {
  return out << v.first << "," << v.second;
}

template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::vector<A,Alloc>& v) {
  bool first = true;
  out << "[";
  for (const auto& p : v) {
    if (!first) out << ",";
    out << p;
    first = false;
  }
  out << "]";
  return out;
}

template<class T, std::size_t Extent>
inline std::ostream& operator<<(std::ostream& out, const std::span<T, Extent>& s) {
  bool first = true;
  out << "[";
  for (const auto& p : s) {
    if (!first) out << ",";
    out << p;
    first = false;
  }
  out << "]";
  return out;
}

template<class A, std::size_t N, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::small_vector<A,N,Alloc>& v) {
  bool first = true;
  out << "[";
  for (const auto& p : v) {
    if (!first) out << ",";
    out << p;
    first = false;
  }
  out << "]";
  return out;
}

template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::deque<A,Alloc>& v) {
  out << "<";
  for (auto p = v.begin(); p != v.end(); ++p) {
    if (p != v.begin()) out << ",";
    out << *p;
  }
  out << ">";
  return out;
}

template<typename... Ts>
inline std::ostream& operator<<(std::ostream& out, const std::tuple<Ts...> &t) {
  auto f = [n = sizeof...(Ts), i = 0U, &out](const auto& e) mutable {
    out << e;
    if (++i != n)
      out << ",";
  };
  ceph::for_each(t, f);
  return out;
}

// Mimics boost::optional
template<typename T>
inline std::ostream& operator<<(std::ostream& out, const std::optional<T> &t) {
  if (!t)
    out << "--" ;
  else
    out << ' ' << *t ;
  return out;
}

template<class A, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::list<A,Alloc>& ilist) {
  for (auto it = ilist.begin();
       it != ilist.end();
       ++it) {
    if (it != ilist.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::set<A, Comp, Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::unordered_set<A, Comp, Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multiset<A,Comp,Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::map<A,B,Comp,Alloc>& m)
{
  out << "{";
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}

template <class A, class B, class Hash, class KeyEqual>
inline std::ostream&
operator<<(std::ostream& out,
	   const std::unordered_map<A, B, Hash, KeyEqual>& m)
{
  out << "{";
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}

template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const std::multimap<A,B,Comp,Alloc>& m)
{
  out << "{{";
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}}";
  return out;
}

} // namespace std

namespace boost {
namespace tuples {
template<typename A, typename B, typename C>
inline std::ostream& operator<<(std::ostream& out, const boost::tuples::tuple<A, B, C> &t) {
  return out << boost::get<0>(t) << ","
	     << boost::get<1>(t) << ","
	     << boost::get<2>(t);
}
}
namespace container {
template<class A, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_set<A, Comp, Alloc>& iset) {
  for (auto it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A, class B, class Comp, class Alloc>
inline std::ostream& operator<<(std::ostream& out, const boost::container::flat_map<A, B, Comp, Alloc>& m) {
  for (auto it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  return out;
}
}
} // namespace boost
