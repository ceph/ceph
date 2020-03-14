// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RADOS_DECODABLE_HPP
#define RADOS_DECODABLE_HPP

#include <cstdint>
#include <cstdlib>
#include <string>
#include <iostream>
#include <tuple>
#include <utility>
#include <vector>

namespace RADOS {
struct Entry {
  std::string nspace;
  std::string oid;
  std::string locator;

  Entry() {}
  Entry(std::string nspace, std::string oid, std::string locator) :
    nspace(std::move(nspace)), oid(std::move(oid)), locator(locator) {}
};
inline bool operator ==(const Entry& l, const Entry r) {
  return std::tie(l.nspace, l.oid, l.locator) ==
    std::tie(r.nspace, r.oid, r.locator);
}
inline bool operator !=(const Entry& l, const Entry r) {
  return std::tie(l.nspace, l.oid, l.locator) !=
    std::tie(r.nspace, r.oid, r.locator);
}
inline bool operator <(const Entry& l, const Entry r) {
  return std::tie(l.nspace, l.oid, l.locator) <
    std::tie(r.nspace, r.oid, r.locator);
}
inline bool operator <=(const Entry& l, const Entry r) {
  return std::tie(l.nspace, l.oid, l.locator) <=
    std::tie(r.nspace, r.oid, r.locator);
}
inline bool operator >=(const Entry& l, const Entry r) {
  return std::tie(l.nspace, l.oid, l.locator) >=
    std::tie(r.nspace, r.oid, r.locator);
}
inline bool operator >(const Entry& l, const Entry r) {
  return std::tie(l.nspace, l.oid, l.locator) >
    std::tie(r.nspace, r.oid, r.locator);
}

inline std::ostream& operator <<(std::ostream& out, const Entry& entry) {
  if (!entry.nspace.empty())
    out << entry.nspace << '/';
  out << entry.oid;
  if (!entry.locator.empty())
    out << '@' << entry.locator;
  return out;
}

struct CloneInfo {
  uint64_t cloneid = 0;
  std::vector<uint64_t> snaps; // ascending
  std::vector<std::pair<uint64_t, uint64_t>> overlap;// with next newest
  uint64_t size = 0;
  CloneInfo() = default;
};

struct SnapSet {
  std::vector<CloneInfo> clones; // ascending
  std::uint64_t seq = 0;   // newest snapid seen by the object
  SnapSet() = default;
};

struct ObjWatcher {
  /// Address of the Watcher
  std::string addr;
  /// Watcher ID
  std::int64_t watcher_id;
  /// Cookie
  std::uint64_t cookie;
  /// Timeout in Seconds
  std::uint32_t timeout_seconds;
};
}

namespace std {
template<>
struct hash<::RADOS::Entry> {
  std::size_t operator ()(::RADOS::Entry e) const {
    hash<std::string> h;
    return (h(e.nspace) << 2) ^ (h(e.oid) << 1) ^ h(e.locator);
  }
};
}

#endif // RADOS_DECODABLE_HPP
