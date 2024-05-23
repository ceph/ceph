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

#ifndef NEORADOS_RADOS_DECODABLE_HPP
#define NEORADOS_RADOS_DECODABLE_HPP

#include <cstdint>
#include <cstdlib>
#include <string>
#include <iostream>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/core.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

namespace neorados {
struct Entry {
  std::string nspace;
  std::string oid;
  std::string locator;

  friend auto operator <=>(const Entry&, const Entry&) = default;
};

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
struct hash<::neorados::Entry> {
  std::size_t operator ()(::neorados::Entry e) const {
    hash<std::string> h;
    return (h(e.nspace) << 2) ^ (h(e.oid) << 1) ^ h(e.locator);
  }
};
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<neorados::Entry> : fmt::ostream_formatter {};
#endif

#endif // RADOS_DECODABLE_HPP
