// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2025 (C) IBM, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#pragma once

#include "include/ceph_assert.h"

#include "common/CanHasPrint.h"

#include <chrono>
#include <ostream>

template<typename T>
concept HasEpoch = requires(T t) {
  { t.inc_epoch() } -> std::same_as<void>;
};

template<typename T>
concept HasEphemeral = requires {
  { T::is_ephemeral() } -> std::same_as<bool>;
};


template<typename Mon, typename Service, typename T>
class PaxosMap {
  static_assert(!(HasEpoch<T> && HasEphemeral<T>), "A type cannot satisfy both HasEpoch and HasEphemeral concepts.");

public:
  PaxosMap(Mon const& m, Service const& s) : mon(m), service(s) {}
  virtual ~PaxosMap() = default;

  const T& get_pending_map() const { ceph_assert(mon.is_leader()); return pending_map; }
  const T& get_map() const { return map; }

  void print(std::ostream& os) const {
    os << "PaxosMap@" << typeid(T).name()
       << "(current=" << map
       << " pending=" << pending_map
       << ")";
  }

protected:
  T& get_pending_map_writeable() { ceph_assert(mon.is_leader()); ceph_assert(service.is_writeable()); return pending_map; }

  T& create_pending() {
    ceph_assert(mon.is_leader());
    if constexpr (HasEpoch<T>) {
      pending_map.inc_epoch();
    } else if constexpr (HasEphemeral<T>) {
      if constexpr (T::is_ephemeral()) {
        pending_map = T();
      }
    } else {
      pending_map = map;
    }
    return pending_map;
  }

  void decode(ceph::buffer::list::const_iterator& blp) {
    using ceph::decode;
    decode(map, blp);
    pending_map = T(); /* nuke it to catch invalid access */
  }

  void clear() {
    map = T();
    pending_map = T(); /* nuke it to catch invalid access */
  }


private:
  friend Service;

  /* Keep these PRIVATE to prevent unprotected manipulation. */
  Mon const& mon;
  Service const& service;
  T map; /* the current epoch */
  T pending_map; /* the next epoch */
};
