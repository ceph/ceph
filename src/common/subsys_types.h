// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_SUBSYS_TYPES_H
#define CEPH_SUBSYS_TYPES_H

#include <algorithm>
#include <array>

enum ceph_subsys_id_t {
  ceph_subsys_,   // default
#define SUBSYS(name, log, gather) \
  ceph_subsys_##name,
#define DEFAULT_SUBSYS(log, gather)
#include "common/subsys.h"
#undef SUBSYS
#undef DEFAULT_SUBSYS
  ceph_subsys_max
};

constexpr static std::size_t ceph_subsys_get_num() {
  return static_cast<std::size_t>(ceph_subsys_max);
}

struct ceph_subsys_item_t {
  const char* name;
  uint8_t log_level;
  uint8_t gather_level;
};

constexpr static std::array<ceph_subsys_item_t, ceph_subsys_get_num()>
ceph_subsys_get_as_array() {
#define SUBSYS(name, log, gather) \
  ceph_subsys_item_t{ #name, log, gather },
#define DEFAULT_SUBSYS(log, gather) \
  ceph_subsys_item_t{ "none", log, gather },

  return {
#include "common/subsys.h"
  };
#undef SUBSYS
#undef DEFAULT_SUBSYS
}

// Compile time-capable version of std::strlen. Resorting to own
// implementation only because C++17 doesn't mandate constexpr
// on the standard one.
constexpr static std::size_t strlen_ct(const char* const s) {
  std::size_t l = 0;
  while (s[l] != '\0') {
    ++l;
  }
  return l;
}

constexpr static std::size_t ceph_subsys_max_name_length() {
  return std::max({
#define SUBSYS(name, log, gather) \
  strlen_ct(#name),
#define DEFAULT_SUBSYS(log, gather) \
  strlen_ct("none"),
#include "common/subsys.h"
#undef SUBSYS
#undef DEFAULT_SUBSYS
  });
}

#endif // CEPH_SUBSYS_TYPES_H

