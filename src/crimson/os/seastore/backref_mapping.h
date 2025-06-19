// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/btree/btree_types.h"

namespace crimson::os::seastore {

class BackrefMapping {
  BackrefCursorRef cursor;

  BackrefMapping(BackrefCursorRef cursor)
      : cursor(std::move(cursor)) {}

public:
  static BackrefMapping create(BackrefCursorRef cursor) {
    return BackrefMapping(std::move(cursor));
  }

  BackrefMapping() = default;

  BackrefMapping(const BackrefMapping &) = delete;
  BackrefMapping(BackrefMapping &&) = default;

  BackrefMapping &operator=(const BackrefMapping &) = delete;
  BackrefMapping &operator=(BackrefMapping &&) = default;

  ~BackrefMapping() = default;

  bool is_viewable() const {
    assert(cursor);
    return cursor->is_viewable();
  }

  extent_len_t get_length() const {
    assert(cursor);
    return cursor->get_length();
  }

  laddr_t get_val() const {
    assert(cursor);
    return cursor->get_laddr();
  }

  paddr_t get_key() const {
    assert(cursor);
    return cursor->get_paddr();
  }

  extent_types_t get_type() const {
    assert(cursor);
    return cursor->get_type();
  }
};

using backref_mapping_list_t = std::list<BackrefMapping>;

} // namespace crimson::os::seastore
