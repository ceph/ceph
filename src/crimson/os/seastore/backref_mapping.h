// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/btree/btree_types.h"

namespace crimson::os::seastore {

class BackrefMapping {
  BackrefCursorRef cursor;
public:
  BackrefMapping() = default;
  BackrefMapping(BackrefCursorRef cursor)
      : cursor(std::move(cursor)) {}

  BackrefMapping(const BackrefMapping &) = delete;
  BackrefMapping(BackrefMapping &&) = default;

  BackrefMapping &operator=(const BackrefMapping &) = delete;
  BackrefMapping &operator=(BackrefMapping &&) = default;

  ~BackrefMapping() = default;

  extent_len_t get_length() const {
    assert(cursor->val);
    return cursor->val->len;
  }

  laddr_t get_val() const {
    assert(cursor->val);
    return cursor->val->laddr;
  }

  paddr_t get_key() const {
    assert(cursor->key != P_ADDR_NULL);
    return cursor->key;
  }

  extent_types_t get_type() const {
    assert(cursor->val);
    return cursor->val->type;
  }
};

using backref_mapping_list_t = std::list<BackrefMapping>;

} // namespace crimson::os::seastore
