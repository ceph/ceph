// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "key_layout.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

void string_key_view_t::append_str(
    NodeExtentMutable& mut, const char* data, size_t len, char*& p_append) {
  p_append -= sizeof(string_size_t);
  assert(len < std::numeric_limits<string_size_t>::max());
  mut.copy_in_absolute(p_append, (string_size_t)len);
  p_append -= len;
  mut.copy_in_absolute(p_append, data, len);
}

void string_key_view_t::append_dedup(
    NodeExtentMutable& mut, const Type& dedup_type, char*& p_append) {
  p_append -= sizeof(string_size_t);
  if (dedup_type == Type::MIN) {
    mut.copy_in_absolute(p_append, (string_size_t)0u);
  } else if (dedup_type == Type::MAX) {
    mut.copy_in_absolute(p_append, std::numeric_limits<string_size_t>::max());
  } else {
    assert(false);
  }
}

}
