// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "key_layout.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_mutable.h"

namespace crimson::os::seastore::onode {

void string_key_view_t::append_str(
    NodeExtentMutable& mut, std::string_view str, char*& p_append) {
  p_append -= sizeof(string_size_t);
  assert(str.length() < std::numeric_limits<string_size_t>::max());
  string_size_t len = str.length();
  assert(len != 0);
  mut.copy_in_absolute(p_append, len);
  p_append -= len;
  mut.copy_in_absolute(p_append, str.data(), len);
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
