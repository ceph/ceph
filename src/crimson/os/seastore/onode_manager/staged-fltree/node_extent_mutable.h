// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "fwd.h"

#pragma once

namespace crimson::os::seastore::onode {

/**
 * NodeExtentMutable
 *
 * A thin wrapper of NodeExtent to make sure that only the newly allocated
 * or the duplicated NodeExtent is mutable, and the memory modifications are
 * safe within the extent range.
 */
class NodeExtentMutable {
 public:
  void copy_in_absolute(void* dst, const void* src, extent_len_t len) {
    assert(is_safe(dst, len));
    std::memcpy(dst, src, len);
  }
  template <typename T>
  void copy_in_absolute(void* dst, const T& src) {
    copy_in_absolute(dst, &src, sizeof(T));
  }

  const void* copy_in_relative(
      extent_len_t dst_offset, const void* src, extent_len_t len) {
    auto dst = get_write() + dst_offset;
    copy_in_absolute(dst, src, len);
    return dst;
  }
  template <typename T>
  const T* copy_in_relative(
      extent_len_t dst_offset, const T& src) {
    auto dst = copy_in_relative(dst_offset, &src, sizeof(T));
    return static_cast<const T*>(dst);
  }

  void shift_absolute(const void* src, extent_len_t len, int offset) {
    assert(is_safe(src, len));
    char* to = (char*)src + offset;
    assert(is_safe(to, len));
    if (len != 0) {
      std::memmove(to, src, len);
    }
  }
  void shift_relative(extent_len_t src_offset, extent_len_t len, int offset) {
    shift_absolute(get_write() + src_offset, len, offset);
  }

  void set_absolute(void* dst, int value, extent_len_t len) {
    assert(is_safe(dst, len));
    std::memset(dst, value, len);
  }
  void set_relative(extent_len_t dst_offset, int value, extent_len_t len) {
    auto dst = get_write() + dst_offset;
    set_absolute(dst, value, len);
  }

  template <typename T>
  void validate_inplace_update(const T& updated) {
    assert(is_safe(&updated, sizeof(T)));
  }

  const char* get_read() const { return p_start; }
  char* get_write() { return p_start; }
  extent_len_t get_length() const {
#ifndef NDEBUG
    if (node_offset == 0) {
      assert(is_valid_node_size(length));
    }
#endif
    return length;
  }
  node_offset_t get_node_offset() const { return node_offset; }

  NodeExtentMutable get_mutable_absolute(const void* dst, node_offset_t len) const {
    assert(node_offset == 0);
    assert(is_safe(dst, len));
    assert((const char*)dst != get_read());
    auto ret = *this;
    node_offset_t offset = (const char*)dst - get_read();
    assert(offset != 0);
    ret.p_start += offset;
    ret.length = len;
    ret.node_offset = offset;
    return ret;
  }
  NodeExtentMutable get_mutable_relative(
      node_offset_t offset, node_offset_t len) const {
    return get_mutable_absolute(get_read() + offset, len);
  }

 private:
  NodeExtentMutable(char* p_start, extent_len_t length)
    : p_start{p_start}, length{length} {}
  bool is_safe(const void* src, extent_len_t len) const {
    return ((const char*)src >= p_start) &&
           ((const char*)src + len <= p_start + length);
  }

  char* p_start;
  extent_len_t length;
  node_offset_t node_offset = 0;

  friend class NodeExtent;
};

}
