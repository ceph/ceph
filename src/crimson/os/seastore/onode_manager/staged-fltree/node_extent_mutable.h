// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "fwd.h"

#pragma once

namespace crimson::os::seastore::onode {

class NodeExtent;

// the wrapper of NodeExtent which is mutable and safe to be mutated
class NodeExtentMutable {
 public:
  void copy_in_absolute(void* dst, const void* src, extent_len_t len) {
    assert((char*)dst >= get_write());
    assert((char*)dst + len <= buf_upper_bound());
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
    assert((const char*)src >= get_write());
    assert((const char*)src + len <= buf_upper_bound());
    char* to = (char*)src + offset;
    assert(to >= get_write());
    assert(to + len <= buf_upper_bound());
    if (len != 0) {
      std::memmove(to, src, len);
    }
  }
  void shift_relative(extent_len_t src_offset, extent_len_t len, int offset) {
    shift_absolute(get_write() + src_offset, len, offset);
  }

  template <typename T>
  void validate_inplace_update(const T& updated) {
    assert((const char*)&updated >= get_write());
    assert((const char*)&updated + sizeof(T) <= buf_upper_bound());
  }

  char* get_write();
  extent_len_t get_length() const;

 private:
  explicit NodeExtentMutable(NodeExtent&);
  const char* get_read() const;
  const char* buf_upper_bound() const;

  NodeExtent& extent;

  friend class NodeExtent;
};

}
