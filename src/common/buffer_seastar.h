// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/temporary_buffer.hh>
#include "include/buffer.h"
#include "common/error_code.h"

namespace details {

template<bool is_const>
class buffer_iterator_impl {
public:
  using pointer = std::conditional_t<is_const, const char*, char *>;
  buffer_iterator_impl(pointer first, const char* last)
    : pos(first), end_ptr(last)
  {}
  pointer get_pos_add(size_t n) {
    auto r = pos;
    pos += n;
    if (pos > end_ptr) {
      throw buffer::end_of_buffer{};
    }
    return r;
  }
  pointer get() const {
    return pos;
  }
protected:
  pointer pos;
  const char* end_ptr;
};
} // namespace details

class seastar_buffer_iterator : details::buffer_iterator_impl<false> {
  using parent = details::buffer_iterator_impl<false>;
  using temporary_buffer = seastar::temporary_buffer<char>;
public:
  seastar_buffer_iterator(temporary_buffer& b)
    : parent(b.get_write(), b.end()), buf(b)
  {}
  using parent::pointer;
  using parent::get_pos_add;
  using parent::get;
  ceph::buffer::ptr get_ptr(size_t len);

private:
  // keep the reference to buf around, so it can be "shared" by get_ptr()
  temporary_buffer& buf;
};

class const_seastar_buffer_iterator : details::buffer_iterator_impl<true> {
  using parent = details::buffer_iterator_impl<true>;
  using temporary_buffer = seastar::temporary_buffer<char>;
public:
  const_seastar_buffer_iterator(temporary_buffer& b)
    : parent(b.get_write(), b.end())
  {}
  using parent::pointer;
  using parent::get_pos_add;
  using parent::get;
  ceph::buffer::ptr get_ptr(size_t len);
};
