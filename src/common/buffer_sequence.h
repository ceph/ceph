// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <boost/asio/buffer.hpp>
#include "include/buffer.h"

namespace ceph::buffer {

// adapts ceph::buffer::list to satisfy the asio ConstBufferSequence concept
class const_sequence {
 public:
  explicit const_sequence(const list& bl)
    : begin_(bl.buffers().begin()),
      end_(bl.buffers().end())
  {}

  class iterator {
   public:
    using value_type = boost::asio::const_buffer;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

    iterator(const iterator&) = default;
    iterator& operator=(const iterator&) = default;
    iterator(iterator&&) = default;
    iterator& operator=(iterator&&) = default;

    bool operator==(const iterator& rhs) const { return i == rhs.i; }

    iterator& operator++() {
      ++i;
      return *this;
    }
    iterator operator++(int) {
      const iterator tmp(*this);
      ++*this;
      return tmp;
    }

    value_type& operator*() const {
      buffer = {i->c_str(), i->length()};
      return buffer;
    }
    value_type* operator->() const {
      buffer = {i->c_str(), i->length()};
      return &buffer;
    }

   private:
    using base_iterator = list::buffers_t::const_iterator;
    base_iterator i;
    mutable value_type buffer;

    friend class const_sequence;
    explicit iterator(base_iterator i) : i(i) {}
  };
  using const_iterator = iterator;

  const_iterator begin() const { return begin_; }
  const_iterator end() const { return end_; }
 private:
  const_iterator begin_;
  const_iterator end_;
};
static_assert(boost::asio::is_const_buffer_sequence<const_sequence>::value);

// adapts ceph::buffer::list to satisfy the asio MutableBufferSequence concept
class mutable_sequence {
 public:
  explicit mutable_sequence(list& bl)
    : begin_(bl.mut_buffers().begin()),
      end_(bl.mut_buffers().end())
  {}

  class iterator {
   public:
    using value_type = boost::asio::mutable_buffer;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

    iterator(const iterator&) = default;
    iterator& operator=(const iterator&) = default;
    iterator(iterator&&) = default;
    iterator& operator=(iterator&&) = default;

    bool operator==(const iterator& rhs) const { return i == rhs.i; }

    iterator& operator++() {
      ++i;
      return *this;
    }
    iterator operator++(int) {
      const iterator tmp(*this);
      ++*this;
      return tmp;
    }

    value_type& operator*() {
      buffer = {i->c_str(), i->length()};
      return buffer;
    }
    value_type* operator->() {
      buffer = {i->c_str(), i->length()};
      return &buffer;
    }

   private:
    using base_iterator = list::buffers_t::iterator;
    base_iterator i;
    mutable value_type buffer;

    friend class mutable_sequence;
    explicit iterator(base_iterator i) : i(i) {}
  };
  using const_iterator = iterator;

  iterator begin() { return begin_; }
  const_iterator begin() const { return begin_; }
  iterator end() { return end_; }
  const_iterator end() const { return end_; }
 private:
  const_iterator begin_;
  const_iterator end_;
};
static_assert(boost::asio::is_mutable_buffer_sequence<mutable_sequence>::value);

} // namespace ceph::buffer
