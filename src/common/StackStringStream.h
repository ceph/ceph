// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef COMMON_STACKSTRINGSTREAM_H
#define COMMON_STACKSTRINGSTREAM_H

#include <boost/container/small_vector.hpp>

#include <algorithm>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string_view>
#include <vector>

#include "include/inline_memory.h"

template<std::size_t SIZE>
class StackStringBuf : public std::basic_streambuf<char>
{
public:
  StackStringBuf()
    : vec{SIZE, boost::container::default_init_t{}}
  {
    setp(vec.data(), vec.data() + vec.size());
  }
  StackStringBuf(const StackStringBuf&) = delete;
  StackStringBuf& operator=(const StackStringBuf&) = delete;
  StackStringBuf(StackStringBuf&& o) = delete;
  StackStringBuf& operator=(StackStringBuf&& o) = delete;
  ~StackStringBuf() override = default;

  void clear()
  {
    vec.resize(SIZE);
    setp(vec.data(), vec.data() + SIZE);
  }

  std::string_view strv() const
  {
    return std::string_view(pbase(), pptr() - pbase());
  }

protected:
  std::streamsize xsputn(const char *s, std::streamsize n)
  {
    std::streamsize capacity = epptr() - pptr();
    std::streamsize left = n;
    if (capacity >= left) {
      maybe_inline_memcpy(pptr(), s, left, 32);
      pbump(left);
    } else {
      maybe_inline_memcpy(pptr(), s, capacity, 64);
      s += capacity;
      left -= capacity;
      vec.insert(vec.end(), s, s + left);
      setp(vec.data(), vec.data() + vec.size());
      pbump(vec.size());
    }
    return n;
  }

  int overflow(int c)
  {
    if (traits_type::not_eof(c)) {
      char str = traits_type::to_char_type(c);
      vec.push_back(str);
      return c;
    } else {
      return traits_type::eof();
    }
  }

private:

  boost::container::small_vector<char, SIZE> vec;
};

template<std::size_t SIZE>
class StackStringStream : public std::basic_ostream<char>
{
public:
  StackStringStream() : basic_ostream<char>(&ssb), default_fmtflags(flags()) {}
  StackStringStream(const StackStringStream& o) = delete;
  StackStringStream& operator=(const StackStringStream& o) = delete;
  StackStringStream(StackStringStream&& o) = delete;
  StackStringStream& operator=(StackStringStream&& o) = delete;
  ~StackStringStream() override = default;

  void reset() {
    clear(); /* reset state flags */
    flags(default_fmtflags); /* reset fmtflags to constructor defaults */
    ssb.clear();
  }

  std::string_view strv() const {
    return ssb.strv();
  }

private:
  StackStringBuf<SIZE> ssb;
  fmtflags const default_fmtflags;
};

/* In an ideal world, we could use StackStringStream indiscriminately, but alas
 * it's very expensive to construct/destruct. So, we cache them in a
 * thread_local vector. DO NOT share these with other threads. The copy/move
 * constructors are deliberately restrictive to make this more difficult to
 * accidentally do.
 */
class CachedStackStringStream {
public:
  using sss = StackStringStream<4096>;
  using osptr = std::unique_ptr<sss>;

  CachedStackStringStream() {
    if (cache.destructed || cache.c.empty()) {
      osp = std::make_unique<sss>();
    } else {
      osp = std::move(cache.c.back());
      cache.c.pop_back();
      osp->reset();
    }
  }
  CachedStackStringStream(const CachedStackStringStream&) = delete;
  CachedStackStringStream& operator=(const CachedStackStringStream&) = delete;
  CachedStackStringStream(CachedStackStringStream&&) = delete;
  CachedStackStringStream& operator=(CachedStackStringStream&&) = delete;
  ~CachedStackStringStream() {
    if (!cache.destructed && cache.c.size() < max_elems) {
      cache.c.emplace_back(std::move(osp));
    }
  }

  sss& operator*() {
    return *osp;
  }
  sss const& operator*() const {
    return *osp;
  }
  sss* operator->() {
    return osp.get();
  }
  sss const* operator->() const {
    return osp.get();
  }

  sss const* get() const {
    return osp.get();
  }
  sss* get() {
    return osp.get();
  }

private:
  static constexpr std::size_t max_elems = 8;

  /* The thread_local cache may be destructed before other static structures.
   * If those destructors try to create a CachedStackStringStream (e.g. for
   * logging) and access this cache, that access will be undefined. So note if
   * the cache has been destructed and check before use.
   */
  struct Cache {
    using container = std::vector<osptr>;

    Cache() {}
    ~Cache() { destructed = true; }

    container c;
    bool destructed = false;
  };

  inline static thread_local Cache cache;
  osptr osp;
};

#endif
