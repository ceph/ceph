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
#include <cstring>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string_view>
#include <vector>

template<std::size_t SIZE>
class StackStringBuf : public std::basic_streambuf<char>
{
public:
  StackStringBuf() = default;
  StackStringBuf(const StackStringBuf&) = delete;
  StackStringBuf& operator=(const StackStringBuf&) = delete;
  StackStringBuf(StackStringBuf&& o) = delete;
  StackStringBuf& operator=(StackStringBuf&& o) = delete;
  ~StackStringBuf() override = default;

  void push(std::string_view sv)
  {
    vec.reserve(vec.size() + sv.size());
    vec.insert(vec.end(), sv.begin(), sv.end());
  }

  void clear()
  {
    vec.clear();
  }

  std::string_view strv() const
  {
    return std::string_view(vec.data(), vec.size());
  }

protected:
  std::streamsize xsputn(const char *s, std::streamsize n)
  {
    push(std::string_view(s, n));
    return n;
  }

  int overflow(int c)
  {
    if (traits_type::not_eof(c)) {
      char str = traits_type::to_char_type(c);
      push(std::string_view(&str, 1));
      return c;
    }
    return EOF;
  }

private:

  boost::container::small_vector<char, SIZE> vec;
};

template<std::size_t SIZE>
class StackStringStream : public std::basic_ostream<char>
{
public:
  StackStringStream() : basic_ostream<char>(&ssb) {}
  StackStringStream(const StackStringStream& o) = delete;
  StackStringStream& operator=(const StackStringStream& o) = delete;
  StackStringStream(StackStringStream&& o) = delete;
  StackStringStream& operator=(StackStringStream&& o) = delete;
  ~StackStringStream() override = default;

  void clear() {
    basic_ostream<char>::clear();
    ssb.clear();
  }

  std::string_view strv() const {
    return ssb.strv();
  }

private:
  StackStringBuf<SIZE> ssb;
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
      osp->clear();
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

  sss& get_stream() {
    return *osp;
  }
  const sss& get_stream() const {
    return *osp;
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
