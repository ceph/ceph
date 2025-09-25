// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_FILEPATH_H
#define CEPH_FILEPATH_H

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/fs_types.h" // for inodeno_t

#include "boost/container/small_vector.hpp"

#include <limits>

#include <iosfwd>
#include <list>
#include <string_view>
#include <vector>

namespace ceph { class Formatter; }

template<typename It>
class path_component_iterator {
public:
  using difference_type   = typename std::iterator_traits<It>::difference_type;
  using value_type        = std::string_view;
  using pointer           = void;
  using reference         = std::string_view;

  path_component_iterator() = default;
  explicit path_component_iterator(It it) : _iter(it) {}

  reference operator*() const {
    const auto& component = *_iter;
    return std::string_view(component.data(), component.size()-1); /* exclude NUL */
  }

  value_type operator->() const {
    return **this;
  }

  path_component_iterator& operator++() {
    ++_iter;
    return *this;
  }
  path_component_iterator operator++(int) {
    path_component_iterator tmp = *this;
    ++(*this);
    return tmp;
  }

  path_component_iterator& operator--() {
    --_iter;
    return *this;
  }
  path_component_iterator operator--(int) {
    path_component_iterator tmp = *this;
    --(*this);
    return tmp;
  }

  bool operator==(const path_component_iterator& other) const {
    return _iter == other._iter;
  };
  bool operator!=(const path_component_iterator& other) const {
    return _iter != other._iter;
  };

private:
  It _iter;
};

class filepath {
public:
  static constexpr unsigned DENTRY_RESERVED = 10; /* for performance */

  using component_t = boost::container::small_vector<char, NAME_MAX>;
  using path_t = boost::container::small_vector<component_t, DENTRY_RESERVED>;
  using const_iterator = path_component_iterator< path_t::const_iterator >;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  static std::list<filepath> generate_test_instances();

  filepath() = default;
  filepath(std::string_view p, inodeno_t i) : _ino(i) { _set_path(p); }
  filepath(const filepath& o) = default;
  filepath(filepath&& o) {
    _ino = o._ino;
    _path = std::move(o._path);
    _bits = std::move(o._bits);
    _bits_dirty = o._bits_dirty;
    o.clear();
  }
  filepath(inodeno_t i) : _ino(i) {}
  filepath(int i) : _ino(inodeno_t(i)) {}
  filepath(int64_t i) : _ino(inodeno_t(i)) {}
  filepath(std::string_view s) { _set_path(s); }
  filepath(const char* s) {
    _set_path(s);
  }

  filepath& operator=(const filepath& o) = default;
  filepath& operator=(const char* path) {
    return operator=(std::string_view(path));
  }
  filepath& operator=(std::string_view path) {
    clear();
    _set_path(path);
    return *this;
  }

  void set_path(std::string_view s, inodeno_t b) {
    clear();
    _ino = b;
    _set_path(s);
  }
  void set_path(std::string_view s) {
    clear();
    _ino = 0;
    _set_path(s);
  }

  inodeno_t get_ino() const {
    return _ino;
  }
  std::string_view get_path() const {
    _check_path();
    return std::string_view(_path.data(), _path.size()-1); /* exclude NUL */
  }
  const char* c_str() const {
    _check_path();
    return _path.data();
  }

  std::size_t length() const {
    return size();
  }
  std::size_t size() const {
    _check_path();
    return _path.size() - 1; /* exclude NUL */
  }
  unsigned depth() const {
    _check_bits();
    return _bits.size();
  }
  bool empty() const { return _path.size() <= 1 && _ino == 0; }

  bool absolute() const { return _ino == 1; }
  bool pure_relative() const { return _ino == 0; }
  bool ino_relative() const { return _ino > 0; }

  std::string_view operator[](int i) const {
    _check_bits();
    auto const& b = _bits.at(i);
    return std::string_view(b.data(), b.size()-1); /* exclude NUL */
  }

  auto begin() const {
    _check_bits();
    return const_iterator(_bits.cbegin());
  }
  auto end() const {
    _check_bits();
    return const_iterator(_bits.cend());
  }

  auto rbegin() const {
    _check_bits();
    return const_reverse_iterator(end());
  }
  auto rend() const {
    _check_bits();
    return const_reverse_iterator(begin());
  }

  std::string_view last_dentry() const {
    _check_bits();
    if (_bits.empty()) {
      return std::string_view("");
    } else {
      auto& last = _bits.back();
      return std::string_view(last.data(), last.size()-1); /* exclude NUL */
    }
  }

  filepath prefixpath(int s) const;
  filepath postfixpath(int s) const;

  void clear() {
    _ino = 0;
    _path = {'\0'};
    _bits.clear();
    _bits_dirty = false;
  }

  void pop_dentry();
  void push_dentry(std::string_view s);
  void push_front_dentry(std::string_view s);
  void append(const filepath& a);

  // encoding
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& blp);
  void dump(ceph::Formatter *f) const;
  void print(std::ostream&) const;

  bool is_last_dot_or_dotdot() const;

  // walk into snapdir?
  bool is_last_snap() const {
    _check_path();
    return _path.size() > 2 && _path[0] == '/' && _path[1] == '/';
  }

private:
  /* 0 is relative (default).
   * 1 is relative to root.
   * Everything else is relative to that inode number.
   */
  mutable inodeno_t _ino = 0;
  /* The canonicalized path. If it is derived from a path beginning with "/",
   * the _ino will also be changed to 1 (root).
   *
   * This path is **always** relative or a relative snappath.
   */
  mutable boost::container::small_vector<char, PATH_MAX> _path = {'\0'};     // relative path.

  /** bits - path segments
   * this is ['a', 'b', 'c'] for both the aboslute and relative case.
   *
   * NOTE: this value is LAZILY maintained... i.e. it's a cache
   */
  mutable bool _bits_dirty = false;
  mutable path_t _bits;

  void _rebuild_path() const;
  void _parse_bits() const;

  /*
   * if we are fed a relative path as a string, either set ino=0 (strictly
   * relative) or 1 (absolute).  throw out any leading '/'.
   */
  void _set_path(std::string_view s);

  void _check_path() const {
    if (_bits_dirty) {
      _rebuild_path();
    }
  }
  void _check_bits() const {
    if (_bits.empty()) {
      _parse_bits();
    }
  }
};

WRITE_CLASS_ENCODER(filepath)

#endif
