// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <memory>
#include <type_traits>

#ifndef CEPH_COMMON_BACKPORT14_H
#define CEPH_COMMON_BACKPORT14_H

// Library code from C++14 that can be implemented in C++11.

namespace ceph {
template<typename T>
using remove_extent_t = typename std::remove_extent<T>::type;
template<typename T>
using remove_reference_t = typename std::remove_reference<T>::type;
template<typename T>
using result_of_t = typename std::result_of<T>::type;

namespace _backport14 {
template<typename T>
struct uniquity {
  using datum = std::unique_ptr<T>;
};

template<typename T>
struct uniquity<T[]> {
  using array = std::unique_ptr<T[]>;
};

template<typename T, std::size_t N>
struct uniquity<T[N]> {
  using verboten = void;
};

template<typename T, typename... Args>
inline typename uniquity<T>::datum make_unique(Args&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

template<typename T>
inline typename uniquity<T>::array make_unique(std::size_t n) {
  return std::unique_ptr<T>(new remove_extent_t<T>[n]());
}

template<typename T, class... Args>
typename uniquity<T>::verboten
make_unique(Args&&...) = delete;

// The constexpr variant of std::max().
template<class T>
constexpr const T& max(const T& a, const T& b) {
  return a < b ? b : a;
}
} // namespace _backport14

namespace _backport17 {
template <class C>
constexpr auto size(const C& c) -> decltype(c.size()) {
  return c.size();
}

template <typename T, std::size_t N>
constexpr std::size_t size(const T (&array)[N]) noexcept {
  return N;
}
} // namespace _backport17
using _backport14::make_unique;
using _backport17::size;
using _backport14::max;
} // namespace ceph

#endif // CEPH_COMMON_BACKPORT14_H
