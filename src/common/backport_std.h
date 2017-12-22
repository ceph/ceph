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

// Library code from later C++ standards and technical specifications
// backported to C++14.

namespace ceph {

namespace _backport17 {
template <typename T, std::size_t N>
constexpr std::size_t size(const T (&array)[N]) noexcept {
  return N;
}

/// http://en.cppreference.com/w/cpp/utility/functional/not_fn
// this implementation uses c++14's result_of_t (above) instead of the c++17
// invoke_result_t, and so may not behave correctly when SFINAE is required
template <typename F>
class not_fn_result {
  using DecayF = std::decay_t<F>;
  DecayF fn;
 public:
  explicit not_fn_result(F&& f) : fn(std::forward<F>(f)) {}
  not_fn_result(not_fn_result&& f) = default;
  not_fn_result(const not_fn_result& f) = default;

  template<class... Args>
  auto operator()(Args&&... args) &
    -> decltype(!std::declval<std::result_of_t<DecayF&(Args...)>>()) {
    return !fn(std::forward<Args>(args)...);
  }
  template<class... Args>
  auto operator()(Args&&... args) const&
    -> decltype(!std::declval<std::result_of_t<DecayF const&(Args...)>>()) {
    return !fn(std::forward<Args>(args)...);
  }

  template<class... Args>
  auto operator()(Args&&... args) &&
    -> decltype(!std::declval<std::result_of_t<DecayF(Args...)>>()) {
    return !std::move(fn)(std::forward<Args>(args)...);
  }
  template<class... Args>
  auto operator()(Args&&... args) const&&
    -> decltype(!std::declval<std::result_of_t<DecayF const(Args...)>>()) {
    return !std::move(fn)(std::forward<Args>(args)...);
  }
};

template <typename F>
not_fn_result<F> not_fn(F&& fn) {
  return not_fn_result<F>(std::forward<F>(fn));
}
} // namespace _backport17

namespace _backport_ts {
template <class DelimT,
          class CharT = char,
          class Traits = std::char_traits<CharT>>
class ostream_joiner {
public:
  using char_type = CharT;
  using traits_type = Traits;
  using ostream_type = std::basic_ostream<CharT, Traits>;
  using iterator_category = std::output_iterator_tag;
  using value_type = void;
  using difference_type = void;
  using pointer = void;
  using reference = void;

  ostream_joiner(ostream_type& s, const DelimT& delimiter)
    : out_stream(std::addressof(out_stream)),
      delim(delimiter),
      first_element(true)
  {}
  ostream_joiner(ostream_type& s, DelimT&& delimiter)
    : out_stream(std::addressof(s)),
      delim(std::move(delimiter)),
      first_element(true)
  {}

  template<typename T>
  ostream_joiner& operator=(const T& value) {
    if (!first_element)
      *out_stream << delim;
    first_element = false;
    *out_stream << value;
    return *this;
  }

  ostream_joiner& operator*() noexcept {
    return *this;
  }
  ostream_joiner& operator++() noexcept {
    return *this;
  }
  ostream_joiner& operator++(int) noexcept {
    return this;
  }

private:
  ostream_type* out_stream;
  DelimT delim;
  bool first_element;
};

template <class CharT, class Traits, class DelimT>
ostream_joiner<std::decay_t<DelimT>, CharT, Traits>
make_ostream_joiner(std::basic_ostream<CharT, Traits>& os,
                    DelimT&& delimiter) {
  return ostream_joiner<std::decay_t<DelimT>,
			CharT, Traits>(os, std::forward<DelimT>(delimiter));
}

} // namespace _backport_ts

using _backport17::size;
using _backport17::not_fn;
using _backport_ts::ostream_joiner;
using _backport_ts::make_ostream_joiner;
} // namespace ceph

#endif // CEPH_COMMON_BACKPORT14_H
