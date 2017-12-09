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
// Template variable aliases from C++17 <type_traits>
// primary type categories
template<class T> constexpr bool is_void_v = std::is_void<T>::value;
template<class T> constexpr bool is_null_pointer_v =
  std::is_null_pointer<T>::value;
template<class T> constexpr bool is_integral_v = std::is_integral<T>::value;
template<class T> constexpr bool is_floating_point_v =
  std::is_floating_point<T>::value;
template<class T> constexpr bool is_array_v = std::is_array<T>::value;
template<class T> constexpr bool is_pointer_v = std::is_pointer<T>::value;
template<class T> constexpr bool is_lvalue_reference_v =
  std::is_lvalue_reference<T>::value;
template<class T> constexpr bool is_rvalue_reference_v =
  std::is_rvalue_reference<T>::value;
template<class T> constexpr bool is_member_object_pointer_v =
  std::is_member_object_pointer<T>::value;
template<class T> constexpr bool is_member_function_pointer_v =
  std::is_member_function_pointer<T>::value;
template<class T> constexpr bool is_enum_v = std::is_enum<T>::value;
template<class T> constexpr bool is_union_v = std::is_union<T>::value;
template<class T> constexpr bool is_class_v = std::is_class<T>::value;
template<class T> constexpr bool is_function_v = std::is_function<T>::value;

// composite type categories
template<class T> constexpr bool is_reference_v = std::is_reference<T>::value;
template<class T> constexpr bool is_arithmetic_v = std::is_arithmetic<T>::value;
template<class T> constexpr bool is_fundamental_v =
  std::is_fundamental<T>::value;
template<class T> constexpr bool is_object_v = std::is_object<T>::value;
template<class T> constexpr bool is_scalar_v = std::is_scalar<T>::value;
template<class T> constexpr bool is_compound_v = std::is_compound<T>::value;
template<class T> constexpr bool is_member_pointer_v =
  std::is_member_pointer<T>::value;

// type properties
template<class T> constexpr bool is_const_v = std::is_const<T>::value;
template<class T> constexpr bool is_volatile_v = std::is_volatile<T>::value;
template<class T> constexpr bool is_trivial_v = std::is_trivial<T>::value;
template<class T> constexpr bool is_trivially_copyable_v =
  std::is_trivially_copyable<T>::value;
template<class T> constexpr bool is_standard_layout_v =
  std::is_standard_layout<T>::value;
template<class T> constexpr bool is_pod_v = std::is_pod<T>::value;
template<class T> constexpr bool is_empty_v = std::is_empty<T>::value;
template<class T> constexpr bool is_polymorphic_v =
  std::is_polymorphic<T>::value;
template<class T> constexpr bool is_abstract_v = std::is_abstract<T>::value;
template<class T> constexpr bool is_final_v = std::is_final<T>::value;
template<class T> constexpr bool is_signed_v = std::is_signed<T>::value;
template<class T> constexpr bool is_unsigned_v = std::is_unsigned<T>::value;
template<class T, class... Args> constexpr bool is_constructible_v =
  std::is_constructible<T, Args...>::value;
template<class T> constexpr bool is_default_constructible_v =
  std::is_default_constructible<T>::value;
template<class T> constexpr bool is_copy_constructible_v =
  std::is_copy_constructible<T>::value;
template<class T> constexpr bool is_move_constructible_v =
  std::is_move_constructible<T>::value;
template<class T, class U> constexpr bool is_assignable_v =
  std::is_assignable<T, U>::value;
template<class T> constexpr bool is_copy_assignable_v =
  std::is_copy_assignable<T>::value;
template<class T> constexpr bool is_move_assignable_v =
  std::is_move_assignable<T>::value;
template<class T> constexpr bool is_destructible_v =
  std::is_destructible<T>::value;
template<class T, class... Args> constexpr bool is_trivially_constructible_v =
  std::is_trivially_constructible<T, Args...>::value;
template<class T> constexpr bool is_trivially_default_constructible_v =
  std::is_trivially_default_constructible<T>::value;
template<class T> constexpr bool is_trivially_copy_constructible_v =
  std::is_trivially_copy_constructible<T>::value;
template<class T> constexpr bool is_trivially_move_constructible_v =
  std::is_trivially_move_constructible<T>::value;
template<class T, class U> constexpr bool is_trivially_assignable_v =
  std::is_trivially_assignable<T, U>::value;
template<class T> constexpr bool is_trivially_copy_assignable_v =
  std::is_trivially_copy_assignable<T>::value;
template<class T> constexpr bool is_trivially_move_assignable_v =
  std::is_trivially_move_assignable<T>::value;
template<class T> constexpr bool is_trivially_destructible_v =
  std::is_trivially_destructible<T>::value;
template<class T, class... Args> constexpr bool is_nothrow_constructible_v =
  std::is_nothrow_constructible<T, Args...>::value;
template<class T> constexpr bool is_nothrow_default_constructible_v =
  std::is_nothrow_default_constructible<T>::value;
template<class T> constexpr bool is_nothrow_copy_constructible_v =
  std::is_nothrow_copy_constructible<T>::value;
template<class T> constexpr bool is_nothrow_move_constructible_v =
  std::is_nothrow_move_constructible<T>::value;
template<class T, class U> constexpr bool is_nothrow_assignable_v =
  std::is_nothrow_assignable<T, U>::value;
template<class T> constexpr bool is_nothrow_copy_assignable_v =
  std::is_nothrow_copy_assignable<T>::value;
template<class T> constexpr bool is_nothrow_move_assignable_v =
  std::is_nothrow_move_assignable<T>::value;
template<class T> constexpr bool is_nothrow_destructible_v =
  std::is_nothrow_destructible<T>::value;
template<class T> constexpr bool has_virtual_destructor_v =
  std::has_virtual_destructor<T>::value;

// type property queries
template<class T> constexpr size_t alignment_of_v = std::alignment_of<T>::value;
template<class T> constexpr size_t rank_v = std::rank<T>::value;
template<class T, unsigned I = 0> constexpr size_t extent_v =
  std::extent<T, I>::value;

// type relations
template<class T, class U> constexpr bool is_same_v = std::is_same<T, U>::value;
template<class Base, class Derived> constexpr bool is_base_of_v =
  std::is_base_of<Base, Derived>::value;
template<class From, class To> constexpr bool is_convertible_v =
  std::is_convertible<From, To>::value;

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
  return std::unique_ptr<T>(new std::remove_extent_t<T>[n]());
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

struct in_place_t {};
constexpr in_place_t in_place{};

template<typename T>
struct in_place_type_t {};

template<typename T>
constexpr in_place_type_t<T> in_place_type{};
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

using _backport14::make_unique;
using _backport17::size;
using _backport14::max;
using _backport17::not_fn;
using _backport17::in_place_t;
using _backport17::in_place;
using _backport17::in_place_type_t;
using _backport17::in_place_type;
using _backport_ts::ostream_joiner;
using _backport_ts::make_ostream_joiner;
} // namespace ceph

#endif // CEPH_COMMON_BACKPORT14_H
