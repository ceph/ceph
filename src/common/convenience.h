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

#include <mutex>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <type_traits>
#include <utility>

#include <boost/optional.hpp>

#include "include/ceph_assert.h" // I despise you. Not you the reader, I'm talking
                            // to the include file.


#ifndef CEPH_COMMON_CONVENIENCE_H
#define CEPH_COMMON_CONVENIENCE_H

namespace ceph {
// boost::optional is wonderful! Unfortunately it lacks a function for
// the thing you would most obviously want to do with it: apply a
// function to its contents.

// There are two obvious candidates. The first is a function that
// takes a function and an optional value and returns an optional
// value, either holding the return value of the function or holding
// nothing.
//
// I'd considered making more overloads for mutable lvalue
// references, but those are going a bit beyond likely use cases.
//
template<typename T, typename F>
auto maybe_do(const boost::optional<T>& t, F&& f) ->
  boost::optional<std::result_of_t<F(const std::decay_t<T>)>>
{
  if (t)
    return { std::forward<F>(f)(*t) };
  else
    return boost::none;
}

// The other obvious function takes an optional but returns an
// ‘unwrapped’ value, either the result of evaluating the function or
// a provided alternate value.
//
template<typename T, typename F, typename U>
auto maybe_do_or(const boost::optional<T>& t, F&& f, U&& u) ->
  std::result_of_t<F(const std::decay_t<T>)>
{
  static_assert(std::is_convertible_v<U, std::result_of_t<F(T)>>,
		"Alternate value must be convertible to function return type.");
  if (t)
    return std::forward<F>(f)(*t);
  else
    return std::forward<U>(u);
}


// Same thing but for std::optional

template<typename T, typename F>
auto maybe_do(const std::optional<T>& t, F&& f) ->
  std::optional<std::result_of_t<F(const std::decay_t<T>)>>
{
  if (t)
    return { std::forward<F>(f)(*t) };
  else
    return std::nullopt;
}

// The other obvious function takes an optional but returns an
// ‘unwrapped’ value, either the result of evaluating the function or
// a provided alternate value.
//
template<typename T, typename F, typename U>
auto maybe_do_or(const std::optional<T>& t, F&& f, U&& u) ->
  std::result_of_t<F(const std::decay_t<T>)>
{
  static_assert(std::is_convertible_v<U, std::result_of_t<F(T)>>,
		"Alternate value must be convertible to function return type.");
  if (t)
    return std::forward<F>(f)(*t);
  else
    return std::forward<U>(u);
}

namespace _convenience {
template<typename... Ts, typename F,  std::size_t... Is>
inline void for_each_helper(const std::tuple<Ts...>& t, const F& f,
			    std::index_sequence<Is...>) {
  (f(std::get<Is>(t)), ..., void());
}
template<typename... Ts, typename F,  std::size_t... Is>
inline void for_each_helper(std::tuple<Ts...>& t, const F& f,
			    std::index_sequence<Is...>) {
  (f(std::get<Is>(t)), ..., void());
}
template<typename... Ts, typename F,  std::size_t... Is>
inline void for_each_helper(const std::tuple<Ts...>& t, F& f,
			    std::index_sequence<Is...>) {
  (f(std::get<Is>(t)), ..., void());
}
template<typename... Ts, typename F,  std::size_t... Is>
inline void for_each_helper(std::tuple<Ts...>& t, F& f,
			    std::index_sequence<Is...>) {
  (f(std::get<Is>(t)), ..., void());
}
}

template<typename... Ts, typename F>
inline void for_each(const std::tuple<Ts...>& t, const F& f) {
  _convenience::for_each_helper(t, f, std::index_sequence_for<Ts...>{});
}
template<typename... Ts, typename F>
inline void for_each(std::tuple<Ts...>& t, const F& f) {
  _convenience::for_each_helper(t, f, std::index_sequence_for<Ts...>{});
}
template<typename... Ts, typename F>
inline void for_each(const std::tuple<Ts...>& t, F& f) {
  _convenience::for_each_helper(t, f, std::index_sequence_for<Ts...>{});
}
template<typename... Ts, typename F>
inline void for_each(std::tuple<Ts...>& t, F& f) {
  _convenience::for_each_helper(t, f, std::index_sequence_for<Ts...>{});
}
}
#endif // CEPH_COMMON_CONVENIENCE_H
