// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <type_traits>

#include <boost/asio/execution/executor.hpp>

#include <boost/asio/disposition.hpp>
#include <boost/asio/execution_context.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>
#include <boost/asio/strand.hpp>

/// \file common/async/concepts
///
/// \brief Because Asio needs to implement more concepts

namespace ceph::async {

/// The constraint from functions taking an ExecutionContext packed
/// into a concept.
template<typename ExecutionContext>
concept execution_context =
  std::is_convertible_v<ExecutionContext&,
                        boost::asio::execution_context&>;
/// A concept for Asio 'disposition's, a generalization of error
/// codes/exception pointers, etc.
template<typename T>
concept disposition =
  boost::asio::is_disposition_v<T>;

namespace detail {
template <typename...>
struct is_strand : public std::false_type {};

template <boost::asio::execution::executor Executor>
struct is_strand<boost::asio::strand<Executor>> : public std::true_type {};
} // namespace detail

/// Type predicate for strands
template <typename T>
inline constexpr bool is_strand_v =
    detail::is_strand<typename std::remove_cv_t<T>>::value;

/// Cocnept requiring a strand
template <typename T>
concept strand = is_strand_v<T>;
} // namespace ceph::async
