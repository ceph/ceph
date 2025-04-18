// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <boost/asio/disposition.hpp>
#include <boost/asio/execution_context.hpp>

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
}
