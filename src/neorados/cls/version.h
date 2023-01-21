// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#pragma once

/// \file neorados/cls/version.h
///
/// \brief NeoRADOS interface to object versioning class
///
/// The `version` object class stores a version in the extended
/// attributes of an object to help coordinate multiple writers. This
/// version comprises a byte string and an integer. The integers are
/// comparable and can be compared with the operators in
/// `VersionCond`. The byte strings are incomparable. Two versions
/// with different strings will always conflict.

#include <coroutine>
#include <string>
#include <utility>

#include <boost/asio/async_result.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_ops.h"
#include "cls/version/cls_version_types.h"

#include "neorados/cls/common.h"

namespace neorados::cls::version {
/// \brief Set the object version
///
/// Append a call to a write operation to set the object's version.
///
/// \param op Write operation to modify
/// \param ver Version to set
void set(WriteOp& op, const obj_version& ver);

/// \brief Unconditional increment version
///
/// Append a call to a write operation to increment the integral
/// portion of a version.
///
/// \param op Write operation to modify
void inc(WriteOp& op);

/// \brief Conditionally increment version
///
/// Append a call to a write operation to increment the object's
/// version if condition is met. If the condition is not met, the
/// operation fails with `std::errc::resource_unavailable_try_again`.
///
/// \param op Write operation to modify
/// \param ver Version to compare stored object version against
/// \param cond Comparison operator
void inc(WriteOp& op, const obj_version& ver, const VersionCond cond);

/// \brief Assert condition on stored version
///
/// Append a call to an operation that verifies the stored version has
/// the specified relationship to the supplied version. If the
/// condition is not met, the operation fails with `std::errc::canceled`.
///
/// \param op Operation to modify
/// \param ver Version to compare stored object version against
/// \param cond Comparison operator
void check(Op& op, const obj_version& ver, const VersionCond cond);

/// \brief Read the stored object version
///
/// Append a call to a read operation that reads the stored version.
///
/// \param op Read operation to modify
/// \param objv Location to store the version
void read(ReadOp& op, obj_version* const objv);

/// \brief Read the stored object version
///
/// Execute an asynchronous operation that reads the stored version.
///
/// \param r RADOS handle
/// \param o Object to query
/// \param ioc IOContext determining the object location
/// \param token Boost.Asio CompletionToken
///
/// \return The object version in a way appropriate to the completion
/// token. See Boost.Asio documentation.
template<boost::asio::completion_token_for<
	   void(boost::system::error_code, obj_version)> CompletionToken>
auto read(RADOS& r, Object o, IOContext ioc,
	  CompletionToken&& token)
{
  using namespace std::literals;
  return exec<cls_version_read_ret>(
    r, std::move(o), std::move(ioc),
    "version"s, "read"s, nullptr,
    [](cls_version_read_ret&& ret) {
      return std::move(ret.objv);
    }, std::forward<CompletionToken>(token));
}
} // namespace neorados::cls::version
