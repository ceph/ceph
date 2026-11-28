// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_ASIO_ADAPTER_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_ASIO_ADAPTER_H

#include "concurrency.h"
#include "common/async/yield_context.h"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/io_context.hpp>

namespace ceph::libfdb::experimental {

template <typename NativeT, typename CompletionTokenT>
auto async_wait(operation<NativeT> work, CompletionTokenT&& token)
{
 return std::move(work).native_handle()(std::forward<CompletionTokenT>(token));
}

template <typename NativeT>
auto wait(operation<NativeT> work, optional_yield y)
 -> detail::asio_value_t<operation<NativeT>>
{
 if (y) {
  auto result = async_wait(std::move(work), y.get_yield_context());

  if (not result) {
   std::rethrow_exception(detail::operation_aborted_exception());
  }

  return std::move(*result);
 }

 // A synchronous boundary drives only this caller's context; no worker pool
 // or application scheduler is created by libfdb.
 boost::asio::io_context context;
 std::exception_ptr error;
 std::optional<detail::asio_value_t<operation<NativeT>>> result;

 async_wait(std::move(work), boost::asio::bind_executor(context.get_executor(),
   [&error, &result](std::exception_ptr failure, auto value) {
    error = std::move(failure);
    if (value) {
     result.emplace(std::move(*value));
    }
   }));
 context.run();

 if (error) {
  std::rethrow_exception(error);
 }

 if (not result) {
  std::rethrow_exception(detail::operation_aborted_exception());
 }

 return std::move(*result);
}

} // namespace ceph::libfdb::experimental

#endif
