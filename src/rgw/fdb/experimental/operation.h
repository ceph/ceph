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

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_OPERATION_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_OPERATION_H

#include <type_traits>
#include <utility>

#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 #include <stdexec/execution.hpp>
#endif

namespace ceph::libfdb::experimental {

// Experimental: preserve the native type and its execution protocol. Backend
// access borrows from lvalues; extracting from an rvalue transfers ownership.
template <typename NativeT>
#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 requires stdexec::sender<NativeT>
#endif
class operation final
{
 NativeT native;

 public:
 using native_handle_type = NativeT;

 explicit operation(NativeT backend_operation)
  noexcept(std::is_nothrow_move_constructible_v<NativeT>)
  : native(std::move(backend_operation))
 {}

 public:
 [[nodiscard]] NativeT& native_handle() & noexcept { return native; }
 [[nodiscard]] const NativeT& native_handle() const& noexcept { return native; }

 [[nodiscard]] NativeT native_handle() &&
  noexcept(std::is_nothrow_move_constructible_v<NativeT>)
 {
  return std::move(native);
 }

 NativeT native_handle() const&& = delete;

#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 public:
 using sender_concept = stdexec::sender_tag;

 template <typename SelfT, typename ...EnvTs>
 static consteval auto get_completion_signatures()
 {
  using native_t = decltype(std::forward_like<SelfT>(std::declval<NativeT&>()));

  return stdexec::get_completion_signatures<native_t, EnvTs...>();
 }

 template <typename SelfT, typename ReceiverT>
 auto connect(this SelfT&& self, ReceiverT&& receiver)
  noexcept(noexcept(stdexec::connect(
    std::forward_like<SelfT>(self.native), std::forward<ReceiverT>(receiver))))
  -> decltype(stdexec::connect(
    std::forward_like<SelfT>(self.native), std::forward<ReceiverT>(receiver)))
 {
  return stdexec::connect(std::forward_like<SelfT>(self.native), std::forward<ReceiverT>(receiver));
 }

 [[nodiscard]] decltype(auto) get_env() const
  noexcept(noexcept(stdexec::get_env(native)))
 {
  return stdexec::get_env(native);
 }

#endif
};

} // namespace ceph::libfdb::experimental

#endif
