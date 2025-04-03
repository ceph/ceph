// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <exception>

#include <boost/asio/async_result.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/rados/librados.hpp"

#include "common/error_code.h"

#include "librados/AioCompletionImpl.h"

// Allow librados::AioCompletion to be provided as a completion
// handler. This is only allowed with a signature of
// (int), (boost::system::error_code), (std::exception_ptr), or (). On
// completion the AioCompletion is completed with the error_code
// converted to an int with ceph::from_error_code. Exceptions we can't
// handle any other way are converted to -EIO.
//
// async_result::return_type is void.

namespace ceph::async {

namespace bs = boost::system;
namespace lr = librados;

namespace detail {

struct librados_handler {
  lr::AioCompletionImpl* pc;

  explicit librados_handler(lr::AioCompletion* c) : pc(c->pc) {
    pc->get();
  }
  ~librados_handler() {
    if (pc) {
      pc->put();
      pc = nullptr;
    }
  }

  librados_handler(const librados_handler&) = delete;
  librados_handler& operator =(const librados_handler&) = delete;
  librados_handler(librados_handler&& rhs) {
    pc = rhs.pc;
    rhs.pc = nullptr;
  }

  void operator()(int r) {
    pc->lock.lock();
    pc->rval = r;
    pc->complete = true;
    pc->lock.unlock();

    auto cb_complete = pc->callback_complete;
    auto cb_complete_arg = pc->callback_complete_arg;
    if (cb_complete)
      cb_complete(pc, cb_complete_arg);

    auto cb_safe = pc->callback_safe;
    auto cb_safe_arg = pc->callback_safe_arg;
    if (cb_safe)
      cb_safe(pc, cb_safe_arg);

    pc->lock.lock();
    pc->callback_complete = NULL;
    pc->callback_safe = NULL;
    pc->cond.notify_all();
    pc->put_unlock();
    pc = nullptr;
  }

  void operator()(bs::error_code ec) {
    (*this)(ceph::from_error_code(ec));
  }

  void operator ()() {
    (*this)(bs::error_code{});
  }

  void operator ()(std::exception_ptr e) {
    (*this)(ceph::from_exception(e));
  }
};
} // namespace detail
} // namespace ceph::async


namespace boost::asio {
template<typename ReturnType>
class async_result<librados::AioCompletion*, ReturnType()> {
public:
  using completion_handler_type = ceph::async::detail::librados_handler;
  explicit async_result(completion_handler_type&) {};
  using return_type = void;
  void get() {
    return;
  }
};

template<typename ReturnType>
class async_result<librados::AioCompletion*,
		   ReturnType(boost::system::error_code)> {
public:
  using completion_handler_type = ceph::async::detail::librados_handler;
  explicit async_result(completion_handler_type&) {};
  using return_type = void;
  void get() {
    return;
  }
};

template<typename ReturnType>
class async_result<librados::AioCompletion*,
		   ReturnType(std::exception_ptr)> {
public:
  using completion_handler_type = ceph::async::detail::librados_handler;
  explicit async_result(completion_handler_type&) {};
  using return_type = void;
  void get() {
    return;
  }
};

template<typename ReturnType>
class async_result<librados::AioCompletion*,
		   ReturnType(int)> {
public:
  using completion_handler_type = ceph::async::detail::librados_handler;
  explicit async_result(completion_handler_type&) {};
  using return_type = void;
  void get() {
    return;
  }
};
}
