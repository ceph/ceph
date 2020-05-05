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

#ifndef CEPH_COMMON_ASYNC_LIBRADOS_COMPLETION_H
#define CEPH_COMMON_ASYNC_LIBRADOS_COMPLETION_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <type_traits>

#include <boost/asio/async_result.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/rados/librados.hpp"
#include "librados/AioCompletionImpl.h"

namespace ceph::async {

namespace bs = boost::system;
namespace lr = librados;

namespace detail {

struct librados_handler {
  lr::AioCompletionImpl* pc;

  explicit librados_handler(lr::AioCompletion* c) : pc(c->pc) {
    pc->get();
  }

  librados_handler(const librados_handler&) = delete;
  librados_handler& operator =(const librados_handler&) = delete;
  librados_handler(librados_handler&& rhs) {
    pc = rhs.pc;
    rhs.pc = nullptr;
  }
  librados_handler& operator =(librados_handler&& rhs) {
    pc = rhs.pc;
    rhs.pc = nullptr;
    return *this;
  }

  void operator()(bs::error_code ec) {
    pc->lock.lock();
    pc->rval = ceph::from_error_code(ec);
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
  }

  void operator ()() {
    (*this)(bs::error_code{});
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
}

#endif // !CEPH_COMMON_ASYNC_LIBRADOS_COMPLETION_H
