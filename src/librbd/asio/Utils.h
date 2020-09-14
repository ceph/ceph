// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_ASIO_UTILS_H
#define CEPH_LIBRBD_ASIO_UTILS_H

#include "include/Context.h"
#include "include/rados/librados_fwd.hpp"
#include <boost/system/error_code.hpp>

namespace librbd {
namespace asio {
namespace util {

template <typename T>
auto get_context_adapter(T&& t) {
  return [t = std::move(t)](boost::system::error_code ec) {
      t->complete(-ec.value());
    };
}

template <typename T>
auto get_callback_adapter(T&& t) {
  return [t = std::move(t)](boost::system::error_code ec, auto&& ... args) {
      t(-ec.value(), std::forward<decltype(args)>(args)...);
    };
}

} // namespace util
} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_UTILS_H
