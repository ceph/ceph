// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_DPDK_CAPTURE_H
#define CEPH_MSG_DPDK_CAPTURE_H

#include <utility>

template <typename T, typename F>
class capture_impl {
  T x;
  F f;
 public:
  capture_impl(capture_impl &) = delete;
  capture_impl( T && x, F && f )
      : x{std::forward<T>(x)}, f{std::forward<F>(f)}
  {}

  template <typename ...Ts> auto operator()( Ts&&...args )
  -> decltype(f( x, std::forward<Ts>(args)... ))
  {
    return f( x, std::forward<Ts>(args)... );
  }

  template <typename ...Ts> auto operator()( Ts&&...args ) const
  -> decltype(f( x, std::forward<Ts>(args)... ))
  {
    return f( x, std::forward<Ts>(args)... );
  }
};

template <typename T, typename F>
capture_impl<T,F> capture( T && x, F && f ) {
  return capture_impl<T,F>(
      std::forward<T>(x), std::forward<F>(f) );
}

#endif //CEPH_MSG_DPDK_CAPTURE_H
